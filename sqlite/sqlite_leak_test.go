package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thalesfsp/dal/v2/internal/shared"
)

// The constructor `New` opens a *sqlx.DB with sqlx.Open (which does not
// connect) and then verifies liveness with Ping. When Ping fails, `New` must
// close the opened handle before returning the error, otherwise the underlying
// database/sql connection pool leaks (it is neither returned to the caller nor
// closed).
//
// These tests drive that ping-failure path against an injected, controllable
// database/sql driver that counts how many physical connections are opened vs
// closed. If `New` closes the handle, DB.Close() propagates to the pooled
// connections and closes == opens. If it leaks, the connections are never
// closed (closes < opens) — which is exactly the bug this asserts against.

// leakDriverName is the name the counting driver is registered under and the
// value `New`'s driverName seam is pointed at for the duration of a test.
const leakDriverName = "sqlite-leak-counting"

// errPing is returned by the counting connection's Ping so the retrier in `New`
// exhausts and `New` takes its ping-failure error return path.
var errPing = errors.New("counting driver: ping always fails")

// connCounter tracks opened vs closed physical connections for the counting
// driver so a test can assert the opened handle was closed (not leaked).
type connCounter struct {
	opens  atomic.Int64
	closes atomic.Int64
}

// countingDriver is a minimal database/sql driver whose connections always fail
// Ping and record their open/close lifecycle in the shared counter.
type countingDriver struct {
	counter *connCounter
}

func (d *countingDriver) Open(_ string) (driver.Conn, error) {
	d.counter.opens.Add(1)

	return &countingConn{counter: d.counter}, nil
}

// countingConn is a no-op connection that always fails Ping and increments the
// close counter exactly once when closed.
type countingConn struct {
	counter *connCounter
	once    sync.Once
}

// Ping always fails, forcing `New`'s retrier to give up and return an error.
func (c *countingConn) Ping(_ context.Context) error { return errPing }

// Close records the close exactly once (guards against database/sql calling
// Close more than once for the same physical connection).
func (c *countingConn) Close() error {
	c.once.Do(func() { c.counter.closes.Add(1) })

	return nil
}

func (c *countingConn) Prepare(_ string) (driver.Stmt, error) {
	return nil, errors.New("counting driver: Prepare not supported")
}

func (c *countingConn) Begin() (driver.Tx, error) {
	return nil, errors.New("counting driver: Begin not supported")
}

// registerCountingDriver registers the counting driver once per process (a
// database/sql driver name may only be registered a single time) and returns
// the shared counter.
var (
	registerOnce sync.Once
	sharedCtr    = &connCounter{}
)

func registerCountingDriver(t *testing.T) *connCounter {
	t.Helper()

	registerOnce.Do(func() {
		sql.Register(leakDriverName, &countingDriver{counter: sharedCtr})
	})

	return sharedCtr
}

// withInjectedDriver points `New`'s driverName seam at the counting driver for
// the duration of the test and restores it afterwards. It also shrinks the ping
// retry backoff so the (guaranteed to fail) ping path exhausts near-instantly
// instead of taking ~70s of real sleeps — keeping the test offline and well
// under the suite's per-test timeout.
func withInjectedDriver(t *testing.T) {
	t.Helper()

	originalDriver := driverName
	driverName = leakDriverName

	originalTimeout := shared.TimeoutPing
	shared.TimeoutPing = time.Millisecond

	t.Cleanup(func() {
		driverName = originalDriver
		shared.TimeoutPing = originalTimeout
	})
}

// TestNew_PingFailure_ClosesHandle proves the opened *sqlx.DB is closed (not
// leaked) when Ping fails. Before the fix this fails because closes stays 0
// while opens is >= 1 (the handle is returned to nobody and never closed);
// after the fix closes == opens.
func TestNew_PingFailure_ClosesHandle(t *testing.T) {
	ctr := registerCountingDriver(t)
	withInjectedDriver(t)

	beforeOpens := ctr.opens.Load()
	beforeCloses := ctr.closes.Load()

	// The counting driver's Ping always fails, so New must return an error.
	storage, err := New(t.Context(), "counting://ignored")
	if err == nil {
		t.Fatalf("New: expected a ping-failure error, got nil (storage=%v)", storage)
	}

	if storage != nil {
		t.Fatalf("New: expected nil storage on error, got %v", storage)
	}

	opens := ctr.opens.Load() - beforeOpens
	closes := ctr.closes.Load() - beforeCloses

	// The ping path must have actually connected at least once (otherwise the
	// test would be vacuously green and prove nothing about closing).
	if opens == 0 {
		t.Fatalf("test seam invalid: driver opened 0 connections; ping path not exercised")
	}

	// The leak: without New closing the handle, DB.Close() never runs, so the
	// pooled connection(s) are never closed (closes < opens).
	if closes < opens {
		t.Fatalf(
			"connection handle leaked: opened %d connection(s) but closed only %d; "+
				"New must close the opened *sqlx.DB on the ping-failure path",
			opens, closes,
		)
	}
}
