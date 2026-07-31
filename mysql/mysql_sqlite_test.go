package mysql

// Offline coverage tests. They exercise the full adapter surface (constructor,
// CRUD, Count/List, helpers, and singleton) without a real MySQL server by
// pointing the `driverName` seam at the embedded SQLite driver. goqu's mysql
// dialect emits interpolated SQL with backtick-quoted identifiers, which SQLite
// accepts verbatim, so every statement the adapter builds runs unmodified.
//
// NOTE: These tests mutate the package-level `driverName` seam and therefore
// must NOT use t.Parallel().

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

// testTableDefinition mirrors internal/shared.TestDataWithIDS's `db` tags.
const testTableDefinition = `id varchar(255) PRIMARY KEY,
	name varchar(255) NOT NULL,
	version varchar(255) NOT NULL`

// evilTarget is an injection attempt ValidateSQLIdentifier must reject.
const evilTarget = `foo"; DROP TABLE x --`

// errBoom is the sentinel returned by failing test hooks.
var errBoom = errors.New("boom")

// sqliteDBSeq isolates each test in its own named in-memory database.
var sqliteDBSeq atomic.Int64

//////
// MySQL-to-SQLite compatibility driver.
//
// goqu's mysql dialect emits `DELETE `+"`t`"+` FROM `+"`t`"+` ...` (MySQL's
// multi-table delete syntax), which SQLite rejects. Everything else the adapter
// generates runs on SQLite verbatim, so a thin wrapping driver rewrites just
// that prefix before delegating to the real sqlite3 driver. Only Prepare is
// wrapped: hiding the sqlite3 conn's Execer/Queryer fast paths forces
// database/sql through the prepared-statement path, so every statement flows
// through the rewrite.
//////

// compatDriverName is the name the rewriting driver is registered under.
const compatDriverName = "sqlite3-mysql-compat"

// mysqlDeleteRe matches the MySQL multi-table delete prefix goqu emits.
var mysqlDeleteRe = regexp.MustCompile("^DELETE `[^`]+` FROM ")

// compatDriver delegates to sqlite3, wrapping connections for SQL rewriting.
type compatDriver struct {
	real sqlite3.SQLiteDriver
}

func (d *compatDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.real.Open(name)
	if err != nil {
		return nil, err
	}

	return &compatConn{real: conn}, nil
}

// compatConn rewrites MySQL-only syntax into SQLite-compatible SQL.
type compatConn struct {
	real driver.Conn
}

func (c *compatConn) Prepare(query string) (driver.Stmt, error) {
	return c.real.Prepare(mysqlDeleteRe.ReplaceAllString(query, "DELETE FROM "))
}

func (c *compatConn) Close() error { return c.real.Close() }

func (c *compatConn) Begin() (driver.Tx, error) {
	return c.real.Begin() //nolint:staticcheck // delegating a legacy interface method.
}

// registerCompatDriverOnce guards the process-global driver registration.
var registerCompatDriverOnce sync.Once

// newSQLiteStorage builds a fully-constructed adapter backed by a fresh
// in-memory SQLite database, restoring the driver seam and closing the client
// when the test finishes.
func newSQLiteStorage(t *testing.T) *MySQL {
	t.Helper()

	registerCompatDriverOnce.Do(func() {
		sql.Register(compatDriverName, &compatDriver{})
	})

	originalDriver := driverName
	driverName = compatDriverName

	t.Cleanup(func() { driverName = originalDriver })

	dsn := fmt.Sprintf("file:mysqltest%d?mode=memory&cache=shared", sqliteDBSeq.Add(1))

	str, err := New(t.Context(), dsn)
	require.NoError(t, err)
	require.NotNil(t, str)

	t.Cleanup(func() { assert.NoError(t, str.Client.Close()) })

	return str
}

// newSQLiteStorageWithTable additionally creates the test table.
func newSQLiteStorageWithTable(t *testing.T) *MySQL {
	t.Helper()

	str := newSQLiteStorage(t)

	require.NoError(t, str.createTable(t.Context(), shared.TableName, testTableDefinition))

	return str
}

// mustCreateTestDoc inserts the canonical test document.
func mustCreateTestDoc(t *testing.T, str *MySQL) {
	t.Helper()

	id, err := str.Create(t.Context(), shared.DocumentID, shared.TableName, shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)
	require.Equal(t, shared.DocumentID, id)
}

// requireStatusCode asserts err carries the given HTTP status code.
func requireStatusCode(t *testing.T, err error, statusCode int) {
	t.Helper()

	require.Error(t, err)

	var cErr *customerror.CustomError

	require.ErrorAs(t, err, &cErr)
	assert.Equal(t, statusCode, cErr.StatusCode)
}

//////
// Helpers.
//////

func TestQuoteIdentifier(t *testing.T) {
	assert.Equal(t, "`users`", QuoteIdentifier("users"))
	assert.Equal(t, "`us``ers`", QuoteIdentifier("us`ers"))
}

func TestToSQLString(t *testing.T) {
	formatted, err := ToSQLString("name:asc,version:desc")
	require.NoError(t, err)
	assert.Equal(t, "name asc, version desc", formatted)

	formatted, err = ToSQLString("not a sort")
	require.Error(t, err)
	assert.Empty(t, formatted)
}

//////
// Constructor and singleton.
//////

func TestNew_SQLite_HappyPath(t *testing.T) {
	str := newSQLiteStorage(t)

	assert.Equal(t, Name, str.GetName())
	assert.Equal(t, compatDriverName, str.Config.DriverName)
	assert.NotEmpty(t, str.Config.DataSourceName)
	assert.Equal(t, str.Client, str.GetClient())
}

func TestNew_OpenError(t *testing.T) {
	originalDriver := driverName
	driverName = "mysql-driver-that-does-not-exist"

	t.Cleanup(func() { driverName = originalDriver })

	str, err := New(t.Context(), "irrelevant")
	require.Error(t, err)
	assert.Nil(t, str)
	assert.Contains(t, err.Error(), "connect")
}

func TestSingletonGetSet(t *testing.T) {
	str := newSQLiteStorage(t)

	// New must have set the singleton.
	assert.Equal(t, storage.IStorage(str), Get())

	previous := singleton

	t.Cleanup(func() { Set(previous) })

	Set(nil)
	assert.Panics(t, func() { Get() })

	Set(str)
	assert.Equal(t, storage.IStorage(str), Get())
}

func TestClose(t *testing.T) {
	t.Run("healthy and idempotent", func(t *testing.T) {
		str := newSQLiteStorage(t)

		require.NoError(t, str.Close())
		require.NoError(t, str.Close())
	})

	t.Run("concurrent", func(t *testing.T) {
		str := newSQLiteStorage(t)

		const callers = 16

		start := make(chan struct{})
		errs := make(chan error, callers)
		var wg sync.WaitGroup

		wg.Add(callers)

		for range callers {
			go func() {
				defer wg.Done()

				<-start
				errs <- str.Close()
			}()
		}

		close(start)
		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}
	})
}

//////
// Table/database helpers.
//////

func TestCreateTable(t *testing.T) {
	str := newSQLiteStorage(t)
	ctx := t.Context()

	// Invalid identifier never reaches the database.
	require.Error(t, str.createTable(ctx, evilTarget, testTableDefinition))

	// Happy path.
	require.NoError(t, str.createTable(ctx, shared.TableName, testTableDefinition))

	// Execution failure (malformed column description).
	err := str.createTable(ctx, "another_table", "id totally((bogus")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "createTable")
}

func TestDeleteTable(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	// Invalid identifier never reaches the database.
	require.Error(t, str.deleteTable(ctx, evilTarget))

	// Happy path.
	require.NoError(t, str.deleteTable(ctx, shared.TableName))

	// Execution failure (table is gone now).
	err := str.deleteTable(ctx, shared.TableName)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleteTable")
}

func TestCreateDB_Error(t *testing.T) {
	str := newSQLiteStorage(t)

	// SQLite has no CREATE DATABASE, driving the error branch. The success
	// branch requires a real server and stays covered by the E2E suite.
	err := str.createDB(t.Context(), "somedb")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "createDB")
}

//////
// Create.
//////

func TestCreate(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	createdBefore := str.GetCounterCreated().Value()

	// Happy path: caller-provided ID is returned as-is.
	mustCreateTestDoc(t, str)
	assert.Equal(t, createdBefore+1, str.GetCounterCreated().Value())

	// Happy path: empty ID falls back to LastInsertId.
	id, err := str.Create(ctx, "", shared.TableName, &shared.TestDataS{
		Name:    shared.DocumentName,
		Version: shared.DocumentVersion,
	}, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	// Missing target.
	_, err = str.Create(ctx, shared.DocumentID, "", shared.TestDataWithID, nil)
	require.Error(t, err)

	// Option application failure.
	_, err = str.Create(ctx, shared.DocumentID, shared.TableName, shared.TestDataWithID, nil,
		storage.WithPreHook[*create.Create](nil))
	require.Error(t, err)

	// Pre-hook failure.
	_, err = str.Create(ctx, shared.DocumentID, shared.TableName, shared.TestDataWithID, nil,
		storage.WithPreHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// SQL build failure (unsupported row type).
	_, err = str.Create(ctx, shared.DocumentID, shared.TableName, make(chan int), nil)
	require.Error(t, err)

	// Execution failure (nonexistent table).
	failedBefore := str.GetCounterCreatedFailed().Value()

	_, err = str.Create(ctx, shared.DocumentID, "nonexistent_table", shared.TestDataWithID, nil)
	require.Error(t, err)
	assert.Greater(t, str.GetCounterCreatedFailed().Value(), failedBefore)

	// Post-hook failure.
	_, err = str.Create(ctx, "post-hook-id", shared.TableName, &shared.TestDataWithIDS{
		ID:      "post-hook-id",
		Name:    shared.DocumentName,
		Version: shared.DocumentVersion,
	}, &create.Create{},
		storage.WithPostHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Hooks run on the happy path too.
	preCalled, postCalled := false, false

	_, err = str.Create(ctx, "hooked-id", shared.TableName, &shared.TestDataWithIDS{
		ID:      "hooked-id",
		Name:    shared.DocumentName,
		Version: shared.DocumentVersion,
	}, &create.Create{},
		storage.WithPreHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			preCalled = true

			return nil
		}),
		storage.WithPostHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			postCalled = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, preCalled)
	assert.True(t, postCalled)
}

//////
// Retrieve.
//////

func TestRetrieve(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	mustCreateTestDoc(t, str)

	// Happy path.
	var retrieved shared.TestDataWithIDS

	require.NoError(t, str.Retrieve(ctx, shared.DocumentID, shared.TableName, &retrieved, &retrieve.Retrieve{}))
	assert.Equal(t, shared.TestDataWithID, &retrieved)

	// Missing ID.
	require.Error(t, str.Retrieve(ctx, "", shared.TableName, &retrieved, nil))

	// Missing document -> 404.
	err := str.Retrieve(ctx, shared.DocumentIDInvalid, shared.TableName, &retrieved, nil)
	requireStatusCode(t, err, http.StatusNotFound)

	// Missing target.
	require.Error(t, str.Retrieve(ctx, shared.DocumentID, "", &retrieved, nil))

	// Option application failure.
	require.Error(t, str.Retrieve(ctx, shared.DocumentID, shared.TableName, &retrieved, nil,
		storage.WithPostHook[*retrieve.Retrieve](nil)))

	// Pre-hook failure.
	err = str.Retrieve(ctx, shared.DocumentID, shared.TableName, &retrieved, nil,
		storage.WithPreHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Post-hook failure.
	err = str.Retrieve(ctx, shared.DocumentID, shared.TableName, &retrieved, nil,
		storage.WithPostHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Execution failure (nonexistent table).
	failedBefore := str.GetCounterRetrievedFailed().Value()

	require.Error(t, str.Retrieve(ctx, shared.DocumentID, "nonexistent_table", &retrieved, nil))
	assert.Greater(t, str.GetCounterRetrievedFailed().Value(), failedBefore)
}

//////
// Update.
//////

func TestUpdate(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	mustCreateTestDoc(t, str)

	// Happy path.
	require.NoError(t, str.Update(ctx, shared.DocumentID, shared.TableName, shared.UpdatedTestDataID, &update.Update{}))

	var retrieved shared.TestDataWithIDS

	require.NoError(t, str.Retrieve(ctx, shared.DocumentID, shared.TableName, &retrieved, nil))
	assert.Equal(t, shared.UpdatedTestDataID, &retrieved)

	// Missing ID.
	require.Error(t, str.Update(ctx, "", shared.TableName, shared.UpdatedTestDataID, nil))

	// Missing document -> 404.
	err := str.Update(ctx, shared.DocumentIDInvalid, shared.TableName, &shared.TestDataWithIDS{
		ID:      shared.DocumentIDInvalid,
		Name:    shared.DocumentName,
		Version: shared.DocumentVersion,
	}, nil)
	requireStatusCode(t, err, http.StatusNotFound)

	// Missing target.
	require.Error(t, str.Update(ctx, shared.DocumentID, "", shared.UpdatedTestDataID, nil))

	// Option application failure.
	require.Error(t, str.Update(ctx, shared.DocumentID, shared.TableName, shared.UpdatedTestDataID, nil,
		storage.WithPreHook[*update.Update](nil)))

	// Pre-hook failure.
	err = str.Update(ctx, shared.DocumentID, shared.TableName, shared.UpdatedTestDataID, nil,
		storage.WithPreHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Post-hook failure.
	err = str.Update(ctx, shared.DocumentID, shared.TableName, shared.UpdatedTestDataID, nil,
		storage.WithPostHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// SQL build failure (no update values).
	require.Error(t, str.Update(ctx, shared.DocumentID, shared.TableName, struct{}{}, nil))

	// Execution failure (nonexistent table).
	failedBefore := str.GetCounterUpdatedFailed().Value()

	require.Error(t, str.Update(ctx, shared.DocumentID, "nonexistent_table", shared.UpdatedTestDataID, nil))
	assert.Greater(t, str.GetCounterUpdatedFailed().Value(), failedBefore)
}

//////
// Delete.
//////

func TestDelete(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	mustCreateTestDoc(t, str)

	// Missing ID.
	require.Error(t, str.Delete(ctx, "", shared.TableName, nil))

	// Missing target.
	require.Error(t, str.Delete(ctx, shared.DocumentID, "", nil))

	// Option application failure.
	require.Error(t, str.Delete(ctx, shared.DocumentID, shared.TableName, nil,
		storage.WithPreHook[*delete.Delete](nil)))

	// Pre-hook failure.
	err := str.Delete(ctx, shared.DocumentID, shared.TableName, nil,
		storage.WithPreHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Post-hook failure.
	err = str.Delete(ctx, shared.DocumentID, shared.TableName, nil,
		storage.WithPostHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Execution failure (nonexistent table).
	failedBefore := str.GetCounterDeletedFailed().Value()

	require.Error(t, str.Delete(ctx, shared.DocumentID, "nonexistent_table", nil))
	assert.Greater(t, str.GetCounterDeletedFailed().Value(), failedBefore)

	// Happy path.
	require.NoError(t, str.Delete(ctx, shared.DocumentID, shared.TableName, &delete.Delete{}))

	// Deleting a missing row is, as coded, still a success (no row count check).
	require.NoError(t, str.Delete(ctx, shared.DocumentID, shared.TableName, &delete.Delete{}))

	docCount, err := str.Count(ctx, shared.TableName, nil)
	require.NoError(t, err)
	assert.Zero(t, docCount)
}

//////
// Count.
//////

func TestCount(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	mustCreateTestDoc(t, str)

	// Default query (nil params).
	docCount, err := str.Count(ctx, shared.TableName, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, docCount)

	// Params provided with an empty Search: defaulting must not mutate them.
	prm := &count.Count{}

	docCount, err = str.Count(ctx, shared.TableName, prm)
	require.NoError(t, err)
	assert.EqualValues(t, 1, docCount)
	assert.Empty(t, prm.Search)

	// Caller-provided Search is honored.
	docCount, err = str.Count(ctx, shared.TableName, &count.Count{
		Search: "SELECT COUNT(*) FROM " + shared.TableName + " WHERE name = 'nope'",
	})
	require.NoError(t, err)
	assert.Zero(t, docCount)

	// Injection attempt through the target is rejected.
	_, err = str.Count(ctx, evilTarget, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid SQL identifier")

	// Missing target.
	_, err = str.Count(ctx, "", nil)
	require.Error(t, err)

	// Option application failure.
	_, err = str.Count(ctx, shared.TableName, nil, storage.WithPreHook[*count.Count](nil))
	require.Error(t, err)

	// Pre-hook failure.
	_, err = str.Count(ctx, shared.TableName, nil,
		storage.WithPreHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Post-hook failure.
	_, err = str.Count(ctx, shared.TableName, nil,
		storage.WithPostHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Post-hook receives the count.
	var hookedCount int64

	_, err = str.Count(ctx, shared.TableName, nil,
		storage.WithPostHook[*count.Count](func(_ context.Context, _ storage.IStorage, _, _ string, data any, _ *count.Count) error {
			c, ok := data.(int64)
			require.True(t, ok)

			hookedCount = c

			return nil
		}))
	require.NoError(t, err)
	assert.EqualValues(t, 1, hookedCount)

	// Execution failure (nonexistent table).
	failedBefore := str.GetCounterCountedFailed().Value()

	_, err = str.Count(ctx, "nonexistent_table", nil)
	require.Error(t, err)
	assert.Greater(t, str.GetCounterCountedFailed().Value(), failedBefore)
}

//////
// List.
//////

func TestList(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	mustCreateTestDoc(t, str)

	// Default query (nil params).
	var items []shared.TestDataWithIDS

	require.NoError(t, str.List(ctx, shared.TableName, &items, nil))
	require.Len(t, items, 1)
	assert.Equal(t, *shared.TestDataWithID, items[0])

	// Params provided with an empty Search: defaulting must not mutate them.
	prm := &list.List{}

	items = nil
	require.NoError(t, str.List(ctx, shared.TableName, &items, prm))
	require.Len(t, items, 1)
	assert.Empty(t, prm.Search)

	// Caller-provided Search is honored.
	items = nil
	require.NoError(t, str.List(ctx, shared.TableName, &items, &list.List{
		Search: "SELECT * FROM " + shared.TableName + " WHERE name = 'nope'",
	}))
	assert.Empty(t, items)

	// Injection attempt through the target is rejected.
	err := str.List(ctx, evilTarget, &items, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid SQL identifier")

	// Missing target.
	require.Error(t, str.List(ctx, "", &items, nil))

	// Option application failure.
	require.Error(t, str.List(ctx, shared.TableName, &items, nil, storage.WithPreHook[*list.List](nil)))

	// Pre-hook failure.
	err = str.List(ctx, shared.TableName, &items, nil,
		storage.WithPreHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Post-hook failure.
	err = str.List(ctx, shared.TableName, &items, nil,
		storage.WithPostHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
			return errBoom
		}))
	require.ErrorIs(t, err, errBoom)

	// Execution failure (nonexistent table).
	failedBefore := str.GetCounterListedFailed().Value()

	require.Error(t, str.List(ctx, "nonexistent_table", &items, nil))
	assert.Greater(t, str.GetCounterListedFailed().Value(), failedBefore)
}

//////
// Static target (m.Target fallback).
//////

func TestStaticTargetFallback(t *testing.T) {
	str := newSQLiteStorageWithTable(t)
	ctx := t.Context()

	str.Target = shared.TableName

	t.Cleanup(func() { str.Target = "" })

	mustCreateTestDoc(t, str)

	// Empty per-operation target falls back to the static one.
	docCount, err := str.Count(ctx, "", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, docCount)
}
