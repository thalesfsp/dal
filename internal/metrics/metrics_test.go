package metrics

import (
	"expvar"
	"sync"
	"testing"
)

// TestNewInt_Idempotent proves NewInt is safe to call more than once with the
// same name in a single process: the second call must NOT panic (Go's expvar
// panics with "Reuse of exported var name" on a duplicate Publish) and must
// return the SAME already-registered *expvar.Int rather than re-registering.
//
// This is the guard behind storage.New being callable >= 2x per process: each
// SQL storage's New -> storage.New registers a fixed per-type set of counters,
// so a second New for the same storage type would re-register identical names.
//
// Negative control: revert NewInt to call expvar.NewInt unconditionally (drop
// the expvar.Get reuse branch) and this test panics on the second NewInt with
// "Reuse of exported var name: metrics_test.idempotent.counter" — i.e. it fails
// for the right reason before the fix.
func TestNewInt_Idempotent(t *testing.T) {
	// A name unique to this test so it cannot collide with any other var
	// published in the process. No DAL_METRICS_PREFIX set, so finalName == name.
	const name = "metrics_test.idempotent.counter"

	first := NewInt(name)
	if first == nil {
		t.Fatalf("NewInt(first): got nil")
	}

	// Mutate so we can prove the SAME instance is returned (not a fresh zeroed
	// one) on the second call — i.e. reuse, not re-registration with reset.
	first.Add(42)

	// Second registration of the exact same name. Without the guard this panics
	// inside expvar.NewInt -> expvar.Publish.
	second := NewInt(name)
	if second == nil {
		t.Fatalf("NewInt(second): got nil")
	}

	// Reuse, not re-registration: identical pointer.
	if first != second {
		t.Fatalf("NewInt(second): expected the reused counter %p, got a different one %p", first, second)
	}

	// The reused counter keeps its accumulated value (no double-count, no reset).
	if got := second.Value(); got != 42 {
		t.Fatalf("reused counter value: expected 42 (preserved), got %d", got)
	}

	// The registry resolves the (now deterministic) name to that exact instance.
	published, ok := expvar.Get(name).(*expvar.Int)
	if !ok {
		t.Fatalf("expvar.Get(%q): expected a published *expvar.Int, got %T", name, expvar.Get(name))
	}

	if published != first {
		t.Fatalf("expvar registry: name %q resolves to a different var than the first registration", name)
	}
}

// TestNewInt_Prefix proves DAL_METRICS_PREFIX is honoured (prefixes the name)
// and that idempotent reuse also holds for the prefixed name. Guards against a
// refactor accidentally dropping the prefix or double-registering the prefixed
// name.
func TestNewInt_Prefix(t *testing.T) {
	t.Setenv("DAL_METRICS_PREFIX", "unit_prefix")

	const base = "metrics_test.prefixed.counter"

	first := NewInt(base)
	first.Add(7)

	second := NewInt(base)
	if first != second {
		t.Fatalf("prefixed NewInt: expected reuse of the same counter, got a different instance")
	}

	// Registered under "<prefix>.<base>", not the bare base.
	if v := expvar.Get("unit_prefix." + base); v == nil {
		t.Fatalf("expected counter published under prefixed name %q", "unit_prefix."+base)
	}

	if got := second.Value(); got != 7 {
		t.Fatalf("reused prefixed counter value: expected 7 (preserved), got %d", got)
	}
}

// TestNewInt_Concurrent proves the check-and-publish in NewInt is atomic:
// registering the SAME name from many goroutines at once must NOT panic and must
// resolve to exactly ONE published *expvar.Int (every caller gets the same
// pointer). "expvar.Get then expvar.NewInt" is a compound check-then-act; without
// serialization two goroutines can both observe a nil Get and both call
// expvar.NewInt, and the loser panics in expvar.Publish ("Reuse of exported var
// name"). expvar.Publish acquires a global lock, so that panic surfaces as a
// crash of the whole test binary, not something a single goroutine's recover
// could contain — hence the assertion is simply "the run completes and all
// pointers match".
//
// Negative control: remove newIntMu (or its Lock/Unlock) from NewInt and run
// this test repeatedly with -race; it panics with "Reuse of exported var name:
// metrics_test.concurrent.counter". (The race is timing-dependent, so the
// negative control is "panics under repeated -race runs", not "panics every
// single run".)
func TestNewInt_Concurrent(t *testing.T) {
	const (
		name       = "metrics_test.concurrent.counter"
		goroutines = 64
	)

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results = make([]*expvar.Int, 0, goroutines)
		start   = make(chan struct{})
	)

	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			<-start // release all goroutines at once to maximise contention.

			c := NewInt(name)

			mu.Lock()
			results = append(results, c)
			mu.Unlock()
		}()
	}

	close(start)
	wg.Wait()

	if len(results) != goroutines {
		t.Fatalf("expected %d results, got %d", goroutines, len(results))
	}

	// Every concurrent caller must have received the SAME single published
	// counter — proving exactly one publish happened and the rest reused it.
	first := results[0]
	if first == nil {
		t.Fatalf("NewInt returned nil under concurrency")
	}

	for i, c := range results {
		if c != first {
			t.Fatalf("goroutine %d got a different counter %p than %p; NewInt published more than once", i, c, first)
		}
	}

	if published, ok := expvar.Get(name).(*expvar.Int); !ok || published != first {
		t.Fatalf("expvar registry: %q does not resolve to the single shared counter", name)
	}
}
