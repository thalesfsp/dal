package metrics

import (
	"expvar"
	"os"
	"sync"
)

// newIntMu serializes the check-and-publish in NewInt. expvar's own registry is
// concurrency-safe per operation, but "Get then NewInt" is a compound
// check-then-act: without this lock two goroutines registering the SAME name
// concurrently could both observe a nil Get and both call expvar.NewInt, and the
// loser would panic in expvar.Publish ("Reuse of exported var name"). The lock
// makes the whole lookup-or-publish atomic so concurrent first registrations of
// the same name resolve to a single published counter.
var newIntMu sync.Mutex

// NewInt creates and initializes a new expvar.Int published under a stable,
// deterministic name, OR reuses the already-published one if a var with that
// name exists.
//
// Idempotency matters because the same name can legitimately be registered more
// than once per process: e.g. each SQL storage's `New` calls `storage.New`,
// which registers a fixed per-type set of counters — so calling `New` twice for
// the same storage type (a leak/regression test followed by an E2E test, or any
// caller re-instantiating a storage) would otherwise re-register the same names.
// Go's `expvar` panics ("Reuse of exported var name: ...") on a duplicate
// `Publish`, so a naive second registration crashes the process.
//
// Guard: under newIntMu, look the name up first with expvar.Get. If it is
// already an *expvar.Int, reuse that exact instance (no re-registration, no
// double-count, value preserved). Only when the name is absent do we create and
// publish a fresh counter (initialised to 0). Holding the lock across the
// Get+NewInt makes the check-and-publish atomic, so even concurrent first
// registrations of the same name cannot both publish (and thus cannot panic).
// First-registration behaviour is otherwise unchanged; the second and subsequent
// calls with the same name are safe no-ops that return the original counter.
func NewInt(name string) *expvar.Int {
	finalName := name

	if prefix := os.Getenv("DAL_METRICS_PREFIX"); prefix != "" {
		finalName = prefix + "." + finalName
	}

	newIntMu.Lock()
	defer newIntMu.Unlock()

	// Reuse an already-published counter of the same name (idempotent).
	if existing := expvar.Get(finalName); existing != nil {
		if iv, ok := existing.(*expvar.Int); ok {
			return iv
		}

		// A var of this name exists but is NOT an *expvar.Int. This is a
		// programming error (a name collision across metric value types), and
		// expvar offers no way to redefine an already-published name. Panicking
		// would defeat the entire purpose of this guard, and returning nil would
		// violate the non-nil contract callers rely on. Return a fresh,
		// unpublished counter so the caller still has a usable *expvar.Int; the
		// pre-existing var keeps the exported name slot.
		return new(expvar.Int)
	}

	counter := expvar.NewInt(finalName)

	counter.Set(0)

	return counter
}
