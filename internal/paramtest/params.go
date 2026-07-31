// Package paramtest contains shared adapter regression assertions.
package paramtest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

const (
	target      = "merge_target"
	id          = "merge-id"
	callerRoute = "caller-route"
)

var errStop = errors.New("params captured")

// Defaults records application defaults added by an adapter.
type Defaults struct {
	CountSearch string
	ListSearch  string
}

// AssertMergedDefaults runs every operation through its pre-hook and verifies
// that setting one caller field preserves all other constructor and adapter
// defaults.
func AssertMergedDefaults(t *testing.T, strg storage.IStorage, defaults Defaults) {
	t.Helper()

	counters := []interface {
		Value() int64
		Set(value int64)
	}{
		strg.GetCounterCounted(),
		strg.GetCounterCountedFailed(),
		strg.GetCounterCreated(),
		strg.GetCounterCreatedFailed(),
		strg.GetCounterDeleted(),
		strg.GetCounterDeletedFailed(),
		strg.GetCounterListed(),
		strg.GetCounterListedFailed(),
		strg.GetCounterRetrieved(),
		strg.GetCounterRetrievedFailed(),
		strg.GetCounterUpdated(),
		strg.GetCounterUpdatedFailed(),
	}
	counterValues := make([]int64, len(counters))
	for i, counter := range counters {
		counterValues[i] = counter.Value()
	}
	t.Cleanup(func() {
		for i, counter := range counters {
			counter.Set(counterValues[i])
		}
	})

	t.Run("Count", func(t *testing.T) {
		var got *count.Count
		prm := &count.Count{Routing: []string{callerRoute}}

		_, err := strg.Count(t.Context(), target, prm, storage.WithPreHook(
			func(_ context.Context, _ storage.IStorage, _, _ string, _ any, merged *count.Count) error {
				captured := *merged
				got = &captured

				return errStop
			},
		))
		requireCaptured(t, err)

		want := &count.Count{Routing: []string{callerRoute}, Search: defaults.CountSearch}
		requireParams(t, "Count", got, want)
	})

	t.Run("Create", func(t *testing.T) {
		var got *create.Create
		prm := &create.Create{Routing: callerRoute}

		_, err := strg.Create(t.Context(), id, target, struct{}{}, prm, storage.WithPreHook(
			func(_ context.Context, _ storage.IStorage, _, _ string, _ any, merged *create.Create) error {
				captured := *merged
				got = &captured

				return errStop
			},
		))
		requireCaptured(t, err)

		want := &create.Create{Routing: callerRoute}
		requireParams(t, "Create", got, want)
	})

	t.Run("Delete", func(t *testing.T) {
		var got *delete.Delete
		prm := &delete.Delete{Routing: callerRoute}

		err := strg.Delete(t.Context(), id, target, prm, storage.WithPreHook(
			func(_ context.Context, _ storage.IStorage, _, _ string, _ any, merged *delete.Delete) error {
				captured := *merged
				got = &captured

				return errStop
			},
		))
		requireCaptured(t, err)

		want := &delete.Delete{Routing: callerRoute}
		requireParams(t, "Delete", got, want)
	})

	t.Run("Retrieve", func(t *testing.T) {
		var got *retrieve.Retrieve
		prm := &retrieve.Retrieve{Routing: callerRoute}

		err := strg.Retrieve(t.Context(), id, target, new(any), prm, storage.WithPreHook(
			func(_ context.Context, _ storage.IStorage, _, _ string, _ any, merged *retrieve.Retrieve) error {
				captured := *merged
				got = &captured

				return errStop
			},
		))
		requireCaptured(t, err)

		want := &retrieve.Retrieve{Routing: callerRoute}
		requireParams(t, "Retrieve", got, want)
	})

	t.Run("List", func(t *testing.T) {
		var got *list.List
		prm := &list.List{SearchRaw: "caller-raw"}

		err := strg.List(t.Context(), target, new(any), prm, storage.WithPreHook(
			func(_ context.Context, _ storage.IStorage, _, _ string, _ any, merged *list.List) error {
				captured := *merged
				got = &captured

				return errStop
			},
		))
		requireCaptured(t, err)

		if got == nil {
			t.Fatal("List pre-hook did not capture params")
		}
		if got.Limit != 10 {
			t.Fatalf("List default Limit = %d, want 10 after caller set SearchRaw", got.Limit)
		}

		want := &list.List{Limit: 10, Search: defaults.ListSearch, SearchRaw: "caller-raw"}
		requireParams(t, "List", got, want)
	})

	t.Run("Update", func(t *testing.T) {
		var got *update.Update
		prm := &update.Update{Routing: callerRoute}

		err := strg.Update(t.Context(), id, target, struct{}{}, prm, storage.WithPreHook(
			func(_ context.Context, _ storage.IStorage, _, _ string, _ any, merged *update.Update) error {
				captured := *merged
				got = &captured

				return errStop
			},
		))
		requireCaptured(t, err)

		want := &update.Update{Routing: callerRoute}
		requireParams(t, "Update", got, want)
	})
}

func requireCaptured(t *testing.T, err error) {
	t.Helper()

	if !errors.Is(err, errStop) {
		t.Fatalf("operation error = %v, want captured-params sentinel", err)
	}
}

func requireParams[T any](t *testing.T, operation string, got, want *T) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s params = %#v, want %#v", operation, got, want)
	}
}
