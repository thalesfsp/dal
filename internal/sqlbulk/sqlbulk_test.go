package sqlbulk

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

func newSQLBulkTestOptions(t *testing.T) *storage.Options[*create.Create] {
	t.Helper()

	options, err := storage.NewOptions[*create.Create]()
	if err != nil {
		t.Fatalf("storage.NewOptions() error = %v", err)
	}

	return options
}

func TestPrepareChunkRejectsInvalidItemsAndPreservesOriginalIndices(t *testing.T) {
	t.Parallel()

	rejected := errors.New("SQL pre-hook rejected item")
	prm := &create.Create{}
	options := newSQLBulkTestOptions(t)
	options.PreHookFunc = func(
		_ context.Context,
		_ storage.IStorage,
		id string,
		target string,
		value any,
		gotPrm *create.Create,
	) error {
		if target != "documents" {
			t.Fatalf("pre-hook target = %q, want documents", target)
		}
		if gotPrm != prm {
			t.Fatalf("pre-hook params = %p, want %p", gotPrm, prm)
		}
		if id == "reject" {
			if value != "bad" {
				t.Fatalf("rejected value = %v, want bad", value)
			}

			return rejected
		}

		return nil
	}
	chunk := []storage.Indexed[storage.BulkItem]{
		{Index: 2, Item: storage.BulkItem{ID: "", Value: "missing"}},
		{Index: 5, Item: storage.BulkItem{ID: "reject", Value: "bad"}},
		{Index: 9, Item: storage.BulkItem{ID: "accepted", Value: "good"}},
	}
	result := storage.BulkResult{}

	accepted, canceled := prepareChunk(
		t.Context(),
		&storage.Mock{},
		"documents",
		chunk,
		prm,
		options,
		&result,
	)
	if canceled != nil {
		t.Fatalf("prepareChunk() cancellation error = %v, want nil", canceled)
	}
	if got, want := accepted, chunk[2:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("prepareChunk() accepted = %#v, want %#v", got, want)
	}
	if len(result.Failures) != 2 {
		t.Fatalf("failure count = %d, want 2", len(result.Failures))
	}
	if got, want := []int{result.Failures[0].Index, result.Failures[1].Index}, []int{2, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("failure indices = %v, want %v", got, want)
	}
	if got, want := result.Failures[0].Err.Error(), "id required"; got != want {
		t.Fatalf("empty-ID error = %q, want %q", got, want)
	}
	if !errors.Is(result.Failures[1].Err, rejected) {
		t.Fatalf("pre-hook error = %v, want %v", result.Failures[1].Err, rejected)
	}
}

func TestPrepareChunkCancellationFailsAcceptedAndPendingItems(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	options := newSQLBulkTestOptions(t)
	options.PreHookFunc = func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
		cancel()

		return nil
	}
	chunk := []storage.Indexed[storage.BulkItem]{
		{Index: 13, Item: storage.BulkItem{ID: "accepted-before-cancel"}},
		{Index: 21, Item: storage.BulkItem{ID: "pending-one"}},
		{Index: 34, Item: storage.BulkItem{ID: "pending-two"}},
	}
	result := storage.BulkResult{}

	accepted, canceled := prepareChunk(
		ctx,
		&storage.Mock{},
		"documents",
		chunk,
		&create.Create{},
		options,
		&result,
	)
	if accepted != nil {
		t.Fatalf("prepareChunk() accepted = %#v, want nil after cancellation", accepted)
	}
	if !errors.Is(canceled, context.Canceled) {
		t.Fatalf("prepareChunk() error = %v, want context.Canceled", canceled)
	}
	if len(result.Failures) != len(chunk) {
		t.Fatalf("failure count = %d, want %d", len(result.Failures), len(chunk))
	}
	for i, failure := range result.Failures {
		if failure.Index != chunk[i].Index {
			t.Fatalf("failure %d index = %d, want %d", i, failure.Index, chunk[i].Index)
		}
		if !errors.Is(failure.Err, context.Canceled) {
			t.Fatalf("failure at input index %d = %v, want context.Canceled", failure.Index, failure.Err)
		}
	}
}

func TestStoppedOutcomeFailsEveryRowAtItsOriginalIndex(t *testing.T) {
	t.Parallel()

	cause := errors.New("database connection canceled")
	rows := []storage.Indexed[storage.BulkItem]{
		{Index: 4, Item: storage.BulkItem{ID: "four"}},
		{Index: 18, Item: storage.BulkItem{ID: "eighteen"}},
		{Index: 27, Item: storage.BulkItem{ID: "twenty-seven"}},
	}

	outcome := stoppedOutcome(rows, cause)
	if !outcome.stopped {
		t.Fatal("stoppedOutcome().stopped = false, want true")
	}
	if len(outcome.failures) != len(rows) {
		t.Fatalf("failure count = %d, want %d", len(outcome.failures), len(rows))
	}
	if len(outcome.succeeded) != 0 {
		t.Fatalf("succeeded rows = %#v, want none", outcome.succeeded)
	}
	for i, failure := range outcome.failures {
		if failure.Index != rows[i].Index {
			t.Fatalf("failure %d index = %d, want %d", i, failure.Index, rows[i].Index)
		}
		if !errors.Is(failure.Err, cause) {
			t.Fatalf("failure at input index %d = %v, want %v", failure.Index, failure.Err, cause)
		}
	}
}

func TestAllFailedReturnsInspectableAggregate(t *testing.T) {
	t.Parallel()

	cause := errors.New("target name required")
	items := []storage.BulkItem{{ID: "zero"}, {ID: "one"}, {ID: "two"}}

	result, err := allFailed(items, cause)
	if got, want := err.Error(), "bulk operation completed with 3 unsuccessful item(s)"; got != want {
		t.Fatalf("allFailed() error = %q, want %q", got, want)
	}
	if result.Stopped {
		t.Fatal("allFailed() result.Stopped = true, want false")
	}
	if len(result.Succeeded) != 0 {
		t.Fatalf("allFailed() succeeded = %v, want none", result.Succeeded)
	}
	if len(result.Failures) != len(items) {
		t.Fatalf("failure count = %d, want %d", len(result.Failures), len(items))
	}
	for index, failure := range result.Failures {
		if failure.Index != index {
			t.Fatalf("failure %d index = %d, want %d", index, failure.Index, index)
		}
		if !errors.Is(failure.Err, cause) {
			t.Fatalf("failure at input index %d = %v, want %v", failure.Index, failure.Err, cause)
		}
	}
	if !errors.Is(err, cause) {
		t.Fatalf("aggregate error does not unwrap original cause: %v", err)
	}
}
