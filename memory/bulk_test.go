package memory

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

type bulkTestDocument struct {
	Value string `json:"value"`
}

func bulkTestItems(count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for i := range items {
		items[i] = storage.BulkItem{
			ID:    fmt.Sprintf("item-%d", i),
			Value: bulkTestDocument{Value: fmt.Sprintf("value-%d", i)},
		}
	}

	return items
}

func TestBulkCreateHappyPathManyItems(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	items := bulkTestItems(60)
	result, err := strg.BulkCreate(t.Context(), "target", items, nil)
	if err != nil {
		t.Fatalf("BulkCreate() error = %v", err)
	}
	if !result.AllSucceeded() {
		t.Fatalf("AllSucceeded() = false, failures = %v", result.Failures)
	}
	if len(result.Succeeded) != len(items) {
		t.Fatalf("succeeded count = %d, want %d", len(result.Succeeded), len(items))
	}
	if result.Stopped {
		t.Fatal("Stopped = true, want false")
	}

	for index, item := range items {
		if result.Succeeded[index] != index {
			t.Fatalf("succeeded index %d = %d, want %d", index, result.Succeeded[index], index)
		}

		var got bulkTestDocument
		if err := strg.Retrieve(t.Context(), item.ID, "target", &got, nil); err != nil {
			t.Fatalf("Retrieve(%q) error = %v", item.ID, err)
		}
		if !reflect.DeepEqual(got, item.Value) {
			t.Fatalf("Retrieve(%q) = %#v, want %#v", item.ID, got, item.Value)
		}
	}
}

func TestBulkCreateEmptyInput(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := strg.BulkCreate(ctx, "target", nil, nil)
	if err != nil {
		t.Fatalf("BulkCreate() error = %v", err)
	}
	if !reflect.DeepEqual(result, storage.BulkResult{}) {
		t.Fatalf("BulkCreate() result = %#v, want zero result", result)
	}
	if !result.AllSucceeded() {
		t.Fatal("AllSucceeded() = false, want true")
	}
}

func TestBulkCreateSingleItem(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	items := bulkTestItems(1)
	result, err := strg.BulkCreate(t.Context(), "target", items, nil)
	if err != nil {
		t.Fatalf("BulkCreate() error = %v", err)
	}
	if !reflect.DeepEqual(result.Succeeded, []int{0}) {
		t.Fatalf("Succeeded = %v, want [0]", result.Succeeded)
	}
	if !result.AllSucceeded() {
		t.Fatal("AllSucceeded() = false, want true")
	}
}

func TestBulkCreateEmptyIDFailsByInputIndex(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	items := []storage.BulkItem{{Value: bulkTestDocument{Value: "no id"}}}
	result, err := strg.BulkCreate(t.Context(), "target", items, nil)
	if err == nil {
		t.Fatal("BulkCreate() error = nil, want required-ID error")
	}
	if len(result.Succeeded) != 0 {
		t.Fatalf("Succeeded = %v, want empty", result.Succeeded)
	}
	if len(result.Failures) != 1 || result.Failures[0].Index != 0 {
		t.Fatalf("Failures = %#v, want one failure at index 0", result.Failures)
	}
}

func TestBulkCreatePartialFailurePreservesOriginalIndices(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rejected := errors.New("rejected by test hook")
	rejectedIDs := map[string]bool{"item-27": true, "item-29": true}
	preHook := func(_ context.Context, _ storage.IStorage, id, _ string, _ any, _ *create.Create) error {
		if rejectedIDs[id] {
			return rejected
		}

		return nil
	}

	items := bulkTestItems(35)
	result, err := strg.BulkCreate(t.Context(), "target", items, nil, storage.WithPreHook(preHook))
	if err == nil {
		t.Fatal("BulkCreate() error = nil, want partial-failure error")
	}
	if result.AllSucceeded() {
		t.Fatal("AllSucceeded() = true, want false")
	}
	if result.Stopped {
		t.Fatal("Stopped = true, want false")
	}

	var bulkErr *storage.BulkError
	if !errors.As(err, &bulkErr) {
		t.Fatalf("BulkCreate() error type = %T, want *storage.BulkError", err)
	}
	if bulkErr.Stopped {
		t.Fatal("BulkError.Stopped = true, want false")
	}
	if !errors.Is(err, rejected) {
		t.Fatalf("errors.Is(error, rejected) = false: %v", err)
	}

	gotFailureIndices := make([]int, len(result.Failures))
	for i, failure := range result.Failures {
		gotFailureIndices[i] = failure.Index
		if !errors.Is(failure.Err, rejected) {
			t.Fatalf("failure %d error = %v, want rejected error", failure.Index, failure.Err)
		}
	}
	if want := []int{27, 29}; !reflect.DeepEqual(gotFailureIndices, want) {
		t.Fatalf("failure indices = %v, want %v", gotFailureIndices, want)
	}

	if len(result.Succeeded) != len(items)-len(rejectedIDs) {
		t.Fatalf("succeeded count = %d, want %d", len(result.Succeeded), len(items)-len(rejectedIDs))
	}

	for index, item := range items {
		var got bulkTestDocument
		err := strg.Retrieve(t.Context(), item.ID, "target", &got, nil)
		if rejectedIDs[item.ID] {
			if err == nil {
				t.Fatalf("Retrieve(%q) error = nil, want not found", item.ID)
			}

			continue
		}
		if err != nil {
			t.Fatalf("Retrieve(%q) at index %d error = %v", item.ID, index, err)
		}
	}
}

func TestBulkCreateTotalFailure(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	rejected := errors.New("all rejected")
	preHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
		return rejected
	}

	items := bulkTestItems(30)
	result, err := strg.BulkCreate(t.Context(), "target", items, nil, storage.WithPreHook(preHook))
	if err == nil {
		t.Fatal("BulkCreate() error = nil, want total-failure error")
	}
	if len(result.Succeeded) != 0 {
		t.Fatalf("Succeeded = %v, want empty", result.Succeeded)
	}
	if len(result.Failures) != len(items) {
		t.Fatalf("failure count = %d, want %d", len(result.Failures), len(items))
	}
	if result.Stopped {
		t.Fatal("Stopped = true, want false because every item was attempted")
	}
	if !errors.Is(err, rejected) {
		t.Fatalf("errors.Is(error, rejected) = false: %v", err)
	}
	for index, failure := range result.Failures {
		if failure.Index != index {
			t.Fatalf("failure index %d = %d, want %d", index, failure.Index, index)
		}
		if !errors.Is(failure.Err, rejected) {
			t.Fatalf("failure %d error = %v, want rejected error", failure.Index, failure.Err)
		}
	}
}

func TestBulkCreateContextCancellationMidBatch(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	written := 0
	postHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
		written++
		if written == 7 {
			cancel()
		}

		return nil
	}

	items := bulkTestItems(40)
	result, err := strg.BulkCreate(ctx, "target", items, nil, storage.WithPostHook(postHook))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BulkCreate() error = %v, want context.Canceled", err)
	}
	if !result.Stopped {
		t.Fatal("Stopped = false, want true")
	}
	if !reflect.DeepEqual(result.Succeeded, []int{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("Succeeded = %v, want [0 1 2 3 4 5 6]", result.Succeeded)
	}
	if len(result.Failures) != len(items)-written {
		t.Fatalf("failure count = %d, want %d", len(result.Failures), len(items)-written)
	}
	for index, failure := range result.Failures {
		wantIndex := index + written
		if failure.Index != wantIndex {
			t.Fatalf("failure index %d = %d, want %d", index, failure.Index, wantIndex)
		}
		if !errors.Is(failure.Err, context.Canceled) {
			t.Fatalf("failure %d error = %v, want context.Canceled", failure.Index, failure.Err)
		}
	}

	for index := range items {
		var got bulkTestDocument
		retrieveErr := strg.Retrieve(t.Context(), items[index].ID, "target", &got, nil)
		if index < written {
			if retrieveErr != nil {
				t.Fatalf("already-written item %d missing: %v", index, retrieveErr)
			}

			continue
		}
		if retrieveErr == nil {
			t.Fatalf("unattempted item %d was written", index)
		}
	}
}

func TestBulkCreateDuplicateKeys(t *testing.T) {
	strg, err := New(t.Context())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first := bulkTestDocument{Value: "first"}
	second := bulkTestDocument{Value: "second"}
	items := []storage.BulkItem{
		{ID: "duplicate", Value: first},
		{ID: "duplicate", Value: second},
	}

	result, err := strg.BulkCreate(t.Context(), "target", items, nil)
	if err != nil {
		t.Fatalf("BulkCreate() error = %v", err)
	}
	if !reflect.DeepEqual(result.Succeeded, []int{0, 1}) {
		t.Fatalf("Succeeded = %v, want [0 1]", result.Succeeded)
	}

	var got bulkTestDocument
	if err := strg.Retrieve(t.Context(), "duplicate", "target", &got, nil); err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if !reflect.DeepEqual(got, first) && !reflect.DeepEqual(got, second) {
		t.Fatalf("duplicate value = %#v, want one of %#v or %#v", got, first, second)
	}
}
