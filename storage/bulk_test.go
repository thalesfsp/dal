package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/thalesfsp/params/v2/create"
)

type bulkTestCauseError struct {
	message string
}

func (e *bulkTestCauseError) Error() string {
	return e.message
}

func TestBulkItemErrorFormattingAndUnwrap(t *testing.T) {
	t.Parallel()

	cause := &bulkTestCauseError{message: "write rejected"}
	failure := &BulkItemError{Index: 7, Err: fmt.Errorf("backend: %w", cause)}

	if got, want := failure.Error(), "bulk item at input index 7: backend: write rejected"; got != want {
		t.Fatalf("BulkItemError.Error() = %q, want %q", got, want)
	}
	if !errors.Is(failure, cause) {
		t.Fatalf("errors.Is(BulkItemError, cause) = false")
	}

	var gotCause *bulkTestCauseError
	if !errors.As(failure, &gotCause) {
		t.Fatalf("errors.As(BulkItemError, *bulkTestCause) = false")
	}
	if gotCause != cause {
		t.Fatalf("unwrapped cause = %p, want %p", gotCause, cause)
	}
}

func TestBulkResultAndBulkErrorBehavior(t *testing.T) {
	t.Parallel()

	firstCause := errors.New("first write failed")
	secondCause := errors.New("second write failed")
	success := BulkResult{Succeeded: []int{0, 1}}
	if !success.AllSucceeded() {
		t.Fatal("BulkResult.AllSucceeded() = false with no failures")
	}
	if err := success.Err(); err != nil {
		t.Fatalf("BulkResult.Err() = %v, want nil", err)
	}

	result := BulkResult{
		Succeeded: []int{1},
		Failures: []BulkItemError{
			{Index: 3, Err: firstCause},
			{Index: 8, Err: secondCause},
		},
	}
	if result.AllSucceeded() {
		t.Fatal("BulkResult.AllSucceeded() = true with failures")
	}

	err := result.Err()
	if got, want := err.Error(), "bulk operation completed with 2 unsuccessful item(s)"; got != want {
		t.Fatalf("BulkError.Error() = %q, want %q", got, want)
	}
	if !errors.Is(err, firstCause) || !errors.Is(err, secondCause) {
		t.Fatalf("BulkError did not expose both causes: %v", err)
	}

	var bulkErr *BulkError
	if !errors.As(err, &bulkErr) {
		t.Fatalf("errors.As(error, *BulkError) = false")
	}
	if bulkErr.Stopped {
		t.Fatal("BulkError.Stopped = true, want false")
	}
	if got, want := bulkErr.Unwrap(), []error{&bulkErr.Failures[0], &bulkErr.Failures[1]}; !reflect.DeepEqual(got, want) {
		t.Fatalf("BulkError.Unwrap() = %#v, want %#v", got, want)
	}

	var itemErr *BulkItemError
	if !errors.As(err, &itemErr) {
		t.Fatalf("errors.As(error, *BulkItemError) = false")
	}
	if itemErr.Index != 3 {
		t.Fatalf("first unwrapped failure index = %d, want 3", itemErr.Index)
	}

	stopped := &BulkError{Failures: []BulkItemError{{Index: 5, Err: context.Canceled}}, Stopped: true}
	if got, want := stopped.Error(), "bulk operation stopped with 1 unsuccessful item(s)"; got != want {
		t.Fatalf("stopped BulkError.Error() = %q, want %q", got, want)
	}
	if !errors.Is(stopped, context.Canceled) {
		t.Fatal("stopped BulkError does not unwrap context.Canceled")
	}
}

func TestBulkCreateWrapperAndMock(t *testing.T) {
	t.Parallel()

	missingResult, missingErr := (&Mock{}).BulkCreate(t.Context(), "documents", nil, nil)
	if !reflect.DeepEqual(missingResult, BulkResult{}) {
		t.Fatalf("Mock.BulkCreate() result = %#v, want zero result", missingResult)
	}
	if got, want := missingErr.Error(), "missing MockBulkCreate"; got != want {
		t.Fatalf("Mock.BulkCreate() error = %q, want %q", got, want)
	}

	type contextKey string
	ctx := context.WithValue(t.Context(), contextKey("request"), "bulk-17")
	items := []BulkItem{{ID: "alpha", Value: map[string]int{"value": 1}}}
	prm := &create.Create{}
	wantResult := BulkResult{Succeeded: []int{0}}
	wantErr := errors.New("mock aggregate result")
	optionCalled := false
	option := func(options *Options[*create.Create]) error {
		optionCalled = true
		options.Database = "archive"

		return nil
	}
	mock := &Mock{MockBulkCreate: func(
		gotCtx context.Context,
		gotTarget string,
		gotItems []BulkItem,
		gotPrm *create.Create,
		gotOptions ...Func[*create.Create],
	) (BulkResult, error) {
		if gotCtx.Value(contextKey("request")) != "bulk-17" {
			t.Fatalf("delegated context request = %v, want bulk-17", gotCtx.Value(contextKey("request")))
		}
		if gotTarget != "documents" {
			t.Fatalf("delegated target = %q, want documents", gotTarget)
		}
		if !reflect.DeepEqual(gotItems, items) {
			t.Fatalf("delegated items = %#v, want %#v", gotItems, items)
		}
		if gotPrm != prm {
			t.Fatalf("delegated params = %p, want %p", gotPrm, prm)
		}
		if len(gotOptions) != 1 {
			t.Fatalf("delegated option count = %d, want 1", len(gotOptions))
		}
		options, err := NewOptions[*create.Create]()
		if err != nil {
			t.Fatalf("NewOptions() error = %v", err)
		}
		if err := gotOptions[0](options); err != nil {
			t.Fatalf("delegated option error = %v", err)
		}
		if options.Database != "archive" {
			t.Fatalf("delegated option database = %q, want archive", options.Database)
		}

		return wantResult, wantErr
	}}

	gotResult, gotErr := BulkCreate(ctx, mock, "documents", items, prm, option)
	if !reflect.DeepEqual(gotResult, wantResult) {
		t.Fatalf("BulkCreate() result = %#v, want %#v", gotResult, wantResult)
	}
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("BulkCreate() error = %v, want %v", gotErr, wantErr)
	}
	if !optionCalled {
		t.Fatal("delegated BulkCreate option was not called")
	}
}

func TestChunkIndexedPreservesOriginalIndices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		itemCount  int
		wantChunks []int
	}{
		{name: "smaller than cap", itemCount: 24, wantChunks: []int{24}},
		{name: "exactly cap", itemCount: 25, wantChunks: []int{25}},
		{name: "one over cap", itemCount: 26, wantChunks: []int{25, 1}},
		{name: "many multiples", itemCount: 100, wantChunks: []int{25, 25, 25, 25}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			items := make([]string, tt.itemCount)
			for i := range items {
				items[i] = string(rune(i))
			}

			chunks, err := ChunkIndexed(items, 25)
			if err != nil {
				t.Fatalf("ChunkIndexed() error = %v", err)
			}
			if len(chunks) != len(tt.wantChunks) {
				t.Fatalf("chunk count = %d, want %d", len(chunks), len(tt.wantChunks))
			}

			originalIndex := 0
			for chunkIndex, chunk := range chunks {
				if len(chunk) != tt.wantChunks[chunkIndex] {
					t.Fatalf("chunk %d length = %d, want %d", chunkIndex, len(chunk), tt.wantChunks[chunkIndex])
				}

				for _, indexed := range chunk {
					if indexed.Index != originalIndex {
						t.Fatalf("original index = %d, want %d", indexed.Index, originalIndex)
					}
					if indexed.Item != items[originalIndex] {
						t.Fatalf("item at original index %d = %q, want %q", originalIndex, indexed.Item, items[originalIndex])
					}

					originalIndex++
				}
			}
		})
	}
}

func TestChunkIndexedRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	chunks, err := ChunkIndexed([]int{1}, 0)
	if !errors.Is(err, ErrInvalidChunkLimit) {
		t.Fatalf("ChunkIndexed() error = %v, want %v", err, ErrInvalidChunkLimit)
	}
	if chunks != nil {
		t.Fatalf("ChunkIndexed() chunks = %v, want nil", chunks)
	}
}

func TestChunkIndexedEmptyInput(t *testing.T) {
	t.Parallel()

	chunks, err := ChunkIndexed([]int(nil), 25)
	if err != nil {
		t.Fatalf("ChunkIndexed() error = %v", err)
	}
	if chunks != nil {
		t.Fatalf("ChunkIndexed() chunks = %v, want nil", chunks)
	}
}
