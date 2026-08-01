package storage

import (
	"errors"
	"fmt"
)

//////
// Vars, consts, and types.
//////

// ErrInvalidChunkLimit is returned when ChunkIndexed receives a non-positive
// backend limit.
var (
	ErrInvalidChunkLimit = errors.New("chunk limit must be greater than zero")

	// ErrBulkCreateNotImplemented is available to third-party IStorage
	// implementations that intentionally do not support bulk creates. Every
	// adapter shipped by this module implements BulkCreate.
	ErrBulkCreateNotImplemented = errors.New("bulk create not implemented")
)

// BulkItem is one input to BulkCreate. ID is the caller-supplied storage key
// and must be non-empty. Value has the same meaning as the corresponding
// argument to Create.
type BulkItem struct {
	ID    string
	Value any
}

// BulkItemError identifies an unsuccessful BulkCreate input and preserves the
// error returned for that input.
type BulkItemError struct {
	Index int
	Err   error
}

// Error implements error.
func (f *BulkItemError) Error() string {
	return fmt.Sprintf("bulk item at input index %d: %v", f.Index, f.Err)
}

// Unwrap returns the error for the failed input.
func (f *BulkItemError) Unwrap() error {
	return f.Err
}

// BulkResult reports the outcome of every BulkCreate input by its original
// zero-based input index. Every input index appears exactly once in either
// Succeeded or Failures. Slice order is unspecified.
type BulkResult struct {
	Succeeded []int
	Failures  []BulkItemError
	Stopped   bool
}

// AllSucceeded reports in constant time whether every input succeeded.
func (r BulkResult) AllSucceeded() bool {
	return len(r.Failures) == 0
}

// Err returns nil when all inputs succeeded and an inspectable *BulkError
// otherwise.
func (r BulkResult) Err() error {
	if r.AllSucceeded() {
		return nil
	}

	return &BulkError{
		Failures: r.Failures,
		Stopped:  r.Stopped,
	}
}

// BulkError summarizes an unsuccessful bulk operation. Failures retain the
// original input indices and errors. Stopped has the same meaning as on
// BulkResult.
type BulkError struct {
	Failures []BulkItemError
	Stopped  bool
}

// Error implements error without flattening the individual failure causes.
func (e *BulkError) Error() string {
	if e.Stopped {
		return fmt.Sprintf("bulk operation stopped with %d unsuccessful item(s)", len(e.Failures))
	}

	return fmt.Sprintf("bulk operation completed with %d unsuccessful item(s)", len(e.Failures))
}

// Unwrap exposes every per-item failure to errors.Is and errors.As.
func (e *BulkError) Unwrap() []error {
	errs := make([]error, len(e.Failures))
	for i := range e.Failures {
		errs[i] = &e.Failures[i]
	}

	return errs
}

// Indexed carries an item together with its zero-based index in the caller's
// original, unchunked input.
type Indexed[T any] struct {
	Index int
	Item  T
}

// ChunkIndexed splits items into backend-sized chunks while attaching each
// item's index in the original input. Adapters must carry Indexed.Index through
// request preparation, retries, and result assembly; a chunk-local position is
// never a substitute for the original index. Empty input returns nil, nil.
func ChunkIndexed[T any](items []T, limit int) ([][]Indexed[T], error) {
	if limit <= 0 {
		return nil, ErrInvalidChunkLimit
	}

	if len(items) == 0 {
		return nil, nil
	}

	chunkCount := len(items) / limit
	if len(items)%limit != 0 {
		chunkCount++
	}

	chunks := make([][]Indexed[T], 0, chunkCount)
	for start := 0; start < len(items); start += limit {
		end := min(start+limit, len(items))
		chunk := make([]Indexed[T], end-start)

		for offset, item := range items[start:end] {
			chunk[offset] = Indexed[T]{
				Index: start + offset,
				Item:  item,
			}
		}

		chunks = append(chunks, chunk)
	}

	return chunks, nil
}
