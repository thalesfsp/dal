package storage

import (
	"context"
	"expvar"

	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/sypl/v2"
)

//////
// Const, vars, and types.
//////

// IStorage defines the data access layer interface.
//
// The Data Access Layer (DAL) is generally more abstract and wider than the
// Data Access Object (DAO).
//
// The DAL is responsible for providing a consistent and unified interface to
// access data from different data sources (such as databases, files, or web
// services) regardless of their underlying implementation details. It abstracts
// away the complexity of data access by providing a simple, unified interface
// that shields the rest of the application from the underlying data storage
// details.
//
// On the other hand, the DAO is typically more specific to a particular data
// source, such as a particular database management system (such as MySQL or
// MongoDB) or a particular web service API. Its primary responsibility is to
// abstract away the details of the underlying data source and provide a
// simplified interface for performing CRUD (Create, Read, Update, Delete)
// operations on that data source.
//
// So while both DAL and DAO are used to abstract away the complexities of data
// access, the DAL is generally wider and more abstract because it deals with
// multiple data sources, while the DAO is more specific and deals with a
// particular data source.
type IStorage interface {
	// Close releases the resources held by the storage.
	//
	// Close must be idempotent: a second call returns nil, so a deferred Close
	// alongside an explicit one is not a bug. In-process adapters that hold no
	// releasable resource implement Close as a no-op so the surface is uniform.
	Close() error

	// Count data.
	Count(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error)

	// Delete data.
	Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error

	// Retrieve data.
	Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error

	// List data.
	List(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error

	// Create data.
	Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (string, error)

	// BulkCreate creates all items using the same target, parameters, and
	// options. Each BulkItem.Value has the same semantics as a call to Create.
	// BulkItem.ID must be non-empty; an empty ID is a per-item failure. Requiring
	// caller-supplied IDs keeps successful identifiers known from the input even
	// on backends whose single-item Create can generate an ID.
	//
	// BulkCreate is not transactional: successful writes remain committed when
	// other items fail. Callers must inspect BulkResult before retrying to avoid
	// blindly repeating successful writes. Backends may chunk, parallelize, or
	// reorder work, so neither processing order nor result-slice order is
	// guaranteed. Original zero-based input indices are the only portable way
	// to correlate outcomes with inputs.
	//
	// Every input index appears exactly once in BulkResult.Succeeded or
	// BulkResult.Failures. A failure retains its underlying error. If any item
	// is unsuccessful, err is non-nil and is an inspectable *BulkError; if every
	// item succeeds, err is nil and BulkResult.AllSucceeded reports true without
	// scanning a slice. BulkResult.Stopped is true when processing stopped early
	// after a failure or context cancellation. In that case, unattempted items
	// appear in Failures with the error that stopped processing. Stopped is
	// false when processing continued through all inputs despite item failures.
	//
	// Options retain Create's per-item hook semantics: the pre-hook runs before
	// each attempted write and the post-hook runs after each successful backend
	// write. A pre-hook failure prevents that item's write. A post-hook failure
	// is reported for that item but does not roll its already-completed write
	// back, consistent with the non-atomic contract.
	//
	// Empty input succeeds with a zero BulkResult and nil error, even if ctx is
	// already canceled. BulkCreate is not atomic and makes no rollback promise.
	// If the same non-empty ID appears more than once, which value remains is
	// unspecified; each occurrence is still classified independently by its
	// input index. Callers requiring a deterministic winner must remove duplicate
	// IDs before calling BulkCreate.
	BulkCreate(
		ctx context.Context,
		target string,
		items []BulkItem,
		prm *create.Create,
		options ...Func[*create.Create],
	) (BulkResult, error)

	// Update data.
	Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error

	// GetType returns its type.
	GetType() string

	// GetClient returns the storage client. Use that to interact with the
	// underlying storage client.
	GetClient() any

	// GetLogger returns the logger.
	GetLogger() sypl.ISypl

	// GetName returns the storage name.
	GetName() string

	// GetCounterCounted returns the metric.
	GetCounterCounted() *expvar.Int

	// GetCounterCountedFailed returns the metric.
	GetCounterCountedFailed() *expvar.Int

	// GetCounterDeleted returns the metric.
	GetCounterDeleted() *expvar.Int

	// GetCounterDeletedFailed returns the metric.
	GetCounterDeletedFailed() *expvar.Int

	// GetCounterRetrieved returns the metric.
	GetCounterRetrieved() *expvar.Int

	// GetCounterRetrievedFailed returns the metric.
	GetCounterRetrievedFailed() *expvar.Int

	// GetCounterListed returns the metric.
	GetCounterListed() *expvar.Int

	// GetCounterListedFailed returns the metric.
	GetCounterListedFailed() *expvar.Int

	// GetCounterPingFailed returns the metric.
	GetCounterPingFailed() *expvar.Int

	// GetCounterCreated returns the metric.
	GetCounterCreated() *expvar.Int

	// GetCounterCreatedFailed returns the metric.
	GetCounterCreatedFailed() *expvar.Int

	// GetCounterUpdated returns the metric.
	GetCounterUpdated() *expvar.Int

	// GetCounterUpdatedFailed returns the metric.
	GetCounterUpdatedFailed() *expvar.Int
}

//////
// Generic functions.
//////

// Count data.
func Count(ctx context.Context, s IStorage, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error) {
	return s.Count(ctx, target, prm, options...)
}

// Delete data.
func Delete(ctx context.Context, s IStorage, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
	return s.Delete(ctx, id, target, prm, options...)
}

// Retrieve data.
func Retrieve[T any](ctx context.Context, s IStorage, id, target string, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) (T, error) {
	var t T

	if err := s.Retrieve(ctx, id, target, &t, prm, options...); err != nil {
		return *new(T), err
	}

	return t, nil
}

// List data.
func List[T any](ctx context.Context, s IStorage, target string, prm *list.List, options ...Func[*list.List]) (T, error) {
	var t T

	if err := s.List(ctx, target, &t, prm, options...); err != nil {
		return *new(T), err
	}

	return t, nil
}

// Create data.
func Create[T any](ctx context.Context, s IStorage, id, target string, t T, prm *create.Create, options ...Func[*create.Create]) (string, error) {
	return s.Create(ctx, id, target, t, prm, options...)
}

// BulkCreate creates many items using the storage's native bulk surface.
func BulkCreate(
	ctx context.Context,
	s IStorage,
	target string,
	items []BulkItem,
	prm *create.Create,
	options ...Func[*create.Create],
) (BulkResult, error) {
	return s.BulkCreate(ctx, target, items, prm, options...)
}

// Update data.
func Update[T any](ctx context.Context, s IStorage, id, target string, t T, prm *update.Update, options ...Func[*update.Update]) error {
	return s.Update(ctx, id, target, t, prm, options...)
}
