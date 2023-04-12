package storage

import (
	"context"
	"expvar"

	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
	"github.com/thalesfsp/sypl"
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
//
//nolint:dupl
type IStorage interface {
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
