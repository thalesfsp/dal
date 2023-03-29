package storage

import (
	"context"

	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/get"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/set"
	"github.com/thalesfsp/params/update"
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
	// Count counts data.
	Count(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error)

	// Delete removes data.
	Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error

	// Get retrieves data.
	Get(ctx context.Context, id, target string, v any, prm *get.Get, options ...Func[*get.Get]) error

	// List data.
	List(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error

	// Set stores data.
	Set(ctx context.Context, id, target string, v any, prm *set.Set, options ...Func[*set.Set]) (string, error)

	// Update updates data.
	Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error

	// GetClient returns the storage client. Use that to interact with the
	// underlying storage client.
	GetClient() any
}
