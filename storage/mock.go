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
// Creates the a struct which satisfies the storage.IStorage interface.
//////

// Mock is a struct which satisfies the storage.IStorage interface.
//
//nolint:dupl
type Mock struct {
	//////
	// Allows to set the returned value of each method.
	//////

	MockCount  func(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error)
	MockDelete func(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error
	MockGet    func(ctx context.Context, id, target string, v any, prm *get.Get, options ...Func[*get.Get]) error
	MockList   func(ctx context.Context, target string, v any, prm *list.List, opts ...Func[*list.List]) error
	MockSet    func(ctx context.Context, id, target string, v any, prm *set.Set, options ...Func[*set.Set]) (string, error)
	MockUpdate func(ctx context.Context, id, target string, v any, prm *update.Update, opts ...Func[*update.Update]) error

	MockGetClient func() any
}

//////
// When the methods are called, it will call the corresponding method in the
// Mock struct returning the desired value. This implements the IStorage
// interface.
//////

// Count counts data.
func (m *Mock) Count(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error) {
	return m.MockCount(ctx, target, prm, options...)
}

// Delete removes data.
func (m *Mock) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
	return m.MockDelete(ctx, id, target, prm, options...)
}

// Get retrieves data.
func (m *Mock) Get(ctx context.Context, id, target string, v any, prm *get.Get, options ...Func[*get.Get]) error {
	return m.MockGet(ctx, id, target, v, prm, options...)
}

// List data.
func (m *Mock) List(ctx context.Context, target string, v any, prm *list.List, opts ...Func[*list.List]) error {
	return m.MockList(ctx, target, v, prm, opts...)
}

// Set stores data.
func (m *Mock) Set(ctx context.Context, id, target string, v any, prm *set.Set, options ...Func[*set.Set]) (string, error) {
	return m.MockSet(ctx, id, target, v, prm, options...)
}

// Update updates data.
func (m *Mock) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...Func[*update.Update]) error {
	return m.MockUpdate(ctx, id, target, v, prm, opts...)
}

// GetClient returns the storage client. Use that to interact with the
// underlying storage client.
func (m *Mock) GetClient() any {
	return m.MockGetClient()
}
