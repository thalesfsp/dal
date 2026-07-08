package storage

import (
	"context"
	"expvar"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/sypl"
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

	// Count data.
	MockCount func(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error)

	// Delete data.
	MockDelete func(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error

	// Retrieve data.
	MockRetrieve func(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error

	// List data.
	MockList func(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error

	// Create data.
	MockCreate func(ctx context.Context, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (string, error)

	// Update data.
	MockUpdate func(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error

	// GetType returns its type.
	MockGetType func() string

	// GetClient returns the storage client. Use that to interact with the underlying storage client.
	MockGetClient func() any

	// GetLogger returns the logger.
	MockGetLogger func() sypl.ISypl

	// GetName returns the storage name.
	MockGetName func() string

	// GetCounterCounted returns the metric.
	MockGetCounterCounted func() *expvar.Int

	// GetCounterCountedFailed returns the metric.
	MockGetCounterCountedFailed func() *expvar.Int

	// GetCounterDeleted returns the metric.
	MockGetCounterDeleted func() *expvar.Int

	// GetCounterDeletedFailed returns the metric.
	MockGetCounterDeletedFailed func() *expvar.Int

	// GetCounterRetrieved returns the metric.
	MockGetCounterRetrieved func() *expvar.Int

	// GetCounterRetrievedFailed returns the metric.
	MockGetCounterRetrievedFailed func() *expvar.Int

	// GetCounterListed returns the metric.
	MockGetCounterListed func() *expvar.Int

	// GetCounterListedFailed returns the metric.
	MockGetCounterListedFailed func() *expvar.Int

	// GetCounterPingFailed returns the metric.
	MockGetCounterPingFailed func() *expvar.Int

	// GetCounterCreated returns the metric.
	MockGetCounterCreated func() *expvar.Int

	// GetCounterCreatedFailed returns the metric.
	MockGetCounterCreatedFailed func() *expvar.Int

	// GetCounterUpdated returns the metric.
	MockGetCounterUpdated func() *expvar.Int

	// GetCounterUpdatedFailed returns the metric.
	MockGetCounterUpdatedFailed func() *expvar.Int
}

//////
// When the methods are called, it will call the corresponding method in the
// Mock struct returning the desired value. This implements the IStorage
// interface.
//////

// Count data.
func (m *Mock) Count(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error) {
	if m.MockCount == nil {
		return 0, customerror.NewMissingError("MockCount")
	}

	return m.MockCount(ctx, target, prm, options...)
}

// Delete data.
func (m *Mock) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
	if m.MockDelete == nil {
		return customerror.NewMissingError("MockDelete")
	}

	return m.MockDelete(ctx, id, target, prm, options...)
}

// Retrieve data.
func (m *Mock) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error {
	if m.MockRetrieve == nil {
		return customerror.NewMissingError("MockRetrieve")
	}

	return m.MockRetrieve(ctx, id, target, v, prm, options...)
}

// List data.
func (m *Mock) List(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error {
	if m.MockList == nil {
		return customerror.NewMissingError("MockList")
	}

	return m.MockList(ctx, target, v, prm, options...)
}

// Create data.
func (m *Mock) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (string, error) {
	if m.MockCreate == nil {
		return "", customerror.NewMissingError("MockCreate")
	}

	return m.MockCreate(ctx, id, target, v, prm, options...)
}

// Update data.
func (m *Mock) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error {
	if m.MockUpdate == nil {
		return customerror.NewMissingError("MockUpdate")
	}

	return m.MockUpdate(ctx, id, target, v, prm, options...)
}

// GetType returns its type.
func (m *Mock) GetType() string {
	if m.MockGetType == nil {
		return ""
	}

	return m.MockGetType()
}

// GetClient returns the storage client. Use that to interact with the underlying storage client.
func (m *Mock) GetClient() any {
	if m.MockGetClient == nil {
		return nil
	}

	return m.MockGetClient()
}

// GetLogger returns the logger.
func (m *Mock) GetLogger() sypl.ISypl {
	if m.MockGetLogger == nil {
		return nil
	}

	return m.MockGetLogger()
}

// GetName returns the storage name.
func (m *Mock) GetName() string {
	if m.MockGetName == nil {
		return ""
	}

	return m.MockGetName()
}

// GetCounterCounted returns the metric.
func (m *Mock) GetCounterCounted() *expvar.Int {
	if m.MockGetCounterCounted == nil {
		return nil
	}

	return m.MockGetCounterCounted()
}

// GetCounterCountedFailed returns the metric.
func (m *Mock) GetCounterCountedFailed() *expvar.Int {
	if m.MockGetCounterCountedFailed == nil {
		return nil
	}

	return m.MockGetCounterCountedFailed()
}

// GetCounterDeleted returns the metric.
func (m *Mock) GetCounterDeleted() *expvar.Int {
	if m.MockGetCounterDeleted == nil {
		return nil
	}

	return m.MockGetCounterDeleted()
}

// GetCounterDeletedFailed returns the metric.
func (m *Mock) GetCounterDeletedFailed() *expvar.Int {
	if m.MockGetCounterDeletedFailed == nil {
		return nil
	}

	return m.MockGetCounterDeletedFailed()
}

// GetCounterRetrieved returns the metric.
func (m *Mock) GetCounterRetrieved() *expvar.Int {
	if m.MockGetCounterRetrieved == nil {
		return nil
	}

	return m.MockGetCounterRetrieved()
}

// GetCounterRetrievedFailed returns the metric.
func (m *Mock) GetCounterRetrievedFailed() *expvar.Int {
	if m.MockGetCounterRetrievedFailed == nil {
		return nil
	}

	return m.MockGetCounterRetrievedFailed()
}

// GetCounterListed returns the metric.
func (m *Mock) GetCounterListed() *expvar.Int {
	if m.MockGetCounterListed == nil {
		return nil
	}

	return m.MockGetCounterListed()
}

// GetCounterListedFailed returns the metric.
func (m *Mock) GetCounterListedFailed() *expvar.Int {
	if m.MockGetCounterListedFailed == nil {
		return nil
	}

	return m.MockGetCounterListedFailed()
}

// GetCounterPingFailed returns the metric.
func (m *Mock) GetCounterPingFailed() *expvar.Int {
	if m.MockGetCounterPingFailed == nil {
		return nil
	}

	return m.MockGetCounterPingFailed()
}

// GetCounterCreated returns the metric.
func (m *Mock) GetCounterCreated() *expvar.Int {
	if m.MockGetCounterCreated == nil {
		return nil
	}

	return m.MockGetCounterCreated()
}

// GetCounterCreatedFailed returns the metric.
func (m *Mock) GetCounterCreatedFailed() *expvar.Int {
	if m.MockGetCounterCreatedFailed == nil {
		return nil
	}

	return m.MockGetCounterCreatedFailed()
}

// GetCounterUpdated returns the metric.
func (m *Mock) GetCounterUpdated() *expvar.Int {
	if m.MockGetCounterUpdated == nil {
		return nil
	}

	return m.MockGetCounterUpdated()
}

// GetCounterUpdatedFailed returns the metric.
func (m *Mock) GetCounterUpdatedFailed() *expvar.Int {
	if m.MockGetCounterUpdatedFailed == nil {
		return nil
	}

	return m.MockGetCounterUpdatedFailed()
}
