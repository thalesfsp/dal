package memory

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/logging"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/fields"
	"github.com/thalesfsp/sypl/v2/level"
	"github.com/thalesfsp/validation"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "memory"

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// Memory storage definition.
type Memory struct {
	*storage.Storage

	client *sync.Map

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For Memory, it isn't used at all.
	Target string `json:"-" validate:"omitempty,gt=0"`
}

//////
// Implements the IStorage interface.
//////

// Close releases the resources held by the storage. Memory holds no
// releasable resources, so Close is a no-op.
func (s *Memory) Close() error {
	return nil
}

// keyMatches reports whether the stored key matches the glob pattern. Keys
// that aren't strings never match. A malformed pattern returns an error.
func keyMatches(pattern string, key any) (bool, error) {
	k, ok := key.(string)
	if !ok {
		return false, nil
	}

	matched, err := path.Match(pattern, k)
	if err != nil {
		return false, customerror.NewInvalidError(
			"search pattern",
			customerror.WithError(err),
		)
	}

	return matched, nil
}

// Count returns the number of items in the storage.
func (s *Memory) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	pattern := finalParam.Search
	if pattern == "" {
		pattern = "*"
	}

	count := 0

	var matchErr error

	s.client.Range(func(key, value any) bool {
		matched, err := keyMatches(pattern, key)
		if err != nil {
			matchErr = err

			return false
		}

		if matched {
			count++
		}

		return true
	})

	if matchErr != nil {
		return 0, customapm.TraceError(ctx, matchErr, s.GetLogger(), s.GetCounterCountedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, "", target, int64(count), finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterCounted().Add(1)

	return int64(count), nil
}

// Delete removes data.
func (s *Memory) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			s.GetLogger(),
			s.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
	}

	// Application's default values.
	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
		}
	}

	s.client.Delete(id)

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (s *Memory) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
	}

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	// Retrieve a value
	val, ok := s.client.Load(id)
	if !ok {
		return customapm.TraceError(
			ctx,
			customerror.NewNotFoundError(storage.OperationRetrieve.String()),
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}

	b, ok := val.([]byte)
	if !ok {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(fmt.Errorf("stored value for id %q is %T, not []byte", id, val)),
			),
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}

	if err := shared.Unmarshal(b, v); err != nil {
		return customapm.TraceError(
			ctx,
			err,
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterRetrieved().Add(1)

	return nil
}

// List data. Iteration order is unspecified without Sort because sync.Map
// does not define an iteration order.
//
// Search is a glob matched against keys. Sort, Limit, Offset, and Fields are
// applied to the decoded documents after matching.
func (s *Memory) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	pattern := finalParam.Search
	if pattern == "" {
		pattern = "*"
	}

	var (
		docs     [][]byte
		matchErr error
	)

	s.client.Range(func(key, value any) bool {
		matched, err := keyMatches(pattern, key)
		if err != nil {
			matchErr = err

			return false
		}

		if !matched {
			return true
		}

		if b, ok := value.([]byte); ok {
			docs = append(docs, b)
		}

		return true
	})

	if matchErr != nil {
		return customapm.TraceError(ctx, matchErr, s.GetLogger(), s.GetCounterListedFailed())
	}

	docs, err = shared.ApplyListParams(docs, prm)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	if err := shared.Unmarshal(shared.AssembleItems(docs), v); err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterListed().Add(1)

	return nil
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (s *Memory) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	// The id is the storage key — an empty one would create a record that
	// Retrieve/Delete/Update (which reject empty ids) could never address.
	if id == "" {
		return "", customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			s.GetLogger(),
			s.GetCounterCreatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	finalParam.TTL = 0

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}
	}

	b, err := shared.Marshal(v)
	if err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	s.client.Store(id, b)

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterCreated().Add(1)

	return id, nil
}

// BulkCreate creates data in backend-sized chunks. Memory has no native batch
// limit; using the strictest adapter limit here makes this reference
// implementation exercise the same original-index bookkeeping as capped
// backends.
func (s *Memory) BulkCreate(
	ctx context.Context,
	target string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	const chunkLimit = 25

	if len(items) == 0 {
		return storage.BulkResult{}, nil
	}

	chunks, err := storage.ChunkIndexed(items, chunkLimit)
	if err != nil {
		return storage.BulkResult{}, err
	}

	result := storage.BulkResult{
		Succeeded: make([]int, 0, len(items)),
	}

	for chunkIndex, chunk := range chunks {
		for itemIndex, indexed := range chunk {
			if err := ctx.Err(); err != nil {
				result.Stopped = true

				for _, remaining := range chunk[itemIndex:] {
					result.Failures = append(result.Failures, storage.BulkItemError{Index: remaining.Index, Err: err})
				}

				for _, remainingChunk := range chunks[chunkIndex+1:] {
					for _, remaining := range remainingChunk {
						result.Failures = append(result.Failures, storage.BulkItemError{Index: remaining.Index, Err: err})
					}
				}

				return result, result.Err()
			}

			if _, err := s.Create(ctx, indexed.Item.ID, target, indexed.Item.Value, prm, options...); err != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

				continue
			}

			result.Succeeded = append(result.Succeeded, indexed.Index)
		}
	}

	return result, result.Err()
}

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (s *Memory) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			s.GetLogger(),
			s.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	finalParam.TTL = 0

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}
	}

	b, err := shared.Marshal(v)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	s.client.Store(id, b)

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterUpdated().Add(1)

	return nil
}

// GetClient returns the client.
func (s *Memory) GetClient() any {
	return s.client
}

//////
// Factory.
//////

// New creates a new Memory storage.
func New(ctx context.Context) (*Memory, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*Memory)(nil)

	//////
	// Storage.
	//////

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	//////
	// Validation.
	//////

	storage := &Memory{
		Storage: s,

		client: &sync.Map{},
	}

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), nil)
	}

	//////
	// Singleton.
	//////

	singletonMutex.Lock()
	singleton = storage
	singletonMutex.Unlock()

	return storage, nil
}

//////
// Exported functionalities.
//////

// Get returns a setup storage, or set it up.
func Get() storage.IStorage {
	singletonMutex.RLock()
	s := singleton
	singletonMutex.RUnlock()

	if s == nil {
		panic(fmt.Sprintf("%s %s not %s", Name, storage.Type, status.Initialized))
	}

	return s
}

// Set sets the storage, primarily used for testing.
func Set(s storage.IStorage) {
	singletonMutex.Lock()
	singleton = s
	singletonMutex.Unlock()
}
