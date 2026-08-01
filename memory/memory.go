package memory

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/envelope"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
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
	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         target,
		Param:          prm,
		Options:        options,
		SuccessCounter: s.GetCounterCounted(),
		FailureCounter: s.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err == nil {
				params.Search = "*"
			}

			return params, err
		},
	}, func(_ context.Context, params *count.Count) (envelope.Result[int64], error) {
		pattern := params.Search
		if pattern == "" {
			pattern = "*"
		}

		itemCount := 0

		var matchErr error

		s.client.Range(func(key, value any) bool {
			matched, err := keyMatches(pattern, key)
			if err != nil {
				matchErr = err

				return false
			}

			if matched {
				itemCount++
			}

			return true
		})

		if matchErr != nil {
			return envelope.Result[int64]{}, matchErr
		}

		result := int64(itemCount)

		return envelope.Result[int64]{Value: result, PostHookData: result}, nil
	})
}

// Delete removes data.
func (s *Memory) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         target,
		Param:          prm,
		Options:        options,
		NewParams:      delete.New,
		RequiresID:     true,
		SuccessCounter: s.GetCounterDeleted(),
		FailureCounter: s.GetCounterDeletedFailed(),
	}, func(_ context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		s.client.Delete(id)

		return envelope.Result[envelope.Void]{}, nil
	})

	return err
}

// Retrieve data.
func (s *Memory) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		NewParams:      retrieve.New,
		RequiresID:     true,
		SuccessCounter: s.GetCounterRetrieved(),
		FailureCounter: s.GetCounterRetrievedFailed(),
	}, func(_ context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		val, ok := s.client.Load(id)
		if !ok {
			return envelope.Result[envelope.Void]{}, customerror.NewNotFoundError(storage.OperationRetrieve.String())
		}

		b, ok := val.([]byte)
		if !ok {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(fmt.Errorf("stored value for id %q is %T, not []byte", id, val)),
			)
		}

		if err := shared.Unmarshal(b, v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data. Iteration order is unspecified without Sort because sync.Map
// does not define an iteration order.
//
// Search is a glob matched against keys. Sort, Limit, Offset, and Fields are
// applied to the decoded documents after matching.
func (s *Memory) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		SuccessCounter: s.GetCounterListed(),
		FailureCounter: s.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err == nil {
				params.Search = "*"
			}

			return params, err
		},
	}, func(_ context.Context, params *list.List) (envelope.Result[envelope.Void], error) {
		pattern := params.Search
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
			return envelope.Result[envelope.Void]{}, matchErr
		}

		var err error

		if prm == nil {
			params = nil
		}

		docs, err = shared.ApplyListParams(docs, params)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		if err := shared.Unmarshal(shared.AssembleItems(docs), v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (s *Memory) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterCreated(),
		FailureCounter: s.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err == nil {
				params.TTL = 0
			}

			return params, err
		},
	}, func(_ context.Context, _ *create.Create) (envelope.Result[string], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[string]{}, err
		}

		s.client.Store(id, b)

		return envelope.Result[string]{Value: id, PostHookData: v}, nil
	})
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
	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterUpdated(),
		FailureCounter: s.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err == nil {
				params.TTL = 0
			}

			return params, err
		},
	}, func(_ context.Context, _ *update.Update) (envelope.Result[envelope.Void], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		s.client.Store(id, b)

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
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
