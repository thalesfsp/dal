package file

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
const Name = "file"

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// File storage definition.
type File struct {
	*storage.Storage

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For File, it isn't used at all.
	Target string `json:"-" validate:"omitempty,gt=0"`
}

//////
// Implements the IStorage interface.
//////

// Close releases the resources held by the storage. File opens and closes
// descriptors per operation, so Close is a no-op.
func (s *File) Close() error {
	return nil
}

// Count returns the number of items in the storage.
func (s *File) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	var trgt string

	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         target,
		Options:        options,
		SuccessCounter: s.GetCounterCounted(),
		FailureCounter: s.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err != nil {
				return nil, err
			}

			params.Search = "*"
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, params *count.Count) (envelope.Result[int64], error) {
		// fs.Glob on a nonexistent directory silently yields zero matches — a
		// missing target must be an error, not a plausible-looking count of 0.
		if _, err := os.Stat(trgt); err != nil {
			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			)
		}

		dir := os.DirFS(trgt)

		matches, err := fs.Glob(dir, params.Search)
		if err != nil {
			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			)
		}

		result := int64(len(matches))

		return envelope.Result[int64]{Value: result, PostHookData: result}, nil
	})
}

// Delete removes data.
func (s *File) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	var trgt string

	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         target,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterDeleted(),
		FailureCounter: s.GetCounterDeletedFailed(),
		NewParams: func() (*delete.Delete, error) {
			params, err := delete.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		// Delete a file in the dir. A missing file is treated as an idempotent,
		// successful delete — but it must still run the post-hook, logging, and
		// success metric below, so don't return early.
		if err := os.Remove(trgt); err != nil && !errors.Is(err, os.ErrNotExist) {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{}, nil
	})

	return err
}

// Retrieve data.
func (s *File) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	var trgt string

	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterRetrieved(),
		FailureCounter: s.GetCounterRetrievedFailed(),
		NewParams: func() (*retrieve.Retrieve, error) {
			params, err := retrieve.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		file, err := os.Open(trgt)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return envelope.Result[envelope.Void]{}, customerror.NewNotFoundError(
					storage.OperationRetrieve.String(),
					customerror.WithError(err),
				)
			}

			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}
		defer file.Close()

		if err := shared.Decode(file, v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// Search is a glob matched against filenames. Sort, Limit, Offset, and Fields
// are applied to the decoded documents after matching.
func (s *File) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	var trgt string

	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         target,
		HookData:       v,
		Options:        options,
		SuccessCounter: s.GetCounterListed(),
		FailureCounter: s.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err != nil {
				return nil, err
			}

			params.Search = "*"
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, params *list.List) (envelope.Result[envelope.Void], error) {
		// Same rationale as Count: a missing directory must surface as an error
		// instead of an empty-but-plausible listing.
		if _, err := os.Stat(trgt); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			)
		}

		dir := os.DirFS(trgt)

		matches, err := fs.Glob(dir, params.Search)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			)
		}

		docs := make([][]byte, 0, len(matches))

		for _, match := range matches {
			doc, err := os.ReadFile(filepath.Join(trgt, match))
			if err != nil {
				return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
					storage.OperationList.String(),
					customerror.WithError(err),
				)
			}

			docs = append(docs, doc)
		}

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

// BulkCreate creates data in chunks of 25. File has no batch primitive, so
// each item's ID is used as its file path and written with Create.
func (s *File) BulkCreate(
	ctx context.Context,
	_ string,
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

	result := storage.BulkResult{Succeeded: make([]int, 0, len(items))}

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

			if _, err := s.Create(ctx, indexed.Item.ID, indexed.Item.ID, indexed.Item.Value, prm, options...); err != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

				continue
			}

			result.Succeeded = append(result.Succeeded, indexed.Index)
		}
	}

	return result, result.Err()
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
//
//nolint:nestif,gocognit
func (s *File) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	var trgt string

	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        options,
		SuccessCounter: s.GetCounterCreated(),
		FailureCounter: s.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err != nil {
				return nil, err
			}

			params.TTL = 0
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)
			if err != nil {
				return nil, err
			}

			// Check if params.Any is type of CreateAny.
			if cA, ok := params.Any.(*CreateAny); ok && cA.CreateIfNotExist {
				// Extract directory from the resolved target which contains the
				// full file path.
				dir := filepath.Dir(trgt)

				// Check if the directory exists.
				if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
					// Create the directory if it doesn't exist.
					if err := os.MkdirAll(dir, 0o755); err != nil {
						return nil, err
					}
				}

				// Check if the file already exists.
				if _, err := os.Stat(trgt); errors.Is(err, fs.ErrNotExist) {
					// Create the file if it doesn't exist, closing the handle so
					// it doesn't leak.
					f, err := os.Create(trgt)
					if err != nil {
						return nil, err
					}

					if err := f.Close(); err != nil {
						return nil, err
					}
				}
			}

			return params, nil
		},
	}, func(_ context.Context, _ *create.Create) (envelope.Result[string], error) {
		file, err := os.Create(trgt)
		if err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}
		if err := shared.Encode(file, v); err != nil {
			file.Close()

			return envelope.Result[string]{}, err
		}

		// A failed close means the data may not have hit the disk — surface it.
		if err := file.Close(); err != nil {
			return envelope.Result[string]{}, err
		}

		return envelope.Result[string]{Value: id, PostHookData: v}, nil
	})
}

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (s *File) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	var trgt string

	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterUpdated(),
		FailureCounter: s.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err != nil {
				return nil, err
			}

			params.TTL = 0
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, _ *update.Update) (envelope.Result[envelope.Void], error) {
		file, err := os.Create(trgt)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}
		if err := shared.Encode(file, v); err != nil {
			file.Close()

			return envelope.Result[envelope.Void]{}, err
		}

		// A failed close means the data may not have hit the disk — surface it.
		if err := file.Close(); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// GetClient returns the client.
func (s *File) GetClient() any {
	return nil
}

//////
// Factory.
//////

// New creates a new File storage.
func New(ctx context.Context) (*File, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*File)(nil)

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

	storage := &File{
		Storage: s,
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
