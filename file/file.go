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
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	// fs.Glob on a nonexistent directory silently yields zero matches — a
	// missing target must be an error, not a plausible-looking count of 0.
	if _, err := os.Stat(trgt); err != nil {
		return 0, customapm.TraceError(ctx, customerror.NewFailedToError(
			storage.OperationCount.String(),
			customerror.WithError(err),
		), s.GetLogger(), s.GetCounterCountedFailed())
	}

	dir := os.DirFS(trgt)

	matches, err := fs.Glob(dir, finalParam.Search)
	if err != nil {
		return 0, customapm.TraceError(ctx, customerror.NewFailedToError(
			storage.OperationCount.String(),
			customerror.WithError(err),
		), s.GetLogger(), s.GetCounterCountedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, "", target, int64(len(matches)), finalParam); err != nil {
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

	return int64(len(matches)), nil
}

// Delete removes data.
func (s *File) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
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
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
		}
	}

	// Delete a file in the dir. A missing file is treated as an idempotent,
	// successful delete — but it must still run the post-hook, logging, and
	// success metric below, so don't return early.
	if err := os.Remove(trgt); err != nil && !errors.Is(err, os.ErrNotExist) { //nolint:gosec // The target is the caller-selected full path; this storage has no trusted root to traverse out of.
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterDeletedFailed())
	}

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
func (s *File) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
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
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	file, err := os.Open(trgt) //nolint:gosec // The target is the caller-selected full path; this storage has no trusted root to traverse out of.
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return customapm.TraceError(
				ctx,
				customerror.NewNotFoundError(storage.OperationRetrieve.String(),
					customerror.WithError(err),
				),
				s.GetLogger(),
				s.GetCounterRetrievedFailed(),
			)
		}

		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}
	defer file.Close()

	if err := shared.Decode(file, v); err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
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

// List data.
//
// NOTE: It uses params.List.Search to query the data.
//
// NOTE: File does not support the concept of "offset" and "limit" in the same
// way that a traditional SQL database does.
func (s *File) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
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
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	// Same rationale as Count: a missing directory must surface as an error
	// instead of an empty-but-plausible listing.
	if _, err := os.Stat(trgt); err != nil {
		return customapm.TraceError(ctx, customerror.NewFailedToError(
			storage.OperationList.String(),
			customerror.WithError(err),
		), s.GetLogger(), s.GetCounterListedFailed())
	}

	dir := os.DirFS(trgt)

	matches, err := fs.Glob(dir, finalParam.Search)
	if err != nil {
		return customapm.TraceError(ctx, customerror.NewFailedToError(
			storage.OperationList.String(),
			customerror.WithError(err),
		), s.GetLogger(), s.GetCounterListedFailed())
	}

	docs := make([][]byte, 0, len(matches))

	for _, match := range matches {
		doc, err := os.ReadFile(filepath.Join(trgt, match))
		if err != nil {
			return customapm.TraceError(ctx, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			), s.GetLogger(), s.GetCounterListedFailed())
		}

		docs = append(docs, doc)
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
//
//nolint:nestif,gocognit
func (s *File) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
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
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	// Check if finalParam.Any is type of CreateAny.
	if cA, ok := finalParam.Any.(*CreateAny); ok {
		if cA.CreateIfNotExist {
			// Extract directory from the resolved target which contains the
			// full file path.
			dir := filepath.Dir(trgt)

			// Check if the directory exists.
			if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) { //nolint:gosec // dir is derived from the caller-selected full target path, not a path beneath a trusted root.
				// Create the directory if it doesn't exist.
				if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // dir is derived from the caller-selected full target path, not a path beneath a trusted root.
					return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
				}
			}

			// Check if the file already exists.
			if _, err := os.Stat(trgt); errors.Is(err, fs.ErrNotExist) { //nolint:gosec // The target is the caller-selected full path; this storage has no trusted root to traverse out of.
				// Create the file if it doesn't exist, closing the handle so
				// it doesn't leak.
				f, err := os.Create(trgt) //nolint:gosec // The target is the caller-selected full path; this storage has no trusted root to traverse out of.
				if err != nil {
					return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
				}

				if err := f.Close(); err != nil {
					return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
				}
			}
		}
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}
	}

	file, err := os.Create(trgt) //nolint:gosec // The target is the caller-selected full path; this storage has no trusted root to traverse out of.
	if err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterCreatedFailed(),
		)
	}
	if err := shared.Encode(file, v); err != nil {
		file.Close()

		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	// A failed close means the data may not have hit the disk — surface it.
	if err := file.Close(); err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

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

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (s *File) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
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
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}
	}

	file, err := os.Create(trgt) //nolint:gosec // The target is the caller-selected full path; this storage has no trusted root to traverse out of.
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterUpdatedFailed(),
		)
	}
	if err := shared.Encode(file, v); err != nil {
		file.Close()

		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	// A failed close means the data may not have hit the disk — surface it.
	if err := file.Close(); err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

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
