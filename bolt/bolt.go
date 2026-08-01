package bolt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"sync"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/envelope"
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
	bbolt "go.etcd.io/bbolt"
	bbolterrors "go.etcd.io/bbolt/errors"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "bolt"

// defaultFileMode is the permission bits used when bbolt creates the
// database file. It isn't user-configurable: the file holds every target's
// data, so it must never be group/world readable regardless of `Options`.
const defaultFileMode = 0o600

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// errKeyNotFound distinguishes "the bucket exists but the key doesn't" from
// bbolt errors, so Retrieve can map both that and a missing bucket to the
// same 404 without depending on error string matching.
var errKeyNotFound = errors.New("key not found")

// Config is the bbolt configuration Client is opened with.
type Config struct {
	// Path is the file path where the bbolt database is stored on disk. Its
	// parent directory must already exist — bbolt does not create it.
	Path string `json:"path" validate:"required"`

	// Options are the driver options bbolt opens the database file with
	// (open timeout, read-only mode, etc). Nil applies bbolt's own defaults.
	//
	// It's skipped entirely by validation ("-"): it's third-party
	// configuration data, not something this package's rules apply to.
	Options *bbolt.Options `json:"-" validate:"-"`
}

// Bolt storage definition.
type Bolt struct {
	*storage.Storage

	closeOnce sync.Once

	// Client is the live bbolt handle. It's tagged `structonly` so
	// validation confirms it's non-nil without descending into it: bbolt
	// runs a background freelist/mmap and its own internal locking, and
	// reflectively walking a live driver's fields while it mutates them is a
	// data race, not just wasted work.
	Client *bbolt.DB `json:"-" validate:"required,structonly"`

	// Config is the configuration Client was opened with.
	Config *Config `json:"-" validate:"required"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For Bolt, a target maps to a bbolt bucket.
	Target string `json:"-" validate:"omitempty,gt=0"`
}

//////
// Helpers.
//////

// bucketMatches reports whether `key` matches the glob `pattern`. A
// malformed pattern returns an error so callers can distinguish "no match"
// from "the pattern itself is broken" — mirrors `memory.keyMatches`.
func bucketMatches(pattern string, key []byte) (bool, error) {
	matched, err := path.Match(pattern, string(key))
	if err != nil {
		return false, customerror.NewInvalidError(
			"search pattern",
			customerror.WithError(err),
		)
	}

	return matched, nil
}

// collectBucketItems walks `b` in key order (bbolt's cursor order), collecting
// the JSON value of every key matching `pattern`. Values are cloned because
// bbolt's byte slices are only valid for the lifetime of the transaction.
func collectBucketItems(b *bbolt.Bucket, pattern string, docs *[][]byte) error {
	c := b.Cursor()

	for k, val := c.First(); k != nil; k, val = c.Next() {
		matched, err := bucketMatches(pattern, k)
		if err != nil {
			return err
		}

		if !matched {
			continue
		}

		*docs = append(*docs, bytes.Clone(val))
	}

	return nil
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (s *Bolt) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
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
		pattern := params.Search
		if pattern == "" {
			pattern = "*"
		}

		var (
			n        int
			matchErr error
		)

		// A missing bucket must not look like a plausible count of 0 — it means
		// the target was never written to, which is an error, not an empty
		// result.
		if viewErr := s.Client.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(trgt))
			if b == nil {
				return bbolterrors.ErrBucketNotFound
			}

			return b.ForEach(func(k, _ []byte) error {
				matched, err := bucketMatches(pattern, k)
				if err != nil {
					matchErr = err

					return err
				}

				if matched {
					n++
				}

				return nil
			})
		}); viewErr != nil {
			if matchErr != nil {
				return envelope.Result[int64]{}, matchErr
			}

			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(viewErr),
			)
		}

		result := int64(n)

		return envelope.Result[int64]{Value: result, PostHookData: result}, nil
	})
}

// Delete removes data.
func (s *Bolt) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
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
		if err := s.Client.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(trgt))
			if b == nil {
				// Nothing was ever created under this target, so there's
				// nothing to delete — an idempotent success, consistent with
				// how `file.Delete` and `sftp.Delete` treat a missing file.
				return nil
			}

			return b.Delete([]byte(id))
		}); err != nil {
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
func (s *Bolt) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
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
		// The value bytes bbolt hands back are only valid for the life of the
		// transaction, so decode into `v` from inside the closure instead of
		// copying them out.
		viewErr := s.Client.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(trgt))
			if b == nil {
				return errKeyNotFound
			}

			raw := b.Get([]byte(id))
			if raw == nil {
				return errKeyNotFound
			}

			return shared.Unmarshal(raw, v)
		})

		if viewErr != nil {
			if errors.Is(viewErr, errKeyNotFound) {
				return envelope.Result[envelope.Void]{}, customerror.NewNotFoundError(
					storage.OperationRetrieve.String(),
					customerror.WithError(viewErr),
				)
			}

			return envelope.Result[envelope.Void]{}, viewErr
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// NOTE: It uses params.List.Search (glob) to filter keys.
func (s *Bolt) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
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
		pattern := params.Search
		if pattern == "" {
			pattern = "*"
		}

		var docs [][]byte

		// Same rationale as Count: a missing bucket must surface as an error
		// instead of an empty-but-plausible listing.
		if viewErr := s.Client.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(trgt))
			if b == nil {
				return bbolterrors.ErrBucketNotFound
			}

			return collectBucketItems(b, pattern, &docs)
		}); viewErr != nil {
			// A malformed Search pattern is already a properly typed
			// customerror courtesy of bucketMatches — propagate it as-is rather
			// than re-wrapping it as a generic "failed to list".
			if errors.Is(viewErr, bbolterrors.ErrBucketNotFound) {
				return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
					storage.OperationList.String(),
					customerror.WithError(viewErr),
				)
			}

			return envelope.Result[envelope.Void]{}, viewErr
		}

		if prm == nil {
			params = nil
		}

		docs, err := shared.ApplyListParams(docs, params)
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

type boltBulkPrepared struct {
	index   int
	item    storage.BulkItem
	target  string
	value   []byte
	params  *create.Create
	options *storage.Options[*create.Create]
}

type boltBulkOutcome struct {
	succeeded bool
	err       error
}

func (s *Bolt) prepareBulkCreateItem(
	ctx context.Context,
	target string,
	indexed storage.Indexed[storage.BulkItem],
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (boltBulkPrepared, error) {
	item := indexed.Item
	if item.ID == "" {
		return boltBulkPrepared{}, customerror.NewRequiredError("id")
	}

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return boltBulkPrepared{}, err
	}

	for _, option := range options {
		if err := option(o); err != nil {
			return boltBulkPrepared{}, err
		}
	}

	finalParam, err := create.New()
	if err != nil {
		return boltBulkPrepared{}, err
	}

	finalParam.TTL = 0
	finalParam = shared.MergeParams(finalParam, prm)

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return boltBulkPrepared{}, err
	}

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, item.ID, target, item.Value, finalParam); err != nil {
			return boltBulkPrepared{}, err
		}
	}

	value, err := shared.Marshal(item.Value)
	if err != nil {
		return boltBulkPrepared{}, err
	}

	return boltBulkPrepared{
		index:   indexed.Index,
		item:    item,
		target:  trgt,
		value:   value,
		params:  finalParam,
		options: o,
	}, nil
}

func (s *Bolt) writeBulkCreateTransaction(prepared []boltBulkPrepared) error {
	return s.Client.Update(func(tx *bbolt.Tx) error {
		for _, entry := range prepared {
			bucket, err := tx.CreateBucketIfNotExists([]byte(entry.target))
			if err != nil {
				return err
			}

			if err := bucket.Put([]byte(entry.item.ID), entry.value); err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *Bolt) finishBulkCreateItem(ctx context.Context, target string, entry boltBulkPrepared) error {
	if entry.options.PostHookFunc != nil {
		if err := entry.options.PostHookFunc(
			ctx,
			s,
			entry.item.ID,
			target,
			entry.item.Value,
			entry.params,
		); err != nil {
			return err
		}
	}

	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)
	s.GetCounterCreated().Add(1)

	return nil
}

func (s *Bolt) commitBulkCreateChunk(
	ctx context.Context,
	target string,
	prepared []boltBulkPrepared,
	outcomes []boltBulkOutcome,
) {
	if len(prepared) == 0 {
		return
	}

	if err := s.writeBulkCreateTransaction(prepared); err != nil {
		for _, entry := range prepared {
			outcomes[entry.index].err = customapm.TraceError(
				ctx,
				customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
				s.GetLogger(),
				s.GetCounterCreatedFailed(),
			)
		}

		return
	}

	for _, entry := range prepared {
		if err := s.finishBulkCreateItem(ctx, target, entry); err != nil {
			outcomes[entry.index].err = customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())

			continue
		}

		outcomes[entry.index].succeeded = true
	}
}

func markBoltBulkFailures(
	chunk []storage.Indexed[storage.BulkItem],
	chunks [][]storage.Indexed[storage.BulkItem],
	err error,
	outcomes []boltBulkOutcome,
) {
	for _, indexed := range chunk {
		outcomes[indexed.Index].err = err
	}

	for _, remainingChunk := range chunks {
		for _, indexed := range remainingChunk {
			outcomes[indexed.Index].err = err
		}
	}
}

func assembleBoltBulkResult(
	items []storage.BulkItem,
	outcomes []boltBulkOutcome,
	stopped bool,
) (storage.BulkResult, error) {
	result := storage.BulkResult{
		Succeeded: make([]int, 0, len(items)),
		Stopped:   stopped,
	}

	for index, outcome := range outcomes {
		if outcome.succeeded {
			result.Succeeded = append(result.Succeeded, index)

			continue
		}

		result.Failures = append(result.Failures, storage.BulkItemError{Index: index, Err: outcome.err})
	}

	return result, result.Err()
}

// BulkCreate creates data in transactions of at most 25 writes. Each
// transaction is committed independently, so the overall operation remains
// deliberately non-atomic while avoiding a transaction per item.
func (s *Bolt) BulkCreate(
	ctx context.Context,
	target string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	const transactionBatchSize = 25

	if len(items) == 0 {
		return storage.BulkResult{}, nil
	}

	chunks, err := storage.ChunkIndexed(items, transactionBatchSize)
	if err != nil {
		return storage.BulkResult{}, err
	}

	outcomes := make([]boltBulkOutcome, len(items))
	stopped := false

	for chunkIndex, chunk := range chunks {
		prepared := make([]boltBulkPrepared, 0, len(chunk))
		stoppedAt := len(chunk)
		var stopErr error

		for itemIndex, indexed := range chunk {
			if err := ctx.Err(); err != nil {
				stopped = true
				stoppedAt = itemIndex
				stopErr = err

				break
			}

			entry, err := s.prepareBulkCreateItem(ctx, target, indexed, prm, options...)
			if err != nil {
				outcomes[indexed.Index].err = customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())

				continue
			}

			prepared = append(prepared, entry)
		}

		s.commitBulkCreateChunk(ctx, target, prepared, outcomes)

		if stopErr == nil {
			stopErr = ctx.Err()
			if stopErr != nil && chunkIndex+1 < len(chunks) {
				stopped = true
			}
		}

		if stopped {
			markBoltBulkFailures(chunk[stoppedAt:], chunks[chunkIndex+1:], stopErr, outcomes)

			break
		}
	}

	return assembleBoltBulkResult(items, outcomes, stopped)
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
//
// NOTE: bbolt has no per-key expiry mechanism, so `finalParam.TTL` is
// accepted for interface parity but has no effect — same as `memory`.
func (s *Bolt) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	var trgt string

	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        options,
		RequiresID:     true,
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

			return params, err
		},
	}, func(_ context.Context, _ *create.Create) (envelope.Result[string], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[string]{}, err
		}

		// The bucket is the target — create it lazily on first write instead of
		// requiring a separate provisioning step.
		if err := s.Client.Update(func(tx *bbolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(trgt))
			if err != nil {
				return err
			}

			return bucket.Put([]byte(id), b)
		}); err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[string]{Value: id, PostHookData: v}, nil
	})
}

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (s *Bolt) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
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
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		// Same as Create: an "update" is an upsert here, so the bucket is
		// created lazily if this is the first write to the target.
		if err := s.Client.Update(func(tx *bbolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(trgt))
			if err != nil {
				return err
			}

			return bucket.Put([]byte(id), b)
		}); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// GetClient returns the client.
func (s *Bolt) GetClient() any {
	return s.Client
}

//////
// Factory.
//////

// New creates a new Bolt storage. `path` is the file the database is stored
// in — its parent directory must already exist, bbolt won't create it.
// `opts` may be nil to use bbolt's defaults.
func New(ctx context.Context, path string, opts *bbolt.Options) (*Bolt, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*Bolt)(nil)

	//////
	// Storage.
	//////

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Path:    path,
		Options: opts,
	}

	client, err := bbolt.Open(cfg.Path, defaultFileMode, cfg.Options)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("open", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	// The client is open but not yet handed to the caller. Close it on
	// every error path below so the file handle and its flock are never
	// leaked.
	success := false

	defer func() {
		if !success {
			_ = client.Close()
		}
	}()

	//////
	// Validation.
	//////

	storage := &Bolt{
		Storage: s,

		Client: client,
		Config: cfg,
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

	success = true

	return storage, nil
}

// Close releases the bbolt handle and the exclusive file lock it holds on
// the database file for as long as it's open. Leaking it blocks every future
// Open() against the same path — worse than leaking a socket, since it
// doesn't get cleaned up by the OS closing an fd on process exit if the
// process itself keeps running.
func (s *Bolt) Close() error {
	var closeErr error

	s.closeOnce.Do(func() {
		if s.Client == nil {
			return
		}

		if err := s.Client.Close(); err != nil {
			closeErr = customerror.NewFailedToError("close", customerror.WithError(err))
		}
	})

	return closeErr
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
