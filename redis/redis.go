package redis

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/redis/go-redis/v9"
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
const Name = "redis"

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// Config is the Redis configuration.
type Config = redis.Options

// Redis storage definition.
type Redis struct {
	*storage.Storage

	// Client is the Redis client.
	Client *redis.Client `json:"-" validate:"required,structonly"`

	closeOnce sync.Once

	// Config is the Redis configuration.
	// Skipped by the validator: it dives into untagged nested structs, and
	// this config holds live client state that background goroutines write.
	Config *Config `json:"-" validate:"-"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For Redis, it isn't used at all.
	Target string `json:"-" validate:"omitempty,gt=0"`
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (r *Redis) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        r,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         target,
		Param:          prm,
		Options:        options,
		SuccessCounter: r.GetCounterCounted(),
		FailureCounter: r.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err == nil {
				params.Search = "*"
			}

			return params, err
		},
	}, func(ctx context.Context, params *count.Count) (envelope.Result[int64], error) {
		pattern := params.Search
		if pattern == "" {
			pattern = "*"
		}

		var result int64

		iter := r.Client.Scan(ctx, 0, pattern, 0).Iterator()

		for iter.Next(ctx) {
			result++
		}

		if err := iter.Err(); err != nil {
			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[int64]{Value: result, PostHookData: result}, nil
	})
}

// Delete removes data.
func (r *Redis) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        r,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         target,
		Param:          prm,
		Options:        options,
		NewParams:      delete.New,
		RequiresID:     true,
		SuccessCounter: r.GetCounterDeleted(),
		FailureCounter: r.GetCounterDeletedFailed(),
	}, func(ctx context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		if err := r.Client.Del(ctx, id).Err(); err != nil {
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
func (r *Redis) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        r,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		NewParams:      retrieve.New,
		RequiresID:     true,
		SuccessCounter: r.GetCounterRetrieved(),
		FailureCounter: r.GetCounterRetrievedFailed(),
	}, func(ctx context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		objStr, err := r.Client.Get(ctx, id).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return envelope.Result[envelope.Void]{}, customerror.NewHTTPError(http.StatusNotFound)
			}

			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}

		if len(objStr) == 0 {
			return envelope.Result[envelope.Void]{}, customerror.NewHTTPError(http.StatusNotFound)
		}

		if err := shared.Unmarshal([]byte(objStr), v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// Search is a glob passed to SCAN. Sort, Limit, Offset, and Fields are applied
// to the decoded values after matching.
func (r *Redis) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        r,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		SuccessCounter: r.GetCounterListed(),
		FailureCounter: r.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err == nil {
				params.Search = "*"
			}

			return params, err
		},
	}, func(ctx context.Context, params *list.List) (envelope.Result[envelope.Void], error) {
		iter := r.Client.Scan(ctx, 0, params.Search, 0).Iterator()

		var docs [][]byte

		for iter.Next(ctx) {
			doc, err := r.Client.Get(ctx, iter.Val()).Bytes()
			if err != nil {
				return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
					storage.OperationList.String(),
					customerror.WithError(err),
				)
			}

			docs = append(docs, doc)
		}

		if err := iter.Err(); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			)
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

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (r *Redis) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        r,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: r.GetCounterCreated(),
		FailureCounter: r.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err == nil {
				params.TTL = 0
			}

			return params, err
		},
	}, func(ctx context.Context, params *create.Create) (envelope.Result[string], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[string]{}, err
		}

		if err := r.Client.Set(ctx, id, b, params.TTL).Err(); err != nil {
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
func (r *Redis) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        r,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: r.GetCounterUpdated(),
		FailureCounter: r.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err == nil {
				params.TTL = 0
			}

			return params, err
		},
	}, func(ctx context.Context, params *update.Update) (envelope.Result[envelope.Void], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		if err := r.Client.Set(ctx, id, b, params.TTL).Err(); err != nil {
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
func (r *Redis) GetClient() any {
	return r.Client
}

// Close releases the resources held by the storage.
func (r *Redis) Close() error {
	var closeErr error

	r.closeOnce.Do(func() {
		closeErr = r.Client.Close()
	})

	return closeErr
}

//////
// Factory.
//////

// New creates a new Redis storage.
func New(ctx context.Context, cfg *Config) (*Redis, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*Redis)(nil)

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(cfg)

	// Close the client if New fails past this point, otherwise its
	// connection pool leaks.
	success := false

	defer func() {
		if !success {
			client.Close()
		}
	}()

	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	if err := r.RunCtx(ctx, func(ctx context.Context) error {
		if _, err := client.Ping(ctx).Result(); err != nil {
			cE := customerror.NewFailedToError(
				"ping",
				customerror.WithError(err),
				customerror.WithTag("retry"),
			)

			s.GetLogger().Errorln(cE)

			return cE
		}

		return nil
	}); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterPingFailed())
	}

	storage := &Redis{
		Storage: s,

		Client: client,
		Config: cfg,
	}

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), nil)
	}

	success = true

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
