package redis

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/redis/go-redis/v9"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v2/internal/customapm"
	"github.com/thalesfsp/dal/v2/internal/logging"
	"github.com/thalesfsp/dal/v2/internal/shared"
	"github.com/thalesfsp/dal/v2/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/fields"
	"github.com/thalesfsp/sypl/level"
	"github.com/thalesfsp/validation"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "redis"

// Singleton.
var singleton storage.IStorage

// Config is the Redis configuration.
type Config = redis.Options

// Redis storage definition.
type Redis struct {
	*storage.Storage

	// Client is the Redis client.
	Client *redis.Client `json:"-" validate:"required"`

	// Config is the Redis configuration.
	Config *Config `json:"-"`

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
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		r.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCountedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCountedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCountedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	if prm != nil {
		finalParam = prm
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, "", target, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCountedFailed())
		}
	}

	// retrieve keys matching a pattern
	keys, err := r.Client.Keys(ctx, finalParam.Search).Result()
	if err != nil {
		return 0, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			),
			r.GetLogger(),
			r.GetCounterCountedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, "", target, int64(len(keys)), finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	r.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	r.GetCounterCounted().Add(1)

	return int64(len(keys)), nil
}

// Delete removes data.
func (r *Redis) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			r.GetLogger(),
			r.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		r.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterDeletedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterDeletedFailed())
	}

	// Application's default values.
	if prm != nil {
		finalParam = prm
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterDeletedFailed())
		}
	}

	if err := r.Client.Del(ctx, id).Err(); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			),
			r.GetLogger(),
			r.GetCounterDeletedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	r.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	r.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (r *Redis) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			r.GetLogger(),
			r.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		r.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterRetrievedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterRetrievedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterRetrievedFailed())
		}
	}

	objStr, err := r.Client.Get(ctx, id).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return customapm.TraceError(
				ctx,
				customerror.NewHTTPError(http.StatusNotFound),
				r.GetLogger(),
				r.GetCounterRetrievedFailed(),
			)
		}

		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(),
				customerror.WithError(err),
			),
			r.GetLogger(),
			r.GetCounterRetrievedFailed())
	}

	if objStr != "" {
		b := []byte(objStr)
		if err := shared.Unmarshal(b, v); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterRetrievedFailed())
		}
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	r.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	r.GetCounterRetrieved().Add(1)

	return nil
}

// List data.
//
// NOTE: It uses params.List.Search to query the data.
//
// NOTE: Redis does not support the concept of "offset" and "limit" in the same
// way that a traditional SQL database does.
func (r *Redis) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		r.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterListedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterListedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterListedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	if prm != nil {
		finalParam = prm
	}

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterListedFailed())
		}
	}

	iter := r.Client.Scan(ctx, 0, finalParam.Search, 0).Iterator()

	keys := ResponseListKeys{[]string{}}

	for iter.Next(ctx) {
		keys.Keys = append(keys.Keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationList.String(),
				customerror.WithError(err),
			),
			r.GetLogger(), r.GetCounterListedFailed(),
		)
	}

	if err := storage.ParseToStruct(keys, v); err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterListedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	r.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	r.GetCounterListed().Add(1)

	return nil
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (r *Redis) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		r.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCreatedFailed())
	}

	finalParam.TTL = 0

	if prm != nil {
		finalParam = prm
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCreatedFailed())
		}
	}

	b, err := shared.Marshal(v)
	if err != nil {
		return "", customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCreatedFailed())
	}

	if err := r.Client.Set(ctx, id, b, finalParam.TTL).Err(); err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			),
			r.GetLogger(),
			r.GetCounterCreatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterCreatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	r.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	r.GetCounterCreated().Add(1)

	return id, nil
}

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (r *Redis) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			r.GetLogger(),
			r.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		r.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterUpdatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterUpdatedFailed())
	}

	finalParam.TTL = 0

	if prm != nil {
		finalParam = prm
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterUpdatedFailed())
		}
	}

	b, err := shared.Marshal(v)
	if err != nil {
		return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterUpdatedFailed())
	}

	if err := r.Client.Set(ctx, id, b, finalParam.TTL).Err(); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(),
				customerror.WithError(err),
			),
			r.GetLogger(),
			r.GetCounterCountedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, r.GetLogger(), r.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	r.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	r.GetCounterUpdated().Add(1)

	return nil
}

// GetClient returns the client.
func (r *Redis) GetClient() any {
	return r.Client
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

	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	if err := r.Run(func() error {
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

	singleton = storage

	return storage, nil
}

//////
// Exported functionalities.
//////

// Get returns a setup storage, or set it up.
func Get() storage.IStorage {
	if singleton == nil {
		panic(fmt.Sprintf("%s %s not %s", Name, storage.Type, status.Initialized))
	}

	return singleton
}

// Set sets the storage, primarily used for testing.
func Set(s storage.IStorage) {
	singleton = s
}
