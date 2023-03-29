package redis

import (
	"context"
	"errors"
	"net/http"

	"github.com/redis/go-redis/v9"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/shared"
	"github.com/thalesfsp/dal/storage"
	"github.com/thalesfsp/dal/validation"
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

const name = "redis"

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
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, err
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
		if err := o.PreHookFunc(ctx, r, "", target, finalParam); err != nil {
			return 0, err
		}
	}

	// retrieve keys matching a pattern
	keys, err := r.Client.Keys(ctx, finalParam.Search).Result()
	if err != nil {
		return 0, customerror.NewFailedToError(storage.OperationCount, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, "", target, finalParam); err != nil {
			return 0, err
		}
	}

	//////
	// Metrics.
	//////

	r.GetCounterCount().Add(1)

	return int64(len(keys)), nil
}

// Delete removes data.
func (r *Redis) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return err
	}

	// Application's default values.
	if prm != nil {
		finalParam = prm
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, finalParam); err != nil {
			return err
		}
	}

	if err := r.Client.Del(ctx, id).Err(); err != nil {
		return customerror.NewFailedToError(storage.OperationDelete, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	r.GetCounterDelete().Add(1)

	return nil
}

// Get data.
func (r *Redis) Get(ctx context.Context, id, target string, v any, prm *get.Get, options ...storage.Func[*get.Get]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*get.Get]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := get.New()
	if err != nil {
		return err
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Get.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, finalParam); err != nil {
			return err
		}
	}

	objStr, err := r.Client.Get(ctx, id).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return customerror.NewHTTPError(http.StatusNotFound)
		}

		return customerror.NewFailedToError(storage.OperationRetrieve, customerror.WithError(err))
	}

	if objStr != "" {
		b := []byte(objStr)
		if err := shared.Unmarshal(b, v); err != nil {
			return err
		}
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	r.GetCounterGet().Add(1)

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
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return err
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
		if err := o.PreHookFunc(ctx, r, "", target, finalParam); err != nil {
			return err
		}
	}

	iter := r.Client.Scan(ctx, 0, finalParam.Search, 0).Iterator()

	keys := ResponseListKeys{[]string{}}

	for iter.Next(ctx) {
		keys.Keys = append(keys.Keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return customerror.NewFailedToError(storage.OperationList, customerror.WithError(err))
	}

	// Unmarshal.
	if err := storage.ParseToStruct(keys, v); err != nil {
		return err
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, "", target, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	r.GetCounterList().Add(1)

	return nil
}

// Set data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (r *Redis) Set(ctx context.Context, id, target string, v any, prm *set.Set, options ...storage.Func[*set.Set]) (string, error) {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*set.Set]()
	if err != nil {
		return "", err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := set.New()
	if err != nil {
		return "", err
	}

	finalParam.TTL = 0

	if prm != nil {
		finalParam = prm
	}

	//////
	// Set.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, finalParam); err != nil {
			return "", err
		}
	}

	b, err := shared.Marshal(v)
	if err != nil {
		return "", err
	}

	if err := r.Client.Set(ctx, id, b, finalParam.TTL).Err(); err != nil {
		return "", customerror.NewFailedToError(storage.OperationCreate, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, finalParam); err != nil {
			return "", err
		}
	}

	//////
	// Metrics.
	//////

	r.GetCounterSet().Add(1)

	return id, nil
}

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (r *Redis) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return err
	}

	finalParam.TTL = 0

	if prm != nil {
		finalParam = prm
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, r, id, target, finalParam); err != nil {
			return err
		}
	}

	b, err := shared.Marshal(v)
	if err != nil {
		return err
	}

	if err := r.Client.Set(ctx, id, b, finalParam.TTL).Err(); err != nil {
		return customerror.NewFailedToError(storage.OperationUpdate, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, r, id, target, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	r.GetCounterUpdate().Add(1)

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

	s, err := storage.New(name)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(cfg)

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, customerror.NewFailedToError("ping", customerror.WithError(err))
	}

	storage := &Redis{
		Storage: s,

		Client: client,
		Config: cfg,
	}

	if err := validation.Validate(storage); err != nil {
		return nil, err
	}

	return storage, nil
}
