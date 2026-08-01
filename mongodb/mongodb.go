package mongodb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/envelope"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/validation"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

//////
// Const, vars, and types.
//////

const (
	// Name of the storage.
	Name = "mongodb"

	idField = "_id"

	// Ten seconds matches the adapter's ping bound: it gives the driver ample
	// time to drain its pool while preventing a stuck topology shutdown from
	// blocking Close forever.
	mongoDisconnectTimeout = 10 * time.Second
)

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// Config is the MongoDB configuration.
type Config = options.ClientOptions

// MongoDB storage definition.
type MongoDB struct {
	*storage.Storage

	// Client is the MongoDB client.
	Client *mongo.Client `json:"-" validate:"required,structonly"`

	closeOnce sync.Once

	// Config is the MongoDB configuration.
	// Skipped by the validator: it dives into untagged nested structs, and
	// this config holds live client state that background goroutines write.
	Config *Config `json:"-" validate:"-"`

	// Database to connect to.
	Database string `json:"database" validate:"required"`

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
// Helpers.
//////

// ToMongoString converts the `Sort` to MongoDB sort format.
func ToMongoString(s customsort.Sort) (bson.D, error) {
	sortMap, err := s.ToMap()
	if err != nil {
		return nil, err
	}

	sortFields := bson.D{}

	for fieldName, sortOrder := range sortMap {
		sortOrderInt := 1

		if strings.ToLower(sortOrder) == customsort.Desc {
			sortOrderInt = -1
		}

		sortFields = append(sortFields, bson.E{Key: fieldName, Value: sortOrderInt})
	}

	return sortFields, nil
}

// decodeCursor iterates cursor into the slice pointed to by v. The destination
// is only updated after the cursor is exhausted successfully, so a getMore
// error cannot return a partial result as a successful list.
func decodeCursor(ctx context.Context, cursor *mongo.Cursor, v any) error {
	resultsValue := reflect.ValueOf(v)
	if resultsValue.Kind() != reflect.Pointer || resultsValue.IsNil() {
		return fmt.Errorf("results argument must be a non-nil pointer to a slice")
	}

	sliceValue := resultsValue.Elem()
	if sliceValue.Kind() != reflect.Slice {
		return fmt.Errorf("results argument must be a pointer to a slice, but was a pointer to %s", sliceValue.Kind())
	}

	elementType := sliceValue.Type().Elem()
	decoded := reflect.Zero(sliceValue.Type())

	for cursor.Next(ctx) {
		element := reflect.New(elementType)

		if err := cursor.Decode(element.Interface()); err != nil {
			return err
		}

		decoded = reflect.Append(decoded, element.Elem())
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	sliceValue.Set(decoded)

	return nil
}

// mongoEnvelopeOptions preserves MongoDB's option-default order and passes
// the collection name resolved inside Run to the user's hooks.
func mongoEnvelopeOptions[T any](
	defaultDatabase string,
	database, target *string,
	options []storage.Func[T],
) []storage.Func[T] {
	result := make([]storage.Func[T], 0, len(options)+2)
	result = append(result, storage.WithDatabase[T](defaultDatabase))
	result = append(result, options...)
	result = append(result, func(o *storage.Options[T]) error {
		*database = o.Database

		if preHook := o.PreHookFunc; preHook != nil {
			o.PreHookFunc = func(ctx context.Context, strg storage.IStorage, id, _ string, data any, param T) error {
				return preHook(ctx, strg, id, *target, data, param)
			}
		}

		if postHook := o.PostHookFunc; postHook != nil {
			o.PostHookFunc = func(ctx context.Context, strg storage.IStorage, id, _ string, data any, param T) error {
				return postHook(ctx, strg, id, *target, data, param)
			}
		}

		return nil
	})

	return result
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (m *MongoDB) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	var (
		database string
		trgt     string
		filter   bson.D
	)

	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        m,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         target,
		Options:        mongoEnvelopeOptions(m.Database, &database, &trgt, options),
		SuccessCounter: m.GetCounterCounted(),
		FailureCounter: m.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			filter = bson.D{}

			if params.Search != "" {
				filterMap := map[string]any{}
				if err := shared.Unmarshal([]byte(params.Search), &filterMap); err != nil {
					return nil, err
				}

				filterBSON, err := bson.Marshal(filterMap)
				if err != nil {
					return nil, customerror.NewFailedToError("bson marshal filter", customerror.WithError(err))
				}

				if err := bson.Unmarshal(filterBSON, &filter); err != nil {
					return nil, customerror.NewFailedToError("bson unmarshal filter", customerror.WithError(err))
				}
			}

			trgt, err = shared.TargetName(target, m.Target)
			if err != nil {
				return nil, err
			}

			return params, nil
		},
	}, func(ctx context.Context, _ *count.Count) (envelope.Result[int64], error) {
		result, err := m.Client.Database(database).Collection(trgt).CountDocuments(ctx, filter)
		if err != nil {
			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[int64]{Value: result, PostHookData: result}, nil
	})
}

// Delete removes data.
func (m *MongoDB) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	var database, trgt string

	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        m,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         target,
		Options:        mongoEnvelopeOptions(m.Database, &database, &trgt, options),
		RequiresID:     true,
		SuccessCounter: m.GetCounterDeleted(),
		FailureCounter: m.GetCounterDeletedFailed(),
		NewParams: func() (*delete.Delete, error) {
			params, err := delete.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, m.Target)

			return params, err
		},
	}, func(ctx context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		if _, err := m.Client.Database(database).Collection(trgt).DeleteOne(ctx, bson.M{idField: id}); err != nil {
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
func (m *MongoDB) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	var database, trgt string

	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        m,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        mongoEnvelopeOptions(m.Database, &database, &trgt, options),
		RequiresID:     true,
		SuccessCounter: m.GetCounterRetrieved(),
		FailureCounter: m.GetCounterRetrievedFailed(),
		NewParams: func() (*retrieve.Retrieve, error) {
			params, err := retrieve.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, m.Target)

			return params, err
		},
	}, func(ctx context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		var result bson.M

		if err := m.Client.Database(database).Collection(trgt).FindOne(ctx, bson.M{idField: id}).Decode(&result); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return envelope.Result[envelope.Void]{}, customerror.NewHTTPError(http.StatusNotFound)
			}

			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}

		resultBytes, err := bson.Marshal(result)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"bson marshal result",
				customerror.WithError(err),
			)
		}

		if err := bson.Unmarshal(resultBytes, v); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"bson unmarshal result",
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// WARN: In general, projections that include non-indexed fields or fields
// that are part of a covered index (i.e., an index that includes all the
// projected fields) are less likely to impact performance.
//
// Any accepts a bson.M filter. Sort, Limit, Offset, and Fields are translated
// into MongoDB find options; Search is not used.
func (m *MongoDB) List(ctx context.Context, target string, v any, prm *list.List, opts ...storage.Func[*list.List]) error {
	var (
		database   string
		trgt       string
		filter     bson.M
		cursorOpts *options.FindOptionsBuilder
	)

	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        m,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         target,
		HookData:       v,
		Options:        mongoEnvelopeOptions(m.Database, &database, &trgt, opts),
		SuccessCounter: m.GetCounterListed(),
		FailureCounter: m.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			cursorOpts = options.Find()

			if len(params.Fields) > 0 {
				projection := bson.M{}
				for _, field := range params.Fields {
					projection[field] = 1
				}

				cursorOpts.SetProjection(projection)
			}

			if len(params.Sort) > 0 {
				sortD, err := ToMongoString(params.Sort.ToSort())
				if err != nil {
					return nil, err
				}

				cursorOpts.SetSort(sortD)
			}

			if params.Offset >= 0 {
				cursorOpts.SetSkip(int64(params.Offset))
			}

			if params.Limit >= 0 {
				cursorOpts.SetLimit(int64(params.Limit))
			}

			if flt, ok := params.Any.(bson.M); ok {
				filter = flt
			}
			if filter == nil {
				filter = bson.M{}
			}

			trgt, err = shared.TargetName(target, m.Target)
			if err != nil {
				return nil, err
			}

			return params, nil
		},
	}, func(ctx context.Context, _ *list.List) (envelope.Result[envelope.Void], error) {
		cursor, err := m.Client.Database(database).Collection(trgt).Find(ctx, filter, cursorOpts)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			)
		}
		defer cursor.Close(ctx)

		if err := decodeCursor(ctx, cursor, v); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"cursor iteration",
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set.
//
// WARN: MongoDB relies on the model (`v`) `_id` field to be set, otherwise it
// will generate a new one. IT'S UP TO THE DEVELOPER TO SET THE `_ID` FIELD.
func (m *MongoDB) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	var database, trgt string

	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        m,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        mongoEnvelopeOptions(m.Database, &database, &trgt, options),
		RequiresID:     true,
		SuccessCounter: m.GetCounterCreated(),
		FailureCounter: m.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, m.Target)

			return params, err
		},
	}, func(ctx context.Context, _ *create.Create) (envelope.Result[string], error) {
		if _, err := m.Client.Database(database).Collection(trgt).InsertOne(ctx, v); err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[string]{Value: id, PostHookData: v}, nil
	})
}

// Update data.
func (m *MongoDB) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
	var database, trgt string

	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        m,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        mongoEnvelopeOptions(m.Database, &database, &trgt, opts),
		RequiresID:     true,
		SuccessCounter: m.GetCounterUpdated(),
		FailureCounter: m.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, m.Target)

			return params, err
		},
	}, func(ctx context.Context, _ *update.Update) (envelope.Result[envelope.Void], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		updateFields := make(map[string]any)
		if err := shared.Unmarshal(b, &updateFields); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		updateResult, err := m.Client.Database(database).Collection(trgt).UpdateOne(ctx, bson.M{idField: id}, bson.M{
			"$set": updateFields,
		})
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}

		if updateResult.MatchedCount == 0 && updateResult.UpsertedCount == 0 {
			return envelope.Result[envelope.Void]{}, customerror.NewHTTPError(http.StatusNotFound)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// GetClient returns the client.
func (m *MongoDB) GetClient() any {
	return m.Client
}

// Close releases the resources held by the storage.
func (m *MongoDB) Close() error {
	var closeErr error

	m.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), mongoDisconnectTimeout)
		defer cancel()

		closeErr = m.Client.Disconnect(ctx)
	})

	return closeErr
}

//////
// Factory.
//////

// New creates a new MongoDB storage.
func New(ctx context.Context, db string, cfg *Config) (*MongoDB, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*MongoDB)(nil)

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(cfg)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("connect", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	// Disconnect the client if New fails past this point, otherwise its
	// connection pool and topology-monitoring goroutines leak.
	success := false

	defer func() {
		if !success {
			//nolint:errcheck
			client.Disconnect(context.WithoutCancel(ctx))
		}
	}()

	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	// RunCtx returns the context's error when the deadline fires during a
	// backoff sleep, which would drop the reason the attempts were failing.
	// Keep the last one so the caller learns it was the ping that failed.
	var lastErr error

	if err := r.RunCtx(ctx, func(ctx context.Context) error {
		if err := client.Ping(ctx, nil); err != nil {
			cE := customerror.NewFailedToError(
				"ping",
				customerror.WithError(err),
				customerror.WithTag("retry"),
			)

			s.GetLogger().Errorln(cE)

			lastErr = cE

			return cE
		}

		return nil
	}); err != nil {
		if lastErr != nil && !errors.Is(err, lastErr) {
			err = errors.Join(lastErr, err)
		}

		return nil, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterPingFailed())
	}

	storage := &MongoDB{
		Storage: s,

		Client:   client,
		Config:   cfg,
		Database: db,
		Target:   db,
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
