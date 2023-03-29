package mongodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/shared"
	"github.com/thalesfsp/dal/status"
	"github.com/thalesfsp/dal/storage"
	"github.com/thalesfsp/dal/validation"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/customsort"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/get"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/set"
	"github.com/thalesfsp/params/update"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//////
// Const, vars, and types.
//////

const name = "mongodb"

// Singleton.
var singleton storage.IStorage

// Config is the MongoDB configuration.
type Config = options.ClientOptions

// MongoDB storage definition.
type MongoDB struct {
	*storage.Storage

	// Client is the MongoDB client.
	Client *mongo.Client `json:"-" validate:"required"`

	// Config is the MongoDB configuration.
	Config *Config `json:"-"`

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

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (m *MongoDB) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
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

	if prm != nil {
		finalParam = prm
	}

	//////
	// Filter.
	//////

	// Matches all.
	filter := bson.D{}

	if finalParam.Search != "" {
		filterMap := map[string]interface{}{}
		if err := shared.Unmarshal([]byte(finalParam.Search), &filterMap); err != nil {
			return 0, err
		}

		filterBson, err := bson.Marshal(filterMap)
		if err != nil {
			return 0, customerror.NewFailedToError("bson marshal filter", customerror.WithError(err))
		}

		if err := bson.Unmarshal(filterBson, &filter); err != nil {
			return 0, customerror.NewFailedToError("bson unmarshal filter", customerror.WithError(err))
		}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return 0, err
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, "", trgt, finalParam); err != nil {
			return 0, err
		}
	}

	count, err := m.
		Client.
		Database(m.Database).
		Collection(trgt).
		CountDocuments(ctx, filter)
	if err != nil {
		return 0, customerror.NewFailedToError(storage.OperationCount, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, "", trgt, finalParam); err != nil {
			return 0, err
		}
	}

	//////
	// Metrics.
	//////

	m.GetCounterCount().Add(1)

	return count, nil
}

// Delete removes data.
func (m *MongoDB) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
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

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return err
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, finalParam); err != nil {
			return err
		}
	}

	if _, err := m.
		Client.
		Database(m.Database).
		Collection(trgt).
		DeleteOne(ctx, bson.D{{"_id", id}}); err != nil {
		return customerror.NewFailedToError(storage.OperationDelete, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	m.GetCounterDelete().Add(1)

	return nil
}

// Get data.
func (m *MongoDB) Get(ctx context.Context, id, target string, v any, prm *get.Get, options ...storage.Func[*get.Get]) error {
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
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return err
	}

	//////
	// Get.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, finalParam); err != nil {
			return err
		}
	}

	var result bson.M

	if err := m.
		Client.
		Database(m.Database).
		Collection(trgt).
		FindOne(ctx, bson.M{"_id": id}).
		Decode(&result); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return customerror.NewHTTPError(http.StatusNotFound)
		}

		return customerror.NewFailedToError(storage.OperationRetrieve, customerror.WithError(err))
	}

	resultBytes, err := bson.Marshal(result)
	if err != nil {
		return customerror.NewFailedToError("bson marshal result", customerror.WithError(err))
	}

	// Convert bson.M to a Go value.
	if err := bson.Unmarshal(resultBytes, v); err != nil {
		return customerror.NewFailedToError("bson unmarshal result", customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	m.GetCounterGet().Add(1)

	return nil
}

// List data.
//
// WARN: In general, projections that include non-indexed fields or fields
// that are part of a covered index (i.e., an index that includes all the
// projected fields) are less likely to impact performance.
//
// NOTE: It uses param.List.Search to query the data.
func (m *MongoDB) List(ctx context.Context, target string, v any, prm *list.List, opts ...storage.Func[*list.List]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range opts {
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

	if prm != nil {
		finalParam = prm
	}

	// Query params.
	cursorOpts := options.Find()

	// Fields.
	if len(finalParam.Fields) > 0 {
		projection := bson.M{}

		for _, field := range finalParam.Fields {
			projection[field] = 1
		}

		cursorOpts.SetProjection(projection)
	}

	// Sort.
	if len(finalParam.Sort) > 0 {
		sortD, err := ToMongoString(finalParam.Sort.ToSort())
		if err != nil {
			return err
		}

		cursorOpts.SetSort(sortD)
	}

	// Offset.
	if finalParam.Offset >= 0 {
		cursorOpts.SetSkip(int64(finalParam.Offset))
	}

	// Limit.
	if finalParam.Limit >= 0 {
		cursorOpts.SetLimit(int64(finalParam.Limit))
	}

	// Filter.
	//
	// Matches all documents
	filter := bson.D{}

	if finalParam.Search != "" {
		filterMap := map[string]interface{}{}
		if err := shared.Unmarshal([]byte(finalParam.Search), &filterMap); err != nil {
			return err
		}

		filterBson, err := bson.Marshal(filterMap)
		if err != nil {
			return customerror.NewFailedToError("bson marshal filter", customerror.WithError(err))
		}

		if err := bson.Unmarshal(filterBson, &filter); err != nil {
			return customerror.NewFailedToError("bson unmarshal filter", customerror.WithError(err))
		}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return err
	}

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, "", trgt, finalParam); err != nil {
			return err
		}
	}

	cursor, err := m.
		Client.
		Database(m.Database).
		Collection(trgt).
		Find(ctx, filter, cursorOpts)
	if err != nil {
		return customerror.NewFailedToError(storage.OperationList, customerror.WithError(err))
	}

	defer cursor.Close(ctx)

	if err := cursor.All(ctx, v); err != nil {
		return customerror.NewFailedToError("cursor all", customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, "", trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	m.GetCounterList().Add(1)

	return nil
}

// Set data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (m *MongoDB) Set(ctx context.Context, id, target string, v any, prm *set.Set, options ...storage.Func[*set.Set]) (string, error) {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*set.Set]()
	if err != nil {
		return id, err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return id, err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := set.New()
	if err != nil {
		return id, err
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return id, err
	}

	//////
	// Set.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, finalParam); err != nil {
			return id, err
		}
	}

	doc, err := m.
		Client.
		Database(m.Database).
		Collection(trgt).
		InsertOne(ctx, v)
	if err != nil {
		return id, customerror.NewFailedToError(storage.OperationCreate, customerror.WithError(err))
	}

	finalID := id

	if id, ok := doc.InsertedID.(string); ok {
		finalID = id
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, finalID, trgt, finalParam); err != nil {
			return id, err
		}
	}

	//////
	// Metrics.
	//////

	m.GetCounterSet().Add(1)

	return finalID, nil
}

// Update data.
func (m *MongoDB) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range opts {
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

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return err
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, finalParam); err != nil {
			return err
		}
	}

	replaceOpts := options.Replace().SetUpsert(true)

	if _, err := m.
		Client.
		Database(m.Database).
		Collection(trgt).
		ReplaceOne(ctx, bson.M{"_id": id}, v, replaceOpts); err != nil {
		return customerror.NewFailedToError(storage.OperationUpdate, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	m.GetCounterUpdate().Add(1)

	return nil
}

// GetClient returns the client.
func (m *MongoDB) GetClient() any {
	return m.Client
}

//////
// Factory.
//////

// New creates a new MongoDB storage.
func New(ctx context.Context, db string, cfg *Config) (*MongoDB, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*MongoDB)(nil)

	s, err := storage.New(name)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Ensures the connection is alive.
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal(err)
	}

	storage := &MongoDB{
		Storage: s,

		Client:   client,
		Config:   cfg,
		Database: db,
		Target:   db,
	}

	if err := validation.Validate(storage); err != nil {
		return nil, err
	}

	singleton = storage

	return storage, nil
}

//////
// Exported functionalities.
//////

// Get returns a setup MongoDB, or set it up.
func Get() storage.IStorage {
	if singleton == nil {
		panic(fmt.Sprintf("%s %s not %s", name, storage.Type, status.Initialized))
	}

	return singleton
}

// Set sets the storage, primarily used for testing.
func Set(s storage.IStorage) {
	singleton = s
}
