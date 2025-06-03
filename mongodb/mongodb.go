package mongodb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v2/internal/customapm"
	"github.com/thalesfsp/dal/v2/internal/logging"
	"github.com/thalesfsp/dal/v2/internal/shared"
	"github.com/thalesfsp/dal/v2/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/fields"
	"github.com/thalesfsp/sypl/level"
	"github.com/thalesfsp/validation"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "mongodb"

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

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (m *MongoDB) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
	}

	// Set the default database to what is set in the storage.
	o.Database = m.Database

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
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
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}

		filterBson, err := bson.Marshal(filterMap)
		if err != nil {
			return 0, customapm.TraceError(
				ctx,
				customerror.NewFailedToError("bson marshal filter", customerror.WithError(err)),
				m.GetLogger(),
				m.GetCounterCountedFailed(),
			)
		}

		if err := bson.Unmarshal(filterBson, &filter); err != nil {
			return 0, customapm.TraceError(
				ctx,
				customerror.NewFailedToError("bson unmarshal filter", customerror.WithError(err)),
				m.GetLogger(),
				m.GetCounterCountedFailed(),
			)
		}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, "", trgt, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}
	}

	count, err := m.
		Client.
		Database(o.Database).
		Collection(trgt).
		CountDocuments(ctx, filter)
	if err != nil {
		return 0, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCount.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterCountedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, "", trgt, count, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterCounted().Add(1)

	return count, nil
}

// Delete removes data.
func (m *MongoDB) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			m.GetLogger(),
			m.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
	}

	// Set the default database to what is set in the storage.
	o.Database = m.Database

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
		}
	}

	if _, err := m.
		Client.
		Database(o.Database).
		Collection(trgt).
		DeleteOne(ctx, bson.M{"_id": id}); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			),
			m.GetLogger(),
			m.GetCounterDeletedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (m *MongoDB) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
	}

	// Set the default database to what is set in the storage.
	o.Database = m.Database

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
		}
	}

	var result bson.M

	if err := m.
		Client.
		Database(o.Database).
		Collection(trgt).
		FindOne(ctx, bson.M{"_id": id}).
		Decode(&result); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return customapm.TraceError(
				ctx,
				customerror.NewHTTPError(http.StatusNotFound),
				m.GetLogger(),
				m.GetCounterRetrievedFailed(),
			)
		}

		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	resultBytes, err := bson.Marshal(result)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("bson marshal result", customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	// Convert bson to a Go value.
	if err := bson.Unmarshal(resultBytes, v); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("bson unmarshal result", customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterRetrieved().Add(1)

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
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
	}

	// Set the default database to what is set in the storage.
	o.Database = m.Database

	// Iterate over the options and apply them against params.
	for _, option := range opts {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
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
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
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
	var filter bson.M

	if flt, ok := prm.Any.(bson.M); ok {
		filter = flt
	}

	// If filter is empty, matches everyrthing.
	if filter == nil {
		filter = bson.M{}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
	}

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	cursor, err := m.
		Client.
		Database(o.Database).
		Collection(trgt).
		Find(ctx, filter, cursorOpts)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationList.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterListedFailed(),
		)
	}

	defer cursor.Close(ctx)

	if err := cursor.All(ctx, v); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("cursor all", customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterListedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterListed().Add(1)

	return nil
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set.
//
// WARN: MongoDB relies on the model (`v`) `_id` field to be set, otherwise it
// will generate a new one. IT'S UP TO THE DEVELOPER TO SET THE `_ID` FIELD.
func (m *MongoDB) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	if id == "" {
		return "", customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			m.GetLogger(),
			m.GetCounterCreatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
	}

	// Set the default database to what is set in the storage.
	o.Database = m.Database

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
		}
	}

	if _, err := m.
		Client.
		Database(o.Database).
		Collection(trgt).
		InsertOne(ctx, v); err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterCreatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterCreated().Add(1)

	return id, nil
}

// Update data.
func (m *MongoDB) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			m.GetLogger(),
			m.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	// Set the default database to what is set in the storage.
	o.Database = m.Database

	// Iterate over the options and apply them against params.
	for _, option := range opts {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
		}
	}

	b, err := shared.Marshal(v)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	updateFields := make(map[string]interface{})
	if err := shared.Unmarshal(b, &updateFields); err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	update := bson.M{
		"$set": updateFields,
	}

	if _, err := m.
		Client.
		Database(o.Database).
		Collection(trgt).
		UpdateOne(ctx, bson.M{"_id": id}, update); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterUpdatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterUpdated().Add(1)

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

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(ctx, cfg)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("connect", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	if err := r.Run(func() error {
		if err := client.Ping(ctx, nil); err != nil {
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
