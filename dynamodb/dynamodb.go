// Package dynamodb provides a DynamoDB implementation of the storage interface.
package dynamodb

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "dynamodb"

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// DynamicTableFunc is a function which defines the name of the table, and
// evaluated at the operation time.
type DynamicTableFunc func() string

// Config is the DynamoDB configuration.
type Config = aws.Config

// DynamoDB storage definition.
type DynamoDB struct {
	*storage.Storage

	// Client is the DynamoDB client.
	//
	// structonly: skips validating *into* the client's fields (only checks
	// non-nilness) - same reasoning as Config below applies to any live SDK
	// state reachable from it.
	Client *awsdynamodb.Client `json:"-" validate:"required,structonly"`

	closeOnce sync.Once

	// Config is the DynamoDB configuration.
	//
	// Skipped by the validator: it dives into untagged nested structs, and an
	// aws.Config holds live SDK state (credentials guarded by a mutex, which
	// background SDK goroutines write) that must not be read reflectively.
	Config Config `json:"-" validate:"-"`

	// Region is the AWS region.
	Region string `json:"region" validate:"required"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For DynamoDB, the target is the table name. This function allows for
	// dynamic table naming, useful for time-based partitioning or other patterns.
	Target DynamicTableFunc `json:"-"`

	// PrimaryKey is the primary key field name for DynamoDB.
	// Default is "id" if not specified.
	PrimaryKey string `json:"primaryKey" validate:"required"`
}

//////
// Helpers.
//////

// ToDynamoDBSort converts the `Sort` to DynamoDB sort format.
func ToDynamoDBSort(s customsort.Sort) (bool, error) {
	sortMap, err := s.ToMap()
	if err != nil {
		return false, err
	}

	// DynamoDB sort is simple: only ascending or descending for scan operations
	// We'll return true for ascending (default) and handle in scan parameters
	for _, sortOrder := range sortMap {
		if strings.ToLower(sortOrder) == customsort.Desc {
			return false, nil // ScanIndexForward = false for descending
		}
	}

	return true, nil // ScanIndexForward = true for ascending
}

// dynamoEnvelopeOptions passes the table name resolved inside Run to the
// user's hooks without moving target resolution outside the traced envelope.
func dynamoEnvelopeOptions[T any](target *string, options []storage.Func[T]) []storage.Func[T] {
	result := make([]storage.Func[T], 0, len(options)+1)
	result = append(result, options...)
	result = append(result, func(o *storage.Options[T]) error {
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
func (d *DynamoDB) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	var trgt string

	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        d,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         target,
		Options:        dynamoEnvelopeOptions(&trgt, options),
		SuccessCounter: d.GetCounterCounted(),
		FailureCounter: d.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, d.Target())

			return params, err
		},
	}, func(ctx context.Context, params *count.Count) (envelope.Result[int64], error) {
		scanInput := &awsdynamodb.ScanInput{
			TableName: aws.String(trgt),
			Select:    types.SelectCount,
		}

		if params.Search != "" {
			filterMap := map[string]any{}
			if err := shared.Unmarshal([]byte(params.Search), &filterMap); err != nil {
				return envelope.Result[int64]{}, customerror.NewFailedToError(
					"unmarshal search filter",
					customerror.WithError(err),
				)
			}

			var filterExpressions []string
			expressionAttributeNames := make(map[string]string)
			expressionAttributeValues := make(map[string]types.AttributeValue)

			for key, value := range filterMap {
				placeholder := sanitizePlaceholder(key)
				attrName := fmt.Sprintf("#%s", placeholder)
				attrValue := fmt.Sprintf(":%s", placeholder)

				filterExpressions = append(filterExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
				expressionAttributeNames[attrName] = key

				av, err := attributevalue.Marshal(value)
				if err != nil {
					return envelope.Result[int64]{}, customerror.NewFailedToError(
						"marshal filter value",
						customerror.WithError(err),
					)
				}
				expressionAttributeValues[attrValue] = av
			}

			if len(filterExpressions) > 0 {
				scanInput.FilterExpression = aws.String(strings.Join(filterExpressions, " AND "))
				scanInput.ExpressionAttributeNames = expressionAttributeNames
				scanInput.ExpressionAttributeValues = expressionAttributeValues
			}
		}

		var resultCount int64
		paginator := awsdynamodb.NewScanPaginator(d.Client, scanInput)
		for paginator.HasMorePages() {
			result, err := paginator.NextPage(ctx)
			if err != nil {
				return envelope.Result[int64]{}, customerror.NewFailedToError(
					storage.OperationCount.String(),
					customerror.WithError(err),
				)
			}

			resultCount += int64(result.Count)
		}

		return envelope.Result[int64]{Value: resultCount, PostHookData: resultCount}, nil
	})
}

// Delete removes data.
func (d *DynamoDB) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	var trgt string

	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        d,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         target,
		Options:        dynamoEnvelopeOptions(&trgt, options),
		RequiresID:     true,
		SuccessCounter: d.GetCounterDeleted(),
		FailureCounter: d.GetCounterDeletedFailed(),
		NewParams: func() (*delete.Delete, error) {
			params, err := delete.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, d.Target())

			return params, err
		},
	}, func(ctx context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		key, err := attributevalue.Marshal(id)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"marshal primary key",
				customerror.WithError(err),
			)
		}

		if _, err := d.Client.DeleteItem(ctx, &awsdynamodb.DeleteItemInput{
			TableName: aws.String(trgt),
			Key: map[string]types.AttributeValue{
				d.PrimaryKey: key,
			},
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
func (d *DynamoDB) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	var trgt string

	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        d,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        dynamoEnvelopeOptions(&trgt, options),
		RequiresID:     true,
		SuccessCounter: d.GetCounterRetrieved(),
		FailureCounter: d.GetCounterRetrievedFailed(),
		NewParams: func() (*retrieve.Retrieve, error) {
			params, err := retrieve.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, d.Target())

			return params, err
		},
	}, func(ctx context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		key, err := attributevalue.Marshal(id)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"marshal primary key",
				customerror.WithError(err),
			)
		}

		result, err := d.Client.GetItem(ctx, &awsdynamodb.GetItemInput{
			TableName: aws.String(trgt),
			Key: map[string]types.AttributeValue{
				d.PrimaryKey: key,
			},
		})
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}

		if len(result.Item) == 0 {
			return envelope.Result[envelope.Void]{}, customerror.NewNotFoundError("item not found")
		}

		if err := attributevalue.UnmarshalMap(result.Item, v); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"unmarshal result",
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// Any accepts a map[string]any for DynamoDB filter expressions. Limit, Offset,
// and Fields are applied; Sort is not currently used.
//
//nolint:gocognit
func (d *DynamoDB) List(ctx context.Context, target string, v any, prm *list.List, opts ...storage.Func[*list.List]) error {
	var (
		trgt      string
		scanInput *awsdynamodb.ScanInput
	)

	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        d,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         target,
		HookData:       v,
		Options:        dynamoEnvelopeOptions(&trgt, opts),
		SuccessCounter: d.GetCounterListed(),
		FailureCounter: d.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, d.Target())
			if err != nil {
				return nil, err
			}

			scanInput = &awsdynamodb.ScanInput{TableName: aws.String(trgt)}

			if len(params.Fields) > 0 {
				var projectionExpressions []string
				expressionAttributeNames := make(map[string]string)

				for _, field := range params.Fields {
					attrName := fmt.Sprintf("#%s", sanitizePlaceholder(field))
					projectionExpressions = append(projectionExpressions, attrName)
					expressionAttributeNames[attrName] = field
				}

				scanInput.ProjectionExpression = aws.String(strings.Join(projectionExpressions, ", "))
				scanInput.ExpressionAttributeNames = expressionAttributeNames
			}

			if params.Limit > 0 {
				if params.Limit > math.MaxInt32 {
					return nil, customerror.NewFailedToError("limit exceeds DynamoDB maximum")
				}

				scanInput.Limit = aws.Int32(int32(params.Limit))
			}

			if filter, ok := params.Any.(map[string]any); ok && len(filter) > 0 {
				var filterExpressions []string
				if scanInput.ExpressionAttributeNames == nil {
					scanInput.ExpressionAttributeNames = make(map[string]string)
				}
				expressionAttributeValues := make(map[string]types.AttributeValue)

				for key, value := range filter {
					placeholder := sanitizePlaceholder(key)
					attrName := fmt.Sprintf("#f%s", placeholder)
					attrValue := fmt.Sprintf(":f%s", placeholder)

					filterExpressions = append(filterExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
					scanInput.ExpressionAttributeNames[attrName] = key

					av, err := attributevalue.Marshal(value)
					if err != nil {
						return nil, customerror.NewFailedToError(
							"marshal filter value",
							customerror.WithError(err),
						)
					}
					expressionAttributeValues[attrValue] = av
				}

				if len(filterExpressions) > 0 {
					scanInput.FilterExpression = aws.String(strings.Join(filterExpressions, " AND "))
					scanInput.ExpressionAttributeValues = expressionAttributeValues
				}
			}

			return params, nil
		},
	}, func(ctx context.Context, params *list.List) (envelope.Result[envelope.Void], error) {
		var allItems []map[string]types.AttributeValue
		currentOffset := 0

		paginator := awsdynamodb.NewScanPaginator(d.Client, scanInput)
		for paginator.HasMorePages() {
			result, err := paginator.NextPage(ctx)
			if err != nil {
				return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
					storage.OperationList.String(),
					customerror.WithError(err),
				)
			}

			for _, item := range result.Items {
				if currentOffset < params.Offset {
					currentOffset++

					continue
				}

				allItems = append(allItems, item)
				if params.Limit > 0 && len(allItems) >= params.Limit {
					break
				}
			}

			if params.Limit > 0 && len(allItems) >= params.Limit {
				break
			}
		}

		if err := attributevalue.UnmarshalListOfMaps(allItems, v); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"unmarshal results",
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// Create data.
//
// NOTE: DynamoDB requires the primary key to be set in the model (`v`).
//
//nolint:gocognit,nestif
func (d *DynamoDB) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	var trgt string

	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        d,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        dynamoEnvelopeOptions(&trgt, options),
		RequiresID:     true,
		SuccessCounter: d.GetCounterCreated(),
		FailureCounter: d.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, d.Target())

			return params, err
		},
	}, func(ctx context.Context, _ *create.Create) (envelope.Result[string], error) {
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Pointer {
			val = val.Elem()
		}

		if val.Kind() == reflect.Struct {
			for i := range val.NumField() {
				field := val.Type().Field(i)
				jsonTag := field.Tag.Get("json")
				dynamoTag := field.Tag.Get("dynamodbav")

				if jsonTag != "" {
					jsonTag = strings.Split(jsonTag, ",")[0]
				}
				if dynamoTag != "" {
					dynamoTag = strings.Split(dynamoTag, ",")[0]
				}

				if field.Name == cases.Title(language.English).String(d.PrimaryKey) ||
					jsonTag == d.PrimaryKey ||
					dynamoTag == d.PrimaryKey ||
					strings.EqualFold(field.Name, d.PrimaryKey) {
					if val.Field(i).CanSet() && val.Field(i).Kind() == reflect.String {
						val.Field(i).SetString(id)
					}

					break
				}
			}
		}

		item, err := attributevalue.MarshalMap(v)
		if err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				"marshal item",
				customerror.WithError(err),
			)
		}

		if _, err := d.Client.PutItem(ctx, &awsdynamodb.PutItemInput{
			TableName: aws.String(trgt),
			Item:      item,
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
func (d *DynamoDB) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
	var trgt string

	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        d,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        dynamoEnvelopeOptions(&trgt, opts),
		RequiresID:     true,
		SuccessCounter: d.GetCounterUpdated(),
		FailureCounter: d.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, d.Target())

			return params, err
		},
	}, func(ctx context.Context, _ *update.Update) (envelope.Result[envelope.Void], error) {
		item, err := attributevalue.MarshalMap(v)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"marshal update item",
				customerror.WithError(err),
			)
		}

		updateExpressions := make([]string, 0, len(item))
		expressionAttributeNames := make(map[string]string)
		expressionAttributeValues := make(map[string]types.AttributeValue)

		for key, value := range item {
			if key == d.PrimaryKey {
				continue
			}

			placeholder := sanitizePlaceholder(key)
			attrName := fmt.Sprintf("#%s", placeholder)
			attrValue := fmt.Sprintf(":%s", placeholder)

			updateExpressions = append(updateExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
			expressionAttributeNames[attrName] = key
			expressionAttributeValues[attrValue] = value
		}

		if len(updateExpressions) == 0 {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError("no fields to update")
		}

		key, err := attributevalue.Marshal(id)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"marshal primary key",
				customerror.WithError(err),
			)
		}

		if _, err := d.Client.UpdateItem(ctx, &awsdynamodb.UpdateItemInput{
			TableName: aws.String(trgt),
			Key: map[string]types.AttributeValue{
				d.PrimaryKey: key,
			},
			UpdateExpression:          aws.String("SET " + strings.Join(updateExpressions, ", ")),
			ExpressionAttributeNames:  expressionAttributeNames,
			ExpressionAttributeValues: expressionAttributeValues,
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
func (d *DynamoDB) GetClient() any {
	return d.Client
}

// Close releases the resources held by the storage.
func (d *DynamoDB) Close() error {
	d.closeOnce.Do(func() {
		// An aws-sdk-go-v2 client holds no closable handle; its only retained
		// network resource is the HTTP client's idle keep-alive pool. The SDK's
		// aws.HTTPClient interface is just Do(*http.Request), so
		// CloseIdleConnections has to be probed for rather than called
		// directly — *http.Client and *http.Transport both provide it, and a
		// caller-supplied client that does not is simply left alone.
		if closer, ok := d.Config.HTTPClient.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	})

	return nil
}

//////
// Factory.
//////

// New creates a new DynamoDB storage.
func New(ctx context.Context, region string, cfg *Config, options ...func(*DynamoDB)) (*DynamoDB, error) {
	return NewWithTable(ctx, region, "", cfg, options...)
}

// NewWithTable creates a new DynamoDB storage with a static table name.
func NewWithTable(
	ctx context.Context,
	region, tableName string,
	cfg *Config,
	options ...func(*DynamoDB),
) (*DynamoDB, error) {
	return NewWithDynamicTable(ctx, region, func() string { return tableName }, cfg, options...)
}

// NewWithDynamicTable creates a new DynamoDB storage with a dynamic table function.
// This allows for table names to be computed at runtime, useful for time-based partitioning.
func NewWithDynamicTable(
	ctx context.Context,
	region string,
	dynamicTableFunc DynamicTableFunc,
	cfg *Config,
	options ...func(*DynamoDB),
) (*DynamoDB, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*DynamoDB)(nil)

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	var finalConfig Config
	if cfg != nil {
		finalConfig = *cfg

		// The region argument still applies when the custom config doesn't
		// set one.
		if finalConfig.Region == "" {
			finalConfig.Region = region
		}
	} else {
		finalConfig, err = awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		if err != nil {
			return nil, customapm.TraceError(
				ctx,
				customerror.NewFailedToError("load AWS configuration", customerror.WithError(err)),
				s.GetLogger(),
				s.GetCounterPingFailed(),
			)
		}
	}

	// Create DynamoDB client
	client := awsdynamodb.NewFromConfig(finalConfig)

	// Test connection
	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	if err := r.RunCtx(ctx, func(ctx context.Context) error {
		// Simple operation to test connectivity
		_, err := client.ListTables(ctx, &awsdynamodb.ListTablesInput{
			Limit: aws.Int32(1),
		})
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("test connection", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	storage := &DynamoDB{
		Storage: s,

		Client:     client,
		Config:     finalConfig,
		Region:     region,
		PrimaryKey: "id", // Default primary key
		Target:     dynamicTableFunc,
	}

	for _, option := range options {
		option(storage)
	}

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("validate storage", customerror.WithError(err)),
			s.GetLogger(),
			nil,
		)
	}

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

//////
// Additional helper functions.
//////

// WithPrimaryKey sets a custom primary key for the DynamoDB storage.
// Default is "id" if not specified.
func WithPrimaryKey(primaryKey string) func(*DynamoDB) {
	return func(d *DynamoDB) {
		d.PrimaryKey = primaryKey
	}
}

// WithTarget sets a static target (table name) for the DynamoDB storage.
func WithTarget(target string) func(*DynamoDB) {
	return func(d *DynamoDB) {
		d.Target = func() string { return target }
	}
}

// WithDynamicTarget sets a dynamic target function for the DynamoDB storage.
func WithDynamicTarget(targetFunc DynamicTableFunc) func(*DynamoDB) {
	return func(d *DynamoDB) {
		d.Target = targetFunc
	}
}
