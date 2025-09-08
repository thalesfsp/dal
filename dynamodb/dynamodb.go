// Package dynamodb provides a DynamoDB implementation of the storage interface.
package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "dynamodb"

// Singleton.
var singleton storage.IStorage

// DynamicTableFunc is a function which defines the name of the table, and
// evaluated at the operation time.
type DynamicTableFunc func() string

// Config is the DynamoDB configuration.
type Config = aws.Config

// DynamoDB storage definition.
type DynamoDB struct {
	*storage.Storage

	// Client is the DynamoDB client.
	Client *dynamodb.DynamoDB `json:"-" validate:"required"`

	// Config is the DynamoDB configuration.
	Config Config `json:"-"`

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

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (d *DynamoDB) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		d.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCountedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCountedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCountedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, d.Target())
	if err != nil {
		return 0, customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCountedFailed())
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, d, "", trgt, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCountedFailed())
		}
	}

	// Prepare scan input
	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(trgt),
		Select:    aws.String(dynamodb.SelectCount),
	}

	// Add filter if search is provided
	if finalParam.Search != "" {
		filterMap := map[string]interface{}{}
		if err := shared.Unmarshal([]byte(finalParam.Search), &filterMap); err != nil {
			return 0, customapm.TraceError(
				ctx,
				customerror.NewFailedToError("unmarshal search filter", customerror.WithError(err)),
				d.GetLogger(),
				d.GetCounterCountedFailed(),
			)
		}

		// Build filter expression for DynamoDB
		var filterExpressions []string
		expressionAttributeNames := make(map[string]*string)
		expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)

		for key, value := range filterMap {
			attrName := fmt.Sprintf("#%s", key)
			attrValue := fmt.Sprintf(":%s", key)

			filterExpressions = append(filterExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
			expressionAttributeNames[attrName] = aws.String(key)

			av, err := dynamodbattribute.Marshal(value)
			if err != nil {
				return 0, customapm.TraceError(
					ctx,
					customerror.NewFailedToError("marshal filter value", customerror.WithError(err)),
					d.GetLogger(),
					d.GetCounterCountedFailed(),
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

	result, err := d.Client.ScanWithContext(ctx, scanInput)
	if err != nil {
		return 0, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCount.String(), customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterCountedFailed(),
		)
	}

	count := *result.Count

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, d, "", trgt, count, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	d.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	d.GetCounterCounted().Add(1)

	return count, nil
}

// Delete removes data.
func (d *DynamoDB) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			d.GetLogger(),
			d.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		d.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterDeletedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterDeletedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, d.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterDeletedFailed())
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, d, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterDeletedFailed())
		}
	}

	key, err := dynamodbattribute.Marshal(id)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("marshal primary key", customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterDeletedFailed(),
		)
	}

	deleteInput := &dynamodb.DeleteItemInput{
		TableName: aws.String(trgt),
		Key: map[string]*dynamodb.AttributeValue{
			d.PrimaryKey: key,
		},
	}

	if _, err := d.Client.DeleteItemWithContext(ctx, deleteInput); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			),
			d.GetLogger(),
			d.GetCounterDeletedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, d, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	d.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	d.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (d *DynamoDB) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			d.GetLogger(),
			d.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		d.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterRetrievedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterRetrievedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, d.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterRetrievedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, d, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterRetrievedFailed())
		}
	}

	key, err := dynamodbattribute.Marshal(id)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("marshal primary key", customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterRetrievedFailed(),
		)
	}

	getInput := &dynamodb.GetItemInput{
		TableName: aws.String(trgt),
		Key: map[string]*dynamodb.AttributeValue{
			d.PrimaryKey: key,
		},
	}

	result, err := d.Client.GetItemWithContext(ctx, getInput)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterRetrievedFailed(),
		)
	}

	if len(result.Item) == 0 {
		return customapm.TraceError(
			ctx,
			customerror.NewNotFoundError("item not found"),
			d.GetLogger(),
			d.GetCounterRetrievedFailed(),
		)
	}

	// Convert DynamoDB item to Go value.
	if err := dynamodbattribute.UnmarshalMap(result.Item, v); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("unmarshal result", customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterRetrievedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, d, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	d.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	d.GetCounterRetrieved().Add(1)

	return nil
}

// List data.
//
// NOTE: It uses param.List.Any for DynamoDB filter expressions.
//
//nolint:gocognit,cyclop,funlen,gocyclo,maintidx
func (d *DynamoDB) List(ctx context.Context, target string, v any, prm *list.List, opts ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		d.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterListedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range opts {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterListedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterListedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, d.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterListedFailed())
	}

	//////
	// Query preparation.
	//////

	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(trgt),
	}

	// Fields projection
	if len(finalParam.Fields) > 0 {
		var projectionExpressions []string
		expressionAttributeNames := make(map[string]*string)

		for _, field := range finalParam.Fields {
			attrName := fmt.Sprintf("#%s", field)
			projectionExpressions = append(projectionExpressions, attrName)
			expressionAttributeNames[attrName] = aws.String(field)
		}

		scanInput.ProjectionExpression = aws.String(strings.Join(projectionExpressions, ", "))
		scanInput.ExpressionAttributeNames = expressionAttributeNames
	}

	// Limit
	if finalParam.Limit >= 0 {
		scanInput.Limit = aws.Int64(int64(finalParam.Limit))
	}

	// Filter
	if filter, ok := finalParam.Any.(map[string]interface{}); ok && len(filter) > 0 {
		var filterExpressions []string
		if scanInput.ExpressionAttributeNames == nil {
			scanInput.ExpressionAttributeNames = make(map[string]*string)
		}
		expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)

		for key, value := range filter {
			attrName := fmt.Sprintf("#f%s", key)
			attrValue := fmt.Sprintf(":f%s", key)

			filterExpressions = append(filterExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
			scanInput.ExpressionAttributeNames[attrName] = aws.String(key)

			av, err := dynamodbattribute.Marshal(value)
			if err != nil {
				return customapm.TraceError(
					ctx,
					customerror.NewFailedToError("marshal filter value", customerror.WithError(err)),
					d.GetLogger(),
					d.GetCounterListedFailed(),
				)
			}
			expressionAttributeValues[attrValue] = av
		}

		if len(filterExpressions) > 0 {
			scanInput.FilterExpression = aws.String(strings.Join(filterExpressions, " AND "))
			scanInput.ExpressionAttributeValues = expressionAttributeValues
		}
	}

	//////
	// Query execution.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, d, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterListedFailed())
		}
	}

	var allItems []map[string]*dynamodb.AttributeValue
	var lastEvaluatedKey map[string]*dynamodb.AttributeValue

	// Handle pagination and offset
	currentOffset := 0
	targetOffset := finalParam.Offset

	for {
		if lastEvaluatedKey != nil {
			scanInput.ExclusiveStartKey = lastEvaluatedKey
		}

		result, err := d.Client.ScanWithContext(ctx, scanInput)
		if err != nil {
			return customapm.TraceError(
				ctx,
				customerror.NewFailedToError(storage.OperationList.String(), customerror.WithError(err)),
				d.GetLogger(),
				d.GetCounterListedFailed(),
			)
		}

		// Handle offset by skipping items
		for _, item := range result.Items {
			if currentOffset < targetOffset {
				currentOffset++

				continue
			}

			allItems = append(allItems, item)

			// Break if we've reached the limit
			if finalParam.Limit > 0 && len(allItems) >= finalParam.Limit {
				break
			}
		}

		// Break if we have enough items or no more pages
		if finalParam.Limit > 0 && len(allItems) >= finalParam.Limit {
			break
		}

		lastEvaluatedKey = result.LastEvaluatedKey
		if lastEvaluatedKey == nil {
			break
		}
	}

	// Convert to the target type
	if err := dynamodbattribute.UnmarshalListOfMaps(allItems, v); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("unmarshal results", customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterListedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, d, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	d.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	d.GetCounterListed().Add(1)

	return nil
}

// Create data.
//
// NOTE: DynamoDB requires the primary key to be set in the model (`v`).
//
//nolint:gocognit,nestif
func (d *DynamoDB) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	if id == "" {
		return "", customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			d.GetLogger(),
			d.GetCounterCreatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		d.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCreatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, d.Target())
	if err != nil {
		return "", customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCreatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, d, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCreatedFailed())
		}
	}

	// Ensure the primary key is set in the item
	val := reflect.ValueOf(v)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() == reflect.Struct {
		// Find the primary key field and set it
		for i := range val.NumField() {
			field := val.Type().Field(i)

			// Check various tag formats for the primary key
			jsonTag := field.Tag.Get("json")
			dynamoTag := field.Tag.Get("dynamodbav")

			// Clean up JSON tag (remove omitempty, etc.)
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

	item, err := dynamodbattribute.MarshalMap(v)
	if err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError("marshal item", customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterCreatedFailed(),
		)
	}

	putInput := &dynamodb.PutItemInput{
		TableName: aws.String(trgt),
		Item:      item,
	}

	if _, err := d.Client.PutItemWithContext(ctx, putInput); err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterCreatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, d, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterCreatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	d.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	d.GetCounterCreated().Add(1)

	return id, nil
}

// Update data.
func (d *DynamoDB) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			d.GetLogger(),
			d.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		d.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterUpdatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range opts {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterUpdatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, d.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterUpdatedFailed())
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, d, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterUpdatedFailed())
		}
	}

	// Marshal the value to get all fields
	item, err := dynamodbattribute.MarshalMap(v)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("marshal update item", customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterUpdatedFailed(),
		)
	}

	// Build update expression
	updateExpressions := make([]string, 0, len(item))
	expressionAttributeNames := make(map[string]*string)
	expressionAttributeValues := make(map[string]*dynamodb.AttributeValue)

	for key, value := range item {
		// Skip the primary key in updates
		if key == d.PrimaryKey {
			continue
		}

		attrName := fmt.Sprintf("#%s", key)
		attrValue := fmt.Sprintf(":%s", key)

		updateExpressions = append(updateExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
		expressionAttributeNames[attrName] = aws.String(key)
		expressionAttributeValues[attrValue] = value
	}

	if len(updateExpressions) == 0 {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("no fields to update"),
			d.GetLogger(),
			d.GetCounterUpdatedFailed(),
		)
	}

	key, err := dynamodbattribute.Marshal(id)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("marshal primary key", customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterUpdatedFailed(),
		)
	}

	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(trgt),
		Key: map[string]*dynamodb.AttributeValue{
			d.PrimaryKey: key,
		},
		UpdateExpression:          aws.String("SET " + strings.Join(updateExpressions, ", ")),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
	}

	if _, err := d.Client.UpdateItemWithContext(ctx, updateInput); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			d.GetLogger(),
			d.GetCounterUpdatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, d, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, d.GetLogger(), d.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	d.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	d.GetCounterUpdated().Add(1)

	return nil
}

// GetClient returns the client.
func (d *DynamoDB) GetClient() any {
	return d.Client
}

//////
// Factory.
//////

// New creates a new DynamoDB storage.
func New(ctx context.Context, region string, cfg *Config) (*DynamoDB, error) {
	return NewWithTable(ctx, region, "", cfg)
}

// NewWithTable creates a new DynamoDB storage with a static table name.
func NewWithTable(ctx context.Context, region, tableName string, cfg *Config) (*DynamoDB, error) {
	return NewWithDynamicTable(ctx, region, func() string { return tableName }, cfg)
}

// NewWithDynamicTable creates a new DynamoDB storage with a dynamic table function.
// This allows for table names to be computed at runtime, useful for time-based partitioning.
func NewWithDynamicTable(ctx context.Context, region string, dynamicTableFunc DynamicTableFunc, cfg *Config) (*DynamoDB, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*DynamoDB)(nil)

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	// Create AWS session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("create session", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	// Apply custom config if provided
	var finalConfig Config
	if cfg != nil {
		finalConfig = *cfg
		sess, err = session.NewSession(cfg)
		if err != nil {
			return nil, customapm.TraceError(
				ctx,
				customerror.NewFailedToError("create session with config", customerror.WithError(err)),
				s.GetLogger(),
				s.GetCounterPingFailed(),
			)
		}
	}

	// Create DynamoDB client
	client := dynamodb.New(sess)

	// Test connection
	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	if err := r.Run(func() error {
		// Simple operation to test connectivity
		_, err := client.ListTablesWithContext(ctx, &dynamodb.ListTablesInput{
			Limit: aws.Int64(1),
		})
		if err != nil {
			var awsErr awserr.Error
			if errors.As(err, &awsErr) {
				// Check if it's a temporary error
				if awsErr.Code() == "RequestLimitExceeded" || awsErr.Code() == "ServiceUnavailable" {
					return err // Retryable
				}
			}

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

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("validate storage", customerror.WithError(err)),
			s.GetLogger(),
			nil,
		)
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
