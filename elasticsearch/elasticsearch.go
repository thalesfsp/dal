package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/internal/customapm"
	"github.com/thalesfsp/dal/internal/logging"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/dal/storage"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/customsort"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
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
const Name = "elasticsearch"

// Singleton.
var singleton storage.IStorage

// DynamicIndexFunc is a function which defines the name of the index, and
// evaluated at the index time.
type DynamicIndexFunc func() string

// Config is the ElasticSearch configuration.
type Config = elasticsearch.Config

// ElasticSearch storage definition.
type ElasticSearch struct {
	*storage.Storage

	// Client is the ElasticSearch client.
	Client *elasticsearch.Client `json:"-" validate:"required"`

	// Config is the ElasticSearch configuration.
	Config Config `json:"-"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For Redis, it isn't used at all.
	Target DynamicIndexFunc `json:"-"`
}

//////
// Helpers.
//////

// ToElasticSearchString converts the `Sort` to ElasticSearch sort format.
func ToElasticSearchString(s customsort.Sort) (string, error) {
	sortMap, err := s.ToMap()
	if err != nil {
		return "", err
	}

	sortFields := make([]string, 0, len(sortMap))

	for fieldName, sortOrder := range sortMap {
		sortFields = append(sortFields, `{"`+fieldName+`": {"order": "`+sortOrder+`"}}`)
	}

	return "[" + strings.Join(sortFields, ",") + "]", nil
}

// buildQuery builds a query string from the list parameters and optional addons.
// It uses the ElasticSearch query syntax to construct the query.
func buildQuery(params *list.List, addons ...string) (string, error) {
	// Validate that the search parameter is not empty.
	if params.Search == "" {
		return "", customerror.NewRequiredError("search")
	}

	// Start building the query string with the opening curly brace.
	//
	// Uses `strings.Builder`, it's a more efficient and safer way to
	// concatenate strings in Go because it preallocates a buffer of the
	// required size and avoids unnecessary memory allocations.
	var builder strings.Builder

	builder.WriteByte('{')

	// Append the search query parameter.
	builder.WriteString(` "query": `)

	builder.WriteString(params.Search)

	// Append the fields query parameter, if any.
	if params.Fields != nil {
		fields, err := params.Fields.ToElasticSearch()
		if err != nil {
			return "", err
		}

		builder.WriteString(`, "_source": `)

		builder.WriteString(fields)
	}

	// Append the sort query parameter, if any.
	if params.Sort != nil {
		sort, err := ToElasticSearchString(params.Sort.ToSort())
		if err != nil {
			return "", err
		}

		builder.WriteString(`, "sort": `)

		builder.WriteString(sort)
	}

	// Append the offset query parameter, if any.
	if params.Offset > 0 {
		builder.WriteString(fmt.Sprintf(`, "from": %d`, params.Offset))
	}

	// Append the limit query parameter, if any.
	if params.Limit > 0 {
		builder.WriteString(fmt.Sprintf(`, "size": %d`, params.Limit))
	}

	// Check if prm.Any is type of ListAny.
	if lA, ok := params.Any.(*ListAny); ok {
		// Append the track_total_hits query parameter, if any.
		if lA.TrackTotalHits {
			builder.WriteString(`, "track_total_hits": true`)
		}
	}

	// Append any additional addons.
	if len(addons) > 0 {
		builder.WriteString(", ")

		builder.WriteString(strings.Join(addons, ", "))
	}

	// Remove the last comma, if any.
	if builder.Len() > 1 && builder.String()[builder.Len()-2] == ',' {
		builder.WriteString("}")

		return strings.Replace(builder.String(), ", }", "}", 1), nil
	}

	// Close the query string with the closing curly brace.
	builder.WriteByte('}')

	return builder.String(), nil
}

// CreateIndex creates a new index in Elasticsearch.
func (es *ElasticSearch) CreateIndex(ctx context.Context, name, mapping string) error {
	indexName, err := shared.TargetName(name, name)
	if err != nil {
		return err
	}

	res, err := es.Client.Indices.Create(
		indexName,
		es.Client.Indices.Create.WithContext(ctx),
		es.Client.Indices.Create.WithBody(strings.NewReader(mapping)),
	)
	if err != nil {
		return customerror.NewFailedToError("createIndex", customerror.WithError(err))
	}

	defer res.Body.Close()

	return checkResponseIsError(res, "already")
}

// DeleteIndex deletes a new index in Elasticsearch.
func (es *ElasticSearch) DeleteIndex(ctx context.Context, name string) error {
	indexName, err := shared.TargetName(name, name)
	if err != nil {
		return err
	}

	res, err := es.Client.Indices.Delete(
		[]string{indexName},
		es.Client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return customerror.NewFailedToError(
			"delete index",
			customerror.WithError(err),
		)
	}

	defer res.Body.Close()

	return checkResponseIsError(res, "no such index")
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (es *ElasticSearch) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		es.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
	}

	finalParam.Search = `{"match_all" : {} }`

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, "", trgt, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
		}
	}

	// Create a new search request.
	req := esapi.SearchRequest{
		Index: []string{trgt},
	}

	// Enables routing if specified.
	if finalParam.Routing == nil {
		req.Routing = finalParam.Routing
	}

	query, err := buildQuery(&list.List{
		Search: finalParam.Search,
	}, `"track_total_hits": true`)
	if err != nil {
		return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
	}

	// Set the query.
	req.Body = strings.NewReader(query)

	// Execute the search request.
	res, err := req.Do(context.Background(), es.Client)
	if err != nil {
		return 0, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCount.String(), customerror.WithError(err)),
			es.GetLogger(),
			es.GetCounterCountedFailed())
	}

	defer res.Body.Close()

	// Parse the response.
	var mapResp map[string]interface{}

	// Decode the JSON response and using a pointer.
	if err := shared.Decode(res.Body, &mapResp); err != nil {
		return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
	}

	// Access the value of the "total" field.
	var total int

	if hits, ok := mapResp["hits"].(map[string]interface{}); ok {
		if totalHits, ok := hits["total"].(map[string]interface{}); ok {
			if value, ok := totalHits["value"].(float64); ok {
				total = int(value)
			}
		}
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, "", trgt, total, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	es.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	es.GetCounterCounted().Add(1)

	return int64(total), nil
}

// Delete removes data.
func (es *ElasticSearch) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			es.GetLogger(),
			es.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		es.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterDeletedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterDeletedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterDeletedFailed())
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterDeletedFailed())
		}
	}

	reqOpts := []func(*esapi.DeleteRequest){
		es.Client.Delete.WithContext(ctx),
	}

	// Enables routing if specified.
	if finalParam.Routing != "" {
		reqOpts = append(reqOpts, es.Client.Delete.WithRouting(finalParam.Routing))
	}

	res, err := es.Client.Delete(trgt, id, reqOpts...)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationDelete.String(), customerror.WithError(err)),
			es.GetLogger(),
			es.GetCounterDeletedFailed())
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterDeletedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	es.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	es.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (es *ElasticSearch) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			es.GetLogger(),
			es.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		es.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
		}
	}

	reqOpts := []func(*esapi.GetRequest){
		es.Client.Get.WithContext(ctx),
	}

	// Enables routing if specified.
	if finalParam.Routing != "" {
		reqOpts = append(reqOpts, es.Client.Get.WithRouting(finalParam.Routing))
	}

	res, err := es.Client.Get(trgt, id, reqOpts...)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			es.GetLogger(),
			es.GetCounterRetrievedFailed(),
		)
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
	}

	if res.Body == nil {
		return customapm.TraceError(
			ctx,
			customerror.NewHTTPError(http.StatusFound),
			es.GetLogger(),
			es.GetCounterRetrievedFailed(),
		)
	}

	// Parse response body.
	getResponse := ResponseSourceFromES{}

	if err := parseResponseBody(res.Body, &getResponse); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
	}

	if err := storage.ParseToStruct(getResponse.Data, v); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	es.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	es.GetCounterRetrieved().Add(1)

	return nil
}

// List data.
//
// NOTE: It uses param.List.Search to query the data.
//
//nolint:nestif,gocognit
func (es *ElasticSearch) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		es.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
	}

	finalParam.Search = `{"match_all" : {} }`

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
	}

	//////
	// List.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
		}
	}

	// Create a new search request.
	req := esapi.SearchRequest{
		Index: []string{trgt},
	}

	// Enables routing if specified.
	if finalParam.Routing == nil {
		req.Routing = finalParam.Routing
	}

	query, err := buildQuery(finalParam)
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
	}

	// Set the query.
	req.Body = strings.NewReader(query)

	// Execute the search request.
	res, err := req.Do(context.Background(), es.Client)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationList.String(), customerror.WithError(err)),
			es.GetLogger(),
			es.GetCounterListedFailed(),
		)
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
	}

	// Parse response body.
	// Iterate the document "hits" returned by API call
	// Instantiate a mapping interface for API response
	var mapResp map[string]interface{}

	// Decode the JSON response and using a pointer
	if err := shared.Decode(res.Body, &mapResp); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
	}

	// Extract the total count and associate it with prm.Total
	if prm != nil {
		if hits, ok := mapResp["hits"].(map[string]interface{}); ok {
			if totalHits, ok := hits["total"].(map[string]interface{}); ok {
				if value, ok := totalHits["value"].(float64); ok {
					prm.Count = int64(value)
				}
			}
		}
	}

	hits := []any{}

	mapHits, ok := mapResp["hits"].(map[string]interface{})["hits"].([]interface{})
	if !ok {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("unexpected type for 'hits'", customerror.WithField("hits", hits)),
			es.GetLogger(),
			es.GetCounterListedFailed(),
		)
	}

	for _, hit := range mapHits {
		var doc map[string]interface{}

		if h, ok := hit.(map[string]interface{}); ok {
			doc = h
		} else {
			return customapm.TraceError(
				ctx,
				customerror.NewFailedToError("unexpected type for 'hit'", customerror.WithField("hit", hit)),
				es.GetLogger(),
				es.GetCounterListedFailed(),
			)
		}

		source := doc["_source"]
		hits = append(hits, source)
	}

	if err := storage.ParseToStruct(hits, v); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	es.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	es.GetCounterListed().Add(1)

	return nil
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (es *ElasticSearch) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		es.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
		}
	}

	valueAsJSON, err := shared.Marshal(v)
	if err != nil {
		return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
	}

	reqOpts := []func(*esapi.IndexRequest){
		es.Client.Index.WithDocumentID(id),
		es.Client.Index.WithContext(ctx),
	}

	// Enables routing if specified.
	if finalParam.Routing != "" {
		reqOpts = append(reqOpts, es.Client.Index.WithRouting(finalParam.Routing))
	}

	res, err := es.Client.Index(trgt, bytes.NewReader(valueAsJSON), reqOpts...)
	if err != nil {
		return id, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
			es.GetLogger(),
			es.GetCounterCreatedFailed(),
		)
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterCreatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	es.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	es.GetCounterCreated().Add(1)

	return id, nil
}

// Update data.
func (es *ElasticSearch) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			es.GetLogger(),
			es.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		es.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
		}
	}

	valueAsJSON, err := shared.Marshal(v)
	if err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
	}

	reqOpts := []func(*esapi.UpdateRequest){
		es.Client.Update.WithContext(ctx),
	}

	// Enables routing if specified.
	if finalParam.Routing != "" {
		reqOpts = append(reqOpts, es.Client.Update.WithRouting(finalParam.Routing))
	}

	// Call client Update API using esapi.Update().
	res, err := es.Client.Update(
		trgt,
		id,
		bytes.NewReader([]byte(fmt.Sprintf(`{"doc":%s}`, valueAsJSON))),
		reqOpts...,
	)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			es.GetLogger(),
			es.GetCounterUpdatedFailed(),
		)
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, es.GetLogger(), es.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	es.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	es.GetCounterUpdated().Add(1)

	return nil
}

// GetClient returns the client.
func (es *ElasticSearch) GetClient() any {
	return es.Client
}

//////
// Factory.
//////

// New returns a new `ElasticSearch` storage.
func New(ctx context.Context, cfg Config) (*ElasticSearch, error) {
	return NewWithIndex(ctx, "", cfg)
}

// NewWithIndex returns a new `ElasticSearch` storage specifying the index name.
func NewWithIndex(
	ctx context.Context,
	indexName string,
	cfg Config,
) (*ElasticSearch, error) {
	return NewWithDynamicIndex(ctx, func() string { return indexName }, cfg)
}

// NewWithDynamicIndex returns a new `ElasticSearch` storage. It allows to
// define a function which defines the name of the index, and evaluated at the
// index time.
func NewWithDynamicIndex(
	ctx context.Context,
	dynamicIndexFunc DynamicIndexFunc,
	cfg Config,
) (*ElasticSearch, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*ElasticSearch)(nil)

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	client, err := elasticsearch.NewClient(cfg)
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
		res, err := client.Info(client.Info.WithContext(ctx))
		if err != nil {
			cE := customerror.NewFailedToError(
				"ping",
				customerror.WithError(err),
				customerror.WithTag("retry"),
			)

			s.GetLogger().Errorln(cE)

			return cE
		}

		defer res.Body.Close()

		if res.IsError() {
			cE := customerror.NewFailedToError(
				"ping",
				customerror.WithTag("retry"),
			)

			s.GetLogger().Errorln(cE)

			return cE
		}

		// NOTE: It is critical to both close the response body and to consume it,
		// in order to re-use persistent TCP connections in the default HTTP
		// transport. If you're not interested in the response body, call
		// `io.Copy(io.Discard, res.Body).`
		if _, err := io.Copy(io.Discard, res.Body); err != nil {
			cE := customerror.NewFailedToError(
				"copy body, retrying...",
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

	storage := &ElasticSearch{
		Storage: s,

		Client: client,
		Config: cfg,
		Target: dynamicIndexFunc,
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

// Get returns a setup MongoDB, or set it up.
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
