package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
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
)

//////
// Const, vars, and types.
//////

const name = "elasticsearch"

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

	if err := checkResponseIsError(res, "already"); err != nil {
		return err
	}

	return nil
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

	if err := checkResponseIsError(res, "no such index"); err != nil {
		return err
	}

	return nil
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (es *ElasticSearch) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
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

	finalParam.Search = `{"match_all" : {} }`

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return 0, err
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, "", trgt, finalParam); err != nil {
			return 0, err
		}
	}

	// Create a new search request.
	req := esapi.SearchRequest{
		Index: []string{trgt},
	}

	query, err := buildQuery(&list.List{
		Search: finalParam.Search,
	}, `"track_total_hits": true`)
	if err != nil {
		return 0, err
	}

	// Set the query.
	req.Body = strings.NewReader(query)

	// Execute the search request.
	res, err := req.Do(context.Background(), es.Client)
	if err != nil {
		return 0, customerror.NewFailedToError(storage.OperationCount, customerror.WithError(err))
	}

	defer res.Body.Close()

	// Parse the response.
	var mapResp map[string]interface{}

	// Decode the JSON response and using a pointer.
	if err := shared.Decode(res.Body, &mapResp); err != nil {
		return 0, err
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
		if err := o.PostHookFunc(ctx, es, "", trgt, finalParam); err != nil {
			return 0, err
		}
	}

	//////
	// Metrics.
	//////

	es.GetCounterCount().Add(1)

	return int64(total), nil
}

// Delete removes data.
func (es *ElasticSearch) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
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

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return err
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return err
		}
	}

	res, err := es.Client.Delete(trgt, id, es.Client.Delete.WithContext(ctx))
	if err != nil {
		return customerror.NewFailedToError(storage.OperationDelete, customerror.WithError(err))
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return err
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	es.GetCounterDelete().Add(1)

	return nil
}

// Get data.
func (es *ElasticSearch) Get(ctx context.Context, id, target string, v any, prm *get.Get, options ...storage.Func[*get.Get]) error {
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

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return err
	}

	//////
	// Get.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return err
		}
	}

	res, err := es.Client.Get(trgt, id, es.Client.Get.WithContext(ctx))
	if err != nil {
		return customerror.NewFailedToError(storage.OperationRetrieve, customerror.WithError(err))
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return err
	}

	if res.Body == nil {
		return customerror.NewHTTPError(http.StatusFound)
	}

	// Parse response body.
	getResponse := ResponseSourceFromES{}

	if err := parseResponseBody(res.Body, &getResponse); err != nil {
		return err
	}

	if err := storage.ParseToStruct(getResponse.Data, v); err != nil {
		return err
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	es.GetCounterGet().Add(1)

	return nil
}

// List data.
//
// NOTE: It uses param.List.Search to query the data.
func (es *ElasticSearch) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
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

	finalParam.Search = `{"match_all" : {} }`

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return err
	}

	//////
	// List.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, "", trgt, finalParam); err != nil {
			return err
		}
	}

	// Create a new search request.
	req := esapi.SearchRequest{
		Index: []string{trgt},
	}

	query, err := buildQuery(finalParam)
	if err != nil {
		return err
	}

	// Set the query.
	req.Body = strings.NewReader(query)

	// Execute the search request.
	res, err := req.Do(context.Background(), es.Client)
	if err != nil {
		return customerror.NewFailedToError(storage.OperationList, customerror.WithError(err))
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return err
	}

	// Parse response body.
	// Iterate the document "hits" returned by API call
	// Instantiate a mapping interface for API response
	var mapResp map[string]interface{}

	// Decode the JSON response and using a pointer
	if err := shared.Decode(res.Body, &mapResp); err != nil {
		return err
	}

	hits := []any{}

	mapHits, ok := mapResp["hits"].(map[string]interface{})["hits"].([]interface{})
	if !ok {
		return customerror.NewFailedToError(
			"unexpected type for 'hits'",
			customerror.WithField("hits", hits),
		)
	}

	for _, hit := range mapHits {
		var doc map[string]interface{}

		if h, ok := hit.(map[string]interface{}); ok {
			doc = h
		} else {
			return customerror.NewFailedToError(
				"unexpected type for 'hit'",
				customerror.WithField("hit", hit),
			)
		}

		source := doc["_source"]
		hits = append(hits, source)
	}

	if err := storage.ParseToStruct(hits, v); err != nil {
		return err
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, "", trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	es.GetCounterList().Add(1)

	return nil
}

// Set data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (es *ElasticSearch) Set(ctx context.Context, id, target string, v any, prm *set.Set, options ...storage.Func[*set.Set]) (string, error) {
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

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return id, err
	}

	//////
	// Set.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return id, err
		}
	}

	valueAsJSON, err := shared.Marshal(v)
	if err != nil {
		return id, err
	}

	res, err := es.Client.Index(
		trgt,
		bytes.NewReader(valueAsJSON),
		es.Client.Index.WithDocumentID(id),
		es.Client.Index.WithContext(ctx),
	)
	if err != nil {
		return id, customerror.NewFailedToError(storage.OperationCreate, customerror.WithError(err))
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return id, err
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return id, err
		}
	}

	//////
	// Metrics.
	//////

	es.GetCounterSet().Add(1)

	return id, nil
}

// Update data.
func (es *ElasticSearch) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
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

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return err
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return err
		}
	}

	valueAsJSON, err := shared.Marshal(v)
	if err != nil {
		return err
	}

	// Call client Update API using esapi.Update().
	res, err := es.Client.Update(
		trgt,
		id,
		bytes.NewReader([]byte(fmt.Sprintf(`{"doc":%s}`, valueAsJSON))),
		es.Client.Update.WithRefresh("true"),
	)
	if err != nil {
		return customerror.NewFailedToError(storage.OperationUpdate, customerror.WithError(err))
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return err
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, es, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	es.GetCounterUpdate().Add(1)

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

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, customerror.NewFailedToError(
			"creating "+name+" client",
			customerror.WithError(err),
		)
	}

	r := retrier.New(retrier.ExponentialBackoff(3, 10*time.Second), nil)

	if err := r.Run(func() error {
		res, err := client.Info(client.Info.WithContext(ctx))
		if err != nil {
			return customerror.NewFailedToError("ping", customerror.WithError(err))
		}

		defer res.Body.Close()

		if res.IsError() {
			return err
		}

		// NOTE: It is critical to both close the response body and to consume it,
		// in order to re-use persistent TCP connections in the default HTTP
		// transport. If you're not interested in the response body, call
		// `io.Copy(io.Discard, res.Body).`
		if _, err := io.Copy(io.Discard, res.Body); err != nil {
			return customerror.NewFailedToError(
				"consume the response body",
				customerror.WithError(err),
			)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	s, err := storage.New(name)
	if err != nil {
		return nil, err
	}

	storage := &ElasticSearch{
		Storage: s,

		Client: client,
		Config: cfg,
		Target: dynamicIndexFunc,
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
