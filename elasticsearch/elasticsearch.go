package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/eapache/go-resiliency/retrier"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/envelope"
	"github.com/thalesfsp/dal/v3/internal/logging"
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
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/fields"
	"github.com/thalesfsp/sypl/v2/level"
	"github.com/thalesfsp/validation"
)

//////
// Const, vars, and types.
//////

const (
	// Name of the storage.
	Name = "elasticsearch"

	matchAllQuery = `{"match_all" : {} }`
)

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// DynamicIndexFunc is a function which defines the name of the index, and
// evaluated at the index time.
type DynamicIndexFunc func() string

// Config is the ElasticSearch configuration.
type Config = elasticsearch.Config

// ElasticSearch storage definition.
type ElasticSearch struct {
	*storage.Storage

	// Client is the ElasticSearch client.
	Client *elasticsearch.Client `json:"-" validate:"required,structonly"`

	closeOnce sync.Once

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

// ToElasticSearchString converts the `SortSlice` to ElasticSearch sort format.
func ToElasticSearchString(s customsort.SortSlice) (string, error) {
	sortFields := make([]string, 0, len(s))

	for _, pair := range s {
		if len(pair) != 2 {
			continue
		}

		fieldName, sortOrder := pair[0], pair[1]
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
		sort, err := ToElasticSearchString(params.Sort)
		if err != nil {
			return "", err
		}

		builder.WriteString(`, "sort": `)

		builder.WriteString(sort)
	}

	// Append the offset query parameter, if any.
	if params.Offset > 0 {
		fmt.Fprintf(&builder, `, "from": %d`, params.Offset)
	}

	// Append the limit query parameter, if any.
	if params.Limit > 0 {
		fmt.Fprintf(&builder, `, "size": %d`, params.Limit)
	}

	// Check if prm.Any is type of ListAny (pointer or value).
	var listAny *ListAny

	switch lA := params.Any.(type) {
	case *ListAny:
		listAny = lA
	case ListAny:
		listAny = &lA
	}

	if listAny != nil {
		// Append the track_total_hits query parameter, if any.
		if listAny.TrackTotalHits {
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

// elasticEnvelopeOptions passes the index name resolved inside Run to the
// user's hooks without moving target resolution outside the traced envelope.
func elasticEnvelopeOptions[T any](target *string, options []storage.Func[T]) []storage.Func[T] {
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
	var trgt string

	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        es,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         target,
		Options:        elasticEnvelopeOptions(&trgt, options),
		SuccessCounter: es.GetCounterCounted(),
		FailureCounter: es.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err != nil {
				return nil, err
			}

			params.Search = matchAllQuery
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, es.Target())

			return params, err
		},
	}, func(ctx context.Context, params *count.Count) (envelope.Result[int64], error) {
		req := esapi.SearchRequest{Index: []string{trgt}}
		if params.Routing != nil {
			req.Routing = params.Routing
		}

		query, err := buildQuery(&list.List{
			Search: params.Search,
			Any:    &ListAny{TrackTotalHits: true},
		})
		if err != nil {
			return envelope.Result[int64]{}, err
		}
		req.Body = strings.NewReader(query)

		res, err := req.Do(ctx, es.Client)
		if err != nil {
			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			)
		}
		defer res.Body.Close()

		if err := checkResponseIsError(res); err != nil {
			return envelope.Result[int64]{}, err
		}

		var mapResp map[string]any
		if err := shared.Decode(res.Body, &mapResp); err != nil {
			return envelope.Result[int64]{}, err
		}

		var total int64
		if hits, ok := mapResp["hits"].(map[string]any); ok {
			if totalHits, ok := hits["total"].(map[string]any); ok {
				if value, ok := totalHits["value"].(float64); ok {
					total = int64(value)
				}
			}
		}

		return envelope.Result[int64]{Value: total, PostHookData: int(total)}, nil
	})
}

// Delete removes data.
func (es *ElasticSearch) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	var trgt string

	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        es,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         target,
		Options:        elasticEnvelopeOptions(&trgt, options),
		RequiresID:     true,
		SuccessCounter: es.GetCounterDeleted(),
		FailureCounter: es.GetCounterDeletedFailed(),
		NewParams: func() (*delete.Delete, error) {
			params, err := delete.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, es.Target())

			return params, err
		},
	}, func(ctx context.Context, params *delete.Delete) (envelope.Result[envelope.Void], error) {
		reqOpts := []func(*esapi.DeleteRequest){es.Client.Delete.WithContext(ctx)}
		if params.Routing != "" {
			reqOpts = append(reqOpts, es.Client.Delete.WithRouting(params.Routing))
		}

		res, err := es.Client.Delete(trgt, id, reqOpts...)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			)
		}
		defer res.Body.Close()

		if err := checkResponseIsError(res); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{}, nil
	})

	return err
}

// Retrieve data.
func (es *ElasticSearch) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	var trgt string

	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        es,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        elasticEnvelopeOptions(&trgt, options),
		RequiresID:     true,
		SuccessCounter: es.GetCounterRetrieved(),
		FailureCounter: es.GetCounterRetrievedFailed(),
		NewParams: func() (*retrieve.Retrieve, error) {
			params, err := retrieve.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, es.Target())

			return params, err
		},
	}, func(ctx context.Context, params *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		reqOpts := []func(*esapi.GetRequest){es.Client.Get.WithContext(ctx)}
		if params.Routing != "" {
			reqOpts = append(reqOpts, es.Client.Get.WithRouting(params.Routing))
		}

		res, err := es.Client.Get(trgt, id, reqOpts...)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}
		defer res.Body.Close()

		if err := checkResponseIsError(res); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		getResponse := ResponseSourceFromES{}
		if err := parseResponseBody(res.Body, &getResponse); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		if err := storage.ParseToStruct(getResponse.Data, v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// Search is the Elasticsearch query body. Sort, Limit, Offset, and Fields are
// translated into the search request.
//
//nolint:nestif,gocognit
func (es *ElasticSearch) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	var trgt string

	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        es,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         target,
		HookData:       v,
		Options:        elasticEnvelopeOptions(&trgt, options),
		SuccessCounter: es.GetCounterListed(),
		FailureCounter: es.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err != nil {
				return nil, err
			}

			params.Search = matchAllQuery
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, es.Target())

			return params, err
		},
	}, func(ctx context.Context, params *list.List) (envelope.Result[envelope.Void], error) {
		req := esapi.SearchRequest{Index: []string{trgt}}
		if params.Routing != nil {
			req.Routing = params.Routing
		}

		query, err := buildQuery(params)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}
		req.Body = strings.NewReader(query)

		res, err := req.Do(ctx, es.Client)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			)
		}
		defer res.Body.Close()

		if err := checkResponseIsError(res); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		var mapResp map[string]any
		if err := shared.Decode(res.Body, &mapResp); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		if prm != nil {
			if hits, ok := mapResp["hits"].(map[string]any); ok {
				if totalHits, ok := hits["total"].(map[string]any); ok {
					if value, ok := totalHits["value"].(float64); ok {
						prm.Count = int64(value)
					}
				}
			}
		}

		hits := []any{}
		hitsField, ok := mapResp["hits"].(map[string]any)
		if !ok {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"unexpected type for 'hits'",
				customerror.WithField("hits", mapResp["hits"]),
			)
		}

		mapHits, ok := hitsField["hits"].([]any)
		if !ok {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				"unexpected type for 'hits'",
				customerror.WithField("hits", hits),
			)
		}

		for _, hit := range mapHits {
			doc, ok := hit.(map[string]any)
			if !ok {
				return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
					"unexpected type for 'hit'",
					customerror.WithField("hit", hit),
				)
			}

			hits = append(hits, doc["_source"])
		}

		if err := storage.ParseToStruct(hits, v); err != nil {
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
func (es *ElasticSearch) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	var (
		trgt           string
		transportError bool
	)

	result, err := envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        es,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        elasticEnvelopeOptions(&trgt, options),
		SuccessCounter: es.GetCounterCreated(),
		FailureCounter: es.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, es.Target())

			return params, err
		},
	}, func(ctx context.Context, params *create.Create) (envelope.Result[string], error) {
		valueAsJSON, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[string]{}, err
		}

		reqOpts := []func(*esapi.IndexRequest){
			es.Client.Index.WithDocumentID(id),
			es.Client.Index.WithContext(ctx),
		}
		if params.Routing != "" {
			reqOpts = append(reqOpts, es.Client.Index.WithRouting(params.Routing))
		}

		res, err := es.Client.Index(trgt, bytes.NewReader(valueAsJSON), reqOpts...)
		if err != nil {
			transportError = true

			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}
		defer res.Body.Close()

		if err := checkResponseIsError(res); err != nil {
			return envelope.Result[string]{}, err
		}

		return envelope.Result[string]{Value: id, PostHookData: v}, nil
	})
	if transportError {
		return id, err
	}

	return result, err
}

// Update data.
func (es *ElasticSearch) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	var trgt string

	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        es,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        elasticEnvelopeOptions(&trgt, options),
		RequiresID:     true,
		SuccessCounter: es.GetCounterUpdated(),
		FailureCounter: es.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, es.Target())

			return params, err
		},
	}, func(ctx context.Context, params *update.Update) (envelope.Result[envelope.Void], error) {
		valueAsJSON, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		reqOpts := []func(*esapi.UpdateRequest){es.Client.Update.WithContext(ctx)}
		if params.Routing != "" {
			reqOpts = append(reqOpts, es.Client.Update.WithRouting(params.Routing))
		}

		res, err := es.Client.Update(
			trgt,
			id,
			bytes.NewReader(fmt.Appendf(nil, `{"doc":%s}`, valueAsJSON)),
			reqOpts...,
		)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}
		defer res.Body.Close()

		if err := checkResponseIsError(res); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// GetClient returns the client.
func (es *ElasticSearch) GetClient() any {
	return es.Client
}

// Close releases the resources held by the storage.
func (es *ElasticSearch) Close() error {
	var closeErr error

	es.closeOnce.Do(func() {
		// The client has no socket handle of its own. Close marks its HTTP
		// transport closed, stops discovery, and drains its connection pool.
		closeErr = es.Client.Close(context.Background())

		// A caller-supplied HTTP transport remains owned by the adapter and may
		// retain keep-alive connections after the ES transport shuts down.
		if closer, ok := es.Config.Transport.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	})

	return closeErr
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

	if err := r.RunCtx(ctx, func(ctx context.Context) error {
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

// Query executes a search query against the ElasticSearch storage.
func Query(
	ctx context.Context,
	s storage.IStorage,
	index string,
	query string,
	v any,
) error {
	// Cast the storage to ElasticSearch.
	esClient, ok := s.GetClient().(*elasticsearch.Client)
	if !ok {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("cast storage to elasticsearch.Client"),
			s.GetLogger(),
			s.GetCounterListedFailed(),
		)
	}

	// Execute the search query.
	res, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(index),
		esClient.Search.WithBody(strings.NewReader(query)),
	)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("failed to execute search query", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterListedFailed(),
		)
	}

	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return customapm.TraceError(
			ctx,
			err,
			s.GetLogger(),
			s.GetCounterListedFailed(),
		)
	}

	// Parse the response body into `v`.
	if err := shared.Decode(res.Body, v); err != nil {
		return customapm.TraceError(
			ctx,
			err,
			s.GetLogger(),
			s.GetCounterListedFailed(),
		)
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterListed().Add(1)

	return nil
}
