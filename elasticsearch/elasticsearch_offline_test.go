package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

type elasticsearchBulkTestDocument struct {
	Value string `json:"value"`
}

func elasticsearchBulkTestItems(count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for i := range items {
		items[i] = storage.BulkItem{
			ID:    fmt.Sprintf("bulk-item-%d", i),
			Value: elasticsearchBulkTestDocument{Value: fmt.Sprintf("value-%d", i)},
		}
	}

	return items
}

func TestElasticsearchPrepareChunkErrorBranches(t *testing.T) {
	t.Parallel()

	es := &ElasticSearch{}
	prm := &create.Create{}
	options, err := storage.NewOptions[*create.Create]()
	require.NoError(t, err)
	rejected := errors.New("Elasticsearch pre-hook rejected item")
	options.PreHookFunc = func(
		_ context.Context,
		adapter storage.IStorage,
		id string,
		target string,
		_ any,
		gotPrm *create.Create,
	) error {
		assert.Same(t, es, adapter)
		assert.Equal(t, "documents", target)
		assert.Same(t, prm, gotPrm)
		if id == "reject" {
			return rejected
		}

		return nil
	}
	chunk := []storage.Indexed[storage.BulkItem]{
		{Index: 2, Item: storage.BulkItem{ID: "", Value: "missing"}},
		{Index: 7, Item: storage.BulkItem{ID: "reject", Value: "rejected"}},
		{Index: 12, Item: storage.BulkItem{ID: "unmarshalable", Value: make(chan int)}},
		{Index: 19, Item: storage.BulkItem{ID: "accepted", Value: map[string]string{"value": "ok"}}},
	}
	result := storage.BulkResult{}

	prepared, stoppedAt := es.elasticsearchPrepareChunk(
		t.Context(),
		"documents",
		chunk,
		prm,
		options,
		&result,
	)
	assert.Equal(t, -1, stoppedAt)
	require.Len(t, prepared, 1)
	assert.Equal(t, 19, prepared[0].indexed.Index)
	assert.Equal(t, []byte(`{"value":"ok"}`), prepared[0].body)
	require.Len(t, result.Failures, 3)
	assert.Equal(t, []int{2, 7, 12}, []int{
		result.Failures[0].Index,
		result.Failures[1].Index,
		result.Failures[2].Index,
	})
	assert.Equal(t, "id required", result.Failures[0].Err.Error())
	assert.ErrorIs(t, result.Failures[1].Err, rejected)
	assert.Equal(
		t,
		"failed to "+"to marshal. Original Error: json: unsupported type: chan int",
		result.Failures[2].Err.Error(),
	)
	var unsupportedType *json.UnsupportedTypeError
	assert.ErrorAs(t, result.Failures[2].Err, &unsupportedType)
	assert.Equal(t, reflect.TypeFor[chan int](), unsupportedType.Type)

	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	canceledResult := storage.BulkResult{}
	prepared, stoppedAt = es.elasticsearchPrepareChunk(
		canceledCtx,
		"documents",
		chunk[3:],
		prm,
		options,
		&canceledResult,
	)
	assert.Empty(t, prepared)
	assert.Equal(t, 0, stoppedAt)
	assert.Empty(t, canceledResult.Failures)
}

func TestElasticsearchFailPreparedPreservesEveryOriginalIndex(t *testing.T) {
	t.Parallel()

	cause := errors.New("bulk response could not be decoded")
	prepared := []elasticsearchBulkDocument{
		{indexed: storage.Indexed[storage.BulkItem]{Index: 3, Item: storage.BulkItem{ID: "three"}}},
		{indexed: storage.Indexed[storage.BulkItem]{Index: 17, Item: storage.BulkItem{ID: "seventeen"}}},
		{indexed: storage.Indexed[storage.BulkItem]{Index: 104, Item: storage.BulkItem{ID: "one-oh-four"}}},
	}
	result := storage.BulkResult{Succeeded: []int{1}}

	elasticsearchFailPrepared(&result, prepared, cause)

	assert.Equal(t, []int{1}, result.Succeeded)
	require.Len(t, result.Failures, len(prepared))
	for i, failure := range result.Failures {
		assert.Equal(t, prepared[i].indexed.Index, failure.Index)
		assert.ErrorIs(t, failure.Err, cause)
	}
}

func TestElasticSearchOffline_BulkCreate(t *testing.T) {
	t.Run("happy path crosses chunk limit and reads back", func(t *testing.T) {
		es, _, srv := newTestES(t, "bulk-index")
		items := elasticsearchBulkTestItems(elasticsearchBulkChunkLimit*2 + 5)

		result, err := es.BulkCreate(t.Context(), "", items, nil)
		require.NoError(t, err)
		assert.True(t, result.AllSucceeded())
		assert.False(t, result.Stopped)
		require.Len(t, result.Succeeded, len(items))
		assert.Equal(t, 3, srv.CountPath(http.MethodPost, "/bulk-index/_bulk"))

		for index, item := range items {
			assert.Equal(t, index, result.Succeeded[index])

			var got elasticsearchBulkTestDocument
			require.NoError(t, es.Retrieve(t.Context(), item.ID, "", &got, nil))
			assert.Equal(t, item.Value, got)
		}
	})

	t.Run("empty input succeeds even with canceled context", func(t *testing.T) {
		es, _, _ := newTestES(t, "bulk-empty")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		result, err := es.BulkCreate(ctx, "", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, storage.BulkResult{}, result)
	})

	t.Run("single item", func(t *testing.T) {
		es, _, _ := newTestES(t, "bulk-single")
		items := elasticsearchBulkTestItems(1)

		result, err := es.BulkCreate(t.Context(), "", items, nil)
		require.NoError(t, err)
		assert.Equal(t, []int{0}, result.Succeeded)

		var got elasticsearchBulkTestDocument
		require.NoError(t, es.Retrieve(t.Context(), items[0].ID, "", &got, nil))
		assert.Equal(t, items[0].Value, got)
	})

	t.Run("HTTP 200 per-item errors preserve original indices", func(t *testing.T) {
		es, store, _ := newTestES(t, "bulk-partial")
		items := elasticsearchBulkTestItems(elasticsearchBulkChunkLimit + 35)
		store.rejectIDs[items[7].ID] = http.StatusBadRequest
		store.rejectIDs[items[127].ID] = http.StatusTooManyRequests

		result, err := es.BulkCreate(t.Context(), "", items, nil)
		require.Error(t, err)
		require.Len(t, result.Failures, 2)
		assert.Equal(t, 7, result.Failures[0].Index)
		assert.Equal(t, 127, result.Failures[1].Index)
		assert.False(t, result.Stopped)
		assert.Len(t, result.Succeeded, len(items)-2)

		for index, item := range items {
			var got elasticsearchBulkTestDocument
			retrieveErr := es.Retrieve(t.Context(), item.ID, "", &got, nil)
			if index == 7 || index == 127 {
				require.Error(t, retrieveErr, "failed item %d was written", index)

				continue
			}
			require.NoError(t, retrieveErr, "successful item %d was not written", index)
			assert.Equal(t, item.Value, got)
		}
	})

	t.Run("context cancellation keeps completed chunk", func(t *testing.T) {
		es, _, _ := newTestES(t, "bulk-cancel")
		items := elasticsearchBulkTestItems(elasticsearchBulkChunkLimit + 5)
		ctx, cancel := context.WithCancel(t.Context())
		postHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			cancel()

			return nil
		}

		result, err := es.BulkCreate(ctx, "", items, nil, storage.WithPostHook(postHook))
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, result.Stopped)
		require.Len(t, result.Succeeded, elasticsearchBulkChunkLimit)
		require.Len(t, result.Failures, 5)
		for index := range elasticsearchBulkChunkLimit {
			assert.Equal(t, index, result.Succeeded[index])
			var got elasticsearchBulkTestDocument
			require.NoError(t, es.Retrieve(t.Context(), items[index].ID, "", &got, nil))
		}
		for offset, failure := range result.Failures {
			assert.Equal(t, elasticsearchBulkChunkLimit+offset, failure.Index)
			assert.ErrorIs(t, failure.Err, context.Canceled)
		}
	})
}

//////
// Offline (no ENVIRONMENT gating) tests exercising the real ElasticSearch
// adapter against an in-process fake server (net/http/httptest). They
// complement elasticsearch_test.go's e2e TestNew, which stays gated behind
// shared.Integration and is left untouched.
//////

var (
	errBranchPre  = errors.New("pre-hook boom")
	errBranchPost = errors.New("post-hook boom")
)

type closeTrackingRoundTripper struct {
	next http.RoundTripper

	mu         sync.Mutex
	closeCalls int
}

func (t *closeTrackingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.next.RoundTrip(req)
}

func (t *closeTrackingRoundTripper) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closeCalls++

	if closer, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (t *closeTrackingRoundTripper) CloseCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.closeCalls
}

type failingCloseESTransport struct {
	perform  func(*http.Request) (*http.Response, error)
	closeErr error
}

func (t *failingCloseESTransport) Perform(req *http.Request) (*http.Response, error) {
	return t.perform(req)
}

func (t *failingCloseESTransport) Close(context.Context) error {
	return t.closeErr
}

// newTestES starts a memStore-backed fake server and returns a connected
// ElasticSearch storage pointed at it, along with the store so tests can
// seed documents directly.
func newTestES(t *testing.T, index string) (*ElasticSearch, *memStore, *fakeServer) {
	t.Helper()

	store := newMemStore()
	srv := newFakeServer(t, store.route)

	es, err := NewWithIndex(t.Context(), index, Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)
	require.NotNil(t, es)
	require.NotNil(t, es.Client)

	return es, store, srv
}

func TestElasticSearchOffline_Close(t *testing.T) {
	newStorage := func(t *testing.T) (*ElasticSearch, *closeTrackingRoundTripper) {
		t.Helper()

		srv := newFakeServer(t, infoRoute(nil))
		baseTransport, ok := http.DefaultTransport.(*http.Transport)
		require.True(t, ok)

		transport := &closeTrackingRoundTripper{
			next: baseTransport.Clone(),
		}

		es, err := New(t.Context(), Config{
			Addresses: []string{srv.URL},
			Transport: transport,
		})
		require.NoError(t, err)

		return es, transport
	}

	t.Run("closes client and idle connections and is idempotent", func(t *testing.T) {
		es, transport := newStorage(t)

		require.NoError(t, es.Close())
		require.NoError(t, es.Close())
		assert.Equal(t, 1, transport.CloseCalls(), "Close must release the HTTP transport's idle connections")

		_, err := es.Client.Info()
		require.Error(t, err, "operation after Close must fail because client is closed")
	})

	t.Run("concurrent calls", func(t *testing.T) {
		es, transport := newStorage(t)

		const callers = 8

		errs := make(chan error, callers)
		for range callers {
			go func() {
				errs <- es.Close()
			}()
		}

		for range callers {
			require.NoError(t, <-errs)
		}

		assert.Equal(t, 1, transport.CloseCalls(), "idle connections must be closed exactly once")
	})

	t.Run("propagates client close error once", func(t *testing.T) {
		es, _ := newStorage(t)
		closeErr := errors.New("elasticsearch transport close failed")
		transport := es.Client.Transport
		es.Client.Transport = &failingCloseESTransport{
			perform:  transport.Perform,
			closeErr: closeErr,
		}

		require.ErrorIs(t, es.Close(), closeErr)
		require.NoError(t, es.Close())
	})
}

//////
// New / NewWithIndex / NewWithDynamicIndex.
//////

func TestNew_HappyPath(t *testing.T) {
	srv := newFakeServer(t, infoRoute(nil))

	es, err := New(t.Context(), Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)
	require.NotNil(t, es)
	assert.NotNil(t, es.Client)
	assert.Equal(t, storage.IStorage(es), Get(), "New must publish the singleton")
}

func TestNewWithDynamicIndex_UsesFunc(t *testing.T) {
	srv := newFakeServer(t, infoRoute(nil))

	calls := 0
	dyn := func() string {
		calls++

		return "dynamic-index"
	}

	es, err := NewWithDynamicIndex(t.Context(), dyn, Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)
	assert.Equal(t, "dynamic-index", es.Target())
	assert.Positive(t, calls)
}

func TestNew_Failure_ServerAlwaysErrors(t *testing.T) {
	srv := newFakeServer(t, func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSON(w, http.StatusInternalServerError, `{"error":{"type":"internal","reason":"boom"},"status":500}`)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	es, err := New(ctx, Config{Addresses: []string{srv.URL}})
	require.Error(t, err)
	assert.Nil(t, es)
}

// A closed server (connection refused) with a short-deadline context must
// fail fast instead of hanging on the retrier's exponential backoff.
func TestNew_Failure_ClosedServer(t *testing.T) {
	srv := newFakeServer(t, infoRoute(nil))
	url := srv.URL
	srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()

	es, err := New(ctx, Config{Addresses: []string{url}})

	require.Error(t, err)
	assert.Nil(t, es)
	assert.Less(t, time.Since(start), 5*time.Second, "must fail fast, not hang until the default 10s ping timeout")
}

// elasticsearch.NewClient itself fails (before any network I/O) when both
// Addresses and CloudID are set.
func TestNew_Failure_ClientConstructionError(t *testing.T) {
	es, err := New(t.Context(), Config{
		Addresses: []string{"http://127.0.0.1:1"},
		CloudID:   "name:ZXhhbXBsZS5jb20kYWJj",
	})
	require.Error(t, err)
	assert.Nil(t, es)
}

//////
// CreateIndex / DeleteIndex.
//////

func TestCreateIndex_HappyPath(t *testing.T) {
	es, _, srv := newTestES(t, "")

	err := es.CreateIndex(t.Context(), "myindex", `{"mappings":{}}`)
	require.NoError(t, err)

	last := srv.Last()
	assert.Equal(t, http.MethodPut, last.Method)
	assert.Equal(t, "/myindex", last.Path)
	assert.Equal(t, `{"mappings":{}}`, last.Body)
}

func TestCreateIndex_EmptyName(t *testing.T) {
	es, _, _ := newTestES(t, "")

	err := es.CreateIndex(t.Context(), "", "{}")
	require.Error(t, err)
}

func TestCreateIndex_ServerError_Ignored(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.Method == http.MethodPut {
			writeJSON(w, http.StatusBadRequest,
				`{"error":{"type":"resource_already_exists_exception","reason":"index already exists"},"status":400}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	err = es.CreateIndex(t.Context(), "myindex", "{}")
	require.NoError(t, err, "an 'already exists' error must be ignored")
}

func TestCreateIndex_ServerError_NotIgnored(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.Method == http.MethodPut {
			writeJSON(w, http.StatusBadRequest,
				`{"error":{"type":"mapper_parsing_exception","reason":"failed to parse mapping"},"status":400}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	err = es.CreateIndex(t.Context(), "myindex", "{}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse mapping")
}

func TestCreateIndex_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "")
	srv.Close()

	err := es.CreateIndex(t.Context(), "myindex", "{}")
	require.Error(t, err)
}

func TestDeleteIndex_HappyPath(t *testing.T) {
	es, _, srv := newTestES(t, "")

	err := es.DeleteIndex(t.Context(), "myindex")
	require.NoError(t, err)

	last := srv.Last()
	assert.Equal(t, http.MethodDelete, last.Method)
	assert.Equal(t, "/myindex", last.Path)
}

func TestDeleteIndex_EmptyName(t *testing.T) {
	es, _, _ := newTestES(t, "")

	err := es.DeleteIndex(t.Context(), "")
	require.Error(t, err)
}

func TestDeleteIndex_ServerError_Ignored(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.Method == http.MethodDelete {
			writeJSON(w, http.StatusNotFound,
				`{"error":{"type":"index_not_found_exception","reason":"no such index [myindex]"},"status":404}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	err = es.DeleteIndex(t.Context(), "myindex")
	require.NoError(t, err, "a 'no such index' error must be ignored")
}

func TestDeleteIndex_ServerError_NotIgnored(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.Method == http.MethodDelete {
			writeJSON(w, http.StatusInternalServerError,
				`{"error":{"type":"internal","reason":"disk on fire"},"status":500}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	err = es.DeleteIndex(t.Context(), "myindex")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk on fire")
}

func TestDeleteIndex_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "")
	srv.Close()

	err := es.DeleteIndex(t.Context(), "myindex")
	require.Error(t, err)
}

//////
// Count.
//////

func TestCount_HappyPath_TrackTotalHits(t *testing.T) {
	es, store, srv := newTestES(t, "idx")

	store.mu.Lock()
	store.docs["idx"] = map[string]map[string]any{
		"1": {"name": "a"},
		"2": {"name": "b"},
	}
	store.mu.Unlock()

	got, err := es.Count(t.Context(), "idx", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 2, got)

	last := srv.Last()
	assert.Equal(t, "/idx/_search", last.Path)
	assert.Contains(t, last.Body, `"track_total_hits": true`)
}

func TestCount_WithRouting(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	_, err := es.Count(t.Context(), "idx", &count.Count{
		Search:  `{"match_all":{}}`,
		Routing: []string{"r1", "r2"},
	})
	require.NoError(t, err)

	last := srv.Last()
	assert.Contains(t, last.Query, "routing=r1%2Cr2")
}

func TestCount_OptionError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Count(t.Context(), "idx", nil, storage.WithPreHook[*count.Count](nil))
	require.Error(t, err)
}

func TestCount_PreHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Count(t.Context(), "idx", nil,
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestCount_PostHookErrorAndSuccess(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Count(t.Context(), "idx", nil,
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var gotData any

	c, err := es.Count(t.Context(), "idx", nil,
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, data any, _ *count.Count) error {
			gotData = data

			return nil
		}))
	require.NoError(t, err)
	assert.Equal(t, 0, gotData)
	assert.EqualValues(t, 0, c)
}

func TestCount_TargetResolutionFailure(t *testing.T) {
	es, _, _ := newTestES(t, "")

	_, err := es.Count(t.Context(), "", nil)
	require.Error(t, err)
}

func TestCount_EmptySearchKeepsDefault(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Count(t.Context(), "idx", &count.Count{Search: ""})
	require.NoError(t, err)
}

func TestCount_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "idx")
	srv.Close()

	_, err := es.Count(t.Context(), "idx", nil)
	require.Error(t, err)
}

// Regression: Count used to decode the response without checking
// `res.IsError()` first, silently returning (0, nil) on an ES-side error.
func TestCount_ServerErrorResponse(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "_search") {
			writeJSON(w, http.StatusBadRequest,
				`{"error":{"type":"parsing_exception","reason":"bad query"},"status":400}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	got, err := es.Count(t.Context(), "idx", nil)
	require.Error(t, err, "an ES-side error response must not be silently swallowed")
	assert.Equal(t, int64(0), got)
	assert.Contains(t, err.Error(), "bad query")
}

//////
// Delete.
//////

func TestDelete_EmptyID(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Delete(t.Context(), "", "idx", &delete.Delete{})
	require.Error(t, err)
}

func TestDelete_OptionError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Delete(t.Context(), "id-1", "idx", &delete.Delete{}, storage.WithPreHook[*delete.Delete](nil))
	require.Error(t, err)
}

func TestDelete_PreHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Delete(t.Context(), "id-1", "idx", &delete.Delete{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestDelete_PostHookErrorAndSuccess(t *testing.T) {
	es, store, _ := newTestES(t, "idx")

	store.mu.Lock()
	store.docs["idx"] = map[string]map[string]any{"id-1": {"name": "a"}}
	store.mu.Unlock()

	err := es.Delete(t.Context(), "id-1", "idx", &delete.Delete{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPost
		}))
	require.Error(t, err)

	store.mu.Lock()
	store.docs["idx"]["id-1"] = map[string]any{"name": "a"}
	store.mu.Unlock()

	var called bool

	err = es.Delete(t.Context(), "id-1", "idx", &delete.Delete{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}

func TestDelete_NotFound404(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Delete(t.Context(), "missing", "idx", &delete.Delete{})
	require.Error(t, err)
	assert.True(t, customerror.IsHTTPStatus(err, http.StatusNotFound))
}

func TestDelete_RoutingPropagation(t *testing.T) {
	es, store, srv := newTestES(t, "idx")

	store.mu.Lock()
	store.docs["idx"] = map[string]map[string]any{"id-1": {"name": "a"}}
	store.mu.Unlock()

	err := es.Delete(t.Context(), "id-1", "idx", &delete.Delete{Routing: "myroute"})
	require.NoError(t, err)

	assert.Contains(t, srv.Last().Query, "routing=myroute")
}

func TestDelete_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "idx")
	srv.Close()

	err := es.Delete(t.Context(), "id-1", "idx", &delete.Delete{})
	require.Error(t, err)
}

func TestDelete_TargetResolutionFailure(t *testing.T) {
	es, _, _ := newTestES(t, "")

	err := es.Delete(t.Context(), "id-1", "", &delete.Delete{})
	require.Error(t, err)
}

//////
// Retrieve.
//////

func TestRetrieve_EmptyID(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Retrieve(t.Context(), "", "idx", &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

func TestRetrieve_OptionError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Retrieve(t.Context(), "id-1", "idx", &shared.TestDataWithIDS{}, &retrieve.Retrieve{}, storage.WithPreHook[*retrieve.Retrieve](nil))
	require.Error(t, err)
}

func TestRetrieve_PreHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Retrieve(t.Context(), "id-1", "idx", &shared.TestDataWithIDS{}, &retrieve.Retrieve{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestRetrieve_HappyPathAndPostHook(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	id, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, "id-1", id)

	var got shared.TestDataWithIDS

	var called bool

	err = es.Retrieve(t.Context(), "id-1", "idx", &got, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, *shared.TestDataWithID, got)
}

func TestRetrieve_PostHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)

	err = es.Retrieve(t.Context(), "id-1", "idx", &shared.TestDataWithIDS{}, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)
}

func TestRetrieve_NotFound404(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Retrieve(t.Context(), "missing", "idx", &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
	require.Error(t, err)
	assert.True(t, customerror.IsHTTPStatus(err, http.StatusNotFound))
}

func TestRetrieve_RoutingPropagation(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)

	err = es.Retrieve(t.Context(), "id-1", "idx", &shared.TestDataWithIDS{}, &retrieve.Retrieve{Routing: "myroute"})
	require.NoError(t, err)
	assert.Contains(t, srv.Last().Query, "routing=myroute")
}

func TestRetrieve_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "idx")
	srv.Close()

	err := es.Retrieve(t.Context(), "id-1", "idx", &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

func TestRetrieve_TargetResolutionFailure(t *testing.T) {
	es, _, _ := newTestES(t, "")

	err := es.Retrieve(t.Context(), "id-1", "", &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

// storage.ParseToStruct fails when the destination isn't a pointer.
func TestRetrieve_ParseToStructFailure(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)

	err = es.Retrieve(t.Context(), "id-1", "idx", shared.TestDataWithIDS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

//////
// List.
//////

func TestList_DefaultSearch_HappyPath(t *testing.T) {
	es, store, _ := newTestES(t, "idx")

	store.mu.Lock()
	store.docs["idx"] = map[string]map[string]any{
		"1": {"name": "a"},
	}
	store.mu.Unlock()

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, nil)
	require.NoError(t, err)
	assert.Len(t, items, 1)
}

// Exercises every buildQuery branch reachable from List: fields, sort,
// offset, limit, and a pointer *ListAny with TrackTotalHits.
func TestList_FullParams_AllBuildQueryBranches(t *testing.T) {
	es, store, srv := newTestES(t, "idx")

	store.mu.Lock()
	store.docs["idx"] = map[string]map[string]any{
		"1": {"name": "a"},
	}
	store.mu.Unlock()

	prm := &list.List{
		Search: `{"match_all":{}}`,
		Fields: []string{"id", "name"},
		Sort:   [][]string{{"id", customsort.Asc}},
		Offset: 1,
		Limit:  5,
		Any:    &ListAny{TrackTotalHits: true},
	}

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, prm)
	require.NoError(t, err)
	assert.EqualValues(t, 1, prm.Count)

	body := srv.Last().Body
	assert.Contains(t, body, `"_source"`)
	assert.Contains(t, body, `"sort"`)
	assert.Contains(t, body, `"from": 1`)
	assert.Contains(t, body, `"size": 5`)
	assert.Contains(t, body, `"track_total_hits": true`)
}

// The `Any` field also accepts a ListAny value (not just a pointer).
func TestList_AnyValueListAny(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	prm := &list.List{
		Search: `{"match_all":{}}`,
		Any:    ListAny{TrackTotalHits: true},
	}

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, prm)
	require.NoError(t, err)
	assert.Contains(t, srv.Last().Body, `"track_total_hits": true`)
}

func TestList_EmptySearchKeepsDefault(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, &list.List{Search: ""})
	require.NoError(t, err)
}

func TestList_OptionError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, nil, storage.WithPreHook[*list.List](nil))
	require.Error(t, err)
}

func TestList_PreHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, nil,
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestList_PostHookErrorAndSuccess(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, nil,
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = es.List(t.Context(), "idx", &items, nil,
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}

func TestList_RoutingPropagation(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, &list.List{
		Search:  `{"match_all":{}}`,
		Routing: []string{"r1", "r2"},
	})
	require.NoError(t, err)
	assert.Contains(t, srv.Last().Query, "routing=r1%2Cr2")
}

func TestList_TargetResolutionFailure(t *testing.T) {
	es, _, _ := newTestES(t, "")

	var items []shared.TestDataS

	err := es.List(t.Context(), "", &items, nil)
	require.Error(t, err)
}

func TestList_ServerErrorResponse(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "_search") {
			writeJSON(w, http.StatusInternalServerError,
				`{"error":{"type":"search_phase_execution_exception","reason":"all shards failed","caused_by":{"type":"x","reason":"root problem"}},"status":500}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	var items []shared.TestDataS

	err = es.List(t.Context(), "idx", &items, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "root problem")
}

func TestList_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "idx")
	srv.Close()

	var items []shared.TestDataS

	err := es.List(t.Context(), "idx", &items, nil)
	require.Error(t, err)
}

// Regression: a response whose top-level "hits" field is missing/not an
// object used to panic via an unchecked chained type assertion.
func TestList_HitsFieldMissing_NoPanic(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "_search") {
			writeJSON(w, http.StatusOK, `{}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	var items []shared.TestDataS

	require.NotPanics(t, func() {
		err = es.List(t.Context(), "idx", &items, nil)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected type for 'hits'")
}

// A hit that isn't a JSON object must error, not panic or get silently
// dropped.
func TestList_HitEntryNotObject(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "_search") {
			writeJSON(w, http.StatusOK, `{"hits":{"total":{"value":1},"hits":["not-an-object"]}}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	var items []shared.TestDataS

	err = es.List(t.Context(), "idx", &items, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected type for 'hit'")
}

func TestList_ParseToStructFailure(t *testing.T) {
	es, store, _ := newTestES(t, "idx")

	store.mu.Lock()
	store.docs["idx"] = map[string]map[string]any{"1": {"name": "a"}}
	store.mu.Unlock()

	err := es.List(t.Context(), "idx", []shared.TestDataS{}, nil)
	require.Error(t, err)
}

//////
// Create.
//////

func TestCreate_HappyPath_UsesPUT(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	id, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, "id-1", id)

	last := srv.Last()
	assert.Equal(t, http.MethodPut, last.Method)
	assert.Equal(t, "/idx/_doc/id-1", last.Path)
}

func TestCreate_EmptyID_UsesPOST(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "", "idx", shared.TestData, &create.Create{})
	require.NoError(t, err)

	last := srv.Last()
	assert.Equal(t, http.MethodPost, last.Method)
	assert.Equal(t, "/idx/_doc", last.Path)
}

func TestCreate_OptionError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestData, &create.Create{}, storage.WithPreHook[*create.Create](nil))
	require.Error(t, err)
}

func TestCreate_PreHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestData, &create.Create{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestCreate_PostHookErrorAndSuccess(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	id, err := es.Create(t.Context(), "id-1", "idx", shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "id-1", id)
}

func TestCreate_MarshalError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", make(chan int), &create.Create{})
	require.Error(t, err)
}

func TestCreate_ServerError(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, _ *http.Request, _ []byte) {
		writeJSON(w, http.StatusBadRequest, `{"error":{"type":"mapper_parsing_exception","reason":"bad doc"},"status":400}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	_, err = es.Create(t.Context(), "id-1", "idx", shared.TestData, &create.Create{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad doc")
}

func TestCreate_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "idx")
	srv.Close()

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestData, &create.Create{})
	require.Error(t, err)
}

func TestCreate_RoutingPropagation(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestData, &create.Create{Routing: "myroute"})
	require.NoError(t, err)
	assert.Contains(t, srv.Last().Query, "routing=myroute")
}

func TestCreate_TargetResolutionFailure(t *testing.T) {
	es, _, _ := newTestES(t, "")

	_, err := es.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.Error(t, err)
}

//////
// Update.
//////

func TestUpdate_EmptyID(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Update(t.Context(), "", "idx", shared.TestData, &update.Update{})
	require.Error(t, err)
}

func TestUpdate_OptionError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Update(t.Context(), "id-1", "idx", shared.TestData, &update.Update{}, storage.WithPreHook[*update.Update](nil))
	require.Error(t, err)
}

func TestUpdate_PreHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Update(t.Context(), "id-1", "idx", shared.TestData, &update.Update{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestUpdate_HappyPathAndPostHook(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)

	var called bool

	err = es.Update(t.Context(), "id-1", "idx", shared.UpdatedTestDataID, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)

	last := srv.Last()
	assert.Equal(t, "/idx/_update/id-1", last.Path)
	assert.Contains(t, last.Body, `"doc":`)
}

func TestUpdate_PostHookError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)

	err = es.Update(t.Context(), "id-1", "idx", shared.UpdatedTestDataID, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)
}

func TestUpdate_MarshalError(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Update(t.Context(), "id-1", "idx", make(chan int), &update.Update{})
	require.Error(t, err)
}

func TestUpdate_NotFound404(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	err := es.Update(t.Context(), "missing", "idx", shared.TestData, &update.Update{})
	require.Error(t, err)
	assert.True(t, customerror.IsHTTPStatus(err, http.StatusNotFound))
}

func TestUpdate_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "idx")
	srv.Close()

	err := es.Update(t.Context(), "id-1", "idx", shared.TestData, &update.Update{})
	require.Error(t, err)
}

func TestUpdate_RoutingPropagation(t *testing.T) {
	es, _, srv := newTestES(t, "idx")

	_, err := es.Create(t.Context(), "id-1", "idx", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)

	err = es.Update(t.Context(), "id-1", "idx", shared.TestData, &update.Update{Routing: "myroute"})
	require.NoError(t, err)
	assert.Contains(t, srv.Last().Query, "routing=myroute")
}

func TestUpdate_TargetResolutionFailure(t *testing.T) {
	es, _, _ := newTestES(t, "")

	err := es.Update(t.Context(), "id-1", "", shared.TestData, &update.Update{})
	require.Error(t, err)
}

//////
// GetClient.
//////

func TestGetClient(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	client, ok := es.GetClient().(*elasticsearch.Client)
	require.True(t, ok)
	assert.Same(t, es.Client, client)
}

//////
// Query (package-level function).
//////

func TestQuery_HappyPath(t *testing.T) {
	es, store, _ := newTestES(t, "idx")

	store.mu.Lock()
	store.docs["idx"] = map[string]map[string]any{"1": {"name": "a"}}
	store.mu.Unlock()

	var result map[string]any

	err := Query(t.Context(), es, "idx", `{"query":{"match_all":{}}}`, &result)
	require.NoError(t, err)
	assert.Contains(t, result, "hits")
}

func TestQuery_CastFailure(t *testing.T) {
	m := &storage.Mock{
		MockGetClient: func() any { return "not-a-client" },
	}

	var result map[string]any

	err := Query(t.Context(), m, "idx", `{}`, &result)
	require.Error(t, err)
}

func TestQuery_TransportError(t *testing.T) {
	es, _, srv := newTestES(t, "idx")
	srv.Close()

	var result map[string]any

	err := Query(t.Context(), es, "idx", `{}`, &result)
	require.Error(t, err)
}

func TestQuery_ServerError(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "_search") {
			writeJSON(w, http.StatusInternalServerError, `{"error":{"type":"internal","reason":"boom"},"status":500}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	var result map[string]any

	err = Query(t.Context(), es, "idx", `{}`, &result)
	require.Error(t, err)
}

func TestQuery_DecodeError(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "_search") {
			writeJSON(w, http.StatusOK, `not-json`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	var result map[string]any

	err = Query(t.Context(), es, "idx", `{}`, &result)
	require.Error(t, err)
}

//////
// Singleton Get/Set.
//////

// Not parallel: mutates the package singleton.
func TestGetSetSingleton(t *testing.T) {
	es, _, _ := newTestES(t, "idx")

	previous := Get()
	t.Cleanup(func() { Set(previous) })

	Set(es)
	assert.Equal(t, storage.IStorage(es), Get())
}

// Not parallel: mutates the package singleton.
func TestGet_PanicsWhenUninitialized(t *testing.T) {
	singletonMutex.RLock()
	previous := singleton
	singletonMutex.RUnlock()

	t.Cleanup(func() { Set(previous) })

	Set(nil)
	require.Panics(t, func() {
		Get()
	})
}

//////
// buildQuery: empty search error branch (not covered by query_test.go's
// table, which only exercises the success path).
//////

func TestBuildQuery_EmptySearchErrors(t *testing.T) {
	_, err := buildQuery(&list.List{})
	require.Error(t, err)
}

// ToElasticSearchString silently skips any pair that isn't exactly
// [field, order].
func TestToElasticSearchString_SkipsMalformedPairs(t *testing.T) {
	got, err := ToElasticSearchString([][]string{{"onlyfield"}, {"b", "desc"}})
	require.NoError(t, err)
	assert.Equal(t, `[{"b": {"order": "desc"}}]`, got)
}

// A single empty-string addon leaves the builder ending in ", " right
// before the closing brace, exercising the trailing-comma cleanup branch.
func TestBuildQuery_TrailingCommaCleanup(t *testing.T) {
	got, err := buildQuery(&list.List{Search: matchAll}, "")
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(got), &decoded), "query must still be valid JSON: %s", got)
}

// Regression: `hits` present as an object but without its own `hits` array
// must error, not panic.
func TestList_HitsSubfieldMissing_NoPanic(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if strings.HasSuffix(r.URL.Path, "_search") {
			writeJSON(w, http.StatusOK, `{"hits":{"total":{"value":1}}}`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	var items []shared.TestDataS

	require.NotPanics(t, func() {
		err = es.List(t.Context(), "idx", &items, nil)
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected type for 'hits'")
}

// checkResponseIsError must surface a decode failure (malformed JSON error
// body) instead of swallowing it.
func TestDelete_MalformedErrorBody(t *testing.T) {
	srv := newFakeServer(t, infoRoute(func(w http.ResponseWriter, r *http.Request, _ []byte) {
		if r.Method == http.MethodDelete {
			writeJSON(w, http.StatusInternalServerError, `not-json`)

			return
		}

		writeJSON(w, http.StatusNotFound, `{}`)
	}))

	es, err := NewWithIndex(t.Context(), "idx", Config{Addresses: []string{srv.URL}})
	require.NoError(t, err)

	err = es.Delete(t.Context(), "id-1", "idx", &delete.Delete{})
	require.Error(t, err)
}
