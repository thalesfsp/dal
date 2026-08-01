package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

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

//////
// Offline (no ENVIRONMENT gating) tests exercising the real Redis adapter
// against an in-process fake RESP server. See fakeserver_test.go.
//////

// newOfflineStorage starts a fake server and connects the adapter to it.
func newOfflineStorage(t *testing.T) (*fakeRedis, *Redis) {
	t.Helper()

	f := startFakeRedis(t)

	str, err := New(t.Context(), &Config{Addr: f.Addr()})
	require.NoError(t, err)
	require.NotNil(t, str)

	return f, str
}

type closeErrorConn struct {
	net.Conn

	err error
}

func (c *closeErrorConn) Close() error {
	_ = c.Conn.Close()

	return c.err
}

// errHookBoom is returned by failing test hooks.
var errHookBoom = errors.New("hook boom")

type redisBulkTestDocument struct {
	Value string `json:"value"`
}

func redisBulkTestItems(count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for i := range items {
		items[i] = storage.BulkItem{
			ID:    fmt.Sprintf("bulk-item-%d", i),
			Value: redisBulkTestDocument{Value: fmt.Sprintf("value-%d", i)},
		}
	}

	return items
}

func TestRedisPrepareChunkErrorBranches(t *testing.T) {
	t.Parallel()

	str := &Redis{}
	prm := &create.Create{}
	options, err := storage.NewOptions[*create.Create]()
	require.NoError(t, err)
	rejected := errors.New("Redis pre-hook rejected item")
	options.PreHookFunc = func(
		_ context.Context,
		adapter storage.IStorage,
		id string,
		target string,
		_ any,
		gotPrm *create.Create,
	) error {
		assert.Same(t, str, adapter)
		assert.Equal(t, "namespace", target)
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

	prepared, stoppedAt := str.redisPrepareChunk(
		t.Context(),
		"namespace",
		chunk,
		prm,
		options,
		&result,
	)
	assert.Equal(t, -1, stoppedAt)
	require.Len(t, prepared, 1)
	assert.Equal(t, 19, prepared[0].indexed.Index)
	assert.Equal(t, []byte(`{"value":"ok"}`), prepared[0].value)
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
	prepared, stoppedAt = str.redisPrepareChunk(
		canceledCtx,
		"namespace",
		chunk[3:],
		prm,
		options,
		&canceledResult,
	)
	assert.Empty(t, prepared)
	assert.Equal(t, 0, stoppedAt)
	assert.Empty(t, canceledResult.Failures)
}

func TestRedisOffline_BulkCreate(t *testing.T) {
	t.Run("happy path crosses pipeline chunk limit and reads back", func(t *testing.T) {
		_, str := newOfflineStorage(t)
		items := redisBulkTestItems(redisBulkChunkLimit*2 + 5)

		result, err := str.BulkCreate(t.Context(), "", items, nil)
		require.NoError(t, err)
		assert.True(t, result.AllSucceeded())
		assert.False(t, result.Stopped)
		require.Len(t, result.Succeeded, len(items))

		for index, item := range items {
			assert.Equal(t, index, result.Succeeded[index])

			var got redisBulkTestDocument
			require.NoError(t, str.Retrieve(t.Context(), item.ID, "", &got, nil))
			assert.Equal(t, item.Value, got)
		}
	})

	t.Run("empty input succeeds even with canceled context", func(t *testing.T) {
		_, str := newOfflineStorage(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		result, err := str.BulkCreate(ctx, "", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, storage.BulkResult{}, result)
	})

	t.Run("single item", func(t *testing.T) {
		_, str := newOfflineStorage(t)
		items := redisBulkTestItems(1)

		result, err := str.BulkCreate(t.Context(), "", items, nil)
		require.NoError(t, err)
		assert.Equal(t, []int{0}, result.Succeeded)

		var got redisBulkTestDocument
		require.NoError(t, str.Retrieve(t.Context(), items[0].ID, "", &got, nil))
		assert.Equal(t, items[0].Value, got)
	})

	t.Run("per-command errors preserve original indices", func(t *testing.T) {
		fake, str := newOfflineStorage(t)
		items := redisBulkTestItems(redisBulkChunkLimit + 35)
		fake.mu.Lock()
		fake.rejectSetKeys[items[7].ID] = true
		fake.rejectSetKeys[items[127].ID] = true
		fake.mu.Unlock()

		result, err := str.BulkCreate(t.Context(), "", items, nil)
		require.Error(t, err)
		require.Len(t, result.Failures, 2)
		assert.Equal(t, 7, result.Failures[0].Index)
		assert.Equal(t, 127, result.Failures[1].Index)
		assert.False(t, result.Stopped)
		assert.Len(t, result.Succeeded, len(items)-2)

		for index, item := range items {
			var got redisBulkTestDocument
			retrieveErr := str.Retrieve(t.Context(), item.ID, "", &got, nil)
			if index == 7 || index == 127 {
				require.Error(t, retrieveErr, "failed item %d was written", index)

				continue
			}
			require.NoError(t, retrieveErr, "successful item %d was not written", index)
			assert.Equal(t, item.Value, got)
		}
	})

	t.Run("context cancellation keeps completed pipeline chunk", func(t *testing.T) {
		_, str := newOfflineStorage(t)
		items := redisBulkTestItems(redisBulkChunkLimit + 5)
		ctx, cancel := context.WithCancel(t.Context())
		postHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			cancel()

			return nil
		}

		result, err := str.BulkCreate(ctx, "", items, nil, storage.WithPostHook(postHook))
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, result.Stopped)
		require.Len(t, result.Succeeded, redisBulkChunkLimit)
		require.Len(t, result.Failures, 5)
		for index := range redisBulkChunkLimit {
			assert.Equal(t, index, result.Succeeded[index])
			var got redisBulkTestDocument
			require.NoError(t, str.Retrieve(t.Context(), items[index].ID, "", &got, nil))
		}
		for offset, failure := range result.Failures {
			assert.Equal(t, redisBulkChunkLimit+offset, failure.Index)
			assert.ErrorIs(t, failure.Err, context.Canceled)
		}
	})
}

func TestRedisOffline_Close(t *testing.T) {
	t.Run("closes client and is idempotent", func(t *testing.T) {
		_, str := newOfflineStorage(t)

		require.NoError(t, str.Close())
		require.NoError(t, str.Close())

		_, err := str.Client.Ping(t.Context()).Result()
		require.Error(t, err, "operation after Close must fail because client is closed")
	})

	t.Run("concurrent calls", func(t *testing.T) {
		_, str := newOfflineStorage(t)

		const callers = 8

		errs := make(chan error, callers)
		for range callers {
			go func() {
				errs <- str.Close()
			}()
		}

		for range callers {
			require.NoError(t, <-errs)
		}
	})

	t.Run("propagates client close error once", func(t *testing.T) {
		f := startFakeRedis(t)
		closeErr := errors.New("redis connection close failed")
		dialer := &net.Dialer{}
		cfg := &Config{
			Addr: f.Addr(),
			Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}

				return &closeErrorConn{Conn: conn, err: closeErr}, nil
			},
		}

		str, err := New(t.Context(), cfg)
		require.NoError(t, err)

		require.ErrorIs(t, str.Close(), closeErr)
		require.NoError(t, str.Close())
	})
}

// Happy path: full CRUD round-trip, plus Count/List with glob patterns.
func TestRedisOffline_CRUDRoundTrip(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	createdBefore := str.GetCounterCreated().Value()

	id, err := str.Create(ctx, shared.DocumentID, "", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, shared.DocumentID, id)
	assert.Equal(t, createdBefore+1, str.GetCounterCreated().Value())

	var got shared.TestDataWithIDS

	require.NoError(t, str.Retrieve(ctx, id, "", &got, &retrieve.Retrieve{}))
	assert.Equal(t, *shared.TestDataWithID, got)

	require.NoError(t, str.Update(ctx, id, "", shared.UpdatedTestDataID, &update.Update{}))

	var updated shared.TestDataWithIDS

	require.NoError(t, str.Retrieve(ctx, id, "", &updated, &retrieve.Retrieve{}))
	assert.Equal(t, shared.DocumentNameUpdated, updated.Name)

	// Count: explicit glob, empty search (falls back to "*"), and no match.
	c, err := str.Count(ctx, "", &count.Count{Search: "VF*"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, c)

	c, err = str.Count(ctx, "", &count.Count{Search: ""})
	require.NoError(t, err)
	assert.EqualValues(t, 1, c)

	c, err = str.Count(ctx, "", &count.Count{Search: "nomatch*"})
	require.NoError(t, err)
	assert.EqualValues(t, 0, c)

	// List: glob match, and no match.
	var docs ResponseList[shared.TestDataWithIDS]

	require.NoError(t, str.List(ctx, "", &docs, &list.List{Search: "VF*"}))
	assert.Equal(t, []shared.TestDataWithIDS{*shared.UpdatedTestDataID}, docs.Items)

	var none ResponseList[shared.TestDataWithIDS]

	require.NoError(t, str.List(ctx, "", &none, &list.List{Search: "zzz*"}))
	assert.Empty(t, none.Items)

	// Delete, then the document must be gone.
	require.NoError(t, str.Delete(ctx, id, "", &delete.Delete{}))

	err = str.Retrieve(ctx, id, "", &got, &retrieve.Retrieve{})
	assert.Error(t, err, "retrieving a deleted document must fail")

	c, err = str.Count(ctx, "", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 0, c)
}

// Nil params are legal for every operation ("use defaults").
func TestRedisOffline_NilParams(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	id, err := str.Create(ctx, "nil-prm", "", shared.TestData, nil)
	require.NoError(t, err)
	assert.Equal(t, "nil-prm", id)

	var got shared.TestDataS

	require.NoError(t, str.Retrieve(ctx, "nil-prm", "", &got, nil))
	assert.Equal(t, *shared.TestData, got)

	c, err := str.Count(ctx, "", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, c)

	var docs ResponseList[shared.TestDataS]

	require.NoError(t, str.List(ctx, "", &docs, nil))
	assert.Equal(t, []shared.TestDataS{*shared.TestData}, docs.Items)

	require.NoError(t, str.Update(ctx, "nil-prm", "", shared.UpdatedTestData, nil))
	require.NoError(t, str.Delete(ctx, "nil-prm", "", nil))
}

// Empty ids are rejected by Create, Retrieve, Update and Delete.
func TestRedisOffline_EmptyID(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	failedBefore := str.GetCounterCreatedFailed().Value()

	id, err := str.Create(ctx, "", "", shared.TestData, nil)
	assert.Error(t, err)
	assert.Empty(t, id)
	assert.Equal(t, failedBefore+1, str.GetCounterCreatedFailed().Value())

	var got shared.TestDataS

	assert.Error(t, str.Retrieve(ctx, "", "", &got, nil))
	assert.Error(t, str.Update(ctx, "", "", shared.TestData, nil))
	assert.Error(t, str.Delete(ctx, "", "", nil))
}

// Retrieve distinguishes missing/empty values from valid empty documents and
// malformed JSON.
func TestRedisOffline_RetrieveEdgeCases(t *testing.T) {
	ctx := t.Context()

	f, str := newOfflineStorage(t)

	f.seed(map[string]string{
		"empty-payload":  "",
		"empty-document": `{}`,
		"bad-document":   `{not-json`,
	})

	var missingDestination shared.TestDataS

	missingErr := str.Retrieve(ctx, shared.DocumentIDInvalid, "", &missingDestination, nil)
	require.Error(t, missingErr, "missing key must fail")
	assert.True(t, customerror.IsHTTPStatus(missingErr, http.StatusNotFound))

	var missingCustomErr *customerror.CustomError
	require.ErrorAs(t, missingErr, &missingCustomErr)
	assert.Equal(t, http.StatusNotFound, missingCustomErr.StatusCode)

	var emptyDestination shared.TestDataS

	emptyErr := str.Retrieve(ctx, "empty-payload", "", &emptyDestination, nil)
	require.Error(t, emptyErr, "empty payload must return NotFound")
	assert.True(t, customerror.IsHTTPStatus(emptyErr, http.StatusNotFound))

	var emptyCustomErr *customerror.CustomError
	require.ErrorAs(t, emptyErr, &emptyCustomErr)
	assert.Equal(t, http.StatusNotFound, emptyCustomErr.StatusCode)

	var emptyDocument *shared.TestDataS

	require.NoError(t, str.Retrieve(ctx, "empty-document", "", &emptyDocument, nil))
	require.NotNil(t, emptyDocument, "decoding {} must populate the destination")
	assert.Equal(t, shared.TestDataS{}, *emptyDocument)

	var malformedDestination shared.TestDataS

	malformedErr := str.Retrieve(ctx, "bad-document", "", &malformedDestination, nil)
	require.Error(t, malformedErr, "malformed JSON must fail")
	assert.False(t, customerror.IsHTTPStatus(malformedErr, http.StatusNotFound))

	var malformedCustomErr *customerror.CustomError
	require.ErrorAs(t, malformedErr, &malformedCustomErr)
	assert.NotEqual(t, missingCustomErr.StatusCode, malformedCustomErr.StatusCode,
		"decode errors and NotFound errors must have different error classifications")

	var malformedSyntaxErr *json.SyntaxError
	require.ErrorAs(t, malformedErr, &malformedSyntaxErr)

	var missingSyntaxErr *json.SyntaxError
	assert.False(t, errors.As(missingErr, &missingSyntaxErr),
		"NotFound and malformed JSON must wrap different error types")

	id, err := str.Create(ctx, "populated-document", "", shared.TestData, nil)
	require.NoError(t, err)

	var populatedDestination shared.TestDataS

	require.NoError(t, str.Retrieve(ctx, id, "", &populatedDestination, nil))
	assert.Equal(t, *shared.TestData, populatedDestination)
}

// Unmarshalable documents make Create and Update fail before hitting Redis.
func TestRedisOffline_MarshalErrors(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	unmarshalable := make(chan int)

	id, err := str.Create(ctx, "marshal-err", "", unmarshalable, nil)
	assert.Error(t, err)
	assert.Empty(t, id)

	assert.Error(t, str.Update(ctx, "marshal-err", "", unmarshalable, nil))
}

// List must fail when the destination cannot hold the response.
func TestRedisOffline_ListParseError(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	var notAPointer string

	assert.Error(t, str.List(ctx, "", notAPointer, nil))
}

// SCAN pagination: enough keys that the iterator walks multiple cursor pages.
func TestRedisOffline_ScanPagination(t *testing.T) {
	ctx := t.Context()

	f, str := newOfflineStorage(t)

	const total = 1050 // pageSize is 100, so 11 SCAN pages.

	kv := make(map[string]string, total)

	for i := range total {
		kv[fmt.Sprintf("bulk:%04d", i)] = `{"name":"test"}`
	}

	f.seed(kv)

	c, err := str.Count(ctx, "", &count.Count{Search: "bulk:*"})
	require.NoError(t, err)
	assert.EqualValues(t, total, c)

	var docs ResponseList[shared.TestDataS]

	require.NoError(t, str.List(ctx, "", &docs, &list.List{Search: "bulk:*"}))
	assert.Len(t, docs.Items, total)
}

type redisListParamsDoc struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Rank  int    `json:"rank"`
	Drop  string `json:"drop"`
}

func TestRedisOffline_ListParams(t *testing.T) {
	ctx := t.Context()
	f, str := newOfflineStorage(t)

	seed := make(map[string]string, 13)
	for i := range 13 {
		seed[fmt.Sprintf("params-%02d", i)] = fmt.Sprintf(
			`{"id":"doc-%02d","group":"%s","rank":%d,"drop":"drop"}`,
			i,
			[]string{"b", "a"}[i%2],
			i,
		)
	}
	f.seed(seed)

	listDocs := func(t *testing.T, prm *list.List) []redisListParamsDoc {
		t.Helper()

		var got ResponseList[redisListParamsDoc]
		require.NoError(t, str.List(ctx, "", &got, prm))

		return got.Items
	}

	t.Run("sort ascending and descending", func(t *testing.T) {
		asc := listDocs(t, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", customsort.Asc}},
		})
		require.Len(t, asc, 13)
		assert.Equal(t, 0, asc[0].Rank)
		assert.Equal(t, 12, asc[12].Rank)

		desc := listDocs(t, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", customsort.Desc}},
		})
		require.Len(t, desc, 13)
		assert.Equal(t, 12, desc[0].Rank)
		assert.Equal(t, 0, desc[12].Rank)
	})

	t.Run("multi-key sort breaks ties with second key", func(t *testing.T) {
		got := listDocs(t, &list.List{
			Search: "params-*",
			Sort: customsort.SortSlice{
				{"group", customsort.Asc},
				{"rank", customsort.Desc},
			},
		})
		require.Len(t, got, 13)
		assert.Equal(t, "a", got[0].Group)
		assert.Equal(t, 11, got[0].Rank)
		assert.Equal(t, 9, got[1].Rank)
		assert.Equal(t, "b", got[6].Group)
		assert.Equal(t, 12, got[6].Rank)
	})

	t.Run("offset alone", func(t *testing.T) {
		assert.Len(t, listDocs(t, &list.List{Search: "params-*", Offset: 3}), 10)
	})

	t.Run("limit alone", func(t *testing.T) {
		assert.Len(t, listDocs(t, &list.List{Search: "params-*", Limit: 4}), 4)
	})

	t.Run("offset and limit", func(t *testing.T) {
		got := listDocs(t, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", customsort.Asc}},
			Offset: 3,
			Limit:  4,
		})
		require.Len(t, got, 4)
		assert.Equal(t, []int{3, 4, 5, 6}, []int{got[0].Rank, got[1].Rank, got[2].Rank, got[3].Rank})
	})

	t.Run("fields projection", func(t *testing.T) {
		var got ResponseList[map[string]any]
		require.NoError(t, str.List(ctx, "", &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", customsort.Asc}},
			Fields: []string{"id"},
		}))
		require.Len(t, got.Items, 13)
		assert.Equal(t, map[string]any{"id": "doc-00"}, got.Items[0])
	})

	t.Run("combined", func(t *testing.T) {
		var got ResponseList[map[string]any]
		require.NoError(t, str.List(ctx, "", &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", customsort.Desc}},
			Offset: 2,
			Limit:  3,
			Fields: []string{"id"},
		}))
		assert.Equal(t, []map[string]any{
			{"id": "doc-10"},
			{"id": "doc-09"},
			{"id": "doc-08"},
		}, got.Items)
	})

	t.Run("list.New default limits to ten", func(t *testing.T) {
		prm, err := list.New()
		require.NoError(t, err)
		prm.Search = "params-*"

		assert.Len(t, listDocs(t, prm), 10)
	})

	t.Run("nil params leave behavior unchanged", func(t *testing.T) {
		assert.Len(t, listDocs(t, nil), 13)
	})

	t.Run("invalid sort order uses normal error path", func(t *testing.T) {
		failedBefore := str.GetCounterListedFailed().Value()

		var got ResponseList[redisListParamsDoc]
		err := str.List(ctx, "", &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", "sideways"}},
		})
		require.Error(t, err)
		assert.Equal(t, failedBefore+1, str.GetCounterListedFailed().Value())
	})
}

// Options and hooks: invalid option, failing pre-hook, failing post-hook and
// successful hooks, for every operation.
func TestRedisOffline_Hooks(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	_, err := str.Create(ctx, "hook-doc", "", shared.TestData, nil)
	require.NoError(t, err)

	t.Run("create", func(t *testing.T) {
		_, err := str.Create(ctx, "hook-doc", "", shared.TestData, nil, storage.WithPreHook[*create.Create](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		_, err = str.Create(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		_, err = str.Create(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPostHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		_, err = str.Create(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("retrieve", func(t *testing.T) {
		var got shared.TestDataS

		err := str.Retrieve(ctx, "hook-doc", "", &got, nil, storage.WithPreHook[*retrieve.Retrieve](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.Retrieve(ctx, "hook-doc", "", &got, nil,
			storage.WithPreHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.Retrieve(ctx, "hook-doc", "", &got, nil,
			storage.WithPostHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.Retrieve(ctx, "hook-doc", "", &got, nil,
			storage.WithPreHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("update", func(t *testing.T) {
		err := str.Update(ctx, "hook-doc", "", shared.TestData, nil, storage.WithPreHook[*update.Update](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.Update(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.Update(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPostHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.Update(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("count", func(t *testing.T) {
		_, err := str.Count(ctx, "", nil, storage.WithPreHook[*count.Count](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		_, err = str.Count(ctx, "", nil,
			storage.WithPreHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		_, err = str.Count(ctx, "", nil,
			storage.WithPostHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		_, err = str.Count(ctx, "", nil,
			storage.WithPreHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("list", func(t *testing.T) {
		var keys ResponseList[shared.TestDataS]

		err := str.List(ctx, "", &keys, nil, storage.WithPreHook[*list.List](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.List(ctx, "", &keys, nil,
			storage.WithPreHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.List(ctx, "", &keys, nil,
			storage.WithPostHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.List(ctx, "", &keys, nil,
			storage.WithPreHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("delete", func(t *testing.T) {
		err := str.Delete(ctx, "hook-doc", "", nil, storage.WithPreHook[*delete.Delete](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.Delete(ctx, "hook-doc", "", nil,
			storage.WithPreHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.Delete(ctx, "hook-doc", "", nil,
			storage.WithPostHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.Delete(ctx, "hook-doc", "", nil,
			storage.WithPreHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})
}

// Every operation must surface server/connection errors.
func TestRedisOffline_ServerDown(t *testing.T) {
	ctx := t.Context()

	f := startFakeRedis(t)

	// MaxRetries -1 disables go-redis command retries, keeping the test fast.
	str, err := New(t.Context(), &Config{Addr: f.Addr(), MaxRetries: -1})
	require.NoError(t, err)

	f.Close()

	_, err = str.Count(ctx, "", nil)
	assert.Error(t, err)

	var keys ResponseList[shared.TestDataS]

	assert.Error(t, str.List(ctx, "", &keys, nil))

	_, err = str.Create(ctx, "down", "", shared.TestData, nil)
	assert.Error(t, err)

	assert.Error(t, str.Update(ctx, "down", "", shared.TestData, nil))

	var got shared.TestDataS

	assert.Error(t, str.Retrieve(ctx, "down", "", &got, nil))
	assert.Error(t, str.Delete(ctx, "down", "", nil))
}

// New: success wires the client and the singleton; failure returns an error.
func TestRedisOffline_NewAndSingleton(t *testing.T) {
	_, str := newOfflineStorage(t)

	client, ok := str.GetClient().(interface{ Close() error })
	assert.True(t, ok)
	assert.NotNil(t, client)
	assert.Same(t, str.Client, str.GetClient())

	// New stores the instance in the singleton.
	assert.Same(t, str, Get())

	// Set replaces the singleton; Get panics when it is empty.
	Set(nil)

	assert.Panics(t, func() { Get() })

	Set(str)

	assert.Same(t, str, Get())
}

// New must fail fast when the server is unreachable.
func TestRedisOffline_NewFailure(t *testing.T) {
	// Grab a loopback port and close it so connections are refused.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	require.NoError(t, listener.Close())

	// The retrier backs off for seconds between ping attempts; the deadline
	// cancels the sleep so the failure path stays fast.
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	str, err := New(ctx, &Config{Addr: addr, MaxRetries: -1})
	assert.Error(t, err)
	assert.Nil(t, str)
}
