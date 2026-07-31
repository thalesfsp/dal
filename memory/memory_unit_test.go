package memory

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

//////
// Offline (no ENVIRONMENT gating) tests exercising the real memory adapter.
//////

func newTestStorage(t *testing.T) *Memory {
	t.Helper()

	str, err := New(t.Context())
	require.NoError(t, err)
	require.NotNil(t, str)

	return str
}

func TestMemory_Close(t *testing.T) {
	str := newTestStorage(t)

	require.NoError(t, str.Close())
	assert.NoError(t, str.Close())
}

// Happy path: full CRUD round-trip.
func TestMemory_CRUDRoundTrip(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	id, err := str.Create(ctx, "doc-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, "doc-1", id)

	var got shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "doc-1", "", &got, &retrieve.Retrieve{}))
	assert.Equal(t, *shared.TestData, got)

	require.NoError(t, str.Update(ctx, "doc-1", "", shared.UpdatedTestData, &update.Update{}))

	var updated shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "doc-1", "", &updated, &retrieve.Retrieve{}))
	assert.Equal(t, shared.DocumentNameUpdated, updated.Name)

	require.NoError(t, str.Delete(ctx, "doc-1", "", &delete.Delete{}))

	err = str.Retrieve(ctx, "doc-1", "", &got, &retrieve.Retrieve{})
	assert.Error(t, err, "retrieving a deleted document must fail")
}

// Nil params are legal for every operation ("use defaults").
func TestMemory_NilParams(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "nil-prm", "", shared.TestData, nil)
	require.NoError(t, err)

	var got shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "nil-prm", "", &got, nil))

	c, err := str.Count(ctx, "", nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, c, int64(1))

	var lst ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "", &lst, nil))
	assert.NotEmpty(t, lst.Items)

	require.NoError(t, str.Update(ctx, "nil-prm", "", shared.UpdatedTestData, nil))
	require.NoError(t, str.Delete(ctx, "nil-prm", "", nil))
}

// Bad path: empty IDs and missing documents map to proper errors.
func TestMemory_BadPaths(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "", "", shared.TestData, &create.Create{})
	assert.Error(t, err, "empty id must be rejected")

	err = str.Retrieve(ctx, "does-not-exist", "", &shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)

	cE, ok := customerror.To(err)
	require.True(t, ok)
	assert.Equal(t, 404, cE.StatusCode)

	err = str.Delete(ctx, "", "", &delete.Delete{})
	assert.Error(t, err, "empty id must be rejected")
}

// Count and List honor the documented Search glob over keys.
func TestMemory_SearchGlobFiltering(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	for i := range 3 {
		_, err := str.Create(ctx, fmt.Sprintf("user-%d", i), "", shared.TestData, &create.Create{})
		require.NoError(t, err)
	}

	_, err := str.Create(ctx, "order-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	c, err := str.Count(ctx, "", &count.Count{Search: "user-*"})
	require.NoError(t, err)
	assert.Equal(t, int64(3), c)

	var lst ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "", &lst, &list.List{Search: "user-*"}))
	assert.Len(t, lst.Items, 3)

	// Edge: a pattern matching nothing yields zero/empty — not an error.
	c, err = str.Count(ctx, "", &count.Count{Search: "nope-*"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), c)

	var empty ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "", &empty, &list.List{Search: "nope-*"}))
	assert.Empty(t, empty.Items)

	// Edge: a malformed glob is an error, not a silent zero.
	_, err = str.Count(ctx, "", &count.Count{Search: "["})
	assert.Error(t, err)

	err = str.List(ctx, "", &empty, &list.List{Search: "["})
	assert.Error(t, err)
}

type memoryListParamsDoc struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Rank  int    `json:"rank"`
	Drop  string `json:"drop"`
}

func TestMemory_ListParams(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	for i := range 13 {
		doc := &memoryListParamsDoc{
			ID:    fmt.Sprintf("doc-%02d", i),
			Group: []string{"b", "a"}[i%2],
			Rank:  i,
			Drop:  "drop",
		}
		_, err := str.Create(ctx, "params-"+doc.ID, "", doc, nil)
		require.NoError(t, err)
	}

	listDocs := func(t *testing.T, prm *list.List) []memoryListParamsDoc {
		t.Helper()

		var got ResponseList[memoryListParamsDoc]
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

		var got ResponseList[memoryListParamsDoc]
		err := str.List(ctx, "", &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", "sideways"}},
		})
		require.Error(t, err)
		assert.Equal(t, failedBefore+1, str.GetCounterListedFailed().Value())
	})
}

// A value planted directly in the underlying sync.Map with a non-[]byte type
// must surface as an error on Retrieve — not silent success with an untouched
// destination.
func TestMemory_RetrieveWrongTypeErrors(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	client, ok := str.GetClient().(*sync.Map)
	require.True(t, ok)

	client.Store("poisoned", 42)

	var got shared.TestDataS
	err := str.Retrieve(ctx, "poisoned", "", &got, &retrieve.Retrieve{})
	assert.Error(t, err)
}

// Concurrent New/Get/Set on the package singleton must be race-free (this is
// meaningful under -race).
func TestMemory_SingletonConcurrency(t *testing.T) {
	ctx := t.Context()

	str := newTestStorage(t)
	Set(str)

	var wg sync.WaitGroup

	for range 8 {
		wg.Add(3)

		go func() {
			defer wg.Done()

			_, _ = New(ctx)
		}()

		go func() {
			defer wg.Done()

			_ = Get()
		}()

		go func() {
			defer wg.Done()

			Set(str)
		}()
	}

	wg.Wait()
}

// Concurrent CRUD against a single storage instance must be race-free.
func TestMemory_ConcurrentCRUD(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	var wg sync.WaitGroup

	for i := range 16 {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			id := fmt.Sprintf("conc-%d", n)

			_, err := str.Create(ctx, id, "", shared.TestData, &create.Create{})
			assert.NoError(t, err)

			var got shared.TestDataS
			assert.NoError(t, str.Retrieve(ctx, id, "", &got, &retrieve.Retrieve{}))

			_, err = str.Count(ctx, "", &count.Count{Search: "conc-*"})
			assert.NoError(t, err)

			assert.NoError(t, str.Delete(ctx, id, "", &delete.Delete{}))
		}(i)
	}

	wg.Wait()
}
