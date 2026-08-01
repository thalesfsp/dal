package bolt

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
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
	bbolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

type boltBulkDocument struct {
	Value string `json:"value"`
}

func boltBulkItems(count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for index := range items {
		items[index] = storage.BulkItem{
			ID:    fmt.Sprintf("bulk-item-%d", index),
			Value: boltBulkDocument{Value: fmt.Sprintf("value-%d", index)},
		}
	}

	return items
}

func TestBoltBulkCreateHappyPathManyItems(t *testing.T) {
	str := newTestStorage(t)
	items := boltBulkItems(60)

	result, err := str.BulkCreate(t.Context(), "bulk-docs", items, nil)
	require.NoError(t, err)
	require.Equal(t, len(items), len(result.Succeeded))
	require.Empty(t, result.Failures)
	require.False(t, result.Stopped)

	for index, item := range items {
		require.Equal(t, index, result.Succeeded[index])

		var got boltBulkDocument
		require.NoError(t, str.Retrieve(t.Context(), item.ID, "bulk-docs", &got, nil))
		require.Equal(t, item.Value, got)
	}
}

func TestBoltBulkCreateEmptyInput(t *testing.T) {
	str := newTestStorage(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := str.BulkCreate(ctx, "bulk-docs", nil, nil)
	require.NoError(t, err)
	require.Equal(t, storage.BulkResult{}, result)
}

func TestBoltBulkCreateSingleItem(t *testing.T) {
	str := newTestStorage(t)
	items := boltBulkItems(1)

	result, err := str.BulkCreate(t.Context(), "bulk-docs", items, nil)
	require.NoError(t, err)
	require.Equal(t, []int{0}, result.Succeeded)

	var got boltBulkDocument
	require.NoError(t, str.Retrieve(t.Context(), items[0].ID, "bulk-docs", &got, nil))
	require.Equal(t, items[0].Value, got)
}

func TestBoltBulkCreatePartialFailureIsNotAtomic(t *testing.T) {
	str := newTestStorage(t)
	items := boltBulkItems(35)
	rejected := errors.New("rejected by bolt bulk test")
	preHook := func(_ context.Context, _ storage.IStorage, id, _ string, _ any, _ *create.Create) error {
		if id == items[27].ID {
			return rejected
		}

		return nil
	}

	result, err := str.BulkCreate(t.Context(), "bulk-docs", items, nil, storage.WithPreHook(preHook))
	require.Error(t, err)
	require.False(t, result.Stopped)
	require.Len(t, result.Failures, 1)
	if failureIndices := []int{result.Failures[0].Index}; !reflect.DeepEqual(failureIndices, []int{27}) {
		t.Fatalf("failure indices = %v, want [27]", failureIndices)
	}
	require.ErrorIs(t, result.Failures[0].Err, rejected)

	for index, item := range items {
		var got boltBulkDocument
		retrieveErr := str.Retrieve(t.Context(), item.ID, "bulk-docs", &got, nil)
		if index == 27 {
			require.Error(t, retrieveErr)

			continue
		}

		require.NoError(t, retrieveErr, "successful item %d was rolled back", index)
		require.Equal(t, item.Value, got)
	}
}

func TestBoltBulkCreateContextCancellationMidBatch(t *testing.T) {
	str := newTestStorage(t)
	items := boltBulkItems(40)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	attempted := 0
	preHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
		attempted++
		if attempted == 7 {
			cancel()
		}

		return nil
	}

	result, err := str.BulkCreate(ctx, "bulk-docs", items, nil, storage.WithPreHook(preHook))
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, result.Stopped)
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6}, result.Succeeded)
	require.Len(t, result.Failures, len(items)-attempted)

	for index, item := range items {
		var got boltBulkDocument
		retrieveErr := str.Retrieve(t.Context(), item.ID, "bulk-docs", &got, nil)
		if index < attempted {
			require.NoError(t, retrieveErr, "already-written item %d", index)
		} else {
			require.Error(t, retrieveErr, "unattempted item %d", index)
		}
	}
}

func TestBoltCommitBulkCreateChunkWithNoPreparedItemsLeavesOutcomesUntouched(t *testing.T) {
	t.Parallel()

	str := newTestStorage(t)
	want := []boltBulkOutcome{{succeeded: true}, {err: errors.New("existing failure")}}
	outcomes := append([]boltBulkOutcome(nil), want...)

	str.commitBulkCreateChunk(t.Context(), "bulk-docs", nil, outcomes)

	if !reflect.DeepEqual(outcomes, want) {
		t.Fatalf("commitBulkCreateChunk() outcomes = %#v, want %#v", outcomes, want)
	}
}

func TestBoltBulkCreatePostHookFailureReportsCommittedItem(t *testing.T) {
	t.Parallel()

	str := newTestStorage(t)
	items := boltBulkItems(3)
	rejected := errors.New("Bolt post-hook rejected item")
	postHook := func(_ context.Context, _ storage.IStorage, id, _ string, _ any, _ *create.Create) error {
		if id == items[1].ID {
			return rejected
		}

		return nil
	}

	result, err := str.BulkCreate(t.Context(), "bulk-docs", items, nil, storage.WithPostHook(postHook))
	if got, want := err.Error(), "bulk operation completed with 1 unsuccessful item(s)"; got != want {
		t.Fatalf("BulkCreate() error = %q, want %q", got, want)
	}
	if result.Stopped {
		t.Fatal("BulkCreate() result.Stopped = true, want false")
	}
	if got, want := result.Succeeded, []int{0, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("succeeded indices = %v, want %v", got, want)
	}
	require.Len(t, result.Failures, 1)
	assert.Equal(t, 1, result.Failures[0].Index)
	assert.ErrorIs(t, result.Failures[0].Err, rejected)
	assert.ErrorIs(t, err, rejected)

	for index, item := range items {
		var got boltBulkDocument
		require.NoError(t, str.Retrieve(t.Context(), item.ID, "bulk-docs", &got, nil), "committed item %d", index)
		assert.Equal(t, item.Value, got)
	}
}

func TestBoltBulkCreateTransactionFailureFansOutToEveryPreparedItem(t *testing.T) {
	t.Parallel()

	str := newTestStorage(t)
	require.NoError(t, str.Close())
	items := boltBulkItems(3)

	result, err := str.BulkCreate(t.Context(), "bulk-docs", items, nil)
	if got, want := err.Error(), "bulk operation completed with 3 unsuccessful item(s)"; got != want {
		t.Fatalf("BulkCreate() error = %q, want %q", got, want)
	}
	assert.False(t, result.Stopped)
	assert.Empty(t, result.Succeeded)
	require.Len(t, result.Failures, len(items))
	for index, failure := range result.Failures {
		assert.Equal(t, index, failure.Index)
		assert.Equal(t, "failed to create. Original Error: database not open", failure.Err.Error())
		assert.ErrorIs(t, failure.Err, berrors.ErrDatabaseNotOpen)
	}
	assert.ErrorIs(t, err, berrors.ErrDatabaseNotOpen)
}

//////
// Offline tests exercising the real bbolt-backed adapter against a
// `t.TempDir()` database file. No fakes needed: bbolt is pure Go and
// entirely local.
//////

func newTestStorage(t *testing.T) *Bolt {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")

	str, err := New(t.Context(), dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, str)

	t.Cleanup(func() {
		require.NoError(t, str.Close())
	})

	return str
}

// Happy path: full CRUD round-trip.
func TestBolt_CRUDRoundTrip(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	id, err := str.Create(ctx, "doc-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, "doc-1", id)

	var got shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "doc-1", "docs", &got, &retrieve.Retrieve{}))
	assert.Equal(t, *shared.TestData, got)

	require.NoError(t, str.Update(ctx, "doc-1", "docs", shared.UpdatedTestData, &update.Update{}))

	var updated shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "doc-1", "docs", &updated, &retrieve.Retrieve{}))
	assert.Equal(t, shared.DocumentNameUpdated, updated.Name)

	c, err := str.Count(ctx, "docs", &count.Count{Search: "*"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, c)

	var lst ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &lst, &list.List{Search: "*"}))
	require.Len(t, lst.Items, 1)
	assert.Equal(t, updated, lst.Items[0])

	require.NoError(t, str.Delete(ctx, "doc-1", "docs", &delete.Delete{}))

	err = str.Retrieve(ctx, "doc-1", "docs", &got, &retrieve.Retrieve{})
	assert.Error(t, err, "retrieving a deleted document must fail")
}

// Nil params are legal for every operation ("use defaults").
func TestBolt_NilParams(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "nil-prm", "docs", shared.TestData, nil)
	require.NoError(t, err)

	var got shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "nil-prm", "docs", &got, nil))

	c, err := str.Count(ctx, "docs", nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, c, int64(1))

	var lst ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &lst, nil))
	assert.NotEmpty(t, lst.Items)

	require.NoError(t, str.Update(ctx, "nil-prm", "docs", shared.UpdatedTestData, nil))
	require.NoError(t, str.Delete(ctx, "nil-prm", "docs", nil))
}

// Bad path: empty IDs are rejected on every id-taking operation.
func TestBolt_EmptyIDRejection(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "", "docs", shared.TestData, &create.Create{})
	assert.Error(t, err, "empty id must be rejected on Create")

	err = str.Retrieve(ctx, "", "docs", &shared.TestDataS{}, &retrieve.Retrieve{})
	assert.Error(t, err, "empty id must be rejected on Retrieve")

	err = str.Update(ctx, "", "docs", shared.TestData, &update.Update{})
	assert.Error(t, err, "empty id must be rejected on Update")

	err = str.Delete(ctx, "", "docs", &delete.Delete{})
	assert.Error(t, err, "empty id must be rejected on Delete")
}

// Bad path: retrieving a document that was never written returns 404.
func TestBolt_RetrieveNotFound(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "doc-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.Retrieve(ctx, "does-not-exist", "docs", &shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)

	cE, ok := customerror.To(err)
	require.True(t, ok)
	assert.Equal(t, 404, cE.StatusCode)
}

// Bad path: the target (bucket) resolution fails when both the operation
// target and the static Target are empty.
func TestBolt_TargetResolutionFailure(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Count(ctx, "", &count.Count{Search: "*"})
	assert.Error(t, err)

	err = str.Delete(ctx, "id-1", "", &delete.Delete{})
	assert.Error(t, err)

	err = str.Retrieve(ctx, "id-1", "", &shared.TestDataS{}, &retrieve.Retrieve{})
	assert.Error(t, err)

	var lst ResponseList[shared.TestDataS]
	err = str.List(ctx, "", &lst, &list.List{Search: "*"})
	assert.Error(t, err)

	_, err = str.Create(ctx, "id-1", "", shared.TestData, &create.Create{})
	assert.Error(t, err)

	err = str.Update(ctx, "id-1", "", shared.TestData, &update.Update{})
	assert.Error(t, err)
}

// A missing bucket (a target nothing was ever written to) must be a hard
// error on Count/List/Retrieve, not a plausible-looking empty result.
func TestBolt_MissingBucketErrors(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Count(ctx, "does-not-exist", &count.Count{Search: "*"})
	assert.Error(t, err)

	var lst ResponseList[shared.TestDataS]
	err = str.List(ctx, "does-not-exist", &lst, &list.List{Search: "*"})
	assert.Error(t, err)

	err = str.Retrieve(ctx, "id-1", "does-not-exist", &shared.TestDataS{}, &retrieve.Retrieve{})
	assert.Error(t, err)

	// Delete against a bucket that was never created is an idempotent
	// success, consistent with the other adapters' Delete semantics.
	err = str.Delete(ctx, "id-1", "does-not-exist", &delete.Delete{})
	assert.NoError(t, err)
}

// Count and List honor the documented Search glob over keys.
func TestBolt_SearchGlobFiltering(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	for i := range 3 {
		_, err := str.Create(ctx, fmt.Sprintf("user-%d", i), "docs", shared.TestData, &create.Create{})
		require.NoError(t, err)
	}

	_, err := str.Create(ctx, "order-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	c, err := str.Count(ctx, "docs", &count.Count{Search: "user-*"})
	require.NoError(t, err)
	assert.Equal(t, int64(3), c)

	var lst ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &lst, &list.List{Search: "user-*"}))
	assert.Len(t, lst.Items, 3)

	// Edge: a pattern matching nothing yields zero/empty — not an error.
	c, err = str.Count(ctx, "docs", &count.Count{Search: "nope-*"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), c)

	var empty ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &empty, &list.List{Search: "nope-*"}))
	assert.Empty(t, empty.Items)

	// Edge: a malformed glob is an error, not a silent zero.
	_, err = str.Count(ctx, "docs", &count.Count{Search: "["})
	assert.Error(t, err)

	err = str.List(ctx, "docs", &empty, &list.List{Search: "["})
	assert.Error(t, err)
}

// List honors Limit (>0) by capping the number of matches returned, walking
// keys in bbolt's cursor order (lexicographic).
func TestBolt_ListLimit(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	for i := range 5 {
		_, err := str.Create(ctx, fmt.Sprintf("item-%d", i), "docs", shared.TestData, &create.Create{})
		require.NoError(t, err)
	}

	var lst ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &lst, &list.List{Search: "*", Limit: 2}))
	assert.Len(t, lst.Items, 2)

	// Limit <= 0 means unlimited.
	var all ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &all, &list.List{Search: "*", Limit: 0}))
	assert.Len(t, all.Items, 5)
}

type boltListParamsDoc struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Rank  int    `json:"rank"`
	Drop  string `json:"drop"`
}

func seedBoltListParams(t *testing.T, str *Bolt, target string) {
	t.Helper()

	for i := range 13 {
		doc := &boltListParamsDoc{
			ID:    fmt.Sprintf("doc-%02d", i),
			Group: []string{"b", "a"}[i%2],
			Rank:  i,
			Drop:  "drop",
		}
		_, err := str.Create(t.Context(), "params-"+doc.ID, target, doc, nil)
		require.NoError(t, err)
	}
}

func TestBolt_ListParams(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)
	seedBoltListParams(t, str, "docs")

	listDocs := func(t *testing.T, prm *list.List) []boltListParamsDoc {
		t.Helper()

		var got ResponseList[boltListParamsDoc]
		require.NoError(t, str.List(ctx, "docs", &got, prm))

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
		require.NoError(t, str.List(ctx, "docs", &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", customsort.Asc}},
			Fields: []string{"id"},
		}))
		require.Len(t, got.Items, 13)
		assert.Equal(t, map[string]any{"id": "doc-00"}, got.Items[0])
	})

	t.Run("combined", func(t *testing.T) {
		var got ResponseList[map[string]any]
		require.NoError(t, str.List(ctx, "docs", &got, &list.List{
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

		var got ResponseList[boltListParamsDoc]
		err := str.List(ctx, "docs", &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", "sideways"}},
		})
		require.Error(t, err)
		assert.Equal(t, failedBefore+1, str.GetCounterListedFailed().Value())
	})
}

func TestBolt_ListSortsFullMatchedSetBeforeLimit(t *testing.T) {
	str := newTestStorage(t)
	seedBoltListParams(t, str, "docs")

	var got ResponseList[boltListParamsDoc]
	require.NoError(t, str.List(t.Context(), "docs", &got, &list.List{
		Search: "params-*",
		Sort:   customsort.SortSlice{{"rank", customsort.Desc}},
		Limit:  3,
	}))

	require.Len(t, got.Items, 3)
	assert.Equal(t, []int{12, 11, 10}, []int{
		got.Items[0].Rank,
		got.Items[1].Rank,
		got.Items[2].Rank,
	})
}

// Two different targets are independent buckets: same id, different data.
func TestBolt_MultipleTargets(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "id-1", "bucket-a", shared.TestData, &create.Create{})
	require.NoError(t, err)

	_, err = str.Create(ctx, "id-1", "bucket-b", shared.UpdatedTestData, &create.Create{})
	require.NoError(t, err)

	var a, b shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "id-1", "bucket-a", &a, &retrieve.Retrieve{}))
	require.NoError(t, str.Retrieve(ctx, "id-1", "bucket-b", &b, &retrieve.Retrieve{}))

	assert.Equal(t, *shared.TestData, a)
	assert.Equal(t, *shared.UpdatedTestData, b)
}

// GetClient returns the live *bbolt.DB handle, giving callers an escape
// hatch to the driver for anything the interface doesn't cover.
func TestBolt_GetClient(t *testing.T) {
	str := newTestStorage(t)

	client, ok := str.GetClient().(*bbolt.DB)
	require.True(t, ok)
	assert.NotNil(t, client)
}

// bbolt rejects keys over MaxKeySize; both Create and Update must surface
// that as a wrapped storage error instead of panicking or losing it.
func TestBolt_Create_Update_KeyTooLarge(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	tooLarge := make([]byte, bbolt.MaxKeySize+1)
	for i := range tooLarge {
		tooLarge[i] = 'a'
	}

	id := string(tooLarge)

	_, err := str.Create(ctx, id, "docs", shared.TestData, &create.Create{})
	require.Error(t, err)

	err = str.Update(ctx, id, "docs", shared.TestData, &update.Update{})
	require.Error(t, err)
}

// Unmarshal failure: a non-pointer destination.
func TestBolt_Retrieve_UnmarshalFailure(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "id-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.Retrieve(ctx, "id-1", "docs", shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

// Unmarshal failure on List: a non-pointer destination.
func TestBolt_List_UnmarshalFailure(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "id-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.List(ctx, "docs", ResponseList[shared.TestDataS]{}, &list.List{Search: "*"})
	require.Error(t, err)
}

// json.Marshal fails on unmarshalable values, such as a channel.
func TestBolt_Create_MarshalFailure(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	_, err := str.Create(ctx, "id-1", "docs", make(chan int), &create.Create{})
	require.Error(t, err)
}

// json.Marshal fails on unmarshalable values, such as a channel.
func TestBolt_Update_MarshalFailure(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	err := str.Update(ctx, "id-1", "docs", make(chan int), &update.Update{})
	require.Error(t, err)
}

// New's option error paths: the DB can't be opened.
func TestBolt_New_OpenFailure(t *testing.T) {
	// A path whose parent directory doesn't exist can never be opened.
	_, err := New(t.Context(), filepath.Join(t.TempDir(), "missing-dir", "test.db"), nil)
	require.Error(t, err)
}

func TestBolt_Close(t *testing.T) {
	t.Run("releases file lock", func(t *testing.T) {
		str := newTestStorage(t)
		dbPath := str.Config.Path

		require.NoError(t, str.Close())

		reopened, err := New(t.Context(), dbPath, &bbolt.Options{Timeout: 100 * time.Millisecond})
		if err != nil {
			t.Fatalf("reopening database after Close failed: %v", err)
		}

		t.Cleanup(func() { _ = reopened.Close() })
	})

	t.Run("idempotent", func(t *testing.T) {
		str := newTestStorage(t)

		require.NoError(t, str.Close())
		assert.NoError(t, str.Close())

		// Close on a Bolt without a Client is a no-op, not a nil dereference.
		var empty Bolt
		assert.NoError(t, empty.Close())
	})

	t.Run("concurrent", func(t *testing.T) {
		str := newTestStorage(t)

		const goroutines = 8

		start := make(chan struct{})
		errs := make(chan error, goroutines)

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for range goroutines {
			go func() {
				defer wg.Done()
				<-start

				errs <- str.Close()
			}()
		}

		close(start)
		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}
	})
}

//////
// Get, Set.
//////

// Not parallel: mutates the package singleton.
func TestBolt_GetSetSingleton(t *testing.T) {
	str := newTestStorage(t)

	Set(nil)
	require.Panics(t, func() {
		Get()
	})

	Set(str)
	assert.Equal(t, str, Get())
}

// Concurrent New/Get/Set on the package singleton must be race-free (this is
// meaningful under -race).
func TestBolt_SingletonConcurrency(t *testing.T) {
	ctx := t.Context()

	str := newTestStorage(t)
	Set(str)

	var wg sync.WaitGroup

	for i := range 8 {
		wg.Add(3)

		dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("conc-%d.db", i))

		go func() {
			defer wg.Done()

			s, err := New(ctx, dbPath, nil)
			if err == nil {
				t.Cleanup(func() { _ = s.Close() })
			}
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
func TestBolt_ConcurrentCRUD(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	var wg sync.WaitGroup

	for i := range 16 {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			id := fmt.Sprintf("conc-%d", n)

			_, err := str.Create(ctx, id, "docs", shared.TestData, &create.Create{})
			assert.NoError(t, err)

			var got shared.TestDataS
			assert.NoError(t, str.Retrieve(ctx, id, "docs", &got, &retrieve.Retrieve{}))

			_, err = str.Count(ctx, "docs", &count.Count{Search: "conc-*"})
			assert.NoError(t, err)

			assert.NoError(t, str.Delete(ctx, id, "docs", &delete.Delete{}))
		}(i)
	}

	wg.Wait()
}
