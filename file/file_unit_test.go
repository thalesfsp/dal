package file

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
// Offline (no ENVIRONMENT gating) tests exercising the real file adapter.
//////

func newTestStorage(t *testing.T) *File {
	t.Helper()

	str, err := New(t.Context())
	require.NoError(t, err)
	require.NotNil(t, str)

	return str
}

func TestFile_Close(t *testing.T) {
	str := newTestStorage(t)

	require.NoError(t, str.Close())
	assert.NoError(t, str.Close())
}

// Happy path: full CRUD round-trip against a temp file.
func TestFile_CRUDRoundTrip(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "data.json")

	id, err := str.Create(ctx, "id-1", target, shared.TestData, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, "id-1", id)

	var got shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "id-1", target, &got, &retrieve.Retrieve{}))
	assert.Equal(t, *shared.TestData, got)

	require.NoError(t, str.Update(ctx, "id-1", target, shared.UpdatedTestData, &update.Update{}))

	var updated shared.TestDataS
	require.NoError(t, str.Retrieve(ctx, "id-1", target, &updated, &retrieve.Retrieve{}))
	assert.Equal(t, shared.DocumentNameUpdated, updated.Name)

	require.NoError(t, str.Delete(ctx, "id-1", target, &delete.Delete{}))
	assert.NoFileExists(t, target)
}

// Regression: Create with a nil params struct used to panic dereferencing
// prm.Any before the nil check.
func TestFile_CreateNilParams(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "nil-prm.json")

	_, err := str.Create(ctx, "id-1", target, shared.TestData, nil)
	require.NoError(t, err)
	assert.FileExists(t, target)
}

// Regression: CreateIfNotExist used the raw target argument instead of the
// resolved one, so it broke whenever the path came from the storage-level
// Target fallback. It also has to create intermediate directories.
func TestFile_CreateIfNotExistUsesResolvedTarget(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	resolved := filepath.Join(t.TempDir(), "sub", "dir", "data.json")
	str.Target = resolved

	// target argument intentionally empty: the storage-level fallback is the
	// real path.
	_, err := str.Create(ctx, "id-1", "", shared.TestData, &create.Create{
		Any: &CreateAny{CreateIfNotExist: true},
	})
	require.NoError(t, err)
	assert.FileExists(t, resolved)

	str.Target = ""
}

// Bad path: retrieving a missing file is a 404, and the failure lands on the
// Retrieve failure counter (regression: it used to increment the Delete one).
func TestFile_RetrieveMissing(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	err := str.Retrieve(ctx, "id-1", filepath.Join(t.TempDir(), "missing.json"), &shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)

	cE, ok := customerror.To(err)
	require.True(t, ok)
	assert.Equal(t, 404, cE.StatusCode)
}

// Regression: a target-resolution failure in Retrieve must be attributed to
// the Retrieve failure counter, not the Delete one.
func TestFile_RetrieveTargetErrorMetricAttribution(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	retrievedBefore := str.GetCounterRetrievedFailed().Value()
	deletedBefore := str.GetCounterDeletedFailed().Value()

	// Empty target with no storage-level fallback → TargetName error.
	err := str.Retrieve(ctx, "id-1", "", &shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)

	assert.Equal(t, retrievedBefore+1, str.GetCounterRetrievedFailed().Value(),
		"retrieve failures must increment the retrieve counter")
	assert.Equal(t, deletedBefore, str.GetCounterDeletedFailed().Value(),
		"retrieve failures must not increment the delete counter")
}

// Count and List glob over the target directory.
func TestFile_CountAndList(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	dir := t.TempDir()

	for _, name := range []string{"a.json", "b.json", "c.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(`{}`), 0o600))
	}

	c, err := str.Count(ctx, dir, &count.Count{Search: "*.json"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), c)

	var docs ResponseList[map[string]any]
	require.NoError(t, str.List(ctx, dir, &docs, &list.List{Search: "*"}))
	assert.Len(t, docs.Items, 3)
}

type fileListParamsDoc struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Rank  int    `json:"rank"`
	Drop  string `json:"drop"`
}

func TestFile_ListParams(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)
	dir := t.TempDir()

	for i := range 13 {
		doc := &fileListParamsDoc{
			ID:    fmt.Sprintf("doc-%02d", i),
			Group: []string{"b", "a"}[i%2],
			Rank:  i,
			Drop:  "drop",
		}
		_, err := str.Create(ctx, doc.ID, filepath.Join(dir, "params-"+doc.ID+".json"), doc, nil)
		require.NoError(t, err)
	}

	listDocs := func(t *testing.T, prm *list.List) []fileListParamsDoc {
		t.Helper()

		var got ResponseList[fileListParamsDoc]
		require.NoError(t, str.List(ctx, dir, &got, prm))

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
		require.NoError(t, str.List(ctx, dir, &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", customsort.Asc}},
			Fields: []string{"id"},
		}))
		require.Len(t, got.Items, 13)
		assert.Equal(t, map[string]any{"id": "doc-00"}, got.Items[0])
	})

	t.Run("combined", func(t *testing.T) {
		var got ResponseList[map[string]any]
		require.NoError(t, str.List(ctx, dir, &got, &list.List{
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

		var got ResponseList[fileListParamsDoc]
		err := str.List(ctx, dir, &got, &list.List{
			Search: "params-*",
			Sort:   customsort.SortSlice{{"rank", "sideways"}},
		})
		require.Error(t, err)
		assert.Equal(t, failedBefore+1, str.GetCounterListedFailed().Value())
	})
}

// Edge: deleting a nonexistent file is idempotent (returns nil).
func TestFile_DeleteMissingIsIdempotent(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	assert.NoError(t, str.Delete(ctx, "id-1", filepath.Join(t.TempDir(), "missing.json"), &delete.Delete{}))
}

// Regression: the CreateIfNotExist pre-creation used to drop its *os.File
// without closing it, leaking one descriptor per call.
func TestFile_CreateIfNotExistDoesNotLeakFDs(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	dir := t.TempDir()

	countFDs := func() int {
		fdDir := "/proc/self/fd"
		if runtime.GOOS == "darwin" {
			fdDir = "/dev/fd"
		}

		dirHandle, err := os.Open(fdDir)
		require.NoError(t, err)
		defer dirHandle.Close()

		names, err := dirHandle.Readdirnames(-1)
		require.NoError(t, err)

		return len(names)
	}

	// Warm up any lazily-opened descriptors.
	_, err := str.Create(ctx, "warmup", filepath.Join(dir, "warmup.json"), shared.TestData, &create.Create{
		Any: &CreateAny{CreateIfNotExist: true},
	})
	require.NoError(t, err)

	before := countFDs()

	const n = 32

	for i := range n {
		_, err := str.Create(ctx, "id", filepath.Join(dir, fmt.Sprintf("f-%d.json", i)), shared.TestData, &create.Create{
			Any: &CreateAny{CreateIfNotExist: true},
		})
		require.NoError(t, err)
	}

	after := countFDs()

	assert.Less(t, after-before, n,
		"open file descriptors must not grow linearly with CreateIfNotExist calls (leak)")
}
