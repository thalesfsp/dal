package bolt

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	bbolt "go.etcd.io/bbolt"
)

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

// Close releases the file lock; bbolt's own Close is idempotent, and a
// second call must stay safe (no panic, no error) rather than surface a
// spurious failure.
func TestBolt_Close(t *testing.T) {
	str := newTestStorage(t)

	require.NoError(t, str.Close())
	assert.NoError(t, str.Close())

	// Close on a Bolt without a Client is a no-op, not a nil dereference.
	var empty Bolt
	assert.NoError(t, empty.Close())
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
