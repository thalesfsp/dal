package memory

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v2/internal/shared"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
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
