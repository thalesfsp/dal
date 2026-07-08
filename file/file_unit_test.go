package file

import (
	"fmt"
	"os"
	"path/filepath"
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
// Offline (no ENVIRONMENT gating) tests exercising the real file adapter.
//////

func newTestStorage(t *testing.T) *File {
	t.Helper()

	str, err := New(t.Context())
	require.NoError(t, err)
	require.NotNil(t, str)

	return str
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

	var keys ResponseListKeys
	require.NoError(t, str.List(ctx, dir, &keys, &list.List{Search: "*"}))
	assert.Len(t, keys.Keys, 3)
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
		entries, err := os.ReadDir("/proc/self/fd")
		require.NoError(t, err)

		return len(entries)
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
