package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

//////
// Additional branch-coverage tests. These complement file_unit_test.go and
// must not duplicate the scenarios already covered there.
//////

var errBranchPre = errors.New("pre-hook boom")

var errBranchPost = errors.New("post-hook boom")

//////
// Count.
//////

func TestFile_Count_OptionError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), t.TempDir(), &count.Count{Search: "*"}, storage.WithPreHook[*count.Count](nil))
	require.Error(t, err)
}

func TestFile_Count_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), t.TempDir(), &count.Count{Search: "*"},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestFile_Count_PreHookSuccess(t *testing.T) {
	str := newTestStorage(t)

	dir := t.TempDir()

	var called bool

	c, err := str.Count(t.Context(), dir, &count.Count{Search: "*"},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, int64(0), c)
}

func TestFile_Count_PostHookError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), t.TempDir(), &count.Count{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)
}

func TestFile_Count_PostHookSuccess(t *testing.T) {
	str := newTestStorage(t)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.json"), []byte(`{}`), 0o600))

	var gotCount any

	c, err := str.Count(t.Context(), dir, &count.Count{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, data any, _ *count.Count) error {
			gotCount = data

			return nil
		}))
	require.NoError(t, err)
	assert.Equal(t, int64(1), c)
	assert.Equal(t, int64(1), gotCount)
}

func TestFile_Count_TargetResolutionFailure(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), "", &count.Count{Search: "*"})
	require.Error(t, err)
}

// A missing directory must error, not silently count 0.
func TestFile_Count_MissingDirectory(t *testing.T) {
	str := newTestStorage(t)

	missing := filepath.Join(t.TempDir(), "does-not-exist")

	_, err := str.Count(t.Context(), missing, &count.Count{Search: "*"})
	require.Error(t, err)
}

// A malformed glob pattern is a fs.Glob error, not a silent zero.
func TestFile_Count_BadGlobPattern(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), t.TempDir(), &count.Count{Search: "["})
	require.Error(t, err)
}

//////
// Delete.
//////

func TestFile_Delete_EmptyID(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "", filepath.Join(t.TempDir(), "x.json"), &delete.Delete{})
	require.Error(t, err)
}

func TestFile_Delete_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), &delete.Delete{}, storage.WithPreHook[*delete.Delete](nil))
	require.Error(t, err)
}

func TestFile_Delete_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), &delete.Delete{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestFile_Delete_PostHookError(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "x.json")
	require.NoError(t, os.WriteFile(target, []byte(`{}`), 0o600))

	err := str.Delete(t.Context(), "id-1", target, &delete.Delete{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)
}

func TestFile_Delete_TargetResolutionFailure(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "id-1", "", &delete.Delete{})
	require.Error(t, err)
}

// Deleting a missing file is idempotent, but must still run the post-hook
// and increment the success counter (no early return).
func TestFile_Delete_MissingFileStillRunsPostHookAndCounter(t *testing.T) {
	str := newTestStorage(t)

	before := str.GetCounterDeleted().Value()

	var postHookCalled bool

	target := filepath.Join(t.TempDir(), "missing.json")

	err := str.Delete(t.Context(), "id-1", target, &delete.Delete{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			postHookCalled = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, postHookCalled, "post-hook must run even when the file did not exist")
	assert.Equal(t, before+1, str.GetCounterDeleted().Value(), "success counter must increment even when the file did not exist")
}

// os.Remove of a non-empty directory fails with something other than
// os.ErrNotExist, and that must surface as an error.
func TestFile_Delete_NonEmptyDirectoryFails(t *testing.T) {
	str := newTestStorage(t)

	dir := t.TempDir()
	sub := filepath.Join(dir, "not-empty")
	require.NoError(t, os.Mkdir(sub, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "child.txt"), []byte("x"), 0o600))

	err := str.Delete(t.Context(), "id-1", sub, &delete.Delete{})
	require.Error(t, err)
}

//////
// Retrieve.
//////

func TestFile_Retrieve_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), &shared.TestDataS{}, &retrieve.Retrieve{}, storage.WithPreHook[*retrieve.Retrieve](nil))
	require.Error(t, err)
}

func TestFile_Retrieve_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), &shared.TestDataS{}, &retrieve.Retrieve{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestFile_Retrieve_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "x.json")
	_, err := str.Create(t.Context(), "id-1", target, shared.TestData, &create.Create{})
	require.NoError(t, err)

	// Error branch.
	err = str.Retrieve(t.Context(), "id-1", target, &shared.TestDataS{}, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	// Success branch.
	var called bool

	var got shared.TestDataS

	err = str.Retrieve(t.Context(), "id-1", target, &got, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, *shared.TestData, got)
}

func TestFile_Retrieve_EmptyID(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "", filepath.Join(t.TempDir(), "x.json"), &shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

// os.Open failing for a reason other than "not exist" (here: a path
// component is a regular file, not a directory) must hit the generic
// FailedToError branch, not the NotFound one.
func TestFile_Retrieve_OpenFailureNotNotExist(t *testing.T) {
	str := newTestStorage(t)

	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))

	target := filepath.Join(blocker, "data.json")

	err := str.Retrieve(t.Context(), "id-1", target, &shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)

	cE, ok := customerror.To(err)
	require.True(t, ok)
	assert.NotEqual(t, 404, cE.StatusCode)
}

func TestFile_Retrieve_TargetResolutionFailure(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "id-1", "", &shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

// Decode failure: v must be a pointer; passing a value type triggers a
// json.Decode error.
func TestFile_Retrieve_DecodeFailure(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "x.json")
	require.NoError(t, os.WriteFile(target, []byte(`{}`), 0o600))

	err := str.Retrieve(t.Context(), "id-1", target, shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

//////
// List.
//////

func TestFile_List_OptionError(t *testing.T) {
	str := newTestStorage(t)

	var keys ResponseList[map[string]any]

	err := str.List(t.Context(), t.TempDir(), &keys, &list.List{Search: "*"}, storage.WithPreHook[*list.List](nil))
	require.Error(t, err)
}

func TestFile_List_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	var keys ResponseList[map[string]any]

	err := str.List(t.Context(), t.TempDir(), &keys, &list.List{Search: "*"},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestFile_List_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.json"), []byte(`{}`), 0o600))

	var docs ResponseList[map[string]any]

	err := str.List(t.Context(), dir, &docs, &list.List{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.List(t.Context(), dir, &docs, &list.List{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Len(t, docs.Items, 1)
}

func TestFile_List_TargetResolutionFailure(t *testing.T) {
	str := newTestStorage(t)

	var keys ResponseList[map[string]any]

	err := str.List(t.Context(), "", &keys, &list.List{Search: "*"})
	require.Error(t, err)
}

// A missing directory must error, not silently list nothing.
func TestFile_List_MissingDirectory(t *testing.T) {
	str := newTestStorage(t)

	missing := filepath.Join(t.TempDir(), "does-not-exist")

	var keys ResponseList[map[string]any]

	err := str.List(t.Context(), missing, &keys, &list.List{Search: "*"})
	require.Error(t, err)
}

// A malformed glob pattern is a fs.Glob error, not a silent empty list.
func TestFile_List_BadGlobPattern(t *testing.T) {
	str := newTestStorage(t)

	var keys ResponseList[map[string]any]

	err := str.List(t.Context(), t.TempDir(), &keys, &list.List{Search: "["})
	require.Error(t, err)
}

// storage.ParseToStruct fails when v isn't a pointer.
func TestFile_List_ParseToStructFailure(t *testing.T) {
	str := newTestStorage(t)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.json"), []byte(`{}`), 0o600))

	err := str.List(t.Context(), dir, ResponseList[map[string]any]{}, &list.List{Search: "*"})
	require.Error(t, err)
}

//////
// Create.
//////

func TestFile_Create_OptionError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), shared.TestData, &create.Create{}, storage.WithPreHook[*create.Create](nil))
	require.Error(t, err)
}

func TestFile_Create_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), shared.TestData, &create.Create{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestFile_Create_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "x.json")

	_, err := str.Create(t.Context(), "id-1", target, shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	id, err := str.Create(t.Context(), "id-1", target, shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "id-1", id)
}

func TestFile_Create_TargetResolutionFailure(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.Error(t, err)
}

// json.Encode fails on unmarshalable values, such as a channel.
func TestFile_Create_EncodeFailure(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "x.json")

	_, err := str.Create(t.Context(), "id-1", target, make(chan int), &create.Create{})
	require.Error(t, err)
}

// Without CreateIfNotExist, os.Create against a missing parent directory
// fails outright.
func TestFile_Create_OSCreateFailsWithoutCreateIfNotExist(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "missing-dir", "data.json")

	_, err := str.Create(t.Context(), "id-1", target, shared.TestData, &create.Create{})
	require.Error(t, err)
}

// CreateIfNotExist's os.MkdirAll fails when a path component that should be
// a directory is actually a regular file.
func TestFile_Create_MkdirAllFailure(t *testing.T) {
	str := newTestStorage(t)

	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o600))

	target := filepath.Join(blocker, "sub", "data.json")

	_, err := str.Create(t.Context(), "id-1", target, shared.TestData, &create.Create{
		Any: &CreateAny{CreateIfNotExist: true},
	})
	require.Error(t, err)
}

//////
// Update.
//////

func TestFile_Update_EmptyID(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "", filepath.Join(t.TempDir(), "x.json"), shared.TestData, &update.Update{})
	require.Error(t, err)
}

func TestFile_Update_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), shared.TestData, &update.Update{}, storage.WithPreHook[*update.Update](nil))
	require.Error(t, err)
}

func TestFile_Update_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", filepath.Join(t.TempDir(), "x.json"), shared.TestData, &update.Update{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestFile_Update_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "x.json")

	err := str.Update(t.Context(), "id-1", target, shared.TestData, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.Update(t.Context(), "id-1", target, shared.TestData, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}

func TestFile_Update_TargetResolutionFailure(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "", shared.TestData, &update.Update{})
	require.Error(t, err)
}

func TestFile_Update_EncodeFailure(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "x.json")

	err := str.Update(t.Context(), "id-1", target, make(chan int), &update.Update{})
	require.Error(t, err)
}

// Update always calls os.Create directly (it's an upsert), so a missing
// parent directory is a hard failure.
func TestFile_Update_OSCreateFailure(t *testing.T) {
	str := newTestStorage(t)

	target := filepath.Join(t.TempDir(), "missing-dir", "data.json")

	err := str.Update(t.Context(), "id-1", target, shared.TestData, &update.Update{})
	require.Error(t, err)
}

//////
// GetClient, Get, Set.
//////

func TestFile_GetClient(t *testing.T) {
	str := newTestStorage(t)

	assert.Nil(t, str.GetClient())
}

// Not parallel: mutates the package singleton.
func TestFile_GetSetSingleton(t *testing.T) {
	str := newTestStorage(t)

	Set(nil)
	require.Panics(t, func() {
		Get()
	})

	Set(str)
	assert.Equal(t, storage.IStorage(str), Get())
}
