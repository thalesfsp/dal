package memory

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v2/internal/shared"
	"github.com/thalesfsp/dal/v2/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

//////
// Additional branch-coverage tests. These complement memory_unit_test.go and
// must not duplicate the scenarios already covered there.
//////

var errBranchPre = errors.New("pre-hook boom")

var errBranchPost = errors.New("post-hook boom")

//////
// Count.
//////

func TestMemory_Count_OptionError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), "", &count.Count{Search: "*"}, storage.WithPreHook[*count.Count](nil))
	require.Error(t, err)
}

func TestMemory_Count_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), "", &count.Count{Search: "*"},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestMemory_Count_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	_, err = str.Count(t.Context(), "", &count.Count{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	var gotCount any

	c, err := str.Count(t.Context(), "", &count.Count{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, data any, _ *count.Count) error {
			called = true
			gotCount = data

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, c, gotCount)
}

//////
// Delete.
//////

func TestMemory_Delete_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "id-1", "", &delete.Delete{}, storage.WithPreHook[*delete.Delete](nil))
	require.Error(t, err)
}

func TestMemory_Delete_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "id-1", "", &delete.Delete{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestMemory_Delete_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.Delete(t.Context(), "id-1", "", &delete.Delete{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.Delete(t.Context(), "id-1", "", &delete.Delete{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}

//////
// Retrieve.
//////

func TestMemory_Retrieve_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "id-1", "", &shared.TestDataS{}, &retrieve.Retrieve{}, storage.WithPreHook[*retrieve.Retrieve](nil))
	require.Error(t, err)
}

func TestMemory_Retrieve_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "id-1", "", &shared.TestDataS{}, &retrieve.Retrieve{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestMemory_Retrieve_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.Retrieve(t.Context(), "id-1", "", &shared.TestDataS{}, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	var got shared.TestDataS

	err = str.Retrieve(t.Context(), "id-1", "", &got, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, *shared.TestData, got)
}

// Unmarshal failure: a non-pointer destination.
func TestMemory_Retrieve_UnmarshalFailure(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.Retrieve(t.Context(), "id-1", "", shared.TestDataS{}, &retrieve.Retrieve{})
	require.Error(t, err)
}

//////
// List.
//////

func TestMemory_List_OptionError(t *testing.T) {
	str := newTestStorage(t)

	var lst ResponseList[shared.TestDataS]

	err := str.List(t.Context(), "", &lst, &list.List{Search: "*"}, storage.WithPreHook[*list.List](nil))
	require.Error(t, err)
}

func TestMemory_List_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	var lst ResponseList[shared.TestDataS]

	err := str.List(t.Context(), "", &lst, &list.List{Search: "*"},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestMemory_List_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	var lst ResponseList[shared.TestDataS]

	err = str.List(t.Context(), "", &lst, &list.List{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.List(t.Context(), "", &lst, &list.List{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}

// Unmarshal failure: a non-pointer destination.
func TestMemory_List_UnmarshalFailure(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.List(t.Context(), "", ResponseList[shared.TestDataS]{}, &list.List{Search: "*"})
	require.Error(t, err)
}

// List builds its JSON payload with a strings.Builder; verify zero, one, and
// multiple items all parse correctly and, for the multi-item case, that all
// items are present regardless of the (unordered) sync.Map iteration order.
func TestMemory_List_BuilderZeroOneMany(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	// Zero items.
	var empty ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "", &empty, &list.List{Search: "zero-*"}))
	assert.Empty(t, empty.Items)

	// Single item.
	_, err := str.Create(ctx, "solo-1", "", shared.TestData, &create.Create{})
	require.NoError(t, err)

	var single ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "", &single, &list.List{Search: "solo-*"}))
	require.Len(t, single.Items, 1)
	assert.Equal(t, *shared.TestData, single.Items[0])

	// Multiple items, order-independent.
	names := map[string]bool{}

	for i := range 5 {
		data := &shared.TestDataS{Name: fmt.Sprintf("multi-name-%d", i), Version: shared.DocumentVersion}
		names[data.Name] = true

		_, err := str.Create(ctx, fmt.Sprintf("multi-%d", i), "", data, &create.Create{})
		require.NoError(t, err)
	}

	var many ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "", &many, &list.List{Search: "multi-*"}))
	require.Len(t, many.Items, 5)

	gotNames := map[string]bool{}
	for _, item := range many.Items {
		gotNames[item.Name] = true
	}

	assert.Equal(t, names, gotNames)
}

//////
// Create.
//////

func TestMemory_Create_OptionError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{}, storage.WithPreHook[*create.Create](nil))
	require.Error(t, err)
}

func TestMemory_Create_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestMemory_Create_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	id, err := str.Create(t.Context(), "id-1", "", shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "id-1", id)
}

// json.Marshal fails on unmarshalable values, such as a channel.
func TestMemory_Create_MarshalFailure(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "", make(chan int), &create.Create{})
	require.Error(t, err)
}

//////
// Update.
//////

func TestMemory_Update_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "", shared.TestData, &update.Update{}, storage.WithPreHook[*update.Update](nil))
	require.Error(t, err)
}

func TestMemory_Update_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "", shared.TestData, &update.Update{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestMemory_Update_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "", shared.TestData, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.Update(t.Context(), "id-1", "", shared.TestData, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}

// json.Marshal fails on unmarshalable values, such as a channel.
func TestMemory_Update_MarshalFailure(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "", make(chan int), &update.Update{})
	require.Error(t, err)
}

//////
// Get, Set.
//////

// Not parallel: mutates the package singleton.
func TestMemory_GetSetSingleton(t *testing.T) {
	str := newTestStorage(t)

	Set(nil)
	assert.Panics(t, func() {
		Get()
	})

	Set(str)
	assert.Equal(t, storage.IStorage(str), Get())
}
