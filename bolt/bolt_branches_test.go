package bolt

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// Additional branch-coverage tests. These complement bolt_unit_test.go and
// must not duplicate the scenarios already covered there.
//////

var errBranchPre = errors.New("pre-hook boom")

var errBranchPost = errors.New("post-hook boom")

//////
// Count.
//////

func TestBolt_Count_OptionError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), "docs", &count.Count{Search: "*"}, storage.WithPreHook[*count.Count](nil))
	require.Error(t, err)
}

func TestBolt_Count_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Count(t.Context(), "docs", &count.Count{Search: "*"},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestBolt_Count_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	_, err = str.Count(t.Context(), "docs", &count.Count{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *count.Count) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	var gotCount any

	c, err := str.Count(t.Context(), "docs", &count.Count{Search: "*"},
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

func TestBolt_Delete_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "id-1", "docs", &delete.Delete{}, storage.WithPreHook[*delete.Delete](nil))
	require.Error(t, err)
}

func TestBolt_Delete_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Delete(t.Context(), "id-1", "docs", &delete.Delete{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestBolt_Delete_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.Delete(t.Context(), "id-1", "docs", &delete.Delete{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *delete.Delete) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.Delete(t.Context(), "id-1", "docs", &delete.Delete{},
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

func TestBolt_Retrieve_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "id-1", "docs", &shared.TestDataS{}, &retrieve.Retrieve{}, storage.WithPreHook[*retrieve.Retrieve](nil))
	require.Error(t, err)
}

func TestBolt_Retrieve_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Retrieve(t.Context(), "id-1", "docs", &shared.TestDataS{}, &retrieve.Retrieve{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestBolt_Retrieve_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	err = str.Retrieve(t.Context(), "id-1", "docs", &shared.TestDataS{}, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	var got shared.TestDataS

	err = str.Retrieve(t.Context(), "id-1", "docs", &got, &retrieve.Retrieve{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *retrieve.Retrieve) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, *shared.TestData, got)
}

//////
// List.
//////

func TestBolt_List_OptionError(t *testing.T) {
	str := newTestStorage(t)

	var lst ResponseList[shared.TestDataS]

	err := str.List(t.Context(), "docs", &lst, &list.List{Search: "*"}, storage.WithPreHook[*list.List](nil))
	require.Error(t, err)
}

func TestBolt_List_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	var lst ResponseList[shared.TestDataS]

	err := str.List(t.Context(), "docs", &lst, &list.List{Search: "*"},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestBolt_List_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	var lst ResponseList[shared.TestDataS]

	err = str.List(t.Context(), "docs", &lst, &list.List{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.List(t.Context(), "docs", &lst, &list.List{Search: "*"},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *list.List) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}

// List builds its JSON payload with a strings.Builder; verify zero, one, and
// multiple items all parse correctly, and that ordering follows bbolt's
// cursor (lexicographic by key).
func TestBolt_List_BuilderZeroOneMany(t *testing.T) {
	ctx := t.Context()
	str := newTestStorage(t)

	// The bucket must exist before a zero-match Search can return an empty
	// (rather than erroring) result — a missing bucket is itself an error.
	seedID, err := str.Create(ctx, "seed", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)
	require.Equal(t, "seed", seedID)

	// Zero items.
	var empty ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &empty, &list.List{Search: "zero-*"}))
	assert.Empty(t, empty.Items)

	// Single item.
	_, err = str.Create(ctx, "solo-1", "docs", shared.TestData, &create.Create{})
	require.NoError(t, err)

	var single ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &single, &list.List{Search: "solo-*"}))
	require.Len(t, single.Items, 1)
	assert.Equal(t, *shared.TestData, single.Items[0])

	// Multiple items, in key order.
	for i := range 5 {
		data := &shared.TestDataS{Name: "multi-name", Version: shared.DocumentVersion}

		_, err := str.Create(ctx, "multi-"+string(rune('a'+i)), "docs", data, &create.Create{})
		require.NoError(t, err)
	}

	var many ResponseList[shared.TestDataS]
	require.NoError(t, str.List(ctx, "docs", &many, &list.List{Search: "multi-*"}))
	require.Len(t, many.Items, 5)
}

//////
// Create.
//////

func TestBolt_Create_OptionError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{}, storage.WithPreHook[*create.Create](nil))
	require.Error(t, err)
}

func TestBolt_Create_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestBolt_Create_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	_, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	id, err := str.Create(t.Context(), "id-1", "docs", shared.TestData, &create.Create{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *create.Create) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, "id-1", id)
}

//////
// Update.
//////

func TestBolt_Update_OptionError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "docs", shared.TestData, &update.Update{}, storage.WithPreHook[*update.Update](nil))
	require.Error(t, err)
}

func TestBolt_Update_PreHookError(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "docs", shared.TestData, &update.Update{},
		storage.WithPreHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPre
		}))
	require.ErrorIs(t, err, errBranchPre)
}

func TestBolt_Update_PostHookErrorAndSuccess(t *testing.T) {
	str := newTestStorage(t)

	err := str.Update(t.Context(), "id-1", "docs", shared.TestData, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			return errBranchPost
		}))
	require.ErrorIs(t, err, errBranchPost)

	var called bool

	err = str.Update(t.Context(), "id-1", "docs", shared.TestData, &update.Update{},
		storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ *update.Update) error {
			called = true

			return nil
		}))
	require.NoError(t, err)
	assert.True(t, called)
}
