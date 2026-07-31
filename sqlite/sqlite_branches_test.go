package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// Offline branch-coverage tests: repeated option/hook/target/SQL-error shapes
// that fan out across Count/Delete/Retrieve/List/Create/Update, plus the
// small standalone helpers (ToSQLString, createDB, createTable, deleteTable,
// GetClient, New failure paths, Get/Set).
//
// Each test gets its own isolated, file-backed database (newIsolatedStorage /
// newIsolatedStorageWithTable) so schema mutation and hook side effects never
// interfere with the process-wide seeded storage in sqlite_list_test.go, or
// with each other. Failure-counter assertions use before/after deltas because
// the underlying expvar counters are process-wide (shared by name across every
// SQLite instance in the binary, see internal/metrics.NewInt) — tests in this
// file never run in parallel, so deltas are deterministic.
//////

// newIsolatedStorage creates a fresh, file-backed SQLite storage in its own
// temp directory.
func newIsolatedStorage(t *testing.T) (*SQLite, context.Context) {
	t.Helper()

	ctx := t.Context()

	dbPath := filepath.Join(t.TempDir(), "dal-branches-test.db")

	str, err := New(ctx, dbPath)
	require.NoError(t, err)
	require.NotNil(t, str)
	require.NotNil(t, str.Client)

	str.Client.SetMaxOpenConns(1)

	return str, ctx
}

// newIsolatedStorageWithTable is newIsolatedStorage plus a ready `test` table,
// for tests that need a real target to operate against.
func newIsolatedStorageWithTable(t *testing.T) (*SQLite, context.Context) {
	t.Helper()

	str, ctx := newIsolatedStorage(t)

	require.NoError(t, str.createTable(ctx, shared.TableName, `
		id varchar(255) PRIMARY KEY,
		name varchar(255) NOT NULL,
		version varchar(255) NOT NULL
	`))

	return str, ctx
}

// seedRow inserts a single row into the `test` table.
func seedRow(t *testing.T, ctx context.Context, str *SQLite, id string) {
	t.Helper()

	_, err := str.Client.ExecContext(ctx,
		"INSERT INTO "+shared.TableName+" (id, name, version) VALUES (?, 'n', 'v')",
		id,
	)
	require.NoError(t, err)
}

//////
// ToSQLString.
//////

func TestToSQLString(t *testing.T) {
	t.Run("happy", func(t *testing.T) {
		got, err := ToSQLString(customsort.Sort("name:asc,age:desc"))
		require.NoError(t, err)
		assert.Equal(t, "name asc, age desc", got)
	})

	t.Run("bad - invalid sort format", func(t *testing.T) {
		_, err := ToSQLString(customsort.Sort("not-a-valid-sort-format"))
		assert.Error(t, err)
	})
}

//////
// createDB / createTable / deleteTable.
//////

func TestSQLite_CreateDB(t *testing.T) {
	str, ctx := newIsolatedStorage(t)

	// SQLite has no CREATE DATABASE statement; this (Postgres-shaped) helper
	// must surface the driver's syntax error rather than silently succeeding.
	assert.Error(t, str.createDB(ctx, "somedb"))
}

func TestSQLite_CreateTable(t *testing.T) {
	t.Run("bad - invalid identifier", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)

		err := str.createTable(ctx, `evil"; DROP TABLE test --`, "id varchar(255)")
		require.Error(t, err)
	})

	t.Run("bad - SQL error", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)

		err := str.createTable(ctx, "broken_table", "this is not valid SQL !!!")
		require.Error(t, err)
	})

	t.Run("happy", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)

		require.NoError(t, str.createTable(ctx, shared.TableName, "id varchar(255) PRIMARY KEY"))
	})
}

func TestSQLite_DeleteTable(t *testing.T) {
	t.Run("bad - invalid identifier", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)

		err := str.deleteTable(ctx, `evil"; DROP TABLE test --`)
		require.Error(t, err)
	})

	t.Run("bad - SQL error on nonexistent table", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)

		err := str.deleteTable(ctx, "no_such_table_xyz")
		require.Error(t, err)
	})

	t.Run("happy", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)

		require.NoError(t, str.deleteTable(ctx, shared.TableName))
	})
}

//////
// GetClient.
//////

func TestSQLite_GetClient(t *testing.T) {
	str, _ := newIsolatedStorage(t)

	client := str.GetClient()
	assert.NotNil(t, client)
	assert.Same(t, str.Client, client)
}

//////
// New failure paths.
//////

// TestSQLite_New_OpenFailure drives the sqlx.Open error return (an
// unregistered driver name), distinct from the ping-failure path covered in
// sqlite_leak_test.go.
func TestSQLite_New_OpenFailure(t *testing.T) {
	original := driverName
	driverName = "dal-sqlite-unregistered-driver-name"

	t.Cleanup(func() { driverName = original })

	str, err := New(t.Context(), "whatever")
	require.Error(t, err)
	assert.Nil(t, str)
}

// TestSQLite_New_ValidationFailure drives validation.Validate(storage)'s error
// return: an empty DSN leaves Config.DataSourceName empty, which fails its
// `validate:"required"` tag even though Open/Ping both succeed.
func TestSQLite_New_ValidationFailure(t *testing.T) {
	str, err := New(t.Context(), "")
	require.Error(t, err)
	assert.Nil(t, str)
	assert.Contains(t, err.Error(), "DataSourceName")
}

//////
// Get / Set.
//////

// TestSQLite_GetSet exercises the nil-singleton panic in Get() and the normal
// Get/Set round trip. No t.Parallel: it mutates package-level singleton state
// that other tests in this package also read/write.
func TestSQLite_GetSet(t *testing.T) {
	singletonMutex.RLock()
	original := singleton
	singletonMutex.RUnlock()

	t.Cleanup(func() {
		Set(original)
	})

	Set(nil)
	assert.Panics(t, func() { Get() })

	str, _ := newIsolatedStorage(t)
	Set(str)

	assert.NotPanics(t, func() { Get() })
	assert.Equal(t, storage.IStorage(str), Get())
}

//////
// Count branches.
//////

func TestSQLite_Count_Branches(t *testing.T) {
	t.Run("bad - option error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCountedFailed().Value()

		_, err := str.Count(ctx, shared.TableName, &count.Count{}, storage.WithPreHook[*count.Count](nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrRequiredPreHook)
		assert.Equal(t, before+1, str.GetCounterCountedFailed().Value())
	})

	t.Run("bad - target resolution failure", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterCountedFailed().Value()

		_, err := str.Count(ctx, "", &count.Count{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterCountedFailed().Value())
	})

	t.Run("bad - pre-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCountedFailed().Value()
		boom := errors.New("pre-hook boom")

		_, err := str.Count(ctx, shared.TableName, &count.Count{}, storage.WithPreHook[*count.Count](
			func(context.Context, storage.IStorage, string, string, any, *count.Count) error { return boom },
		))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterCountedFailed().Value())
	})

	t.Run("happy - pre-hook and post-hook invoked", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "c-1")
		before := str.GetCounterCounted().Value()

		var preCalled, postCalled bool

		got, err := str.Count(ctx, shared.TableName, &count.Count{},
			storage.WithPreHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				preCalled = true
				return nil
			}),
			storage.WithPostHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				postCalled = true
				return nil
			}),
		)
		require.NoError(t, err)
		assert.EqualValues(t, 1, got)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
		assert.Equal(t, before+1, str.GetCounterCounted().Value())
	})

	t.Run("bad - post-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCountedFailed().Value()
		boom := errors.New("post-hook boom")

		_, err := str.Count(ctx, shared.TableName, &count.Count{}, storage.WithPostHook[*count.Count](
			func(context.Context, storage.IStorage, string, string, any, *count.Count) error { return boom },
		))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterCountedFailed().Value())
	})

	t.Run("bad - SQL error on nonexistent table (default query path)", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterCountedFailed().Value()

		_, err := str.Count(ctx, "no_such_table_xyz", &count.Count{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterCountedFailed().Value())
	})
}

//////
// Delete branches.
//////

func TestSQLite_Delete_Branches(t *testing.T) {
	t.Run("bad - empty id", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterDeletedFailed().Value()

		err := str.Delete(ctx, "", shared.TableName, &delete.Delete{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterDeletedFailed().Value())
	})

	t.Run("bad - option error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterDeletedFailed().Value()

		err := str.Delete(ctx, "some-id", shared.TableName, &delete.Delete{}, storage.WithPreHook[*delete.Delete](nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrRequiredPreHook)
		assert.Equal(t, before+1, str.GetCounterDeletedFailed().Value())
	})

	t.Run("bad - target resolution failure", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterDeletedFailed().Value()

		err := str.Delete(ctx, "some-id", "", &delete.Delete{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterDeletedFailed().Value())
	})

	t.Run("bad - pre-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterDeletedFailed().Value()
		boom := errors.New("pre-hook boom")

		err := str.Delete(ctx, "some-id", shared.TableName, &delete.Delete{}, storage.WithPreHook[*delete.Delete](
			func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error { return boom },
		))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterDeletedFailed().Value())
	})

	t.Run("happy - pre-hook and post-hook invoked", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "del-1")
		before := str.GetCounterDeleted().Value()

		var preCalled, postCalled bool

		err := str.Delete(ctx, "del-1", shared.TableName, &delete.Delete{},
			storage.WithPreHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				preCalled = true
				return nil
			}),
			storage.WithPostHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				postCalled = true
				return nil
			}),
		)
		require.NoError(t, err)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
		assert.Equal(t, before+1, str.GetCounterDeleted().Value())
	})

	t.Run("bad - post-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "del-2")
		before := str.GetCounterDeletedFailed().Value()
		boom := errors.New("post-hook boom")

		err := str.Delete(ctx, "del-2", shared.TableName, &delete.Delete{}, storage.WithPostHook[*delete.Delete](
			func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error { return boom },
		))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterDeletedFailed().Value())
	})

	t.Run("bad - SQL error on nonexistent table", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterDeletedFailed().Value()

		err := str.Delete(ctx, "some-id", "no_such_table_xyz", &delete.Delete{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterDeletedFailed().Value())
	})
}

//////
// Retrieve branches.
//////

func TestSQLite_Retrieve_Branches(t *testing.T) {
	t.Run("bad - empty id", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(ctx, "", shared.TableName, &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("bad - option error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(ctx, "some-id", shared.TableName, &shared.TestDataWithIDS{}, &retrieve.Retrieve{},
			storage.WithPreHook[*retrieve.Retrieve](nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrRequiredPreHook)
		assert.Equal(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("bad - target resolution failure", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(ctx, "some-id", "", &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("bad - pre-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterRetrievedFailed().Value()
		boom := errors.New("pre-hook boom")

		err := str.Retrieve(ctx, "some-id", shared.TableName, &shared.TestDataWithIDS{}, &retrieve.Retrieve{},
			storage.WithPreHook[*retrieve.Retrieve](
				func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error { return boom },
			))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("happy - pre-hook and post-hook invoked", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "ret-1")
		before := str.GetCounterRetrieved().Value()

		var preCalled, postCalled bool
		var got shared.TestDataWithIDS

		err := str.Retrieve(ctx, "ret-1", shared.TableName, &got, &retrieve.Retrieve{},
			storage.WithPreHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				preCalled = true
				return nil
			}),
			storage.WithPostHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				postCalled = true
				return nil
			}),
		)
		require.NoError(t, err)
		assert.Equal(t, "ret-1", got.ID)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
		assert.Equal(t, before+1, str.GetCounterRetrieved().Value())
	})

	t.Run("bad - post-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "ret-2")
		before := str.GetCounterRetrievedFailed().Value()
		boom := errors.New("post-hook boom")

		err := str.Retrieve(ctx, "ret-2", shared.TableName, &shared.TestDataWithIDS{}, &retrieve.Retrieve{},
			storage.WithPostHook[*retrieve.Retrieve](
				func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error { return boom },
			))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("bad - SQL error on nonexistent table (not a 404)", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(ctx, "some-id", "no_such_table_xyz", &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterRetrievedFailed().Value())
	})
}

//////
// List branches.
//////

func TestSQLite_List_Branches(t *testing.T) {
	t.Run("bad - option error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterListedFailed().Value()

		var got []shared.TestDataWithIDS
		err := str.List(ctx, shared.TableName, &got, &list.List{}, storage.WithPreHook[*list.List](nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrRequiredPreHook)
		assert.Equal(t, before+1, str.GetCounterListedFailed().Value())
	})

	t.Run("bad - target resolution failure", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterListedFailed().Value()

		var got []shared.TestDataWithIDS
		err := str.List(ctx, "", &got, &list.List{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterListedFailed().Value())
	})

	t.Run("bad - pre-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterListedFailed().Value()
		boom := errors.New("pre-hook boom")

		var got []shared.TestDataWithIDS
		err := str.List(ctx, shared.TableName, &got, &list.List{}, storage.WithPreHook[*list.List](
			func(context.Context, storage.IStorage, string, string, any, *list.List) error { return boom },
		))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterListedFailed().Value())
	})

	t.Run("happy - pre-hook and post-hook invoked", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "l-1")
		before := str.GetCounterListed().Value()

		var preCalled, postCalled bool
		var got []shared.TestDataWithIDS

		err := str.List(ctx, shared.TableName, &got, &list.List{},
			storage.WithPreHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				preCalled = true
				return nil
			}),
			storage.WithPostHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				postCalled = true
				return nil
			}),
		)
		require.NoError(t, err)
		assert.Len(t, got, 1)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
		assert.Equal(t, before+1, str.GetCounterListed().Value())
	})

	t.Run("bad - post-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterListedFailed().Value()
		boom := errors.New("post-hook boom")

		var got []shared.TestDataWithIDS
		err := str.List(ctx, shared.TableName, &got, &list.List{}, storage.WithPostHook[*list.List](
			func(context.Context, storage.IStorage, string, string, any, *list.List) error { return boom },
		))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterListedFailed().Value())
	})

	t.Run("bad - SQL error on nonexistent table (default query path)", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterListedFailed().Value()

		var got []shared.TestDataWithIDS
		err := str.List(ctx, "no_such_table_xyz", &got, &list.List{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterListedFailed().Value())
	})
}

//////
// Create branches.
//////

func TestSQLite_Create_Branches(t *testing.T) {
	t.Run("bad - option error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCreatedFailed().Value()

		_, err := str.Create(ctx, "cr-1", shared.TableName, &shared.TestDataWithIDS{ID: "cr-1"}, &create.Create{},
			storage.WithPreHook[*create.Create](nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrRequiredPreHook)
		assert.Equal(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("bad - target resolution failure", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterCreatedFailed().Value()

		_, err := str.Create(ctx, "cr-2", "", &shared.TestDataWithIDS{ID: "cr-2"}, &create.Create{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("bad - pre-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCreatedFailed().Value()
		boom := errors.New("pre-hook boom")

		_, err := str.Create(ctx, "cr-3", shared.TableName, &shared.TestDataWithIDS{ID: "cr-3"}, &create.Create{},
			storage.WithPreHook[*create.Create](
				func(context.Context, storage.IStorage, string, string, any, *create.Create) error { return boom },
			))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("bad - ToSQL error on unsupported value type", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCreatedFailed().Value()

		_, err := str.Create(ctx, "cr-4", shared.TableName, make(chan int), &create.Create{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("bad - SQL error on nonexistent table", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterCreatedFailed().Value()

		_, err := str.Create(ctx, "cr-5", "no_such_table_xyz", &shared.TestDataWithIDS{ID: "cr-5"}, &create.Create{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("happy - pre-hook and post-hook invoked", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCreated().Value()

		var preCalled, postCalled bool

		id, err := str.Create(ctx, "cr-6", shared.TableName, &shared.TestDataWithIDS{ID: "cr-6", Name: "n", Version: "v"}, &create.Create{},
			storage.WithPreHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				preCalled = true
				return nil
			}),
			storage.WithPostHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				postCalled = true
				return nil
			}),
		)
		require.NoError(t, err)
		assert.Equal(t, "cr-6", id)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
		assert.Equal(t, before+1, str.GetCounterCreated().Value())
	})

	// Regression: before the fix, Create's post-hook error path returned the
	// raw error without calling customapm.TraceError, so the CreatedFailed
	// counter was never incremented (unlike every other operation's
	// post-hook error path).
	t.Run("bad - post-hook error increments CreatedFailed", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterCreatedFailed().Value()
		boom := errors.New("post-hook boom")

		_, err := str.Create(ctx, "cr-7", shared.TableName, &shared.TestDataWithIDS{ID: "cr-7", Name: "n", Version: "v"}, &create.Create{},
			storage.WithPostHook[*create.Create](
				func(context.Context, storage.IStorage, string, string, any, *create.Create) error { return boom },
			))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterCreatedFailed().Value())
	})
}

//////
// Update branches.
//////

func TestSQLite_Update_Branches(t *testing.T) {
	t.Run("bad - empty id", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(ctx, "", shared.TableName, &shared.TestDataWithIDS{}, &update.Update{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("bad - option error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(ctx, "some-id", shared.TableName, &shared.TestDataWithIDS{}, &update.Update{},
			storage.WithPreHook[*update.Update](nil))
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrRequiredPreHook)
		assert.Equal(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("bad - target resolution failure", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(ctx, "some-id", "", &shared.TestDataWithIDS{}, &update.Update{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("bad - pre-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		before := str.GetCounterUpdatedFailed().Value()
		boom := errors.New("pre-hook boom")

		err := str.Update(ctx, "some-id", shared.TableName, &shared.TestDataWithIDS{}, &update.Update{},
			storage.WithPreHook[*update.Update](
				func(context.Context, storage.IStorage, string, string, any, *update.Update) error { return boom },
			))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("bad - ToSQL error on unsupported value type", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "up-1")
		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(ctx, "up-1", shared.TableName, make(chan int), &update.Update{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("bad - SQL error on nonexistent table", func(t *testing.T) {
		str, ctx := newIsolatedStorage(t)
		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(ctx, "some-id", "no_such_table_xyz", &shared.TestDataWithIDS{ID: "some-id", Name: "n", Version: "v"}, &update.Update{})
		require.Error(t, err)
		assert.Equal(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("happy - pre-hook and post-hook invoked", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "up-2")
		before := str.GetCounterUpdated().Value()

		var preCalled, postCalled bool

		err := str.Update(ctx, "up-2", shared.TableName, &shared.TestDataWithIDS{ID: "up-2", Name: "n2", Version: "v2"}, &update.Update{},
			storage.WithPreHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				preCalled = true
				return nil
			}),
			storage.WithPostHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				postCalled = true
				return nil
			}),
		)
		require.NoError(t, err)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
		assert.Equal(t, before+1, str.GetCounterUpdated().Value())
	})

	t.Run("bad - post-hook error", func(t *testing.T) {
		str, ctx := newIsolatedStorageWithTable(t)
		seedRow(t, ctx, str, "up-3")
		before := str.GetCounterUpdatedFailed().Value()
		boom := errors.New("post-hook boom")

		err := str.Update(ctx, "up-3", shared.TableName, &shared.TestDataWithIDS{ID: "up-3", Name: "n2", Version: "v2"}, &update.Update{},
			storage.WithPostHook[*update.Update](
				func(context.Context, storage.IStorage, string, string, any, *update.Update) error { return boom },
			))
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
		assert.Equal(t, before+1, str.GetCounterUpdatedFailed().Value())
	})
}
