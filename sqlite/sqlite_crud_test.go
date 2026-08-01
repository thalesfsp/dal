package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

type bulkCreateRow struct {
	ID      string  `db:"id"`
	Name    *string `db:"name"`
	Version string  `db:"version"`
}

func sqliteBulkItems(prefix string, count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for i := range items {
		name := fmt.Sprintf("name-%d", i)
		id := fmt.Sprintf("%s-%d", prefix, i)
		items[i] = storage.BulkItem{
			ID: id,
			Value: bulkCreateRow{
				ID:      id,
				Name:    &name,
				Version: "1.0.0",
			},
		}
	}

	return items
}

func sqliteBulkTable(t *testing.T, str *SQLite, name string) {
	t.Helper()
	require.NoError(t, str.createTable(t.Context(), name, `
		id varchar(255) PRIMARY KEY,
		name varchar(255) NOT NULL,
		version varchar(255) NOT NULL
	`))
	t.Cleanup(func() { require.NoError(t, str.deleteTable(context.Background(), name)) })
}

func sqliteBulkCount(t *testing.T, str *SQLite, table string) int {
	t.Helper()
	var count int
	require.NoError(t, str.Client.GetContext(t.Context(), &count, "SELECT COUNT(*) FROM "+table))

	return count
}

func testSQLiteBulkCreate(t *testing.T, str *SQLite) {
	t.Helper()

	t.Run("happy path many items", func(t *testing.T) {
		const table = "bulk_happy"
		sqliteBulkTable(t, str, table)
		items := sqliteBulkItems("happy", 60)

		result, err := str.BulkCreate(t.Context(), table, items, nil)
		require.NoError(t, err)
		require.True(t, result.AllSucceeded())
		require.False(t, result.Stopped)
		require.Equal(t, 60, sqliteBulkCount(t, str, table))
		for i, item := range items {
			require.Equal(t, i, result.Succeeded[i])
			var got bulkCreateRow
			require.NoError(t, str.Client.GetContext(t.Context(), &got,
				"SELECT id, name, version FROM "+table+" WHERE id = ?", item.ID))
			require.Equal(t, item.Value, got)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		result, err := str.BulkCreate(ctx, "unused", nil, nil)
		require.NoError(t, err)
		require.Equal(t, storage.BulkResult{}, result)
	})

	t.Run("single item", func(t *testing.T) {
		const table = "bulk_single"
		sqliteBulkTable(t, str, table)

		result, err := str.BulkCreate(t.Context(), table, sqliteBulkItems("single", 1), nil)
		require.NoError(t, err)
		require.Equal(t, []int{0}, result.Succeeded)
		require.Equal(t, 1, sqliteBulkCount(t, str, table))
	})

	t.Run("partial constraint failure", func(t *testing.T) {
		const table = "bulk_partial"
		sqliteBulkTable(t, str, table)
		items := sqliteBulkItems("partial", 12)
		bad, ok := items[7].Value.(bulkCreateRow)
		require.True(t, ok)
		bad.Name = nil
		items[7].Value = bad

		result, err := str.BulkCreate(t.Context(), table, items, nil)
		require.Error(t, err)
		require.Equal(t, 11, sqliteBulkCount(t, str, table))
		require.Len(t, result.Failures, 1)
		require.Equal(t, 7, result.Failures[0].Index)
		require.Error(t, result.Failures[0].Err)
		require.Len(t, result.Succeeded, 11)
	})

	t.Run("chunked failure preserves original index", func(t *testing.T) {
		const table = "bulk_chunked"
		sqliteBulkTable(t, str, table)
		const badIndex = 340
		itemCount := bulkPlaceholderLimit/3 + 12
		items := sqliteBulkItems("chunked", itemCount)
		bad, ok := items[badIndex].Value.(bulkCreateRow)
		require.True(t, ok)
		bad.Name = nil
		items[badIndex].Value = bad

		result, err := str.BulkCreate(t.Context(), table, items, nil)
		require.Error(t, err)
		gotFailureIndices := make([]int, len(result.Failures))
		for i := range result.Failures {
			gotFailureIndices[i] = result.Failures[i].Index
		}
		if want := []int{badIndex}; !reflect.DeepEqual(gotFailureIndices, want) {
			t.Fatalf("failure indices = %v, want %v", gotFailureIndices, want)
		}
		require.Equal(t, itemCount-1, sqliteBulkCount(t, str, table))
		require.Len(t, result.Succeeded, itemCount-1)
	})

	t.Run("context cancellation keeps written batch", func(t *testing.T) {
		const table = "bulk_canceled"
		sqliteBulkTable(t, str, table)
		itemCount := bulkPlaceholderLimit/3 + 10
		items := sqliteBulkItems("canceled", itemCount)
		ctx, cancel := context.WithCancel(t.Context())
		postCalls := 0
		postHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			postCalls++
			cancel()

			return nil
		}

		result, err := str.BulkCreate(ctx, table, items, nil, storage.WithPostHook(postHook))
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, result.Stopped)
		require.Equal(t, 1, postCalls)
		rowsInFirstChunk := bulkPlaceholderLimit / 3
		require.Equal(t, rowsInFirstChunk, sqliteBulkCount(t, str, table))
		require.Len(t, result.Succeeded, rowsInFirstChunk)
		for i := range result.Succeeded {
			require.Equal(t, i, result.Succeeded[i])
		}
		require.Len(t, result.Failures, itemCount-rowsInFirstChunk)
		for i, failure := range result.Failures {
			require.Equal(t, rowsInFirstChunk+i, failure.Index)
			require.True(t, errors.Is(failure.Err, context.Canceled))
		}
	})

	t.Run("quoted value round trips", func(t *testing.T) {
		const table = "bulk_quote"
		sqliteBulkTable(t, str, table)
		items := sqliteBulkItems("quote", 1)
		quoted := "O'Brien said 'hello'"
		row, ok := items[0].Value.(bulkCreateRow)
		require.True(t, ok)
		row.Name = &quoted
		items[0].Value = row

		result, err := str.BulkCreate(t.Context(), table, items, nil)
		require.NoError(t, err)
		require.Equal(t, []int{0}, result.Succeeded)
		var got string
		require.NoError(t, str.Client.GetContext(t.Context(), &got,
			"SELECT name FROM "+table+" WHERE id = ?", items[0].ID))
		require.Equal(t, quoted, got)
	})
}

//////
// Offline end-to-end tests against a real SQLite database, reusing the shared
// seeded storage from sqlite_list_test.go.
//////

// Full CRUD pass through the public IStorage surface against a real database.
func TestSQLite_CRUDRoundTrip(t *testing.T) {
	ctx := t.Context()
	str := getTestStorage(ctx, t)
	t.Run("BulkCreate", func(t *testing.T) { testSQLiteBulkCreate(t, str) })

	doc := &shared.TestDataWithIDS{ID: "crud-1", Name: "gamma", Version: "1.2.3"}

	id, err := str.Create(ctx, doc.ID, shared.TableName, doc, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, doc.ID, id)

	defer func() {
		assert.NoError(t, str.Delete(ctx, doc.ID, shared.TableName, &delete.Delete{}))
	}()

	var got shared.TestDataWithIDS
	require.NoError(t, str.Retrieve(ctx, doc.ID, shared.TableName, &got, &retrieve.Retrieve{}))
	assert.Equal(t, *doc, got)

	updated := &shared.TestDataWithIDS{ID: doc.ID, Name: "gamma-2", Version: "1.2.4"}
	require.NoError(t, str.Update(ctx, doc.ID, shared.TableName, updated, &update.Update{}))

	var afterUpdate shared.TestDataWithIDS
	require.NoError(t, str.Retrieve(ctx, doc.ID, shared.TableName, &afterUpdate, &retrieve.Retrieve{}))
	assert.Equal(t, "gamma-2", afterUpdate.Name)
}

// Regression: updating a nonexistent row must be a 404, not silent success.
func TestSQLite_UpdateMissingRowIs404(t *testing.T) {
	ctx := t.Context()
	str := getTestStorage(ctx, t)

	err := str.Update(
		ctx,
		"does-not-exist",
		shared.TableName,
		&shared.TestDataWithIDS{ID: "does-not-exist", Name: "x", Version: "y"},
		&update.Update{},
	)
	require.Error(t, err)

	cE, ok := customerror.To(err)
	require.True(t, ok, "error must be a customerror: %v", err)
	assert.Equal(t, 404, cE.StatusCode)
}

// Retrieving a missing row stays a 404 (consistency anchor for the above).
func TestSQLite_RetrieveMissingRowIs404(t *testing.T) {
	ctx := t.Context()
	str := getTestStorage(ctx, t)

	err := str.Retrieve(ctx, "does-not-exist", shared.TableName, &shared.TestDataWithIDS{}, &retrieve.Retrieve{})
	require.Error(t, err)

	cE, ok := customerror.To(err)
	require.True(t, ok)
	assert.Equal(t, 404, cE.StatusCode)
}

// SQL-injection guard: a malicious target must be rejected on the default
// Count/List paths, where it used to be interpolated verbatim.
func TestSQLite_InjectionGuardOnTarget(t *testing.T) {
	ctx := t.Context()
	str := getTestStorage(ctx, t)

	for _, target := range []string{
		shared.TableName + "; DROP TABLE " + shared.TableName + "; --",
		shared.TableName + " WHERE 1=1 UNION SELECT id, name, version FROM secrets",
		`test"`,
	} {
		_, err := str.Count(ctx, target, &count.Count{})
		assert.Error(t, err, "Count must reject %q", target)

		var got []shared.TestDataWithIDS
		err = str.List(ctx, target, &got, &list.List{})
		assert.Error(t, err, "List must reject %q", target)
	}

	// The guard must not break legitimate identifiers.
	c, err := str.Count(ctx, shared.TableName, &count.Count{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, c, int64(3))

	// And the seeded table must still exist (nothing was dropped).
	var rows []shared.TestDataWithIDS
	require.NoError(t, str.List(ctx, shared.TableName, &rows, nil))
	assert.NotEmpty(t, rows)
}
