package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v2/internal/shared"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/list"
)

//////
// Shared, process-wide storage.
//
// The global metrics registry (internal/metrics.NewInt) panics on a duplicate
// metric name, so New() — which registers per-storage counters — may only be
// called ONCE per process. All List/Count behavior tests therefore share a
// single seeded, file-backed storage built here.
//////

var (
	listTestOnce    sync.Once
	listTestStorage *SQLite
)

// getTestStorage returns a process-wide, isolated, file-backed SQLite storage
// with a seeded `test` table plus a `secrets` table (used by the injection
// test). It runs fully offline (no ENVIRONMENT gating, no docker) so it can
// exercise the real List/Count code path under `make test`.
func getTestStorage(ctx context.Context, t *testing.T) *SQLite {
	t.Helper()

	listTestOnce.Do(func() {
		// File-backed DB (not shared-cache in-memory) to avoid SQLite shared
		// cache lock hangs; single connection keeps it deterministic.
		dbPath := filepath.Join(t.TempDir(), "dal-list-test.db")

		str, err := New(ctx, dbPath)
		require.NoError(t, err)
		require.NotNil(t, str)
		require.NotNil(t, str.Client)

		str.Client.SetMaxOpenConns(1)

		require.NoError(t, str.createTable(ctx, shared.TableName, `
			id varchar(255) PRIMARY KEY,
			name varchar(255) NOT NULL,
			version varchar(255) NOT NULL
		`))

		// Seed three rows so filters have something to discriminate.
		seed := []shared.TestDataWithIDS{
			{ID: "id-1", Name: "alpha", Version: "1.0.0"},
			{ID: "id-2", Name: "beta", Version: "2.0.0"},
			{ID: "id-3", Name: "alpha", Version: "3.0.0"},
		}

		for _, row := range seed {
			_, err := str.Client.ExecContext(ctx,
				"INSERT INTO "+shared.TableName+" (id, name, version) VALUES (?, ?, ?)",
				row.ID, row.Name, row.Version,
			)
			require.NoError(t, err)
		}

		// A second table with TWO rows, one of which has a quote in its value.
		// Two rows are required so a WHERE clause that matches only one of them
		// yields a result distinct from the default `SELECT * FROM secrets`
		// (all rows) — otherwise the verbatim test would pass even on the buggy
		// code for the wrong reason.
		_, err = str.Client.ExecContext(ctx,
			"CREATE TABLE IF NOT EXISTS secrets (id varchar(255) PRIMARY KEY, name varchar(255), version varchar(255))")
		require.NoError(t, err)
		_, err = str.Client.ExecContext(ctx,
			"INSERT INTO secrets (id, name, version) VALUES ('s-1', 'O''Brien', '9.9.9')")
		require.NoError(t, err)
		_, err = str.Client.ExecContext(ctx,
			"INSERT INTO secrets (id, name, version) VALUES ('s-2', 'Nobody', '0.0.1')")
		require.NoError(t, err)

		listTestStorage = str
	})

	return listTestStorage
}

// TestList_HonorsSearch is the negative control for the Search-overwrite bug.
//
// On the buggy code, List OVERWRITES a caller-provided prm.Search with the
// default `SELECT * FROM test`, so a WHERE filter is ignored and ALL rows come
// back (the "filters" / LIMIT / ORDER BY sub-tests fail for exactly that
// reason before the fix).
func TestList_HonorsSearch(t *testing.T) {
	ctx := t.Context()
	str := getTestStorage(ctx, t)

	t.Run("happy - nil params returns all rows", func(t *testing.T) {
		var got []shared.TestDataWithIDS
		require.NoError(t, str.List(ctx, shared.TableName, &got, nil))
		assert.Len(t, got, 3, "nil params must return every row (backward compatible)")
	})

	t.Run("happy - Search filters to the correct subset", func(t *testing.T) {
		var got []shared.TestDataWithIDS
		prm := &list.List{Search: "SELECT * FROM " + shared.TableName + " WHERE name = 'beta'"}

		require.NoError(t, str.List(ctx, shared.TableName, &got, prm))

		// BUG (pre-fix): returns all 3 rows because prm.Search is overwritten.
		require.Len(t, got, 1, "Search WHERE name='beta' must return exactly one row")
		assert.Equal(t, "id-2", got[0].ID)
		assert.Equal(t, "beta", got[0].Name)
	})

	t.Run("edge - Search with LIMIT/OFFSET pagination is honored", func(t *testing.T) {
		var page1 []shared.TestDataWithIDS
		require.NoError(t, str.List(ctx, shared.TableName,
			&page1, &list.List{Search: "SELECT * FROM " + shared.TableName + " ORDER BY id LIMIT 2 OFFSET 0"}))
		require.Len(t, page1, 2, "LIMIT 2 must cap the first page")
		assert.Equal(t, "id-1", page1[0].ID)
		assert.Equal(t, "id-2", page1[1].ID)

		var page2 []shared.TestDataWithIDS
		require.NoError(t, str.List(ctx, shared.TableName,
			&page2, &list.List{Search: "SELECT * FROM " + shared.TableName + " ORDER BY id LIMIT 2 OFFSET 2"}))
		require.Len(t, page2, 1, "OFFSET 2 must return the remainder")
		assert.Equal(t, "id-3", page2[0].ID)
	})

	t.Run("edge - Search with ORDER BY controls order", func(t *testing.T) {
		var got []shared.TestDataWithIDS
		prm := &list.List{Search: "SELECT * FROM " + shared.TableName + " ORDER BY id DESC"}

		require.NoError(t, str.List(ctx, shared.TableName, &got, prm))

		require.Len(t, got, 3)
		assert.Equal(t, "id-3", got[0].ID, "ORDER BY id DESC must reverse the order")
	})

	t.Run("edge - empty Search string falls back to default (all rows)", func(t *testing.T) {
		var got []shared.TestDataWithIDS
		// A non-nil params struct with an empty Search must behave like nil.
		// Pre-fix this produced an empty query string and HUNG the driver; the
		// fix falls back to the default SELECT.
		prm := &list.List{Search: ""}

		require.NoError(t, str.List(ctx, shared.TableName, &got, prm))
		assert.Len(t, got, 3, "empty Search must behave like nil params")
	})

	t.Run("bad - malformed Search surfaces an error", func(t *testing.T) {
		var got []shared.TestDataWithIDS
		prm := &list.List{Search: "SELECT * FROM " + shared.TableName + " WHERE"}

		err := str.List(ctx, shared.TableName, &got, prm)
		assert.Error(t, err, "a syntactically invalid Search must return an error, not silently succeed")
	})

	t.Run("edge - Search with a quote in a value is executed verbatim", func(t *testing.T) {
		// The contract: Search is a caller-controlled query template (documented:
		// 'It uses param.List.Search to query the data'). DAL must pass it
		// VERBATIM and never concatenate anything into it. The `secrets` table
		// has TWO rows; a WHERE that matches exactly one (with a single-quote in
		// the value, O'Brien) returns 1 row iff the caller's Search actually ran.
		// On the buggy code prm.Search was overwritten with `SELECT * FROM
		// secrets` and returned 2 rows, so this discriminates fixed vs buggy.
		var got []shared.TestDataWithIDS
		prm := &list.List{Search: "SELECT * FROM secrets WHERE name = 'O''Brien'"}

		require.NoError(t, str.List(ctx, "secrets", &got, prm))
		require.Len(t, got, 1, "quoted-value WHERE must match exactly one of the two rows")
		assert.Equal(t, "O'Brien", got[0].Name)
	})

	t.Run("edge - default path builds SELECT * FROM <target> without touching prm.Search", func(t *testing.T) {
		// The only interpolation the fix performs is `SELECT * FROM <target>`.
		// `target` resolves via shared.TargetName from the caller's target arg
		// (or the storage's configured m.Target) — it never comes from
		// prm.Search, so a caller Search can never inject into the default path.
		// With nil params the query is the plain default and returns all rows.
		var got []shared.TestDataWithIDS
		require.NoError(t, str.List(ctx, shared.TableName, &got, nil))
		assert.Len(t, got, 3)
	})

	t.Run("edge - empty Search does NOT mutate the caller-owned params struct", func(t *testing.T) {
		// Regression guard: defaulting must not write the default query back into
		// the caller's struct (it may be reused). Search must remain "" after the
		// call even though the executed query defaulted to SELECT * FROM target.
		prm := &list.List{Search: ""}
		var got []shared.TestDataWithIDS

		require.NoError(t, str.List(ctx, shared.TableName, &got, prm))
		assert.Len(t, got, 3)
		assert.Equal(t, "", prm.Search, "caller params.Search must not be mutated by List")
	})
}

// TestCount_HonorsSearch guards the identical overwrite bug in Count. It shares
// the process-wide storage (see getTestStorage) so New() is called only once.
func TestCount_HonorsSearch(t *testing.T) {
	ctx := t.Context()
	str := getTestStorage(ctx, t)

	t.Run("happy - nil params counts all rows", func(t *testing.T) {
		got, err := str.Count(ctx, shared.TableName, nil)
		require.NoError(t, err)
		assert.EqualValues(t, 3, got)
	})

	t.Run("happy - Search filters the count", func(t *testing.T) {
		prm := &count.Count{Search: "SELECT COUNT(*) FROM " + shared.TableName + " WHERE name = 'alpha'"}

		got, err := str.Count(ctx, shared.TableName, prm)
		require.NoError(t, err)

		// BUG (pre-fix): returns 3 because prm.Search is overwritten.
		assert.EqualValues(t, 2, got, "Count WHERE name='alpha' must return 2")
	})

	t.Run("edge - empty Search falls back to default count without mutating params", func(t *testing.T) {
		prm := &count.Count{Search: ""}
		got, err := str.Count(ctx, shared.TableName, prm)
		require.NoError(t, err)
		assert.EqualValues(t, 3, got, "empty Search must behave like nil params")
		assert.Equal(t, "", prm.Search, "caller params.Search must not be mutated by Count")
	})

	t.Run("bad - malformed Search surfaces an error", func(t *testing.T) {
		_, err := str.Count(ctx, shared.TableName,
			&count.Count{Search: "SELECT COUNT(*) FROM " + shared.TableName + " WHERE"})
		require.Error(t, err, "a malformed count Search must error")
		assert.Contains(t, err.Error(), "count", "error should be tagged as a failed count operation")
	})
}
