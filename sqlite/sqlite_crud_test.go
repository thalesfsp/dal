package sqlite

import (
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
// Offline end-to-end tests against a real SQLite database, reusing the shared
// seeded storage from sqlite_list_test.go.
//////

// Full CRUD pass through the public IStorage surface against a real database.
func TestSQLite_CRUDRoundTrip(t *testing.T) {
	ctx := t.Context()
	str := getTestStorage(ctx, t)

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
