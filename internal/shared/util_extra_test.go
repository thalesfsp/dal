package shared

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateSQLIdentifier_HappyPaths(t *testing.T) {
	for _, id := range []string{
		"users",
		"Users",
		"_private",
		"table1",
		"public.users",
		"db.schema.table",
		"a$b",
	} {
		assert.NoError(t, ValidateSQLIdentifier(id), id)
	}
}

func TestValidateSQLIdentifier_RejectsInjection(t *testing.T) {
	for _, id := range []string{
		"",
		"users; DROP TABLE users; --",
		"users WHERE 1=1",
		`users"`,
		"users'",
		"users`",
		"users--",
		"1users",
		"us ers",
		"users;",
		"users\n",
		".users",
		"users.",
	} {
		assert.Error(t, ValidateSQLIdentifier(id), "%q must be rejected", id)
	}
}

// An empty expected substring must never vacuously match (regression).
func TestErrorContains_EmptyTextNeverMatches(t *testing.T) {
	err := errors.New("some failure")

	assert.False(t, ErrorContains(err, ""))
	assert.False(t, ErrorContains(err, "", "unrelated"))
	assert.True(t, ErrorContains(err, "", "failure"), "real substrings still match")
}

func TestErrorContains_Basics(t *testing.T) {
	assert.False(t, ErrorContains(nil, "anything"))
	assert.True(t, ErrorContains(errors.New("boom today"), "boom"))
	assert.False(t, ErrorContains(errors.New("boom"), "bang"))
}
