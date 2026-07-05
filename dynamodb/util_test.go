package dynamodb

import (
	"net/http"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
)

var placeholderRE = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func TestSanitizePlaceholder(t *testing.T) {
	// Safe names pass through untouched.
	for _, name := range []string{"name", "Name_1", "created_at", "a1"} {
		assert.Equal(t, name, sanitizePlaceholder(name))
	}

	// Unsafe names become expression-safe.
	for _, name := range []string{"created-at", "user.email", "with space", "emoji😀"} {
		got := sanitizePlaceholder(name)
		assert.Regexp(t, placeholderRE, got, "sanitized %q must be expression-safe", name)
	}

	// Distinct unsafe names must not collide after sanitization.
	assert.NotEqual(t, sanitizePlaceholder("a-b"), sanitizePlaceholder("a.b"))

	// Deterministic.
	assert.Equal(t, sanitizePlaceholder("a-b"), sanitizePlaceholder("a-b"))
}

func TestBuildFilterExpression_HappyPath(t *testing.T) {
	expr, names, values, err := BuildFilterExpression(map[string]interface{}{
		"name": "alpha",
	})
	require.NoError(t, err)
	require.NotNil(t, expr)

	assert.Equal(t, "(#name = :name)", *expr)
	assert.Equal(t, "name", *names["#name"])
	assert.Equal(t, "alpha", *values[":name"].S)
}

// Attribute names DynamoDB placeholders can't carry directly (dashes, dots)
// must still produce a valid expression (regression: ValidationException).
func TestBuildFilterExpression_UnsafeAttributeNames(t *testing.T) {
	expr, names, values, err := BuildFilterExpression(map[string]interface{}{
		"created-at": "2020-01-01",
	})
	require.NoError(t, err)
	require.NotNil(t, expr)

	require.Len(t, names, 1)
	require.Len(t, values, 1)

	for placeholder, original := range names {
		assert.Regexp(t, `^#[A-Za-z0-9_]+$`, placeholder)
		assert.Equal(t, "created-at", *original, "the real attribute name is carried in the names map")
	}
}

func TestBuildFilterExpression_Empty(t *testing.T) {
	expr, names, values, err := BuildFilterExpression(nil)
	require.NoError(t, err)
	assert.Nil(t, expr)
	assert.Nil(t, names)
	assert.Nil(t, values)
}

func TestBuildUpdateExpression_SkipsPrimaryKeyAndErrors(t *testing.T) {
	// Happy path.
	expr, names, values, err := BuildUpdateExpression(map[string]interface{}{
		"id":   "pk-value",
		"name": "alpha",
	}, "id")
	require.NoError(t, err)
	require.NotNil(t, expr)

	assert.Equal(t, "SET #name = :name", *expr)
	assert.NotContains(t, names, "#id", "the primary key must not be updated")
	assert.Contains(t, values, ":name")

	// Bad path: nothing to update.
	expr, names, values, err = BuildUpdateExpression(map[string]interface{}{}, "id")
	assert.Error(t, err)
	assert.Nil(t, expr)
	assert.Nil(t, names)
	assert.Nil(t, values)

	// Edge: only the primary key → error, not an empty SET.
	expr, names, values, err = BuildUpdateExpression(map[string]interface{}{"id": "x"}, "id")
	assert.Error(t, err)
	assert.Nil(t, expr)
	assert.Nil(t, names)
	assert.Nil(t, values)
}

func TestBuildProjectionExpression(t *testing.T) {
	expr, names := BuildProjectionExpression([]string{"name", "created-at"})
	require.NotNil(t, expr)
	assert.Len(t, names, 2)

	// Empty input.
	expr, names = BuildProjectionExpression(nil)
	assert.Nil(t, expr)
	assert.Nil(t, names)
}

func TestErrorHelpers(t *testing.T) {
	notFound := awserr.New(dynamodb.ErrCodeResourceNotFoundException, "nope", nil)
	condFailed := awserr.New(dynamodb.ErrCodeConditionalCheckFailedException, "cond", nil)
	throughput := awserr.New(dynamodb.ErrCodeProvisionedThroughputExceededException, "slow", nil)

	assert.True(t, IsNotFoundError(notFound))
	assert.False(t, IsNotFoundError(nil))
	assert.False(t, IsNotFoundError(assert.AnError))
	assert.True(t, IsNotFoundError(customerror.NewHTTPError(http.StatusNotFound)))

	assert.True(t, IsConditionalCheckFailedError(condFailed))
	assert.False(t, IsConditionalCheckFailedError(notFound))

	assert.True(t, IsProvisionedThroughputExceededError(throughput))
	assert.False(t, IsProvisionedThroughputExceededError(condFailed))
}

func TestMarshalRoundTrip(t *testing.T) {
	type row struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	item, err := MarshalItem(row{ID: "1", Name: "alpha"})
	require.NoError(t, err)

	var got row
	require.NoError(t, UnmarshalItem(item, &got))
	assert.Equal(t, row{ID: "1", Name: "alpha"}, got)

	key, err := MarshalKey("id", "1")
	require.NoError(t, err)
	assert.Equal(t, "1", *key["id"].S)
}
