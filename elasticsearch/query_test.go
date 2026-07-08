package elasticsearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/params/v2/list"
)

const matchAll = `{"match_all":{}}`

// track_total_hits must be emitted whether ListAny is passed by pointer or by
// value (regression: Count passed it by value, the pointer-only assertion
// dropped it, and counts silently capped at 10k).
func TestBuildQuery_TrackTotalHitsPointerAndValue(t *testing.T) {
	byPointer, err := buildQuery(&list.List{
		Search: matchAll,
		Any:    &ListAny{TrackTotalHits: true},
	})
	require.NoError(t, err)
	assert.Contains(t, byPointer, `"track_total_hits": true`)

	byValue, err := buildQuery(&list.List{
		Search: matchAll,
		Any:    ListAny{TrackTotalHits: true},
	})
	require.NoError(t, err)
	assert.Contains(t, byValue, `"track_total_hits": true`)
}

func TestBuildQuery_NoTrackTotalHits(t *testing.T) {
	q, err := buildQuery(&list.List{Search: matchAll})
	require.NoError(t, err)
	assert.NotContains(t, q, "track_total_hits")

	q, err = buildQuery(&list.List{
		Search: matchAll,
		Any:    &ListAny{TrackTotalHits: false},
	})
	require.NoError(t, err)
	assert.NotContains(t, q, "track_total_hits")
}

// The generated query must be valid JSON with from/size only when positive.
func TestBuildQuery_OffsetLimitAndValidJSON(t *testing.T) {
	q, err := buildQuery(&list.List{
		Search: matchAll,
		Offset: 20,
		Limit:  10,
		Any:    &ListAny{TrackTotalHits: true},
	})
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(q), &decoded), "query must be valid JSON: %s", q)

	assert.EqualValues(t, 20, decoded["from"])
	assert.EqualValues(t, 10, decoded["size"])
	assert.Equal(t, true, decoded["track_total_hits"])

	// Zero offset/limit must be omitted.
	q, err = buildQuery(&list.List{Search: matchAll})
	require.NoError(t, err)
	assert.NotContains(t, q, `"from"`)
	assert.NotContains(t, q, `"size"`)
}
