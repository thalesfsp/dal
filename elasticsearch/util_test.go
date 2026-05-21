package elasticsearch

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real-shape sample: a production `search_phase_execution_exception` whose
// top-level reason is the misleading "all shards failed", with the
// actionable detail nested under caused_by + duplicated under root_cause[0].
// Before v2.1.1, parseResponseBodyError dropped both chains and consumers
// saw only "all shards failed".
const sampleSearchPhaseError = `{
  "error": {
    "type": "search_phase_execution_exception",
    "reason": "all shards failed",
    "root_cause": [
      {
        "type": "illegal_argument_exception",
        "reason": "Result window is too large, from + size must be less than or equal to: [10000] but was [517800]"
      }
    ],
    "caused_by": {
      "type": "illegal_argument_exception",
      "reason": "Result window is too large, from + size must be less than or equal to: [10000] but was [517800]"
    }
  },
  "status": 500
}`

// Common shape: a 404 from a missing index, no caused_by chain. The
// surfaced reason must remain identical to pre-v2.1.1 output so existing
// callers that substring-match on "no such index" keep working.
const sampleSimpleError = `{
  "error": {
    "type": "resource_not_found_exception",
    "reason": "no such index [foo]"
  },
  "status": 404
}`

// Nested chain with three distinct levels, each adding new information.
const sampleNestedCausedByError = `{
  "error": {
    "type": "exception",
    "reason": "top-level",
    "caused_by": {
      "type": "exception",
      "reason": "mid",
      "caused_by": {
        "type": "exception",
        "reason": "root cause"
      }
    }
  },
  "status": 500
}`

// root_cause present, caused_by absent. Should surface root_cause[0].
const sampleRootCauseOnlyError = `{
  "error": {
    "type": "search_phase_execution_exception",
    "reason": "all shards failed",
    "root_cause": [
      {
        "type": "index_closed_exception",
        "reason": "closed"
      }
    ]
  },
  "status": 400
}`

func TestParseResponseBodyError_SurfacesCausedByChain(t *testing.T) {
	re, err := parseResponseBodyError(bytes.NewReader([]byte(sampleSearchPhaseError)))
	require.NoError(t, err)
	require.NotNil(t, re)

	assert.Equal(t, 500, re.Status, "Status must be populated (was a latent bug pre-v2.1.1)")
	assert.True(t, strings.HasPrefix(re.Reason, "all shards failed"),
		"top-level reason must come first — preserves substring-match compatibility with existing callers")
	assert.Contains(t, re.Reason, "Result window is too large",
		"caused_by reason must be surfaced — this is the whole point of v2.1.1")

	// root_cause duplicates caused_by here; dedup must prevent double-print.
	occurrences := strings.Count(re.Reason, "Result window is too large")
	assert.Equal(t, 1, occurrences, "duplicate caused_by + root_cause text must be deduplicated")
}

func TestParseResponseBodyError_SimpleErrorUnchanged(t *testing.T) {
	re, err := parseResponseBodyError(bytes.NewReader([]byte(sampleSimpleError)))
	require.NoError(t, err)
	require.NotNil(t, re)

	assert.Equal(t, 404, re.Status)
	assert.Equal(t, "no such index [foo]", re.Reason,
		"without caused_by/root_cause, output must exactly match pre-v2.1.1 behavior")
}

func TestParseResponseBodyError_RecursiveCausedByChain(t *testing.T) {
	re, err := parseResponseBodyError(bytes.NewReader([]byte(sampleNestedCausedByError)))
	require.NoError(t, err)
	require.NotNil(t, re)

	assert.Equal(t, 500, re.Status)
	assert.Equal(t, "top-level: mid: root cause", re.Reason,
		"three-level chain must surface in top → leaf order with `: ` separators")
}

func TestParseResponseBodyError_RootCauseOnly(t *testing.T) {
	re, err := parseResponseBodyError(bytes.NewReader([]byte(sampleRootCauseOnlyError)))
	require.NoError(t, err)
	require.NotNil(t, re)

	assert.Equal(t, 400, re.Status)
	assert.Equal(t, "all shards failed: closed", re.Reason,
		"when caused_by absent, root_cause[0] supplies the leaf detail")
}

func TestParseResponseBodyError_DepthBound(t *testing.T) {
	// Build a 17-level chain. The 16-deep cap means level 16 (zero-indexed)
	// is dropped to prevent pathological CPU consumption on malformed
	// responses.
	var b strings.Builder
	b.WriteString(`{"error": {"type": "x", "reason": "L0"`)
	for i := 1; i <= 16; i++ {
		fmt.Fprintf(&b, `, "caused_by": {"type": "x", "reason": "L%d"`, i)
	}
	for i := 1; i <= 16; i++ {
		b.WriteString("}")
	}
	b.WriteString(`}, "status": 500}`)

	re, err := parseResponseBodyError(bytes.NewReader([]byte(b.String())))
	require.NoError(t, err)
	require.NotNil(t, re)

	for i := 0; i <= 15; i++ {
		assert.Contains(t, re.Reason, fmt.Sprintf("L%d", i),
			"depths 0..15 must be in the chain")
	}
	assert.NotContains(t, re.Reason, "L16",
		"depth beyond maxCausedByDepth must be dropped — bounds the recursion")
}

func TestBuildReasonChain_EmptyTopReason(t *testing.T) {
	// Edge: top-level reason absent but caused_by present.
	r := ResponseErrorFromESReason{
		CausedBy: &ResponseErrorFromESReason{Reason: "only cause"},
	}
	assert.Equal(t, "only cause", buildReasonChain(r))
}

func TestBuildReasonChain_NilEverything(t *testing.T) {
	assert.Equal(t, "", buildReasonChain(ResponseErrorFromESReason{}))
}

func TestParseResponseBodyError_MalformedJSON_ReturnsError(t *testing.T) {
	_, err := parseResponseBodyError(bytes.NewReader([]byte(`{not json`)))
	require.Error(t, err, "malformed JSON must produce a parse error, not a panic")
}
