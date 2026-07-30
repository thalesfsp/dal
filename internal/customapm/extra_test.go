package customapm

import (
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
)

func TestOutcome_String(t *testing.T) {
	assert.Equal(t, "failure", Failure.String())
	assert.Equal(t, "success", Success.String())
}

func TestTXFromCtx(t *testing.T) {
	// No transaction in the context: a new one is created.
	tx := TXFromCtx(t.Context(), "tx-name", "tx-type")
	require.NotNil(t, tx)
	tx.End()

	// Existing transaction is reused, not replaced.
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	existing := tracer.StartTransaction("existing", "test")
	defer existing.End()

	ctx := apm.ContextWithTransaction(t.Context(), existing)

	got := TXFromCtx(ctx, "ignored", "ignored")
	assert.Same(t, existing, got)
}

func TestNewLoggerAndMethods(t *testing.T) {
	l, err := NewLogger()
	require.NoError(t, err)
	require.NotNil(t, l)

	// Must not panic; Debugf is muted at the default ERROR level and Errorf
	// writes to stderr.
	l.Debugf("debug %s", "message")
	l.Errorf("error %s", "message")
}

func TestTraceError_SpanOutcomeSetToFailure(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	tx := tracer.StartTransaction("tx", "test")
	ctx := apm.ContextWithTransaction(t.Context(), tx)

	span, ctx := apm.StartSpan(ctx, "op", "test")

	counter := expvar.NewInt("customapm_extra_test.failed")

	err := TraceError(ctx, assert.AnError, nil, counter)
	require.ErrorIs(t, err, assert.AnError)

	assert.Equal(t, string(Failure), span.Outcome,
		"TraceError must mark the active span as failed")
	assert.Equal(t, int64(1), counter.Value())

	span.End()
	tx.End()
}

func TestTraceError_NilMetricAndLogger(t *testing.T) {
	// Both metric and logger nil: still returns the original error, no panic.
	err := TraceError(t.Context(), assert.AnError, nil, nil)
	assert.ErrorIs(t, err, assert.AnError)
}
