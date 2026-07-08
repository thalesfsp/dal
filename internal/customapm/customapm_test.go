package customapm

import (
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
)

// Trace without an inbound transaction starts one implicitly; Span.End must
// end BOTH the span and that transaction so it is reported instead of leaked.
func TestTrace_ImplicitTransactionIsEnded(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	prev := apm.DefaultTracer
	apm.DefaultTracer = tracer.Tracer

	defer func() { apm.DefaultTracer = prev }()

	ctx, span := Trace(t.Context(), "storage", "memory", "created")
	require.NotNil(t, span)
	require.NotNil(t, ctx)

	span.End()

	tracer.Flush(nil)

	payloads := tracer.Payloads()
	assert.Len(t, payloads.Transactions, 1,
		"the implicitly-created transaction must be ended and reported")
	assert.Len(t, payloads.Spans, 1)
}

// Trace with an inbound transaction must reuse it and must NOT end it — the
// caller owns its lifecycle.
func TestTrace_ExistingTransactionNotEnded(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	tx := tracer.StartTransaction("parent", "request")
	ctx := apm.ContextWithTransaction(t.Context(), tx)

	ctx, span := Trace(ctx, "storage", "memory", "created")
	require.NotNil(t, span)
	require.NotNil(t, ctx)

	span.End()

	tracer.Flush(nil)

	payloads := tracer.Payloads()
	assert.Empty(t, payloads.Transactions,
		"the caller-owned transaction must not be ended by Span.End")
	assert.Len(t, payloads.Spans, 1)

	tx.End()
}

// End on a nil Span or a zero Span must be a no-op, never a panic.
func TestSpan_EndNilSafe(t *testing.T) {
	var s *Span

	assert.NotPanics(t, func() { s.End() })
	assert.NotPanics(t, func() { (&Span{}).End() })
}

// TraceError(nil) is a no-op returning nil — it must not touch the metric.
func TestTraceError_NilError(t *testing.T) {
	metric := &expvar.Int{}

	err := TraceError(t.Context(), nil, nil, metric)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), metric.Value())
}

// TraceError returns the ORIGINAL (wrapped) error and increments the metric.
func TestTraceError_ReturnsOriginalAndCounts(t *testing.T) {
	metric := &expvar.Int{}
	original := customerror.NewFailedToError("op", customerror.WithError(assert.AnError))

	got := TraceError(t.Context(), original, nil, metric)
	assert.Same(t, original, got)
	assert.Equal(t, int64(1), metric.Value())
}
