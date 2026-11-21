package elasticapm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/elasticapm"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/memory"
	"github.com/thalesfsp/dal/v3/tracing"
	"github.com/thalesfsp/params/v2/retrieve"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
)

func recording(t *testing.T) *apmtest.RecordingTracer {
	t.Helper()

	tracer := apmtest.NewRecordingTracer()
	previous := tracing.Set(elasticapm.New(tracer.Tracer))

	t.Cleanup(func() {
		tracing.Set(previous)
		tracer.Close()
	})

	return tracer
}

// Opt-in path, end to end: a consumer installs the Elastic tracer and every
// dal operation reports a span and its implicit transaction; a failing
// operation also reports the error.
func TestOptIn_EndToEndThroughAnAdapter(t *testing.T) {
	tracer := recording(t)
	ctx := t.Context()

	str, err := memory.New(ctx)
	require.NoError(t, err)

	_, err = str.Create(ctx, "id-1", "docs", shared.TestData, nil)
	require.NoError(t, err)

	var got shared.TestDataS

	err = str.Retrieve(ctx, "missing", "docs", &got, &retrieve.Retrieve{})
	require.Error(t, err)

	tracer.Flush(nil)
	payloads := tracer.Payloads()

	require.Len(t, payloads.Spans, 2, "one span per operation")
	assert.Equal(t, "memory.created", payloads.Spans[0].Name)
	assert.Equal(t, "storage", payloads.Spans[0].Type)
	assert.Equal(t, "memory", payloads.Spans[0].Subtype)
	assert.Equal(t, "created", payloads.Spans[0].Action)
	assert.Equal(t, "success", payloads.Spans[0].Outcome)
	assert.Equal(t, "failure", payloads.Spans[1].Outcome, "a failed operation marks its span failed")
	assert.Len(t, payloads.Transactions, 2, "each implicit transaction is ended and reported")
	assert.Len(t, payloads.Errors, 1, "the failure is captured")
}

// Enable installs Elastic APM on apm.DefaultTracer and returns the tracer it
// replaced.
func TestEnable_UsesDefaultTracer(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	prevDefault := apm.DefaultTracer
	apm.DefaultTracer = tracer.Tracer

	defer func() { apm.DefaultTracer = prevDefault }()

	previous := elasticapm.Enable()
	defer tracing.Set(previous)

	assert.Equal(t, tracing.Noop(), previous)
	assert.IsType(t, &elasticapm.Tracer{}, tracing.Get())

	_, span := tracing.Start(t.Context(), "storage", "memory", "created")
	span.End()

	tracer.Flush(nil)
	assert.Len(t, tracer.Payloads().Spans, 1)
	assert.Len(t, tracer.Payloads().Transactions, 1)
}

// An inbound transaction is reused and NOT ended — the caller owns it.
func TestStart_ExistingTransactionNotEnded(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	tx := tracer.StartTransaction("parent", "request")
	ctx := apm.ContextWithTransaction(t.Context(), tx)

	ctx, span := elasticapm.New(tracer.Tracer).Start(ctx, "storage", "memory", "created")
	require.NotNil(t, span)
	assert.Same(t, tx, apm.TransactionFromContext(ctx))

	span.End()
	tracer.Flush(nil)

	payloads := tracer.Payloads()
	assert.Empty(t, payloads.Transactions, "the caller-owned transaction must not be ended")
	assert.Len(t, payloads.Spans, 1)

	tx.End()
}

// RecordError marks the span failed and captures the innermost error;
// nil is ignored.
func TestRecordError(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	et := elasticapm.New(tracer.Tracer)
	ctx, span := et.Start(t.Context(), "storage", "memory", "created")

	et.RecordError(ctx, nil)
	et.RecordError(ctx, customerror.NewFailedToError("op", customerror.WithError(assert.AnError)))

	assert.Equal(t, "failure", apm.SpanFromContext(ctx).Outcome)

	span.End()
	tracer.Flush(nil)

	errs := tracer.Payloads().Errors
	require.Len(t, errs, 1)
	assert.Equal(t, assert.AnError.Error(), errs[0].Exception.Message, "the innermost error is captured")
}

// LogFields carries the trace, transaction and span ids, or nil with no
// transaction.
func TestLogFields(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	et := elasticapm.New(tracer.Tracer)
	assert.Nil(t, et.LogFields(t.Context()))

	tx := tracer.StartTransaction("tx", "test")
	defer tx.End()

	ctx := apm.ContextWithTransaction(t.Context(), tx)

	f := et.LogFields(ctx)
	assert.Equal(t, tx.TraceContext().Trace.String(), f["trace.id"])
	assert.Equal(t, tx.TraceContext().Span.String(), f["transaction.id"])
	assert.NotContains(t, f, "span.id")

	span, ctx := apm.StartSpan(ctx, "span", "test")
	defer span.End()

	assert.Equal(t, span.TraceContext().Span.String(), et.LogFields(ctx)["span.id"])
}
