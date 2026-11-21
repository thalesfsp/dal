package customapm

import (
	"context"
	"expvar"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/tracing"
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/level"
	"github.com/thalesfsp/sypl/v2/output"
)

type ctxKey struct{}

type fakeSpan struct{ ended *int }

func (s fakeSpan) End() { *s.ended++ }

type fakeTracer struct {
	started []string
	ended   int
	errs    []error
}

func (f *fakeTracer) Start(ctx context.Context, what, nameOf, operation string) (context.Context, tracing.Span) {
	f.started = append(f.started, what+"."+nameOf+"."+operation)

	return context.WithValue(ctx, ctxKey{}, "traced"), fakeSpan{ended: &f.ended}
}

func (f *fakeTracer) RecordError(_ context.Context, err error) { f.errs = append(f.errs, err) }

func (f *fakeTracer) LogFields(ctx context.Context) map[string]any {
	if ctx.Value(ctxKey{}) == nil {
		return nil
	}

	return map[string]any{"trace.id": "t-1"}
}

func install(t *testing.T) *fakeTracer {
	t.Helper()

	fake := &fakeTracer{}
	previous := tracing.Set(fake)

	t.Cleanup(func() { tracing.Set(previous) })

	return fake
}

func recorder() (*output.RecorderOutput, sypl.ISypl) {
	rec, out := output.Recorder(level.Error)

	return rec, sypl.New("test", out)
}

// With no tracer installed, Trace hands back the same context and a usable span.
func TestTrace_DefaultIsNoop(t *testing.T) {
	ctx := t.Context()

	got, span := Trace(ctx, "storage", "memory", "created")
	require.NotNil(t, span)
	assert.Equal(t, ctx, got)
	assert.NotPanics(t, span.End)
}

// Trace delegates to the installed tracer, with the operation's names.
func TestTrace_UsesInstalledTracer(t *testing.T) {
	fake := install(t)

	ctx, span := Trace(t.Context(), "storage", "memory", "created")
	span.End()

	assert.Equal(t, []string{"storage.memory.created"}, fake.started)
	assert.Equal(t, 1, fake.ended)
	assert.Equal(t, "traced", ctx.Value(ctxKey{}))
}

// TraceError(nil) is a no-op returning nil: no metric, no report.
func TestTraceError_NilError(t *testing.T) {
	fake := install(t)
	metric := &expvar.Int{}

	require.NoError(t, TraceError(t.Context(), nil, nil, metric))
	assert.Equal(t, int64(0), metric.Value())
	assert.Empty(t, fake.errs)
}

// TraceError returns the ORIGINAL error, counts it, reports it to the tracer,
// and logs the innermost message with the tracer's correlation fields.
func TestTraceError_ReportsCountsAndLogs(t *testing.T) {
	fake := install(t)
	metric := &expvar.Int{}
	original := customerror.NewFailedToError("op", customerror.WithError(assert.AnError))
	rec, logger := recorder()

	ctx, span := Trace(t.Context(), "storage", "memory", "created")
	defer span.End()

	got := TraceError(ctx, original, logger, metric)

	assert.Same(t, original, got)
	assert.Equal(t, int64(1), metric.Value())
	require.Len(t, fake.errs, 1)
	assert.Same(t, original, fake.errs[0], "the tracer receives the original error")

	msgs := rec.Messages()
	require.Len(t, msgs, 1)
	assert.Equal(t, assert.AnError.Error(), strings.TrimSpace(msgs[0].OriginalContent), "the innermost message is logged")
	assert.Equal(t, "t-1", msgs[0].Fields["trace.id"])
}

// Nil metric and nil logger: still returns the original error, no panic.
func TestTraceError_NilMetricAndLogger(t *testing.T) {
	err := TraceError(t.Context(), assert.AnError, nil, nil)
	assert.ErrorIs(t, err, assert.AnError)
}
