package logging

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v3/tracing"
	"github.com/thalesfsp/sypl/v2/fields"
)

type fieldsTracer struct{ fields map[string]any }

func (fieldsTracer) Start(ctx context.Context, _, _, _ string) (context.Context, tracing.Span) {
	return tracing.Noop().Start(ctx, "", "", "")
}

func (fieldsTracer) RecordError(context.Context, error) {}

func (f fieldsTracer) LogFields(context.Context) map[string]any { return f.fields }

func TestGet_Singleton(t *testing.T) {
	l1 := Get()
	l2 := Get()

	require.NotNil(t, l1)
	assert.Same(t, l1, l2, "Get must always return the same logger instance")
}

func TestToAPM_NoTracer(t *testing.T) {
	// Nil fields + the default no-op tracer: returns an initialized, empty
	// fields map.
	f := ToAPM(t.Context(), nil)

	require.NotNil(t, f)
	assert.Empty(t, f)
}

func TestToAPM_MergesTracerFields(t *testing.T) {
	previous := tracing.Set(fieldsTracer{fields: map[string]any{
		"trace.id": "t-1",
		"span.id":  "s-1",
	}})
	t.Cleanup(func() { tracing.Set(previous) })

	f := ToAPM(t.Context(), fields.Fields{"pre": "kept"})

	assert.Equal(t, fields.Fields{"pre": "kept", "trace.id": "t-1", "span.id": "s-1"}, f)
}
