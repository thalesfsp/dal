package logging

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/sypl/v2/fields"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
)

func TestGet_Singleton(t *testing.T) {
	l1 := Get()
	l2 := Get()

	require.NotNil(t, l1)
	assert.Same(t, l1, l2, "Get must always return the same logger instance")
}

func TestToAPM_NoTransaction(t *testing.T) {
	// Nil fields + no transaction in context: returns an initialized, empty
	// fields map.
	f := ToAPM(t.Context(), nil)

	require.NotNil(t, f)
	assert.Empty(t, f)
}

func TestToAPM_WithTransactionAndSpan(t *testing.T) {
	tracer := apmtest.NewRecordingTracer()
	defer tracer.Close()

	tx := tracer.StartTransaction("tx", "test")
	defer tx.End()

	ctx := apm.ContextWithTransaction(t.Context(), tx)

	span, ctx := apm.StartSpan(ctx, "span", "test")
	defer span.End()

	f := ToAPM(ctx, fields.Fields{"pre": "kept"})

	assert.Equal(t, "kept", f["pre"])
	assert.NotEmpty(t, f["trace.id"])
	assert.NotEmpty(t, f["transaction.id"])
	assert.NotEmpty(t, f["span.id"])
}
