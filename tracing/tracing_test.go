package tracing

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ctxKey struct{}

type fakeSpan struct{ ended *int }

func (s fakeSpan) End() { *s.ended++ }

type fakeTracer struct {
	started int
	ended   int
	errs    []error
	nilSpan bool
}

func (f *fakeTracer) Start(ctx context.Context, storageType, adapter, operation string) (context.Context, Span) {
	f.started++

	if f.nilSpan {
		return ctx, nil
	}

	return context.WithValue(ctx, ctxKey{}, storageType+"."+adapter+"."+operation), fakeSpan{ended: &f.ended}
}

func (f *fakeTracer) RecordError(_ context.Context, err error) { f.errs = append(f.errs, err) }

func (f *fakeTracer) LogFields(context.Context) map[string]any { return map[string]any{"k": "v"} }

func TestDefaultIsNoop(t *testing.T) {
	ctx := t.Context()

	got, span := Start(ctx, "storage", "memory", "created")
	require.NotNil(t, span)
	assert.Equal(t, ctx, got, "the no-op tracer must not change the context")
	assert.NotPanics(t, span.End)

	assert.NotPanics(t, func() { Get().RecordError(ctx, errors.New("x")) })
	assert.Nil(t, Get().LogFields(ctx))
	assert.Equal(t, Noop(), Get())
}

func TestSetInstallsAndRestores(t *testing.T) {
	fake := &fakeTracer{}

	previous := Set(fake)
	t.Cleanup(func() { Set(previous) })

	assert.Equal(t, Noop(), previous, "the first Set returns the no-op tracer")
	assert.Same(t, fake, Get())

	ctx, span := Start(t.Context(), "storage", "memory", "created")
	assert.Equal(t, "storage.memory.created", ctx.Value(ctxKey{}))

	span.End()
	assert.Equal(t, 1, fake.started)
	assert.Equal(t, 1, fake.ended)

	assert.Same(t, fake, Set(nil), "Set returns the tracer it replaces")
	assert.Equal(t, Noop(), Get(), "Set(nil) restores the no-op tracer")
}

func TestStartNeverReturnsNilSpan(t *testing.T) {
	previous := Set(&fakeTracer{nilSpan: true})
	t.Cleanup(func() { Set(previous) })

	_, span := Start(t.Context(), "storage", "memory", "created")
	require.NotNil(t, span)
	assert.NotPanics(t, span.End)
}

// statelessTracer is safe to share between goroutines, unlike fakeTracer.
type statelessTracer struct{ noopTracer }

func TestSetIsConcurrencySafe(t *testing.T) {
	previous := Get()
	t.Cleanup(func() { Set(previous) })

	var wg sync.WaitGroup

	for range 50 {
		wg.Add(2)

		go func() {
			defer wg.Done()

			Set(&statelessTracer{})
		}()

		go func() {
			defer wg.Done()

			_, span := Start(context.Background(), "storage", "memory", "created")
			span.End()
		}()
	}

	wg.Wait()
}
