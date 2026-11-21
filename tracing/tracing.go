// Package tracing is the hook dal calls around every storage operation.
//
// By default it does nothing: dal links no tracing backend and starts nothing
// at init. To send dal's spans somewhere, install a Tracer once at start-up,
// before the first storage operation:
//
//	tracing.Set(myTracer)
//
// The Elastic APM implementation lives in the opt-in package
// github.com/thalesfsp/dal/v3/elasticapm; only programs that import it link
// Elastic APM.
package tracing

import (
	"context"
	"sync/atomic"
)

//////
// Vars, consts, and types.
//////

// Span is an operation's span. End is called exactly once, when the operation
// returns.
type Span interface {
	End()
}

// Tracer receives dal's operation telemetry. Implementations must be safe for
// concurrent use.
type Tracer interface {
	// Start starts a span for one operation and returns the context the
	// operation runs in. storageType is storage.Type ("storage"), adapter the
	// adapter name ("memory", "postgres", ...) and operation the status of the
	// operation ("created", "listed", ...).
	Start(ctx context.Context, storageType, adapter, operation string) (context.Context, Span)

	// RecordError reports a failed operation. err is never nil.
	RecordError(ctx context.Context, err error)

	// LogFields returns the fields that correlate a log line with the trace in
	// ctx, such as a trace id. It may return nil.
	LogFields(ctx context.Context) map[string]any
}

// holder lets atomic.Pointer store an interface value.
type holder struct{ tracer Tracer }

type noopSpan struct{}

func (noopSpan) End() {}

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _, _, _ string) (context.Context, Span) {
	return ctx, noopSpan{}
}

func (noopTracer) RecordError(context.Context, error) {}

func (noopTracer) LogFields(context.Context) map[string]any { return nil }

var current atomic.Pointer[holder]

//////
// Exported functionalities.
//////

// Noop returns the default Tracer, which does nothing.
func Noop() Tracer {
	return noopTracer{}
}

// Get returns the installed Tracer, or the no-op one when none is installed.
func Get() Tracer {
	if h := current.Load(); h != nil {
		return h.tracer
	}

	return noopTracer{}
}

// Set installs t as the Tracer for every dal operation and returns the one it
// replaces. Set(nil) restores the no-op Tracer.
func Set(t Tracer) Tracer {
	if t == nil {
		t = noopTracer{}
	}

	previous := current.Swap(&holder{tracer: t})
	if previous == nil {
		return noopTracer{}
	}

	return previous.tracer
}

// Start starts a span on the installed Tracer. It never returns a nil Span.
func Start(ctx context.Context, storageType, adapter, operation string) (context.Context, Span) {
	ctx, span := Get().Start(ctx, storageType, adapter, operation)
	if span == nil {
		span = noopSpan{}
	}

	return ctx, span
}
