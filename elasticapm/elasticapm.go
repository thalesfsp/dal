// Package elasticapm sends dal's operation spans, errors and log-correlation
// fields to Elastic APM. It is opt-in: dal's other packages do not import it,
// so a program links Elastic APM only when it imports this package.
//
// Importing go.elastic.co/apm (which this package does) builds
// apm.DefaultTracer from the ELASTIC_APM_* environment variables at init and
// starts its background goroutines; that is the reason it is not in dal's core.
//
// Enable it once at start-up, before the first storage operation:
//
//	elasticapm.Enable()                       // apm.DefaultTracer
//	tracing.Set(elasticapm.New(myTracer))     // or a tracer of your own
package elasticapm

import (
	"context"
	"errors"
	"fmt"

	"github.com/thalesfsp/dal/v3/tracing"
	"go.elastic.co/apm"
)

//////
// Vars, consts, and types.
//////

// outcomeFailure is the APM span outcome of a failed operation.
const outcomeFailure = "failure"

// Tracer implements tracing.Tracer on an Elastic APM tracer.
type Tracer struct {
	// tracer is nil when New was given nil: apm.DefaultTracer is then read on
	// every call, so replacing apm.DefaultTracer later is honoured.
	tracer *apm.Tracer
}

// span is the APM span of one operation and, when the operation had to start
// its own transaction (none was found in the incoming context), that
// transaction as well. End ends both, so implicitly-created transactions are
// reported and returned to the tracer's pool instead of leaking.
type span struct {
	span *apm.Span

	// tx is non-nil only when Start created the transaction itself.
	tx *apm.Transaction
}

// End ends the span, and the transaction if this operation created it.
func (s *span) End() {
	if s.span != nil {
		s.span.End()
	}

	if s.tx != nil {
		s.tx.End()
	}
}

//////
// Methods.
//////

func (t *Tracer) apmTracer() *apm.Tracer {
	if t.tracer != nil {
		return t.tracer
	}

	return apm.DefaultTracer
}

// Start reuses the APM transaction in ctx, or starts one named after the
// adapter when there is none, and starts a span within it named
// "<adapter>.<operation>" with type "<storageType>.<adapter>.<operation>".
func (t *Tracer) Start(
	ctx context.Context,
	storageType, adapter, operation string,
) (context.Context, tracing.Span) {
	var createdTX *apm.Transaction

	tx := apm.TransactionFromContext(ctx)
	if tx == nil {
		tx = t.apmTracer().StartTransaction(adapter, storageType)

		createdTX = tx
	}

	ctx = apm.ContextWithTransaction(ctx, tx)

	apmSpan, ctx := apm.StartSpan(
		ctx,
		fmt.Sprintf("%s.%s", adapter, operation),
		fmt.Sprintf("%s.%s.%s", storageType, adapter, operation),
	)

	return ctx, &span{span: apmSpan, tx: createdTX}
}

// RecordError marks the span in ctx as failed and sends the innermost error
// of err's chain to APM. apm.CaptureError does not unwrap errors itself.
func (t *Tracer) RecordError(ctx context.Context, err error) {
	if err == nil {
		return
	}

	if s := apm.SpanFromContext(ctx); s != nil {
		s.Outcome = outcomeFailure
	}

	for unwrapped := errors.Unwrap(err); unwrapped != nil; unwrapped = errors.Unwrap(err) {
		err = unwrapped
	}

	if apmErr := apm.CaptureError(ctx, err); apmErr != nil {
		apmErr.Send()
	}
}

// LogFields returns the `trace.id`, `transaction.id` and `span.id` of the APM
// transaction and span in ctx, so Elastic can correlate log lines with traces.
// It returns nil when ctx carries no transaction.
func (t *Tracer) LogFields(ctx context.Context) map[string]any {
	tx := apm.TransactionFromContext(ctx)
	if tx == nil {
		return nil
	}

	traceContext := tx.TraceContext()

	f := map[string]any{
		"trace.id":       traceContext.Trace.String(),
		"transaction.id": traceContext.Span.String(),
	}

	if s := apm.SpanFromContext(ctx); s != nil {
		f["span.id"] = s.TraceContext().Span.String()
	}

	return f
}

//////
// Factory.
//////

// New returns a tracing.Tracer backed by t. A nil t means apm.DefaultTracer,
// read at each call.
func New(t *apm.Tracer) *Tracer {
	return &Tracer{tracer: t}
}

// Enable installs New(nil) — Elastic APM on apm.DefaultTracer — as dal's
// tracer, and returns the tracer it replaces.
func Enable() tracing.Tracer {
	return tracing.Set(New(nil))
}
