package customapm

import (
	"context"

	"github.com/thalesfsp/dal/v3/tracing"
)

//////
// Exported functionalities.
//////

// Trace starts the span of one storage operation on the installed
// tracing.Tracer (a no-op unless the application installed one, e.g. with
// elasticapm.Enable). The span is never nil.
//
// NOTE: It's up to the developer to call `span.End()`.
func Trace(
	ctx context.Context,
	what, nameOf, operation string,
) (context.Context, tracing.Span) {
	return tracing.Start(ctx, what, nameOf, operation)
}
