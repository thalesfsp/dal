package customapm

import (
	"context"
	"errors"
	"expvar"

	"github.com/thalesfsp/dal/v3/internal/logging"
	"github.com/thalesfsp/dal/v3/tracing"
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/fields"
	"github.com/thalesfsp/sypl/v2/level"
)

//////
// Exported functionalities.
//////

// TraceError is a helper function to trace an error. It increments metric,
// reports the error to the installed tracing.Tracer, and logs it with the
// tracer's correlation fields. It returns err unchanged.
func TraceError(
	ctx context.Context,
	err error,
	l sypl.ISypl,
	metric *expvar.Int,
) error {
	if err == nil {
		return nil
	}

	//////
	// Metric.
	//////

	if metric != nil {
		metric.Add(1)
	}

	//////
	// Tracing.
	//////

	tracing.Get().RecordError(ctx, err)

	//////
	// Logging
	//////

	// Correlates the trace and log, and logs the innermost error's message,
	// as it always has.
	if l != nil {
		innermost := err
		for unwrapped := errors.Unwrap(innermost); unwrapped != nil; unwrapped = errors.Unwrap(innermost) {
			innermost = unwrapped
		}

		l.PrintlnWithOptions(
			level.Error,
			innermost.Error(),
			sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
		)
	}

	return err
}
