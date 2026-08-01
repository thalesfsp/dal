// Package envelope provides the common execution envelope for storage operations.
package envelope

import (
	"context"
	"expvar"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/logging"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/fields"
	"github.com/thalesfsp/sypl/v2/level"
)

// Void is the result type for operations that return only an error.
type Void struct{}

// Result contains the operation result and the data passed to the post-hook.
type Result[R any] struct {
	Value        R
	PostHookData any
}

// Config contains the operation-specific inputs to Run.
type Config[P shared.Param] struct {
	Storage storage.IStorage

	Adapter string
	Status  string

	ID       string
	Target   string
	HookData any

	Param      *P
	Options    []storage.Func[*P]
	NewParams  func() (*P, error)
	RequiresID bool

	SuccessCounter *expvar.Int
	FailureCounter *expvar.Int
}

// Run executes a storage operation inside its tracing, options, params, hooks,
// logging, metrics, and error-reporting envelope.
func Run[P shared.Param, R any](
	ctx context.Context,
	config Config[P],
	body func(context.Context, *P) (Result[R], error),
) (R, error) {
	var zero R

	if config.RequiresID && config.ID == "" {
		return zero, customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			config.Storage.GetLogger(),
			config.FailureCounter,
		)
	}

	ctx, span := customapm.Trace(
		ctx,
		config.Storage.GetType(),
		config.Adapter,
		config.Status,
	)
	defer span.End()

	fail := func(err error) (R, error) {
		return zero, customapm.TraceError(
			ctx,
			err,
			config.Storage.GetLogger(),
			config.FailureCounter,
		)
	}

	options, err := storage.NewOptions[*P]()
	if err != nil {
		return fail(err)
	}

	for _, option := range config.Options {
		if err := option(options); err != nil {
			return fail(err)
		}
	}

	params, err := config.NewParams()
	if err != nil {
		return fail(err)
	}

	params = shared.MergeParams(params, config.Param)

	if options.PreHookFunc != nil {
		if err := options.PreHookFunc(
			ctx,
			config.Storage,
			config.ID,
			config.Target,
			config.HookData,
			params,
		); err != nil {
			return fail(err)
		}
	}

	result, err := body(ctx, params)
	if err != nil {
		return fail(err)
	}

	if options.PostHookFunc != nil {
		if err := options.PostHookFunc(
			ctx,
			config.Storage,
			config.ID,
			config.Target,
			result.PostHookData,
			params,
		); err != nil {
			return fail(err)
		}
	}

	config.Storage.GetLogger().PrintlnWithOptions(
		level.Debug,
		config.Status,
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	config.SuccessCounter.Add(1)

	return result.Value, nil
}
