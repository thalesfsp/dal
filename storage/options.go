package storage

import (
	"context"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/validation"
)

//////
// Vars, consts, and types.
//////

var (
	// ErrRequiredPostHook is the error returned when the post-hook function is
	// missing.
	ErrRequiredPostHook = customerror.NewRequiredError("post-hook function", customerror.WithCode("ERR_REQUIRED_POST_HOOK"))

	// ErrRequiredPreHook is the error returned when the pre-hook function is
	// missing.
	ErrRequiredPreHook = customerror.NewRequiredError("pre-hook function", customerror.WithCode("ERR_REQUIRED_PRE_HOOK"))
)

// HookFunc specifies the function that will be called before and after the
// operation.
type HookFunc[T any] func(ctx context.Context, strg IStorage, id, target string, data any, param T) error

// Func allows to set options.
type Func[T any] func(o *Options[T]) error

// Options for operations.
type Options[T any] struct {
	// PreHookFunc is the function which runs before the operation.
	PreHookFunc HookFunc[T] `json:"-"`

	// PostHookFunc is the function which runs after the operation.
	PostHookFunc HookFunc[T] `json:"-"`
}

//////
// Exported built-in options.
//////

// WithPreHook set the pre-hook function.
func WithPreHook[T any](fn HookFunc[T]) Func[T] {
	return func(o *Options[T]) error {
		if fn == nil {
			return ErrRequiredPreHook
		}

		o.PreHookFunc = fn

		return nil
	}
}

// WithPostHook set the post-hook function.
func WithPostHook[T any](fn HookFunc[T]) Func[T] {
	return func(o *Options[T]) error {
		if fn == nil {
			return ErrRequiredPostHook
		}

		o.PostHookFunc = fn

		return nil
	}
}

// NewOptions creates Options.
func NewOptions[T any]() (*Options[T], error) {
	o := &Options[T]{}

	if err := validation.Validate(o); err != nil {
		return nil, err
	}

	return o, nil
}
