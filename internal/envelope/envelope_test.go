package envelope

import (
	"context"
	"errors"
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v3/internal/logging"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/sypl/v2"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
)

func testConfig(
	t *testing.T,
	success, failure *expvar.Int,
	options ...storage.Func[*count.Count],
) Config[count.Count] {
	t.Helper()

	logger := logging.Get().New("envelope-test")
	strg := &storage.Mock{
		MockGetType:   func() string { return storage.Type },
		MockGetLogger: func() sypl.ISypl { return logger },
	}

	return Config[count.Count]{
		Storage:        strg,
		Adapter:        "test",
		Status:         "counted",
		ID:             "id",
		Target:         "target",
		HookData:       "input",
		Param:          &count.Count{Search: "override"},
		Options:        options,
		SuccessCounter: success,
		FailureCounter: failure,
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err == nil {
				params.Search = "default"
			}

			return params, err
		},
	}
}

func recordingTracer(t *testing.T) *apmtest.RecordingTracer {
	t.Helper()

	tracer := apmtest.NewRecordingTracer()
	previous := apm.DefaultTracer
	apm.DefaultTracer = tracer.Tracer

	t.Cleanup(func() {
		apm.DefaultTracer = previous
		tracer.Close()
	})

	return tracer
}

func TestRunSuccessAndHookOrdering(t *testing.T) {
	tracer := recordingTracer(t)
	success, failure := new(expvar.Int), new(expvar.Int)
	events := []string{}

	pre := storage.WithPreHook(func(_ context.Context, _ storage.IStorage, id, target string, data any, params *count.Count) error {
		events = append(events, "pre")
		assert.Equal(t, "id", id)
		assert.Equal(t, "target", target)
		assert.Equal(t, "input", data)
		assert.Equal(t, "override", params.Search, "caller params must be merged before hooks")

		return nil
	})
	post := storage.WithPostHook(func(_ context.Context, _ storage.IStorage, _ string, _ string, data any, _ *count.Count) error {
		events = append(events, "post")
		assert.Equal(t, "output", data)

		return nil
	})

	got, err := Run(t.Context(), testConfig(t, success, failure, pre, post), func(_ context.Context, params *count.Count) (Result[int], error) {
		events = append(events, "body")
		assert.Equal(t, "override", params.Search)

		return Result[int]{Value: 42, PostHookData: "output"}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, got)
	assert.Equal(t, []string{"pre", "body", "post"}, events)
	assert.Equal(t, int64(1), success.Value())
	assert.Equal(t, int64(0), failure.Value())

	tracer.Flush(nil)
	assert.Len(t, tracer.Payloads().Spans, 1)
	assert.Len(t, tracer.Payloads().Transactions, 1)
}

func TestRunFailure(t *testing.T) {
	tracer := recordingTracer(t)
	success, failure := new(expvar.Int), new(expvar.Int)
	boom := errors.New("body failed")

	got, err := Run(t.Context(), testConfig(t, success, failure), func(context.Context, *count.Count) (Result[int], error) {
		return Result[int]{Value: 42}, boom
	})

	assert.Zero(t, got)
	assert.Same(t, boom, err)
	assert.Equal(t, int64(0), success.Value())
	assert.Equal(t, int64(1), failure.Value())

	tracer.Flush(nil)
	assert.Len(t, tracer.Payloads().Spans, 1)
	assert.Len(t, tracer.Payloads().Transactions, 1)
}

func TestRunPreHookErrorAbortsOperation(t *testing.T) {
	recordingTracer(t)
	success, failure := new(expvar.Int), new(expvar.Int)
	boom := errors.New("pre-hook failed")
	bodyCalled := false
	postCalled := false

	pre := storage.WithPreHook(func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
		return boom
	})
	post := storage.WithPostHook(func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
		postCalled = true

		return nil
	})

	_, err := Run(t.Context(), testConfig(t, success, failure, pre, post), func(context.Context, *count.Count) (Result[int], error) {
		bodyCalled = true

		return Result[int]{Value: 42}, nil
	})

	assert.Same(t, boom, err)
	assert.False(t, bodyCalled)
	assert.False(t, postCalled)
	assert.Equal(t, int64(0), success.Value())
	assert.Equal(t, int64(1), failure.Value())
}

func TestRunPanicClosesSpan(t *testing.T) {
	tracer := recordingTracer(t)
	success, failure := new(expvar.Int), new(expvar.Int)

	var recovered any

	func() {
		defer func() {
			recovered = recover()
		}()

		_, _ = Run(t.Context(), testConfig(t, success, failure), func(context.Context, *count.Count) (Result[int], error) {
			panic("body panic")
		})
	}()

	assert.Equal(t, "body panic", recovered, "Run must not swallow or translate a panic")
	assert.Equal(t, int64(0), success.Value())
	assert.Equal(t, int64(0), failure.Value(), "a panic is not converted into a returned failure")

	tracer.Flush(nil)
	assert.Len(t, tracer.Payloads().Spans, 1, "the deferred span close must run during panic unwinding")
	assert.Len(t, tracer.Payloads().Transactions, 1)
}
