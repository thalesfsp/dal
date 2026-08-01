package elasticsearch

import (
	"context"
	"errors"
	"expvar"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
	"go.elastic.co/apm/transport/transporttest"
)

type elasticEnvelopeCase struct {
	name             string
	success, failure *expvar.Int
	want, failVal    any
	invoke           func(error, *[]string) (any, error)
}

func elasticEnvelopeHooks[T any](preErr error, events *[]string) []storage.Func[T] {
	return []storage.Func[T]{
		storage.WithPreHook(func(context.Context, storage.IStorage, string, string, any, T) error {
			*events = append(*events, "pre")
			return preErr
		}),
		storage.WithPostHook(func(context.Context, storage.IStorage, string, string, any, T) error {
			*events = append(*events, "post")
			return nil
		}),
	}
}

func recordElasticEnvelope(t *testing.T, fn func() (any, error)) (any, transporttest.Payloads, error) {
	t.Helper()
	tracer := apmtest.NewRecordingTracer()
	previous := apm.DefaultTracer
	apm.DefaultTracer = tracer.Tracer
	got, err := fn()
	tracer.Flush(nil)
	payloads := tracer.Payloads()
	apm.DefaultTracer = previous
	tracer.Close()
	return got, payloads, err
}

func runElasticEnvelopeCase(t *testing.T, tc elasticEnvelopeCase) {
	t.Helper()
	t.Run("elasticsearch/"+tc.name+"/success", func(t *testing.T) {
		beforeSuccess, beforeFailure := tc.success.Value(), tc.failure.Value()
		events := []string{}
		got, payloads, err := recordElasticEnvelope(t, func() (any, error) { return tc.invoke(nil, &events) })
		require.NoError(t, err)
		assert.Equal(t, tc.want, got, "operation return value changed")
		assert.Equal(t, []string{"pre", "post"}, events, "hook order or cardinality changed")
		assert.Equal(t, int64(1), tc.success.Value()-beforeSuccess)
		assert.Equal(t, int64(0), tc.failure.Value()-beforeFailure)
		assert.Len(t, payloads.Spans, 1)
		assert.Len(t, payloads.Transactions, 1)
	})
	t.Run("elasticsearch/"+tc.name+"/failure", func(t *testing.T) {
		beforeSuccess, beforeFailure := tc.success.Value(), tc.failure.Value()
		events := []string{}
		boom := customerror.NewFailedToError(tc.name, customerror.WithError(errors.New("envelope hook failure")))
		got, payloads, err := recordElasticEnvelope(t, func() (any, error) { return tc.invoke(boom, &events) })
		assert.Equal(t, tc.failVal, got, "failure return value changed")
		require.Error(t, err)
		assert.Same(t, boom, err, "TraceError currently returns the hook error object unchanged")
		assert.Equal(t, reflect.TypeOf(boom), reflect.TypeOf(err))
		assert.Equal(t, boom.Error(), err.Error(), "error message shape changed")
		assert.Equal(t, []string{"pre"}, events, "post-hook currently does not run after pre-hook failure")
		assert.Equal(t, int64(0), tc.success.Value()-beforeSuccess)
		assert.Equal(t, int64(1), tc.failure.Value()-beforeFailure)
		assert.Len(t, payloads.Spans, 1)
		assert.Len(t, payloads.Transactions, 1)
	})
}

func TestElasticSearchOperationEnvelope(t *testing.T) {
	ctx := t.Context()
	str, _, _ := newTestES(t, "envelope-index")
	const target = "envelope-index"
	_, err := str.Create(ctx, "seed", target, &shared.TestDataWithIDS{ID: "seed", Name: "n", Version: "v"}, nil)
	require.NoError(t, err)

	cases := []elasticEnvelopeCase{
		{
			name: "count", success: str.GetCounterCounted(), failure: str.GetCounterCountedFailed(), want: int64(1), failVal: int64(0),
			invoke: func(preErr error, events *[]string) (any, error) {
				return str.Count(ctx, target, nil, elasticEnvelopeHooks[*count.Count](preErr, events)...)
			},
		},
		{
			name: "create", success: str.GetCounterCreated(), failure: str.GetCounterCreatedFailed(), want: "created", failVal: "",
			invoke: func(preErr error, events *[]string) (any, error) {
				doc := &shared.TestDataWithIDS{ID: "created", Name: "n", Version: "v"}
				return str.Create(ctx, "created", target, doc, nil, elasticEnvelopeHooks[*create.Create](preErr, events)...)
			},
		},
		{
			name: "delete", success: str.GetCounterDeleted(), failure: str.GetCounterDeletedFailed(),
			invoke: func(preErr error, events *[]string) (any, error) {
				err := str.Delete(ctx, "created", target, nil, elasticEnvelopeHooks[*delete.Delete](preErr, events)...)
				return nil, err
			},
		},
		{
			name: "retrieve", success: str.GetCounterRetrieved(), failure: str.GetCounterRetrievedFailed(), want: "seed",
			invoke: func(preErr error, events *[]string) (any, error) {
				var got shared.TestDataWithIDS
				err := str.Retrieve(ctx, "seed", target, &got, nil, elasticEnvelopeHooks[*retrieve.Retrieve](preErr, events)...)
				if err != nil {
					return nil, err
				}
				return got.ID, nil
			},
		},
		{
			name: "list", success: str.GetCounterListed(), failure: str.GetCounterListedFailed(), want: 1,
			invoke: func(preErr error, events *[]string) (any, error) {
				var got []shared.TestDataWithIDS
				err := str.List(ctx, target, &got, nil, elasticEnvelopeHooks[*list.List](preErr, events)...)
				if err != nil {
					return nil, err
				}
				return len(got), nil
			},
		},
		{
			name: "update", success: str.GetCounterUpdated(), failure: str.GetCounterUpdatedFailed(),
			invoke: func(preErr error, events *[]string) (any, error) {
				doc := &shared.TestDataWithIDS{ID: "seed", Name: "updated", Version: "v"}
				err := str.Update(ctx, "seed", target, doc, nil, elasticEnvelopeHooks[*update.Update](preErr, events)...)
				return nil, err
			},
		},
	}
	for _, tc := range cases {
		runElasticEnvelopeCase(t, tc)
	}

	// Current behavior: all three operations validate an empty ID before Trace,
	// so these failures increment their failure counters but open no span or
	// transaction. Pin that asymmetry as-is for the later envelope refactor.
	untraced := []struct {
		name             string
		success, failure *expvar.Int
		invoke           func() error
	}{
		{name: "delete", success: str.GetCounterDeleted(), failure: str.GetCounterDeletedFailed(), invoke: func() error {
			return str.Delete(ctx, "", "", nil)
		}},
		{name: "retrieve", success: str.GetCounterRetrieved(), failure: str.GetCounterRetrievedFailed(), invoke: func() error {
			return str.Retrieve(ctx, "", "", nil, nil)
		}},
		{name: "update", success: str.GetCounterUpdated(), failure: str.GetCounterUpdatedFailed(), invoke: func() error {
			return str.Update(ctx, "", "", nil, nil)
		}},
	}

	for _, uc := range untraced {
		t.Run("elasticsearch/"+uc.name+"/empty-id-is-untraced", func(t *testing.T) {
			beforeSuccess, beforeFailure := uc.success.Value(), uc.failure.Value()
			expected := customerror.NewRequiredError("id")

			got, payloads, err := recordElasticEnvelope(t, func() (any, error) {
				return nil, uc.invoke()
			})

			assert.Nil(t, got)
			require.Error(t, err)
			assert.Equal(t, reflect.TypeOf(expected), reflect.TypeOf(err))
			assert.Equal(t, expected.Error(), err.Error(), "error message shape changed")
			assert.Equal(t, int64(0), uc.success.Value()-beforeSuccess)
			assert.Equal(t, int64(1), uc.failure.Value()-beforeFailure)
			assert.Empty(t, payloads.Spans, "empty-ID validation currently happens before span creation")
			assert.Empty(t, payloads.Transactions, "empty-ID validation currently happens before transaction creation")
		})
	}
}
