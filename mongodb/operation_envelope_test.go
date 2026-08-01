package mongodb

import (
	"context"
	"errors"
	"expvar"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
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
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoEnvelopeCase struct {
	name             string
	success, failure *expvar.Int
	want, failVal    any
	invoke           func(error, *[]string) (any, error)
}

func mongoEnvelopeHooks[T any](preErr error, events *[]string) []storage.Func[T] {
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

func recordMongoEnvelope(t *testing.T, fn func() (any, error)) (any, transporttest.Payloads, error) {
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

func runMongoEnvelopeCase(t *testing.T, tc mongoEnvelopeCase) {
	t.Helper()
	t.Run("mongodb/"+tc.name+"/success", func(t *testing.T) {
		beforeSuccess, beforeFailure := tc.success.Value(), tc.failure.Value()
		events := []string{}
		got, payloads, err := recordMongoEnvelope(t, func() (any, error) { return tc.invoke(nil, &events) })
		require.NoError(t, err)
		assert.Equal(t, tc.want, got, "operation return value changed")
		assert.Equal(t, []string{"pre", "post"}, events, "hook order or cardinality changed")
		assert.Equal(t, int64(1), tc.success.Value()-beforeSuccess)
		assert.Equal(t, int64(0), tc.failure.Value()-beforeFailure)
		assert.Len(t, payloads.Spans, 1)
		assert.Len(t, payloads.Transactions, 1)
	})
	t.Run("mongodb/"+tc.name+"/failure", func(t *testing.T) {
		beforeSuccess, beforeFailure := tc.success.Value(), tc.failure.Value()
		events := []string{}
		boom := customerror.NewFailedToError(tc.name, customerror.WithError(errors.New("envelope hook failure")))
		got, payloads, err := recordMongoEnvelope(t, func() (any, error) { return tc.invoke(boom, &events) })
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

func TestMongoDBOperationEnvelope(t *testing.T) {
	ctx := t.Context()
	str, dep := newMockedStorage(t)
	doc := &testDoc{ID: "seed", Name: "n", Version: "v"}

	cases := []mongoEnvelopeCase{
		{
			name: "count", success: str.GetCounterCounted(), failure: str.GetCounterCountedFailed(), want: int64(1), failVal: int64(0),
			invoke: func(preErr error, events *[]string) (any, error) {
				if preErr == nil {
					dep.addResponses(countResponse(1))
				}
				return str.Count(ctx, testColl, nil, mongoEnvelopeHooks[*count.Count](preErr, events)...)
			},
		},
		{
			name: "create", success: str.GetCounterCreated(), failure: str.GetCounterCreatedFailed(), want: "seed", failVal: "",
			invoke: func(preErr error, events *[]string) (any, error) {
				if preErr == nil {
					dep.addResponses(mockCreateSuccessResponse())
				}
				return str.Create(ctx, "seed", testColl, doc, nil, mongoEnvelopeHooks[*create.Create](preErr, events)...)
			},
		},
		{
			name: "delete", success: str.GetCounterDeleted(), failure: str.GetCounterDeletedFailed(),
			invoke: func(preErr error, events *[]string) (any, error) {
				if preErr == nil {
					dep.addResponses(mockCreateSuccessResponse(bson.E{Key: "n", Value: 1}))
				}
				err := str.Delete(ctx, "seed", testColl, nil, mongoEnvelopeHooks[*delete.Delete](preErr, events)...)
				return nil, err
			},
		},
		{
			name: "retrieve", success: str.GetCounterRetrieved(), failure: str.GetCounterRetrievedFailed(), want: "seed",
			invoke: func(preErr error, events *[]string) (any, error) {
				if preErr == nil {
					dep.addResponses(mockCreateCursorResponse(0, mockFirstBatch, bson.D{
						{Key: "_id", Value: "seed"}, {Key: "name", Value: "n"}, {Key: "version", Value: "v"},
					}))
				}
				var got testDoc
				err := str.Retrieve(ctx, "seed", testColl, &got, nil, mongoEnvelopeHooks[*retrieve.Retrieve](preErr, events)...)
				if err != nil {
					return nil, err
				}
				return got.ID, nil
			},
		},
		{
			name: "list", success: str.GetCounterListed(), failure: str.GetCounterListedFailed(), want: 1,
			invoke: func(preErr error, events *[]string) (any, error) {
				if preErr == nil {
					dep.addResponses(mockCreateCursorResponse(0, mockFirstBatch, listDoc("seed", "n")))
				}
				var got []testDoc
				err := str.List(ctx, testColl, &got, nil, mongoEnvelopeHooks[*list.List](preErr, events)...)
				if err != nil {
					return nil, err
				}
				return len(got), nil
			},
		},
		{
			name: "update", success: str.GetCounterUpdated(), failure: str.GetCounterUpdatedFailed(),
			invoke: func(preErr error, events *[]string) (any, error) {
				if preErr == nil {
					dep.addResponses(mockCreateSuccessResponse(
						bson.E{Key: "n", Value: 1}, bson.E{Key: "nModified", Value: 1},
					))
				}
				err := str.Update(ctx, "seed", testColl, doc, nil, mongoEnvelopeHooks[*update.Update](preErr, events)...)
				return nil, err
			},
		},
	}
	for _, tc := range cases {
		runMongoEnvelopeCase(t, tc)
	}

	// Current behavior: all four operations validate an empty ID before Trace,
	// so these failures increment their failure counters but open no span or
	// transaction. Pin that asymmetry as-is for the later envelope refactor.
	untraced := []struct {
		name             string
		success, failure *expvar.Int
		invoke           func() error
	}{
		{name: "create", success: str.GetCounterCreated(), failure: str.GetCounterCreatedFailed(), invoke: func() error {
			_, err := str.Create(ctx, "", "", nil, nil)

			return err
		}},
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
		t.Run("mongodb/"+uc.name+"/empty-id-is-untraced", func(t *testing.T) {
			beforeSuccess, beforeFailure := uc.success.Value(), uc.failure.Value()
			expected := customerror.NewRequiredError("id")

			got, payloads, err := recordMongoEnvelope(t, func() (any, error) {
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
