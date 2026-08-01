package mongodb

// Offline unit tests for the MongoDB adapter. They run the real driver against
// the in-memory mock deployment defined in mockdeployment_test.go, so every
// code path - command building, response decoding, error mapping, metrics - is
// exercised without Docker or network access.

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	testDB   = "testdb"
	testColl = "testcoll"
	testNS   = testDB + "." + testColl
)

// errBoom is a sentinel used by failing options and hooks.
var errBoom = errors.New("boom")

// testDoc is the document shape used by the offline tests.
type testDoc struct {
	ID      string `bson:"_id"     json:"id"`
	Name    string `bson:"name"    json:"name"`
	Version string `bson:"version" json:"version"`
}

func mongoBulkTestItems(count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for i := range items {
		items[i] = storage.BulkItem{
			ID: fmt.Sprintf("bulk-item-%d", i),
			Value: testDoc{
				ID:      fmt.Sprintf("bulk-item-%d", i),
				Name:    fmt.Sprintf("value-%d", i),
				Version: "bulk",
			},
		}
	}

	return items
}

func mongoBulkReadResponses(items []storage.BulkItem, failed map[int]bool) []bson.D {
	responses := make([]bson.D, 0, len(items))
	for index, item := range items {
		if failed[index] {
			responses = append(responses, mockCreateCursorResponse(0, mockFirstBatch))

			continue
		}
		doc, ok := item.Value.(testDoc)
		if !ok {
			panic(fmt.Sprintf("mongo bulk test item %d has type %T, want testDoc", index, item.Value))
		}
		responses = append(responses, mockCreateCursorResponse(0, mockFirstBatch, bson.D{
			{Key: "_id", Value: doc.ID},
			{Key: "name", Value: doc.Name},
			{Key: "version", Value: doc.Version},
		}))
	}

	return responses
}

func TestMongoDBOffline_BulkCreate(t *testing.T) {
	t.Run("happy path crosses adapter chunk limit and reads back", func(t *testing.T) {
		st, dep := newMockedStorage(t)
		items := mongoBulkTestItems(mongoBulkChunkLimit*2 + 5)
		dep.addResponses(
			mockCreateSuccessResponse(),
			mockCreateSuccessResponse(),
			mockCreateSuccessResponse(),
		)

		result, err := st.BulkCreate(t.Context(), testColl, items, nil)
		require.NoError(t, err)
		assert.True(t, result.AllSucceeded())
		assert.False(t, result.Stopped)
		require.Len(t, result.Succeeded, len(items))

		dep.addResponses(mongoBulkReadResponses(items, nil)...)
		for index, item := range items {
			assert.Equal(t, index, result.Succeeded[index])

			var got testDoc
			require.NoError(t, st.Retrieve(t.Context(), item.ID, testColl, &got, nil))
			assert.Equal(t, item.Value, got)
		}
	})

	t.Run("empty input succeeds even with canceled context", func(t *testing.T) {
		st, _ := newMockedStorage(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		result, err := st.BulkCreate(ctx, testColl, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, storage.BulkResult{}, result)
	})

	t.Run("single item", func(t *testing.T) {
		st, dep := newMockedStorage(t)
		items := mongoBulkTestItems(1)
		dep.addResponses(mockCreateSuccessResponse())

		result, err := st.BulkCreate(t.Context(), testColl, items, nil)
		require.NoError(t, err)
		assert.Equal(t, []int{0}, result.Succeeded)

		dep.addResponses(mongoBulkReadResponses(items, nil)...)
		var got testDoc
		require.NoError(t, st.Retrieve(t.Context(), items[0].ID, testColl, &got, nil))
		assert.Equal(t, items[0].Value, got)
	})

	t.Run("unordered write errors preserve original indices", func(t *testing.T) {
		st, dep := newMockedStorage(t)
		items := mongoBulkTestItems(mongoBulkChunkLimit + 35)
		dep.addResponses(
			mockCreateWriteErrorsResponse(mockWriteError{Index: 7, Code: 11000, Message: "duplicate key"}),
			mockCreateWriteErrorsResponse(mockWriteError{Index: 27, Code: 11000, Message: "duplicate key"}),
		)

		result, err := st.BulkCreate(t.Context(), testColl, items, nil)
		require.Error(t, err)
		require.Len(t, result.Failures, 2)
		assert.Equal(t, 7, result.Failures[0].Index)
		assert.Equal(t, 127, result.Failures[1].Index)
		assert.False(t, result.Stopped)
		assert.Len(t, result.Succeeded, len(items)-2)

		failed := map[int]bool{7: true, 127: true}
		dep.addResponses(mongoBulkReadResponses(items, failed)...)
		for index, item := range items {
			var got testDoc
			retrieveErr := st.Retrieve(t.Context(), item.ID, testColl, &got, nil)
			if failed[index] {
				require.Error(t, retrieveErr, "failed item %d was written", index)

				continue
			}
			require.NoError(t, retrieveErr, "successful item %d was not written", index)
			assert.Equal(t, item.Value, got)
		}
	})

	t.Run("context cancellation keeps completed InsertMany chunk", func(t *testing.T) {
		st, dep := newMockedStorage(t)
		items := mongoBulkTestItems(mongoBulkChunkLimit + 5)
		dep.addResponses(mockCreateSuccessResponse())
		ctx, cancel := context.WithCancel(t.Context())
		postHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			cancel()

			return nil
		}

		result, err := st.BulkCreate(ctx, testColl, items, nil, storage.WithPostHook(postHook))
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, result.Stopped)
		require.Len(t, result.Succeeded, mongoBulkChunkLimit)
		require.Len(t, result.Failures, 5)

		dep.addResponses(mongoBulkReadResponses(items[:mongoBulkChunkLimit], nil)...)
		for index := range mongoBulkChunkLimit {
			assert.Equal(t, index, result.Succeeded[index])
			var got testDoc
			require.NoError(t, st.Retrieve(t.Context(), items[index].ID, testColl, &got, nil))
		}
		for offset, failure := range result.Failures {
			assert.Equal(t, mongoBulkChunkLimit+offset, failure.Index)
			assert.ErrorIs(t, failure.Err, context.Canceled)
		}
	})
}

// newMockedStorage builds a fully-initialized adapter through New() (covering
// the factory success path) backed by an offline mock deployment. The ping
// issued by New consumes the first queued response.
func newMockedStorage(t *testing.T) (*MongoDB, *mockDeployment) {
	t.Helper()

	dep := newMockDeployment(mockCreateSuccessResponse()) // consumed by ping.

	cfg := options.Client()
	// Deployment is the only exported seam for an offline driver: setting it
	// skips topology discovery and routes commands at our mock.
	//nolint:staticcheck // SA1019: intentional test-only use of the mock seam.
	cfg.Deployment = dep

	st, err := New(t.Context(), testDB, cfg)
	require.NoError(t, err)
	require.NotNil(t, st)

	t.Cleanup(func() {
		_ = st.Client.Disconnect(context.Background())
	})

	return st, dep
}

// requireCounterIncremented runs fn and asserts the given counter went up by
// exactly one.
func requireCounterIncremented(t *testing.T, c *expvar.Int, fn func()) {
	t.Helper()

	before := c.Value()

	fn()

	require.Equal(t, int64(1), c.Value()-before, "unexpected counter delta")
}

// requireHTTPStatus asserts err is a customerror carrying the given HTTP
// status code.
func requireHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()

	require.Error(t, err)

	var cErr *customerror.CustomError

	require.ErrorAs(t, err, &cErr)
	require.Equal(t, want, cErr.StatusCode)
}

//////
// Factory (New) paths.
//////

func TestNewOffline(t *testing.T) {
	t.Run("success sets singleton, target falls back to database", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		assert.Equal(t, testDB, st.Database)
		// Target is defaulted to the database name by New.
		assert.Equal(t, testDB, st.Target)
		assert.Equal(t, Name, st.GetName())
		assert.Equal(t, storage.Type, st.GetType())

		client, ok := st.GetClient().(*mongo.Client)
		require.True(t, ok)
		assert.Same(t, st.Client, client)
		assert.NoError(t, st.Close())

		// New must have stored the instance in the singleton.
		assert.Same(t, storage.IStorage(st), Get())
	})

	t.Run("invalid client options fail at connect", func(t *testing.T) {
		st, err := New(t.Context(), testDB, options.Client().ApplyURI("not-a-valid-scheme://"))

		require.Error(t, err)
		assert.Nil(t, st)
		assert.Contains(t, err.Error(), "connect")
	})

	t.Run("unreachable server fails ping and leaks nothing", func(t *testing.T) {
		before := runtime.NumGoroutine()

		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()

		cfg := options.Client().
			ApplyURI("mongodb://127.0.0.1:1").
			SetServerSelectionTimeout(50 * time.Millisecond).
			SetConnectTimeout(50 * time.Millisecond)

		st, err := New(ctx, testDB, cfg)

		require.Error(t, err)
		assert.Nil(t, st)
		assert.Contains(t, err.Error(), "ping")

		// The failed constructor must have disconnected the client it
		// created; give its background goroutines a bounded moment to wind
		// down and verify none leaked.
		require.Eventually(t, func() bool {
			return runtime.NumGoroutine() <= before+2
		}, 5*time.Second, 50*time.Millisecond, "goroutines leaked by failed New")
	})

	t.Run("validation failure disconnects the created client", func(t *testing.T) {
		dep := newMockDeployment(mockCreateSuccessResponse()) // ping succeeds.

		cfg := options.Client()
		// Same mock seam as newMockedStorage.
		//nolint:staticcheck // SA1019: intentional test-only use of the mock seam.
		cfg.Deployment = dep

		// Empty database name fails struct validation AFTER connect+ping.
		st, err := New(t.Context(), "", cfg)

		require.Error(t, err)
		assert.Nil(t, st)

		// The success-flag defer must have disconnected the client.
		require.Eventually(t, dep.Disconnected, 2*time.Second, 10*time.Millisecond)
	})
}

func TestMongoDBOffline_Close(t *testing.T) {
	t.Run("disconnects client and is idempotent", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		require.NoError(t, st.Close())
		require.NoError(t, st.Close())
		assert.True(t, dep.Disconnected(), "Close must disconnect the MongoDB deployment")

		err := st.Client.Ping(t.Context(), nil)
		require.Error(t, err, "operation after Close must fail because client is disconnected")
	})

	t.Run("concurrent calls", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		const callers = 8

		errs := make(chan error, callers)
		for range callers {
			go func() {
				errs <- st.Close()
			}()
		}

		for range callers {
			require.NoError(t, <-errs)
		}

		assert.True(t, dep.Disconnected(), "Close must disconnect the MongoDB deployment")
	})

	t.Run("propagates disconnect error once", func(t *testing.T) {
		st, dep := newMockedStorage(t)
		disconnectErr := errors.New("mongodb disconnect failed")
		dep.disconnectErr = disconnectErr

		require.ErrorIs(t, st.Close(), disconnectErr)
		require.NoError(t, st.Close())
	})
}

//////
// Singleton accessors.
//////

func TestGetSetSingleton(t *testing.T) {
	st, _ := newMockedStorage(t)

	// Restore whatever this test leaves behind.
	t.Cleanup(func() { Set(st) })

	Set(nil)
	assert.Panics(t, func() { Get() })

	Set(st)
	assert.Same(t, storage.IStorage(st), Get())
}

//////
// ToMongoString.
//////

func TestToMongoString(t *testing.T) {
	t.Run("asc and desc", func(t *testing.T) {
		got, err := ToMongoString(customsort.Sort("id:asc,name:desc"))

		require.NoError(t, err)
		assert.ElementsMatch(t, bson.D{{Key: "id", Value: 1}, {Key: "name", Value: -1}}, got)
	})

	t.Run("invalid sort", func(t *testing.T) {
		got, err := ToMongoString(customsort.Sort("no-separator"))

		require.Error(t, err)
		assert.Nil(t, got)
	})
}

//////
// Count.
//////

// countResponse is the aggregate cursor response CountDocuments expects.
func countResponse(n int32) bson.D {
	return mockCreateCursorResponse(0, mockFirstBatch, bson.D{{Key: "n", Value: n}})
}

func TestCountOffline(t *testing.T) {
	t.Run("success with nil params and hooks", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(countResponse(5))

		var preCalled, postCalled bool

		requireCounterIncremented(t, st.GetCounterCounted(), func() {
			got, err := st.Count(t.Context(), testColl, nil,
				storage.WithPreHook[*count.Count](func(_ context.Context, _ storage.IStorage, _, target string, _ any, _ *count.Count) error {
					preCalled = true

					assert.Equal(t, testColl, target)

					return nil
				}),
				storage.WithPostHook[*count.Count](func(_ context.Context, _ storage.IStorage, _, _ string, data any, _ *count.Count) error {
					postCalled = true

					assert.EqualValues(t, 5, data)

					return nil
				}),
			)

			require.NoError(t, err)
			assert.EqualValues(t, 5, got)
		})

		assert.True(t, preCalled)
		assert.True(t, postCalled)
	})

	t.Run("success with search filter", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(countResponse(2))

		got, err := st.Count(t.Context(), testColl, &count.Count{Search: `{"version":"1"}`})

		require.NoError(t, err)
		assert.EqualValues(t, 2, got)
	})

	t.Run("invalid search filter", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		requireCounterIncremented(t, st.GetCounterCountedFailed(), func() {
			got, err := st.Count(t.Context(), testColl, &count.Count{Search: `{not-json`})

			require.Error(t, err)
			assert.Zero(t, got)
		})
	})

	t.Run("command error", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCommandErrorResponse(mockCommandError{
			Code:    8000,
			Name:    "AtlasError",
			Message: "count failed",
		}))

		requireCounterIncremented(t, st.GetCounterCountedFailed(), func() {
			got, err := st.Count(t.Context(), testColl, nil)

			require.Error(t, err)
			assert.Zero(t, got)
			assert.Contains(t, err.Error(), "count failed")
		})
	})

	t.Run("context cancellation", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		requireCounterIncremented(t, st.GetCounterCountedFailed(), func() {
			got, err := st.Count(ctx, testColl, nil)

			require.ErrorIs(t, err, context.Canceled)
			assert.Zero(t, got)
		})
	})

	t.Run("missing target", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		st.Target = ""

		got, err := st.Count(t.Context(), "", nil)

		require.Error(t, err)
		assert.Zero(t, got)
	})

	t.Run("target falls back to static target", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		st.Target = testColl

		dep.addResponses(countResponse(1))

		got, err := st.Count(t.Context(), "", nil)

		require.NoError(t, err)
		assert.EqualValues(t, 1, got)
	})

	t.Run("failing option", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		got, err := st.Count(t.Context(), testColl, nil,
			func(*storage.Options[*count.Count]) error { return errBoom },
		)

		require.ErrorIs(t, err, errBoom)
		assert.Zero(t, got)
	})

	t.Run("failing pre-hook", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		got, err := st.Count(t.Context(), testColl, nil,
			storage.WithPreHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				return errBoom
			}),
		)

		require.ErrorIs(t, err, errBoom)
		assert.Zero(t, got)
	})

	t.Run("failing post-hook", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(countResponse(1))

		got, err := st.Count(t.Context(), testColl, nil,
			storage.WithPostHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				return errBoom
			}),
		)

		require.ErrorIs(t, err, errBoom)
		assert.Zero(t, got)
	})
}

//////
// Create.
//////

func TestCreateOffline(t *testing.T) {
	doc := &testDoc{ID: "id-1", Name: "name-1", Version: "1"}

	t.Run("success", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateSuccessResponse())

		requireCounterIncremented(t, st.GetCounterCreated(), func() {
			id, err := st.Create(t.Context(), doc.ID, testColl, doc, &create.Create{})

			require.NoError(t, err)
			assert.Equal(t, doc.ID, id)
		})
	})

	t.Run("success with nil params and hooks", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateSuccessResponse())

		var hookCalls int

		hook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			hookCalls++

			return nil
		}

		id, err := st.Create(t.Context(), doc.ID, testColl, doc, nil,
			storage.WithPreHook[*create.Create](hook),
			storage.WithPostHook[*create.Create](hook),
		)

		require.NoError(t, err)
		assert.Equal(t, doc.ID, id)
		assert.Equal(t, 2, hookCalls)
	})

	t.Run("write error", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateWriteErrorsResponse(mockWriteError{
			Index:   0,
			Code:    11000,
			Message: "duplicate key",
		}))

		requireCounterIncremented(t, st.GetCounterCreatedFailed(), func() {
			id, err := st.Create(t.Context(), doc.ID, testColl, doc, &create.Create{})

			require.Error(t, err)
			assert.Empty(t, id)
			assert.Contains(t, err.Error(), "duplicate key")
			assert.True(t, mongo.IsDuplicateKeyError(err))
		})
	})

	t.Run("missing id", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		requireCounterIncremented(t, st.GetCounterCreatedFailed(), func() {
			id, err := st.Create(t.Context(), "", testColl, doc, &create.Create{})

			require.Error(t, err)
			assert.Empty(t, id)
		})
	})

	t.Run("missing target", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		st.Target = ""

		id, err := st.Create(t.Context(), doc.ID, "", doc, &create.Create{})

		require.Error(t, err)
		assert.Empty(t, id)
	})

	t.Run("failing option", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		id, err := st.Create(t.Context(), doc.ID, testColl, doc, nil,
			func(*storage.Options[*create.Create]) error { return errBoom },
		)

		require.ErrorIs(t, err, errBoom)
		assert.Empty(t, id)
	})
}

//////
// Retrieve.
//////

func TestRetrieveOffline(t *testing.T) {
	stored := bson.D{
		{Key: "_id", Value: "id-1"},
		{Key: "name", Value: "name-1"},
		{Key: "version", Value: "1"},
	}

	t.Run("found", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCursorResponse(0, mockFirstBatch, stored))

		var got testDoc

		var hookCalls int

		// The nil result is dictated by storage.HookFunc's signature.
		//nolint:unparam
		hook := func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
			hookCalls++

			return nil
		}

		requireCounterIncremented(t, st.GetCounterRetrieved(), func() {
			require.NoError(t, st.Retrieve(t.Context(), "id-1", testColl, &got, nil,
				storage.WithPreHook[*retrieve.Retrieve](hook),
				storage.WithPostHook[*retrieve.Retrieve](hook),
			))
		})

		assert.Equal(t, testDoc{ID: "id-1", Name: "name-1", Version: "1"}, got)
		assert.Equal(t, 2, hookCalls)
	})

	t.Run("not found is 404", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		// Empty first batch -> mongo.ErrNoDocuments.
		dep.addResponses(mockCreateCursorResponse(0, mockFirstBatch))

		var got testDoc

		requireCounterIncremented(t, st.GetCounterRetrievedFailed(), func() {
			err := st.Retrieve(t.Context(), "id-1", testColl, &got, &retrieve.Retrieve{})

			requireHTTPStatus(t, err, 404)
		})
	})

	t.Run("command error", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCommandErrorResponse(mockCommandError{
			Code:    8000,
			Name:    "AtlasError",
			Message: "find failed",
		}))

		var got testDoc

		requireCounterIncremented(t, st.GetCounterRetrievedFailed(), func() {
			err := st.Retrieve(t.Context(), "id-1", testColl, &got, &retrieve.Retrieve{})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "find failed")
		})
	})

	t.Run("undecodable destination", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCursorResponse(0, mockFirstBatch, stored))

		// A document cannot be unmarshaled into an *int.
		got := new(int)

		requireCounterIncremented(t, st.GetCounterRetrievedFailed(), func() {
			err := st.Retrieve(t.Context(), "id-1", testColl, got, &retrieve.Retrieve{})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "unmarshal")
		})
	})

	t.Run("missing id", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		var got testDoc

		require.Error(t, st.Retrieve(t.Context(), "", testColl, &got, &retrieve.Retrieve{}))
	})

	t.Run("missing target", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		st.Target = ""

		var got testDoc

		require.Error(t, st.Retrieve(t.Context(), "id-1", "", &got, &retrieve.Retrieve{}))
	})

	t.Run("failing option", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		var got testDoc

		err := st.Retrieve(t.Context(), "id-1", testColl, &got, nil,
			func(*storage.Options[*retrieve.Retrieve]) error { return errBoom },
		)

		require.ErrorIs(t, err, errBoom)
	})
}

//////
// Update.
//////

func TestUpdateOffline(t *testing.T) {
	doc := &testDoc{ID: "id-1", Name: "name-2", Version: "2"}

	t.Run("matched", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateSuccessResponse(
			bson.E{Key: "n", Value: 1},
			bson.E{Key: "nModified", Value: 1},
		))

		var hookCalls int

		// The nil result is dictated by storage.HookFunc's signature.
		//nolint:unparam
		hook := func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
			hookCalls++

			return nil
		}

		requireCounterIncremented(t, st.GetCounterUpdated(), func() {
			require.NoError(t, st.Update(t.Context(), doc.ID, testColl, doc, nil,
				storage.WithPreHook[*update.Update](hook),
				storage.WithPostHook[*update.Update](hook),
			))
		})

		assert.Equal(t, 2, hookCalls)
	})

	t.Run("no match is 404", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateSuccessResponse(
			bson.E{Key: "n", Value: 0},
			bson.E{Key: "nModified", Value: 0},
		))

		requireCounterIncremented(t, st.GetCounterUpdatedFailed(), func() {
			err := st.Update(t.Context(), doc.ID, testColl, doc, &update.Update{})

			requireHTTPStatus(t, err, 404)
		})
	})

	t.Run("command error", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCommandErrorResponse(mockCommandError{
			Code:    8000,
			Name:    "AtlasError",
			Message: "update failed",
		}))

		requireCounterIncremented(t, st.GetCounterUpdatedFailed(), func() {
			err := st.Update(t.Context(), doc.ID, testColl, doc, &update.Update{})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "update failed")
		})
	})

	t.Run("unmarshalable value", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		// A channel cannot be JSON-marshaled.
		err := st.Update(t.Context(), doc.ID, testColl, make(chan int), &update.Update{})

		require.Error(t, err)
	})

	t.Run("non-object value", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		// A scalar marshals fine but cannot unmarshal into a map.
		err := st.Update(t.Context(), doc.ID, testColl, "scalar", &update.Update{})

		require.Error(t, err)
	})

	t.Run("missing id", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		require.Error(t, st.Update(t.Context(), "", testColl, doc, &update.Update{}))
	})

	t.Run("missing target", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		st.Target = ""

		require.Error(t, st.Update(t.Context(), doc.ID, "", doc, &update.Update{}))
	})

	t.Run("failing option", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		err := st.Update(t.Context(), doc.ID, testColl, doc, nil,
			func(*storage.Options[*update.Update]) error { return errBoom },
		)

		require.ErrorIs(t, err, errBoom)
	})
}

//////
// Delete.
//////

func TestDeleteOffline(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateSuccessResponse(bson.E{Key: "n", Value: 1}))

		var hookCalls int

		// The nil result is dictated by storage.HookFunc's signature.
		//nolint:unparam
		hook := func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
			hookCalls++

			return nil
		}

		requireCounterIncremented(t, st.GetCounterDeleted(), func() {
			require.NoError(t, st.Delete(t.Context(), "id-1", testColl, nil,
				storage.WithPreHook[*delete.Delete](hook),
				storage.WithPostHook[*delete.Delete](hook),
			))
		})

		assert.Equal(t, 2, hookCalls)
	})

	t.Run("no match still succeeds (as coded)", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		// DeletedCount is not inspected by the adapter: deleting a missing
		// document is NOT an error (unlike Update, which maps it to 404).
		dep.addResponses(mockCreateSuccessResponse(bson.E{Key: "n", Value: 0}))

		requireCounterIncremented(t, st.GetCounterDeleted(), func() {
			require.NoError(t, st.Delete(t.Context(), "id-1", testColl, &delete.Delete{}))
		})
	})

	t.Run("command error", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCommandErrorResponse(mockCommandError{
			Code:    8000,
			Name:    "AtlasError",
			Message: "delete failed",
		}))

		requireCounterIncremented(t, st.GetCounterDeletedFailed(), func() {
			err := st.Delete(t.Context(), "id-1", testColl, &delete.Delete{})

			require.Error(t, err)
			assert.Contains(t, err.Error(), "delete failed")
		})
	})

	t.Run("missing id", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		requireCounterIncremented(t, st.GetCounterDeletedFailed(), func() {
			require.Error(t, st.Delete(t.Context(), "", testColl, &delete.Delete{}))
		})
	})

	t.Run("missing target", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		st.Target = ""

		require.Error(t, st.Delete(t.Context(), "id-1", "", &delete.Delete{}))
	})

	t.Run("failing option", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		err := st.Delete(t.Context(), "id-1", testColl, nil,
			func(*storage.Options[*delete.Delete]) error { return errBoom },
		)

		require.ErrorIs(t, err, errBoom)
	})
}

//////
// List.
//////

func listDoc(id, name string) bson.D {
	return bson.D{
		{Key: "_id", Value: id},
		{Key: "name", Value: name},
		{Key: "version", Value: "1"},
	}
}

func TestDecodeCursorRejectsInvalidDestination(t *testing.T) {
	require.EqualError(
		t,
		decodeCursor(t.Context(), nil, nil),
		"results argument must be a non-nil pointer to a slice",
	)

	var notASlice int

	require.EqualError(
		t,
		decodeCursor(t.Context(), nil, &notASlice),
		"results argument must be a pointer to a slice, but was a pointer to int",
	)
}

func TestListOffline(t *testing.T) {
	t.Run("multiple batches with full params", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		// find -> first batch (cursor stays open), getMore -> final batch
		// (cursor id 0 closes it, so no killCursors is issued).
		dep.addResponses(
			mockCreateCursorResponse(7, mockFirstBatch, listDoc("id-1", "a"), listDoc("id-2", "b")),
			mockCreateCursorResponse(0, mockNextBatch, listDoc("id-3", "c")),
		)

		prm := &list.List{
			Limit:  10,
			Fields: []string{"id", "name", "version"},
			Offset: 0,
			Sort:   customsort.SortSlice{{"id", customsort.Asc}, {"name", customsort.Desc}},
			Any:    bson.M{"version": "1"},
		}

		var got []testDoc

		var hookCalls int

		// The nil result is dictated by storage.HookFunc's signature.
		//nolint:unparam
		hook := func(context.Context, storage.IStorage, string, string, any, *list.List) error {
			hookCalls++

			return nil
		}

		requireCounterIncremented(t, st.GetCounterListed(), func() {
			require.NoError(t, st.List(t.Context(), testColl, &got, prm,
				storage.WithPreHook[*list.List](hook),
				storage.WithPostHook[*list.List](hook),
			))
		})

		assert.Len(t, got, 3)
		assert.Equal(t, testDoc{ID: "id-1", Name: "a", Version: "1"}, got[0])
		assert.Equal(t, 2, hookCalls)
	})

	t.Run("nil params uses defaults", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCursorResponse(0, mockFirstBatch, listDoc("id-1", "a")))

		var got []testDoc

		require.NoError(t, st.List(t.Context(), testColl, &got, nil))
		assert.Len(t, got, 1)
	})

	t.Run("invalid sort", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		prm := &list.List{Sort: customsort.SortSlice{{"id", "not-an-order"}}}

		var got []testDoc

		requireCounterIncremented(t, st.GetCounterListedFailed(), func() {
			require.Error(t, st.List(t.Context(), testColl, &got, prm))
		})
	})

	t.Run("command error", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCommandErrorResponse(mockCommandError{
			Code:    8000,
			Name:    "AtlasError",
			Message: "find failed",
		}))

		var got []testDoc

		requireCounterIncremented(t, st.GetCounterListedFailed(), func() {
			err := st.List(t.Context(), testColl, &got, nil)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "find failed")
		})
	})

	t.Run("cursor decode error", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(mockCreateCursorResponse(0, mockFirstBatch, listDoc("id-1", "a")))

		// Documents cannot be decoded into ints.
		var got []int

		requireCounterIncremented(t, st.GetCounterListedFailed(), func() {
			err := st.List(t.Context(), testColl, &got, nil)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "cursor iteration")
		})
	})

	t.Run("cursor error mid-stream", func(t *testing.T) {
		st, dep := newMockedStorage(t)

		dep.addResponses(
			mockCreateCursorResponse(
				7,
				mockFirstBatch,
				listDoc("id-1", "a"),
				listDoc("id-2", "b"),
			),
			mockCreateCommandErrorResponse(mockCommandError{
				Code:    43,
				Name:    "CursorNotFound",
				Message: "getMore failed",
			}),
		)

		var got []testDoc

		requireCounterIncremented(t, st.GetCounterListedFailed(), func() {
			err := st.List(t.Context(), testColl, &got, nil)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "cursor iteration")
			assert.Contains(t, err.Error(), "getMore failed")
			assert.Nil(t, got, "partial cursor results must not be returned")
		})
	})

	t.Run("missing target", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		st.Target = ""

		var got []testDoc

		require.Error(t, st.List(t.Context(), "", &got, nil))
	})

	t.Run("failing option", func(t *testing.T) {
		st, _ := newMockedStorage(t)

		var got []testDoc

		err := st.List(t.Context(), testColl, &got, nil,
			func(*storage.Options[*list.List]) error { return errBoom },
		)

		require.ErrorIs(t, err, errBoom)
	})
}
