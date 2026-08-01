package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

// TestMain clears AWS_CA_BUNDLE once for the whole binary. Nothing in this
// package's tests makes a real TLS connection, so the custom bundle is moot.
func TestMain(m *testing.M) {
	os.Unsetenv("AWS_CA_BUNDLE")
	os.Exit(m.Run())
}

//////
// Test helpers.
//////

// dynamoDoc is a small dynamodbav-tagged document used across tests.
type dynamoDoc struct {
	ID      string `dynamodbav:"id"`
	Name    string `dynamodbav:"name"`
	Version string `dynamodbav:"version"`
}

func dynamoBulkTestItems(count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for i := range items {
		items[i] = storage.BulkItem{
			ID: fmt.Sprintf("bulk-item-%d", i),
			Value: dynamoDoc{
				Name:    fmt.Sprintf("value-%d", i),
				Version: "bulk",
			},
		}
	}

	return items
}

func TestDynamoDBOffline_BulkCreate(t *testing.T) {
	t.Run("happy path crosses 25 item cap and reads back", func(t *testing.T) {
		str, fake := sharedTestDynamo(t)
		table := "bulk-happy"
		items := dynamoBulkTestItems(dynamoBulkChunkLimit*2 + 5)
		callsBefore := fake.requestCount("BatchWriteItem")

		result, err := str.BulkCreate(t.Context(), table, items, nil)
		require.NoError(t, err)
		assert.True(t, result.AllSucceeded())
		assert.False(t, result.Stopped)
		require.Len(t, result.Succeeded, len(items))
		assert.Equal(t, 3, fake.requestCount("BatchWriteItem")-callsBefore)

		for index, item := range items {
			assert.Equal(t, index, result.Succeeded[index])

			var got dynamoDoc
			require.NoError(t, str.Retrieve(t.Context(), item.ID, table, &got, nil))
			assert.Equal(t, item.ID, got.ID)
			want, ok := item.Value.(dynamoDoc)
			require.True(t, ok)
			assert.Equal(t, want.Name, got.Name)
		}
	})

	t.Run("empty input succeeds even with canceled context", func(t *testing.T) {
		str, _ := sharedTestDynamo(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		result, err := str.BulkCreate(ctx, "bulk-empty", nil, nil)
		require.NoError(t, err)
		assert.Equal(t, storage.BulkResult{}, result)
	})

	t.Run("single item", func(t *testing.T) {
		str, _ := sharedTestDynamo(t)
		items := dynamoBulkTestItems(1)

		result, err := str.BulkCreate(t.Context(), "bulk-single", items, nil)
		require.NoError(t, err)
		assert.Equal(t, []int{0}, result.Succeeded)

		var got dynamoDoc
		require.NoError(t, str.Retrieve(t.Context(), items[0].ID, "bulk-single", &got, nil))
		assert.Equal(t, items[0].ID, got.ID)
	})

	t.Run("partial preparation failure preserves original index", func(t *testing.T) {
		str, _ := sharedTestDynamo(t)
		table := "bulk-partial"
		items := dynamoBulkTestItems(dynamoBulkChunkLimit + 10)
		items[27].ID = ""

		result, err := str.BulkCreate(t.Context(), table, items, nil)
		require.Error(t, err)
		require.Len(t, result.Failures, 1)
		assert.Equal(t, 27, result.Failures[0].Index)
		assert.False(t, result.Stopped)
		assert.Len(t, result.Succeeded, len(items)-1)

		for index, item := range items {
			if index == 27 {
				continue
			}

			var got dynamoDoc
			require.NoError(t, str.Retrieve(t.Context(), item.ID, table, &got, nil), "successful item %d was not written", index)
			assert.Equal(t, item.ID, got.ID)
		}
	})

	t.Run("unprocessed item retries twice then succeeds", func(t *testing.T) {
		str, fake := sharedTestDynamo(t)
		items := dynamoBulkTestItems(1)
		fake.mu.Lock()
		fake.unprocessedRemaining[items[0].ID] = 2
		fake.mu.Unlock()
		callsBefore := fake.requestCount("BatchWriteItem")

		result, err := str.BulkCreate(t.Context(), "bulk-unprocessed", items, nil)
		require.NoError(t, err)
		assert.Equal(t, []int{0}, result.Succeeded)
		assert.Empty(t, result.Failures)
		assert.Equal(t, 3, fake.requestCount("BatchWriteItem")-callsBefore)

		var got dynamoDoc
		require.NoError(t, str.Retrieve(t.Context(), items[0].ID, "bulk-unprocessed", &got, nil))
		assert.Equal(t, items[0].ID, got.ID)
	})

	t.Run("context cancellation keeps completed 25 item batch", func(t *testing.T) {
		str, _ := sharedTestDynamo(t)
		table := "bulk-cancel"
		items := dynamoBulkTestItems(dynamoBulkChunkLimit + 5)
		ctx, cancel := context.WithCancel(t.Context())
		postHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
			cancel()

			return nil
		}

		result, err := str.BulkCreate(ctx, table, items, nil, storage.WithPostHook(postHook))
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, result.Stopped)
		require.Len(t, result.Succeeded, dynamoBulkChunkLimit)
		require.Len(t, result.Failures, 5)
		for index := range dynamoBulkChunkLimit {
			assert.Equal(t, index, result.Succeeded[index])
			var got dynamoDoc
			require.NoError(t, str.Retrieve(t.Context(), items[index].ID, table, &got, nil))
		}
		for offset, failure := range result.Failures {
			assert.Equal(t, dynamoBulkChunkLimit+offset, failure.Index)
			assert.ErrorIs(t, failure.Err, context.Canceled)
		}
	})
}

type capturedScanInput struct {
	Select                    types.Select               `json:"Select"`
	ProjectionExpression      *string                    `json:"ProjectionExpression"`
	FilterExpression          *string                    `json:"FilterExpression"`
	Limit                     *int32                     `json:"Limit"`
	ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]json.RawMessage `json:"ExpressionAttributeValues"`
}

// errHook is returned by the failing branch of hookRecorder.
var errHook = errors.New("hook failure")

// closeTrackingTransport records CloseIdleConnections so a Close test can
// assert the transport was genuinely released, not merely that Close returned
// nil.
type closeTrackingTransport struct {
	next http.RoundTripper

	mu         sync.Mutex
	closeCalls int
}

func (t *closeTrackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.next.RoundTrip(req)
}

func (t *closeTrackingTransport) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closeCalls++
}

func (t *closeTrackingTransport) CloseCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.closeCalls
}

var errCredentials = errors.New("credential retrieval failed")

// failingCredentialsProvider drives the aws-sdk-go-v2 credential-failure path.
type failingCredentialsProvider struct {
	called *atomic.Bool
}

func (p failingCredentialsProvider) Retrieve(context.Context) (aws.Credentials, error) {
	p.called.Store(true)

	return aws.Credentials{}, errCredentials
}

// hookRecorder returns a storage.HookFunc that records it was called (via
// *called) and returns err. Parameterizing err keeps every generated
// closure's result genuinely variable across the success/error call sites
// that share it, which is also what keeps unparam from flagging it.
func hookRecorder[T any](called *bool, err error) storage.HookFunc[T] {
	return func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ T) error {
		*called = true

		return err
	}
}

// dynamoPlaceholderEndpoint is a syntactically valid endpoint that is never
// actually dialed: newTestDynamo routes every request in-process (see
// dynamoLocalTransport), so nothing needs to be listening on it.
const dynamoPlaceholderEndpoint = "http://dynamodb.invalid"

// newTestDynamo wires a *DynamoDB storage to fake in-process (no real
// socket): a request built by the AWS SDK is handed directly to fake.handle
// and never touches the network.
func newTestDynamo(t *testing.T, fake *dynamoFake) *DynamoDB {
	t.Helper()

	str, err := New(t.Context(), "us-east-1", dynamoTestConfig(fake.client()))
	require.NoError(t, err)
	require.NotNil(t, str)
	require.NotNil(t, str.Client)

	return str
}

func TestDynamoDBOffline_Close(t *testing.T) {
	newStorage := func(t *testing.T) (*DynamoDB, *closeTrackingTransport) {
		t.Helper()

		fake := newDynamoFake()
		transport := &closeTrackingTransport{next: fake.client().Transport}
		client := &http.Client{Transport: transport}

		str, err := New(t.Context(), "us-east-1", dynamoTestConfig(client))
		require.NoError(t, err)

		return str, transport
	}

	t.Run("closes idle connections and is idempotent", func(t *testing.T) {
		str, transport := newStorage(t)

		require.NoError(t, str.Close())
		require.NoError(t, str.Close())
		assert.Equal(t, 1, transport.CloseCalls(), "Close must release the AWS HTTP client's idle connections")
	})

	t.Run("concurrent calls", func(t *testing.T) {
		str, transport := newStorage(t)

		const callers = 8

		errs := make(chan error, callers)
		for range callers {
			go func() {
				errs <- str.Close()
			}()
		}

		for range callers {
			require.NoError(t, <-errs)
		}

		assert.Equal(t, 1, transport.CloseCalls(), "idle connections must be closed exactly once")
	})
}

// sharedDynamo, sharedFake back sharedTestDynamo: every CRUD/hook/edge-case
// test below uses its own table name, and the fake scopes items by table
// name, so sharing one client+fake (built once) across them all is safe -
// there's no whole-bucket-style pollution risk like S3's Count/List had.
// Sharing also minimizes the number of New() calls, which matters under
// -race (see s3's localTransport doc comment for the goroutine-lifecycle
// class of flakiness that many independent New() calls can trigger).
var (
	sharedDynamoOnce sync.Once
	sharedDynamoInst *DynamoDB
	sharedDynamoFake *dynamoFake
)

// sharedTestDynamo returns the package's shared fake+client, built on first
// use.
func sharedTestDynamo(t *testing.T) (*DynamoDB, *dynamoFake) {
	t.Helper()

	sharedDynamoOnce.Do(func() {
		sharedDynamoFake = newDynamoFake()
		sharedDynamoInst = newTestDynamo(t, sharedDynamoFake)
	})

	require.NotNil(t, sharedDynamoInst, "shared *DynamoDB failed to initialize in an earlier test")

	return sharedDynamoInst, sharedDynamoFake
}

//////
// New.
//////

func TestNew_HappyPath(t *testing.T) {
	fake := newDynamoFake()
	str := newTestDynamo(t, fake)

	assert.Equal(t, "us-east-1", str.Region)
	assert.Equal(t, "id", str.PrimaryKey)
	assert.Equal(t, str.Client, str.GetClient())
	assert.Equal(t, "", str.Target())
}

func TestNew_FunctionalOptions(t *testing.T) {
	t.Run("option satisfies required field", func(t *testing.T) {
		fake := newDynamoFake()

		str, err := New(
			t.Context(),
			"",
			dynamoTestConfig(fake.client()),
			func(d *DynamoDB) { d.Region = "us-west-2" },
		)

		require.NoError(t, err)
		require.NotNil(t, str)
		assert.Equal(t, "us-west-2", str.Region)
	})

	t.Run("invalid option is rejected by validation", func(t *testing.T) {
		fake := newDynamoFake()

		str, err := New(
			t.Context(),
			"us-east-1",
			dynamoTestConfig(fake.client()),
			func(d *DynamoDB) { d.PrimaryKey = "" },
		)

		require.Error(t, err)
		assert.Nil(t, str)
		assert.ErrorContains(t, err, "PrimaryKey")
	})

	t.Run("zero options preserves defaults", func(t *testing.T) {
		fake := newDynamoFake()

		str, err := New(t.Context(), "us-east-1", dynamoTestConfig(fake.client()))

		require.NoError(t, err)
		require.NotNil(t, str)
		assert.Equal(t, "us-east-1", str.Region)
		assert.Equal(t, "id", str.PrimaryKey)
	})

	t.Run("multiple options apply in order", func(t *testing.T) {
		fake := newDynamoFake()

		str, err := New(
			t.Context(),
			"us-east-1",
			dynamoTestConfig(fake.client()),
			func(d *DynamoDB) { d.PrimaryKey = "first" },
			func(d *DynamoDB) { d.PrimaryKey = "last" },
		)

		require.NoError(t, err)
		require.NotNil(t, str)
		assert.Equal(t, "last", str.PrimaryKey)
	})
}

func TestNew_PingFailure(t *testing.T) {
	fake := newDynamoFake()
	fake.setOverride("ListTables", http.StatusInternalServerError, "InternalServerError")

	// Bound the retrier's exponential backoff (10s/20s/40s) with a short
	// deadline: RunCtx aborts as soon as ctx.Done() fires while sleeping
	// between retries, so this returns in well under a second instead of
	// waiting out the full backoff schedule.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	str, err := New(ctx, "us-east-1", dynamoTestConfig(fake.client()))
	require.Error(t, err)
	assert.Nil(t, str)
}

func TestNew_CredentialFailure(t *testing.T) {
	fake := newDynamoFake()
	cfg := dynamoTestConfig(fake.client())

	var called atomic.Bool
	cfg.Credentials = failingCredentialsProvider{called: &called}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	str, err := New(ctx, "us-east-1", cfg)
	require.Error(t, err)
	assert.Nil(t, str)
	assert.True(t, called.Load(), "the configured credential provider was not called")
}

func TestNew_RetriesThrottling(t *testing.T) {
	fake := newDynamoFake()
	fake.listTablesFailures = 1

	str := newTestDynamo(t, fake)
	require.NotNil(t, str)
	assert.GreaterOrEqual(t, fake.requestCount("ListTables"), 2)
}

func TestNewWithTable_And_NewWithDynamicTable(t *testing.T) {
	fake := newDynamoFake()

	str, err := NewWithTable(t.Context(), "us-east-1", "static-table", dynamoTestConfig(fake.client()))
	require.NoError(t, err)
	assert.Equal(t, "static-table", str.Target())

	var called bool

	str2, err := NewWithDynamicTable(t.Context(), "us-east-1", func() string {
		called = true

		return "dynamic-table"
	}, dynamoTestConfig(fake.client()))
	require.NoError(t, err)
	assert.Equal(t, "dynamic-table", str2.Target())
	assert.True(t, called)
}

//////
// Singleton.
//////

func TestGetSet_Singleton(t *testing.T) {
	orig := singleton

	t.Cleanup(func() {
		singletonMutex.Lock()
		singleton = orig
		singletonMutex.Unlock()
	})

	singletonMutex.Lock()
	singleton = nil
	singletonMutex.Unlock()

	require.Panics(t, func() { Get() })

	mock := &storage.Mock{}

	Set(mock)

	assert.Equal(t, storage.IStorage(mock), Get())
}

//////
// GetClient.
//////

func TestDynamoDB_GetClient(t *testing.T) {
	str, _ := sharedTestDynamo(t)

	assert.Same(t, str.Client, str.GetClient())
}

func TestDynamoDB_ContextCancellation(t *testing.T) {
	str, _ := sharedTestDynamo(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := str.Count(ctx, "table-canceled", &count.Count{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

//////
// Functional-option helpers (not wired into New/NewWithTable/
// NewWithDynamicTable - see the audit note in the final report - but valid
// to call manually against a *DynamoDB).
//////

func TestWithOptions(t *testing.T) {
	d := &DynamoDB{}

	WithPrimaryKey("pk")(d)
	assert.Equal(t, "pk", d.PrimaryKey)

	WithTarget("mytable")(d)
	assert.Equal(t, "mytable", d.Target())

	var called bool

	WithDynamicTarget(func() string {
		called = true

		return "dyntable"
	})(d)
	assert.Equal(t, "dyntable", d.Target())
	assert.True(t, called)
}

//////
// ToDynamoDBSort.
//////

func TestToDynamoDBSort(t *testing.T) {
	asc, err := ToDynamoDBSort(customsort.Sort("id:asc"))
	require.NoError(t, err)
	assert.True(t, asc)

	desc, err := ToDynamoDBSort(customsort.Sort("id:desc"))
	require.NoError(t, err)
	assert.False(t, desc)

	_, err = ToDynamoDBSort(customsort.Sort("not-a-valid-sort"))
	require.Error(t, err)
}

//////
// CRUD happy path (mirrors the E2E test, offline).
//////

func TestDynamoDB_CRUD_HappyPath(t *testing.T) {
	str, _ := sharedTestDynamo(t)

	const table = "table-crud"

	doc := &dynamoDoc{ID: "1", Name: "alpha", Version: "1.0.0"}

	beforeCreated := str.GetCounterCreated().Value()

	id, err := str.Create(t.Context(), "1", table, doc, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, "1", id)
	assert.EqualValues(t, beforeCreated+1, str.GetCounterCreated().Value())

	var got dynamoDoc

	beforeRetrieved := str.GetCounterRetrieved().Value()

	require.NoError(t, str.Retrieve(t.Context(), "1", table, &got, &retrieve.Retrieve{}))
	assert.Equal(t, *doc, got)
	assert.EqualValues(t, beforeRetrieved+1, str.GetCounterRetrieved().Value())

	updated := &dynamoDoc{ID: "1", Name: "beta", Version: "1.0.1"}

	beforeUpdated := str.GetCounterUpdated().Value()

	require.NoError(t, str.Update(t.Context(), "1", table, updated, &update.Update{}))
	assert.EqualValues(t, beforeUpdated+1, str.GetCounterUpdated().Value())

	var gotUpdated dynamoDoc

	require.NoError(t, str.Retrieve(t.Context(), "1", table, &gotUpdated, &retrieve.Retrieve{}))
	assert.Equal(t, *updated, gotUpdated)

	beforeCounted := str.GetCounterCounted().Value()

	cnt, err := str.Count(t.Context(), table, &count.Count{Search: `{}`})
	require.NoError(t, err)
	assert.EqualValues(t, 1, cnt)
	assert.EqualValues(t, beforeCounted+1, str.GetCounterCounted().Value())

	var listed []dynamoDoc

	beforeListed := str.GetCounterListed().Value()

	require.NoError(t, str.List(t.Context(), table, &listed, &list.List{}))
	require.Len(t, listed, 1)
	assert.Equal(t, *updated, listed[0])
	assert.EqualValues(t, beforeListed+1, str.GetCounterListed().Value())

	beforeDeleted := str.GetCounterDeleted().Value()

	require.NoError(t, str.Delete(t.Context(), "1", table, &delete.Delete{}))
	assert.EqualValues(t, beforeDeleted+1, str.GetCounterDeleted().Value())

	beforeRetrievedFailed := str.GetCounterRetrievedFailed().Value()

	err = str.Retrieve(t.Context(), "1", table, &got, &retrieve.Retrieve{})
	require.Error(t, err)
	assert.EqualValues(t, beforeRetrievedFailed+1, str.GetCounterRetrievedFailed().Value())
	assert.True(t, IsNotFoundError(err))
}

//////
// Create.
//////

func TestDynamoDB_Create(t *testing.T) {
	str, fake := sharedTestDynamo(t)

	const table = "table-create"

	t.Run("empty-target-name-ok-since-static-empty", func(t *testing.T) {
		// DynamoDB.Create doesn't require id to be non-empty up front like
		// Delete/Retrieve/Update do - it validates by trying to use it as
		// the primary key. Confirm the empty-id branch still fails cleanly.
		_, err := str.Create(t.Context(), "", table, &dynamoDoc{}, &create.Create{})
		require.Error(t, err)
	})

	t.Run("struct-pk-autoset", func(t *testing.T) {
		doc := &dynamoDoc{Name: "gamma", Version: "1.0.0"} // ID left blank on purpose.

		id, err := str.Create(t.Context(), "auto-1", table, doc, &create.Create{})
		require.NoError(t, err)
		assert.Equal(t, "auto-1", id)

		item, ok := fake.getItem(table, "auto-1")
		require.True(t, ok)
		assert.Equal(t, "auto-1", attrString(item["id"]))
		assert.Equal(t, "gamma", attrString(item["name"]))
	})

	t.Run("map-value-no-pk-autoset", func(t *testing.T) {
		v := map[string]any{"id": "map-1", "name": "delta"}

		id, err := str.Create(t.Context(), "map-1", table, v, &create.Create{})
		require.NoError(t, err)
		assert.Equal(t, "map-1", id)

		item, ok := fake.getItem(table, "map-1")
		require.True(t, ok)
		assert.Equal(t, "delta", attrString(item["name"]))
	})

	t.Run("marshal-error", func(t *testing.T) {
		before := str.GetCounterCreatedFailed().Value()

		// A map with an empty-string key is what attributevalue rejects
		// ("map key cannot be empty") - most Go values marshal into
		// *something*, so this is the reliable way to hit MarshalMap's
		// error branch.
		_, err := str.Create(t.Context(), "bad-1", table, map[string]any{"": "x"}, &create.Create{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.setOverride("PutItem", http.StatusBadRequest, "ValidationException")

		t.Cleanup(func() { fake.clearOverride("PutItem") })

		before := str.GetCounterCreatedFailed().Value()

		_, err := str.Create(t.Context(), "err-1", table, &dynamoDoc{}, &create.Create{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterCreatedFailed().Value())
	})
}

func TestDynamoDB_Create_OptionsAndHooks(t *testing.T) {
	str, _ := sharedTestDynamo(t)

	const table = "table-create-hooks"

	t.Run("option-error", func(t *testing.T) {
		_, err := str.Create(t.Context(), "id", table, &dynamoDoc{}, &create.Create{}, storage.WithPreHook[*create.Create](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", table, &dynamoDoc{}, &create.Create{}, storage.WithPreHook(hookRecorder[*create.Create](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("pre-hook-success", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", table, &dynamoDoc{}, &create.Create{}, storage.WithPreHook(hookRecorder[*create.Create](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", table, &dynamoDoc{}, &create.Create{}, storage.WithPostHook(hookRecorder[*create.Create](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-success", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", table, &dynamoDoc{}, &create.Create{}, storage.WithPostHook(hookRecorder[*create.Create](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})
}

//////
// Update.
//////

func TestDynamoDB_Update(t *testing.T) {
	str, fake := sharedTestDynamo(t)

	const table = "table-update"

	fake.putItem(table, map[string]types.AttributeValue{
		"id":   stringAttribute("u1"),
		"name": stringAttribute("old"),
	})

	t.Run("empty-id", func(t *testing.T) {
		err := str.Update(t.Context(), "", table, &dynamoDoc{}, &update.Update{})
		require.Error(t, err)
	})

	t.Run("target-missing", func(t *testing.T) {
		err := str.Update(t.Context(), "u1", "", &dynamoDoc{Name: "x"}, &update.Update{})
		require.Error(t, err)
	})

	t.Run("happy-path", func(t *testing.T) {
		err := str.Update(t.Context(), "u1", table, &dynamoDoc{ID: "u1", Name: "new", Version: "2.0.0"}, &update.Update{})
		require.NoError(t, err)

		item, ok := fake.getItem(table, "u1")
		require.True(t, ok)
		assert.Equal(t, "new", attrString(item["name"]))
		assert.Equal(t, "2.0.0", attrString(item["version"]))
	})

	t.Run("marshal-error", func(t *testing.T) {
		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(t.Context(), "u1", table, map[string]any{"": "x"}, &update.Update{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("no-fields-to-update", func(t *testing.T) {
		type idOnly struct {
			ID string `dynamodbav:"id"`
		}

		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(t.Context(), "u1", table, idOnly{ID: "u1"}, &update.Update{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.setOverride("UpdateItem", http.StatusBadRequest, "ValidationException")

		t.Cleanup(func() { fake.clearOverride("UpdateItem") })

		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(t.Context(), "u1", table, &dynamoDoc{ID: "u1", Name: "x"}, &update.Update{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterUpdatedFailed().Value())
	})
}

func TestDynamoDB_Update_OptionsAndHooks(t *testing.T) {
	str, _ := sharedTestDynamo(t)

	const table = "table-update-hooks"

	t.Run("option-error", func(t *testing.T) {
		err := str.Update(t.Context(), "id", table, &dynamoDoc{Name: "x"}, &update.Update{}, storage.WithPreHook[*update.Update](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", table, &dynamoDoc{Name: "x"}, &update.Update{}, storage.WithPreHook(hookRecorder[*update.Update](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("pre-hook-success", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", table, &dynamoDoc{Name: "x"}, &update.Update{}, storage.WithPreHook(hookRecorder[*update.Update](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", table, &dynamoDoc{Name: "x"}, &update.Update{}, storage.WithPostHook(hookRecorder[*update.Update](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-success", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", table, &dynamoDoc{Name: "x"}, &update.Update{}, storage.WithPostHook(hookRecorder[*update.Update](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})
}

//////
// Delete.
//////

func TestDynamoDB_Delete(t *testing.T) {
	str, fake := sharedTestDynamo(t)

	const table = "table-delete"

	t.Run("empty-id", func(t *testing.T) {
		err := str.Delete(t.Context(), "", table, &delete.Delete{})
		require.Error(t, err)
	})

	t.Run("target-missing", func(t *testing.T) {
		err := str.Delete(t.Context(), "id", "", &delete.Delete{})
		require.Error(t, err)
	})

	t.Run("option-error", func(t *testing.T) {
		err := str.Delete(t.Context(), "id", table, &delete.Delete{}, storage.WithPreHook[*delete.Delete](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		err := str.Delete(t.Context(), "id", table, &delete.Delete{}, storage.WithPreHook(hookRecorder[*delete.Delete](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		err := str.Delete(t.Context(), "id", table, &delete.Delete{}, storage.WithPostHook(hookRecorder[*delete.Delete](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.setOverride("DeleteItem", http.StatusBadRequest, "ValidationException")

		t.Cleanup(func() { fake.clearOverride("DeleteItem") })

		before := str.GetCounterDeletedFailed().Value()

		err := str.Delete(t.Context(), "id", table, &delete.Delete{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterDeletedFailed().Value())
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		fake.putItem(table, map[string]types.AttributeValue{"id": stringAttribute("d1")})

		var preCalled, postCalled bool

		err := str.Delete(
			t.Context(), "d1", table, &delete.Delete{},
			storage.WithPreHook(hookRecorder[*delete.Delete](&preCalled, nil)),
			storage.WithPostHook(hookRecorder[*delete.Delete](&postCalled, nil)),
		)
		require.NoError(t, err)
		assert.True(t, preCalled)
		assert.True(t, postCalled)

		_, ok := fake.getItem(table, "d1")
		assert.False(t, ok)
	})
}

//////
// Retrieve.
//////

func TestDynamoDB_Retrieve(t *testing.T) {
	str, fake := sharedTestDynamo(t)

	const table = "table-retrieve"

	fake.putItem(table, map[string]types.AttributeValue{
		"id":   stringAttribute("r1"),
		"name": stringAttribute("alpha"),
	})

	t.Run("empty-id", func(t *testing.T) {
		var v dynamoDoc

		err := str.Retrieve(t.Context(), "", table, &v, &retrieve.Retrieve{})
		require.Error(t, err)
	})

	t.Run("target-missing", func(t *testing.T) {
		var v dynamoDoc

		err := str.Retrieve(t.Context(), "id", "", &v, &retrieve.Retrieve{})
		require.Error(t, err)
	})

	t.Run("option-error", func(t *testing.T) {
		var v dynamoDoc

		err := str.Retrieve(t.Context(), "r1", table, &v, &retrieve.Retrieve{}, storage.WithPreHook[*retrieve.Retrieve](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var v dynamoDoc

		var called bool

		err := str.Retrieve(t.Context(), "r1", table, &v, &retrieve.Retrieve{}, storage.WithPreHook(hookRecorder[*retrieve.Retrieve](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("not-found", func(t *testing.T) {
		var v dynamoDoc

		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(t.Context(), "missing", table, &v, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterRetrievedFailed().Value())
		assert.True(t, IsNotFoundError(err))
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.setOverride("GetItem", http.StatusBadRequest, "ValidationException")

		t.Cleanup(func() { fake.clearOverride("GetItem") })

		var v dynamoDoc

		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(t.Context(), "r1", table, &v, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		type badTarget struct {
			ID   string `dynamodbav:"id"`
			Name int    `dynamodbav:"name"` // fake item's "name" is an S ("alpha"), which won't decode into int.
		}

		var v badTarget

		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(t.Context(), "r1", table, &v, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var v dynamoDoc

		var called bool

		err := str.Retrieve(t.Context(), "r1", table, &v, &retrieve.Retrieve{}, storage.WithPostHook(hookRecorder[*retrieve.Retrieve](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		var v dynamoDoc

		var preCalled, postCalled bool

		err := str.Retrieve(
			t.Context(), "r1", table, &v, &retrieve.Retrieve{},
			storage.WithPreHook(hookRecorder[*retrieve.Retrieve](&preCalled, nil)),
			storage.WithPostHook(hookRecorder[*retrieve.Retrieve](&postCalled, nil)),
		)
		require.NoError(t, err)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
		assert.Equal(t, "alpha", v.Name)
	})
}

//////
// Count.
//////

func TestDynamoDB_Count(t *testing.T) {
	str, fake := sharedTestDynamo(t)

	const table = "table-count"

	fake.putItem(table, map[string]types.AttributeValue{"id": stringAttribute("c1"), "status": stringAttribute("active")})
	fake.putItem(table, map[string]types.AttributeValue{"id": stringAttribute("c2"), "status": stringAttribute("active")})

	t.Run("happy-path-no-filter", func(t *testing.T) {
		cnt, err := str.Count(t.Context(), table, &count.Count{})
		require.NoError(t, err)
		assert.EqualValues(t, 2, cnt)

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent capturedScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		assert.Equal(t, types.SelectCount, sent.Select)
	})

	t.Run("happy-path-with-filter", func(t *testing.T) {
		cnt, err := str.Count(t.Context(), table, &count.Count{Search: `{"status":"active"}`})
		require.NoError(t, err)
		assert.EqualValues(t, 2, cnt)
	})

	t.Run("unsafe-attribute-name-sanitized", func(t *testing.T) {
		_, err := str.Count(t.Context(), table, &count.Count{Search: `{"created-at":"2020-01-01"}`})
		require.NoError(t, err)

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent capturedScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.Len(t, sent.ExpressionAttributeNames, 1)

		for placeholder, real := range sent.ExpressionAttributeNames {
			assert.Regexp(t, `^#[A-Za-z0-9_]+$`, placeholder)
			assert.Equal(t, "created-at", real)
		}
	})

	t.Run("search-unmarshal-error", func(t *testing.T) {
		_, err := str.Count(t.Context(), table, &count.Count{Search: "not-json"})
		require.Error(t, err)
	})

	t.Run("option-error", func(t *testing.T) {
		_, err := str.Count(t.Context(), table, &count.Count{}, storage.WithPreHook[*count.Count](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Count(t.Context(), table, &count.Count{}, storage.WithPreHook(hookRecorder[*count.Count](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Count(t.Context(), table, &count.Count{}, storage.WithPostHook(hookRecorder[*count.Count](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		var preCalled, postCalled bool

		cnt, err := str.Count(
			t.Context(), table, &count.Count{},
			storage.WithPreHook(hookRecorder[*count.Count](&preCalled, nil)),
			storage.WithPostHook(hookRecorder[*count.Count](&postCalled, nil)),
		)
		require.NoError(t, err)
		assert.EqualValues(t, 2, cnt)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
	})

	t.Run("target-missing", func(t *testing.T) {
		_, err := str.Count(t.Context(), "", &count.Count{})
		require.Error(t, err)
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.setOverride("Scan", http.StatusBadRequest, "ValidationException")

		t.Cleanup(func() { fake.clearOverride("Scan") })

		before := str.GetCounterCountedFailed().Value()

		_, err := str.Count(t.Context(), table, &count.Count{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterCountedFailed().Value())
	})

	t.Run("pagination", func(t *testing.T) {
		restore := fake.setScanQueue([]dynamodb.ScanOutput{
			{
				Count:            2,
				LastEvaluatedKey: map[string]types.AttributeValue{"id": stringAttribute("c2")},
			},
			{
				Count: 1,
			},
		})
		t.Cleanup(restore)

		cnt, err := str.Count(t.Context(), table, &count.Count{})
		require.NoError(t, err)
		assert.EqualValues(t, 3, cnt)
	})

	t.Run("empty", func(t *testing.T) {
		cnt, err := str.Count(t.Context(), "table-count-empty-"+table, &count.Count{})
		require.NoError(t, err)
		assert.EqualValues(t, 0, cnt)
	})
}

//////
// List.
//////

func TestDynamoDB_List(t *testing.T) {
	str, fake := sharedTestDynamo(t)

	const table = "table-list"

	fake.putItem(table, map[string]types.AttributeValue{
		"id": stringAttribute("l1"), "name": stringAttribute("alpha"), "created-at": stringAttribute("2020-01-01"),
	})

	t.Run("happy-path-returns-items-not-just-count", func(t *testing.T) {
		// Regression guard for the fixed prod bug: List must NOT set
		// Select=COUNT (that makes DynamoDB omit Items entirely).
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{}))
		require.Len(t, got, 1)
		assert.Equal(t, "l1", got[0].ID)
		assert.Equal(t, "alpha", got[0].Name)

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent capturedScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		assert.Empty(t, sent.Select)
	})

	t.Run("fields-projection-sanitizes-unsafe-names", func(t *testing.T) {
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{Fields: []string{"created-at"}}))

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent capturedScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.NotNil(t, sent.ProjectionExpression)
		require.Len(t, sent.ExpressionAttributeNames, 1)

		for placeholder, real := range sent.ExpressionAttributeNames {
			assert.Regexp(t, `^#[A-Za-z0-9_]+$`, placeholder)
			assert.Equal(t, "created-at", real)
		}
	})

	t.Run("limit-sets-scan-limit", func(t *testing.T) {
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{Limit: 5}))

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent capturedScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.NotNil(t, sent.Limit)
		assert.EqualValues(t, 5, *sent.Limit)
	})

	t.Run("limit-over-int32-is-rejected", func(t *testing.T) {
		var got []dynamoDoc

		err := str.List(t.Context(), table, &got, &list.List{Limit: math.MaxInt32 + 1})
		require.Error(t, err)
	})

	t.Run("filter-sanitizes-unsafe-names", func(t *testing.T) {
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{Any: map[string]any{"created-at": "2020-01-01"}}))

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent capturedScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.NotNil(t, sent.FilterExpression)
		require.Len(t, sent.ExpressionAttributeValues, 1)

		var foundSanitized bool

		for placeholder, real := range sent.ExpressionAttributeNames {
			if real == "created-at" {
				assert.Regexp(t, `^#f[A-Za-z0-9_]+$`, placeholder)

				foundSanitized = true
			}
		}

		assert.True(t, foundSanitized)
	})

	t.Run("offset-and-limit-across-pages", func(t *testing.T) {
		items := []map[string]types.AttributeValue{
			{"id": stringAttribute("a"), "name": stringAttribute("a")},
			{"id": stringAttribute("b"), "name": stringAttribute("b")},
			{"id": stringAttribute("c"), "name": stringAttribute("c")},
		}
		items2 := []map[string]types.AttributeValue{
			{"id": stringAttribute("d"), "name": stringAttribute("d")},
			{"id": stringAttribute("e"), "name": stringAttribute("e")},
		}

		restore := fake.setScanQueue([]dynamodb.ScanOutput{
			{Items: items, LastEvaluatedKey: map[string]types.AttributeValue{"id": stringAttribute("c")}},
			{Items: items2},
		})
		t.Cleanup(restore)

		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{Offset: 2, Limit: 2}))
		require.Len(t, got, 2)
		assert.Equal(t, "c", got[0].Name)
		assert.Equal(t, "d", got[1].Name)
	})

	t.Run("option-error", func(t *testing.T) {
		var got []dynamoDoc

		err := str.List(t.Context(), table, &got, &list.List{}, storage.WithPreHook[*list.List](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var got []dynamoDoc

		var called bool

		err := str.List(t.Context(), table, &got, &list.List{}, storage.WithPreHook(hookRecorder[*list.List](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var got []dynamoDoc

		var called bool

		err := str.List(t.Context(), table, &got, &list.List{}, storage.WithPostHook(hookRecorder[*list.List](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		var got []dynamoDoc

		var preCalled, postCalled bool

		err := str.List(
			t.Context(), table, &got, &list.List{},
			storage.WithPreHook(hookRecorder[*list.List](&preCalled, nil)),
			storage.WithPostHook(hookRecorder[*list.List](&postCalled, nil)),
		)
		require.NoError(t, err)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
	})

	t.Run("target-missing", func(t *testing.T) {
		var got []dynamoDoc

		err := str.List(t.Context(), "", &got, &list.List{})
		require.Error(t, err)
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.setOverride("Scan", http.StatusBadRequest, "ValidationException")

		t.Cleanup(func() { fake.clearOverride("Scan") })

		var got []dynamoDoc

		before := str.GetCounterListedFailed().Value()

		err := str.List(t.Context(), table, &got, &list.List{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterListedFailed().Value())
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		type badTarget struct {
			ID   string `dynamodbav:"id"`
			Name int    `dynamodbav:"name"` // stored "name" is an S, not numeric.
		}

		var got []badTarget

		before := str.GetCounterListedFailed().Value()

		err := str.List(t.Context(), table, &got, &list.List{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterListedFailed().Value())
	})

	t.Run("empty", func(t *testing.T) {
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), "table-list-empty-"+table, &got, &list.List{}))
		assert.Empty(t, got)
	})
}
