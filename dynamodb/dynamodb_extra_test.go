package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v2/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

// TestMain clears AWS_CA_BUNDLE once for the whole binary. The sandbox's
// outbound-proxy setup exports it; session.New picks it up regardless of
// what's in our *aws.Config and then requires HTTPClient.Transport to be a
// concrete *http.Transport to install it, which dynamoLocalTransport (by
// design: it never dials out) is not. Nothing in this package's tests makes
// a real TLS connection, so the custom bundle is moot.
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

// errHook is returned by the failing branch of hookRecorder.
var errHook = errors.New("hook failure")

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
		v := map[string]interface{}{"id": "map-1", "name": "delta"}

		id, err := str.Create(t.Context(), "map-1", table, v, &create.Create{})
		require.NoError(t, err)
		assert.Equal(t, "map-1", id)

		item, ok := fake.getItem(table, "map-1")
		require.True(t, ok)
		assert.Equal(t, "delta", attrString(item["name"]))
	})

	t.Run("marshal-error", func(t *testing.T) {
		before := str.GetCounterCreatedFailed().Value()

		// A map with an empty-string key is what dynamodbattribute rejects
		// ("map key cannot be empty") - most Go values marshal into
		// *something*, so this is the reliable way to hit MarshalMap's
		// error branch.
		_, err := str.Create(t.Context(), "bad-1", table, map[string]interface{}{"": "x"}, &create.Create{})
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

	fake.putItem(table, map[string]*dynamodb.AttributeValue{
		"id":   {S: aws.String("u1")},
		"name": {S: aws.String("old")},
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

		err := str.Update(t.Context(), "u1", table, map[string]interface{}{"": "x"}, &update.Update{})
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
		fake.putItem(table, map[string]*dynamodb.AttributeValue{"id": {S: aws.String("d1")}})

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

	fake.putItem(table, map[string]*dynamodb.AttributeValue{
		"id":   {S: aws.String("r1")},
		"name": {S: aws.String("alpha")},
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

	fake.putItem(table, map[string]*dynamodb.AttributeValue{"id": {S: aws.String("c1")}, "status": {S: aws.String("active")}})
	fake.putItem(table, map[string]*dynamodb.AttributeValue{"id": {S: aws.String("c2")}, "status": {S: aws.String("active")}})

	t.Run("happy-path-no-filter", func(t *testing.T) {
		cnt, err := str.Count(t.Context(), table, &count.Count{})
		require.NoError(t, err)
		assert.EqualValues(t, 2, cnt)

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent dynamodb.ScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		assert.Equal(t, dynamodb.SelectCount, aws.StringValue(sent.Select))
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

		var sent dynamodb.ScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.Len(t, sent.ExpressionAttributeNames, 1)

		for placeholder, real := range sent.ExpressionAttributeNames {
			assert.Regexp(t, `^#[A-Za-z0-9_]+$`, placeholder)
			assert.Equal(t, "created-at", aws.StringValue(real))
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
		one := int64(1)
		two := int64(2)

		restore := fake.setScanQueue([]dynamodb.ScanOutput{
			{
				Count:            &two,
				LastEvaluatedKey: map[string]*dynamodb.AttributeValue{"id": {S: aws.String("c2")}},
			},
			{
				Count: &one,
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

	fake.putItem(table, map[string]*dynamodb.AttributeValue{
		"id": {S: aws.String("l1")}, "name": {S: aws.String("alpha")}, "created-at": {S: aws.String("2020-01-01")},
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

		var sent dynamodb.ScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		assert.Empty(t, aws.StringValue(sent.Select))
	})

	t.Run("fields-projection-sanitizes-unsafe-names", func(t *testing.T) {
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{Fields: []string{"created-at"}}))

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent dynamodb.ScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.NotNil(t, sent.ProjectionExpression)
		require.Len(t, sent.ExpressionAttributeNames, 1)

		for placeholder, real := range sent.ExpressionAttributeNames {
			assert.Regexp(t, `^#[A-Za-z0-9_]+$`, placeholder)
			assert.Equal(t, "created-at", aws.StringValue(real))
		}
	})

	t.Run("limit-sets-scan-limit", func(t *testing.T) {
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{Limit: 5}))

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent dynamodb.ScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.NotNil(t, sent.Limit)
		assert.EqualValues(t, 5, *sent.Limit)
	})

	t.Run("filter-sanitizes-unsafe-names", func(t *testing.T) {
		var got []dynamoDoc

		require.NoError(t, str.List(t.Context(), table, &got, &list.List{Any: map[string]interface{}{"created-at": "2020-01-01"}}))

		reqs := fake.scanRequests()
		require.NotEmpty(t, reqs)

		var sent dynamodb.ScanInput

		require.NoError(t, json.Unmarshal(reqs[len(reqs)-1].body, &sent))
		require.NotNil(t, sent.FilterExpression)
		require.Len(t, sent.ExpressionAttributeValues, 1)

		var foundSanitized bool

		for placeholder, real := range sent.ExpressionAttributeNames {
			if aws.StringValue(real) == "created-at" {
				assert.Regexp(t, `^#f[A-Za-z0-9_]+$`, placeholder)

				foundSanitized = true
			}
		}

		assert.True(t, foundSanitized)
	})

	t.Run("offset-and-limit-across-pages", func(t *testing.T) {
		items := []map[string]*dynamodb.AttributeValue{
			{"id": {S: aws.String("a")}, "name": {S: aws.String("a")}},
			{"id": {S: aws.String("b")}, "name": {S: aws.String("b")}},
			{"id": {S: aws.String("c")}, "name": {S: aws.String("c")}},
		}
		items2 := []map[string]*dynamodb.AttributeValue{
			{"id": {S: aws.String("d")}, "name": {S: aws.String("d")}},
			{"id": {S: aws.String("e")}, "name": {S: aws.String("e")}},
		}

		restore := fake.setScanQueue([]dynamodb.ScanOutput{
			{Items: items, LastEvaluatedKey: map[string]*dynamodb.AttributeValue{"id": {S: aws.String("c")}}},
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
