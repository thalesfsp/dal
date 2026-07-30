package s3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v2/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

// TestMain clears AWS_CA_BUNDLE once for the whole binary. The sandbox's
// outbound-proxy setup exports it; session.New picks it up regardless of
// what's in our *aws.Config and then requires HTTPClient.Transport to be a
// concrete *http.Transport to install it, which localTransport (by design:
// it never dials out) is not. Nothing in this package's tests makes a real
// TLS connection, so the custom bundle is moot.
func TestMain(m *testing.M) {
	os.Unsetenv("AWS_CA_BUNDLE")
	os.Exit(m.Run())
}

//////
// Test helpers.
//////

// sampleDoc is a small JSON-able document used across tests.
type sampleDoc struct {
	ID      string `json:"id,omitempty"`
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// errHook is returned by the failing branch of hookRecorder.
var errHook = errors.New("hook failure")

// hookRecorder returns a storage.HookFunc that records it was called (via
// *called) and returns err. Parameterizing err (rather than hardcoding nil or
// a fixed error in each call site's closure) keeps every generated closure's
// result genuinely variable across the success/error call sites that share
// it, which is also what keeps unparam from flagging it.
func hookRecorder[T any](called *bool, err error) storage.HookFunc[T] {
	return func(_ context.Context, _ storage.IStorage, _, _ string, _ any, _ T) error {
		*called = true

		return err
	}
}

// s3PlaceholderEndpoint is a syntactically valid endpoint that is never
// actually dialed: newTestS3 routes every request in-process (see
// localTransport), so nothing needs to be listening on it.
const s3PlaceholderEndpoint = "http://s3.invalid"

// newTestS3 wires a *S3 storage to fake in-process (no real socket, see
// localTransport): a request built by the AWS SDK is handed directly to
// fake.handle and never touches the network.
func newTestS3(t *testing.T, fake *s3Fake) *S3 {
	t.Helper()

	str, err := New(t.Context(), fake.bucket, s3TestConfig(s3PlaceholderEndpoint, fake.client()))
	require.NoError(t, err)
	require.NotNil(t, str)
	require.NotNil(t, str.Client)

	return str
}

// sharedS3, sharedFake back sharedTestS3: most tests below only care that
// CRUD/hook/edge-case behavior works against *some* fake S3, not that each
// gets its own client, so they share one client+fake (built once) instead of
// each paying for their own New() (session + ping + struct validation).
// Every caller uses its own key/bucket-path prefix, so sharing the fake's
// object map is safe; sequential (non-parallel) subtests make sharing its
// mutable status-override fields safe too, since every test that sets one
// resets it via t.Cleanup.
//
// This also sidesteps a goroutine-lifecycle flakiness class under -race:
// each aws-sdk-go v1 credentials fetch spawns a short-lived internal
// singleflight goroutine, and New()'s validator.Validate() call right after
// the ping is allocation-heavy; with ~15 independent New() calls in this
// file, a prior test's not-yet-unwound singleflight goroutine occasionally
// raced a later test's reused memory address. A single shared New() call
// all but eliminates that window.
var (
	sharedS3Once sync.Once
	sharedS3Inst *S3
	sharedS3Fake *s3Fake
)

// sharedTestS3 returns the package's shared fake+client, built on first use.
func sharedTestS3(t *testing.T) (*S3, *s3Fake) {
	t.Helper()

	sharedS3Once.Do(func() {
		sharedS3Fake = newS3Fake("bucket-shared")
		sharedS3Inst = newTestS3(t, sharedS3Fake)
	})

	require.NotNil(t, sharedS3Inst, "shared *S3 failed to initialize in an earlier test")

	return sharedS3Inst, sharedS3Fake
}

//////
// New.
//////

func TestNew_HappyPath(t *testing.T) {
	fake := newS3Fake("bucket-new-ok")
	str := newTestS3(t, fake)

	assert.Equal(t, "bucket-new-ok", str.Bucket)
	assert.Equal(t, str.Client, str.GetClient())
	assert.NotNil(t, str.Config)
}

func TestNew_PingFailure(t *testing.T) {
	fake := newS3Fake("bucket-new-fail")
	fake.headStatus = http.StatusInternalServerError

	ts := fake.server()
	t.Cleanup(ts.Close)

	// Bound the retrier's exponential backoff (10s/20s/40s) with a short
	// deadline: RunCtx aborts as soon as ctx.Done() fires while sleeping
	// between retries, so this returns in well under a second instead of
	// waiting out the full backoff schedule.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	str, err := New(ctx, fake.bucket, s3TestConfig(ts.URL, &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}))
	require.Error(t, err)
	assert.Nil(t, str)
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
// RetrieveSigned.
//////

func TestRetrieveSigned_HappyPath(t *testing.T) {
	// Unlike every other test in this file, this one needs a real, dialable
	// endpoint: the presigned URL it produces is later fetched with a plain
	// http.Get, bypassing the AWS SDK (and thus newTestS3's in-process
	// localTransport) entirely.
	fake := newS3Fake("bucket-signed")

	ts := fake.server()
	t.Cleanup(ts.Close)

	str, err := New(t.Context(), fake.bucket, s3TestConfig(ts.URL, &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}))
	require.NoError(t, err)

	fake.put("signed/doc.json", []byte(`{"id":"1"}`))

	url, err := RetrieveSigned(t.Context(), str, "signed/doc.json", fake.bucket, time.Minute)
	require.NoError(t, err)
	assert.NotEmpty(t, url)

	// The URL is the presigned one produced above, pointing at the local
	// fake — not attacker-controlled input.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":"1"}`, string(body))
}

func TestRetrieveSigned_BadClientType(t *testing.T) {
	mock := &storage.Mock{
		MockGetClient: func() any { return "not-an-s3-client" },
	}

	url, err := RetrieveSigned(t.Context(), mock, "key", "bucket", time.Minute)
	require.Error(t, err)
	assert.Empty(t, url)
}

//////
// GetClient.
//////

func TestS3_GetClient(t *testing.T) {
	str, _ := sharedTestS3(t)

	assert.Same(t, str.Client, str.GetClient())
}

//////
// CRUD happy path (mirrors the E2E test, offline).
//////

func TestS3_CRUD_HappyPath(t *testing.T) {
	// Isolated (not shared): Count/List have no target/prefix filtering in
	// the production code (see the audit note on s3.go's Count/List), so
	// they reflect the *whole* bucket - this test needs one all to itself.
	fake := newS3Fake("bucket-crud")
	str := newTestS3(t, fake)

	const target = "docs/happy.json"

	doc := &sampleDoc{ID: "1", Name: "alpha", Version: "1.0.0"}

	beforeCreated := str.GetCounterCreated().Value()

	id, err := str.Create(t.Context(), "1", target, doc, &create.Create{})
	require.NoError(t, err)
	assert.NotEmpty(t, id)
	assert.EqualValues(t, beforeCreated+1, str.GetCounterCreated().Value())

	var got sampleDoc

	beforeRetrieved := str.GetCounterRetrieved().Value()

	require.NoError(t, str.Retrieve(t.Context(), "1", target, &got, &retrieve.Retrieve{}))
	assert.Equal(t, *doc, got)
	assert.EqualValues(t, beforeRetrieved+1, str.GetCounterRetrieved().Value())

	updated := &sampleDoc{ID: "1", Name: "beta", Version: "1.0.1"}

	beforeUpdated := str.GetCounterUpdated().Value()

	require.NoError(t, str.Update(t.Context(), "1", target, updated, &update.Update{}))
	assert.EqualValues(t, beforeUpdated+1, str.GetCounterUpdated().Value())

	var gotUpdated sampleDoc

	require.NoError(t, str.Retrieve(t.Context(), "1", target, &gotUpdated, &retrieve.Retrieve{}))
	assert.Equal(t, *updated, gotUpdated)

	beforeCounted := str.GetCounterCounted().Value()

	cnt, err := str.Count(t.Context(), target, &count.Count{Search: "*"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, cnt)
	assert.EqualValues(t, beforeCounted+1, str.GetCounterCounted().Value())

	var keys ResponseListKeys

	beforeListed := str.GetCounterListed().Value()

	require.NoError(t, str.List(t.Context(), target, &keys, &list.List{Search: "*"}))
	assert.Contains(t, keys.Keys, target)
	assert.EqualValues(t, beforeListed+1, str.GetCounterListed().Value())

	beforeDeleted := str.GetCounterDeleted().Value()

	require.NoError(t, str.Delete(t.Context(), "1", target, &delete.Delete{}))
	assert.EqualValues(t, beforeDeleted+1, str.GetCounterDeleted().Value())

	beforeRetrievedFailed := str.GetCounterRetrievedFailed().Value()

	err = str.Retrieve(t.Context(), "1", target, &got, &retrieve.Retrieve{})
	require.Error(t, err)
	assert.EqualValues(t, beforeRetrievedFailed+1, str.GetCounterRetrievedFailed().Value())

	cErr, ok := customerror.To(err)
	require.True(t, ok)
	assert.Equal(t, http.StatusNotFound, cErr.StatusCode)
}

//////
// Create: content-type switch + error branches.
//////

func TestS3_Create_ContentTypes(t *testing.T) {
	str, fake := sharedTestS3(t)

	t.Run("file", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "s3-*.txt")
		require.NoError(t, err)

		_, err = f.WriteString("file-content")
		require.NoError(t, err)
		require.NoError(t, f.Sync())

		_, err = f.Seek(0, io.SeekStart)
		require.NoError(t, err)

		id, err := str.Create(t.Context(), "f1", "ct/file.txt", f, &create.Create{})
		require.NoError(t, err)
		assert.NotEmpty(t, id)

		body, ok := fake.get("ct/file.txt")
		require.True(t, ok)
		assert.Equal(t, "file-content", string(body))
	})

	t.Run("string", func(t *testing.T) {
		id, err := str.Create(t.Context(), "s1", "ct/string.txt", "hello-string", &create.Create{})
		require.NoError(t, err)
		assert.NotEmpty(t, id)

		body, ok := fake.get("ct/string.txt")
		require.True(t, ok)
		assert.Equal(t, "hello-string", string(body))
	})

	t.Run("bytes", func(t *testing.T) {
		raw := []byte{0x01, 0x02, 0x03}

		id, err := str.Create(t.Context(), "b1", "ct/bytes.bin", raw, &create.Create{})
		require.NoError(t, err)
		assert.NotEmpty(t, id)

		body, ok := fake.get("ct/bytes.bin")
		require.True(t, ok)
		assert.Equal(t, raw, body)
	})

	t.Run("struct-default", func(t *testing.T) {
		doc := &sampleDoc{ID: "d1", Name: "gamma"}

		id, err := str.Create(t.Context(), "d1", "ct/struct.json", doc, &create.Create{})
		require.NoError(t, err)
		assert.NotEmpty(t, id)

		body, ok := fake.get("ct/struct.json")
		require.True(t, ok)

		var got sampleDoc

		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, *doc, got)
	})

	t.Run("file-read-error", func(t *testing.T) {
		dir, err := os.Open(t.TempDir())
		require.NoError(t, err)

		defer dir.Close()

		before := str.GetCounterCreatedFailed().Value()

		_, err = str.Create(t.Context(), "e1", "ct/dir.txt", dir, &create.Create{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("marshal-error", func(t *testing.T) {
		before := str.GetCounterCreatedFailed().Value()

		_, err := str.Create(t.Context(), "e2", "ct/chan.json", make(chan int), &create.Create{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterCreatedFailed().Value())
	})

	t.Run("upload-error", func(t *testing.T) {
		fake.putStatus = http.StatusInternalServerError

		t.Cleanup(func() { fake.putStatus = 0 })

		before := str.GetCounterCreatedFailed().Value()

		_, err := str.Create(t.Context(), "e3", "ct/fail.json", &sampleDoc{}, &create.Create{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterCreatedFailed().Value())
	})
}

func TestS3_Create_TargetMissing(t *testing.T) {
	str, _ := sharedTestS3(t)

	_, err := str.Create(t.Context(), "id1", "", "content", &create.Create{})
	require.Error(t, err)
}

func TestS3_Create_OptionsAndHooks(t *testing.T) {
	str, _ := sharedTestS3(t)

	t.Run("option-error", func(t *testing.T) {
		_, err := str.Create(t.Context(), "id", "hooks/a.json", "x", &create.Create{}, storage.WithPreHook[*create.Create](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", "hooks/b.json", "x", &create.Create{}, storage.WithPreHook(hookRecorder[*create.Create](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("pre-hook-success", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", "hooks/c.json", "x", &create.Create{}, storage.WithPreHook(hookRecorder[*create.Create](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", "hooks/d.json", "x", &create.Create{}, storage.WithPostHook(hookRecorder[*create.Create](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-success", func(t *testing.T) {
		var called bool

		_, err := str.Create(t.Context(), "id", "hooks/e.json", "x", &create.Create{}, storage.WithPostHook(hookRecorder[*create.Create](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})
}

//////
// Update: content-type switch + error branches.
//////

func TestS3_Update_ContentTypes(t *testing.T) {
	str, fake := sharedTestS3(t)

	t.Run("empty-id", func(t *testing.T) {
		err := str.Update(t.Context(), "", "ct/x.json", "x", &update.Update{})
		require.Error(t, err)
	})

	t.Run("target-missing", func(t *testing.T) {
		err := str.Update(t.Context(), "id", "", "x", &update.Update{})
		require.Error(t, err)
	})

	t.Run("file", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "s3-*.txt")
		require.NoError(t, err)

		_, err = f.WriteString("updated-file-content")
		require.NoError(t, err)
		require.NoError(t, f.Sync())

		_, err = f.Seek(0, io.SeekStart)
		require.NoError(t, err)

		require.NoError(t, str.Update(t.Context(), "f1", "ct/file.txt", f, &update.Update{}))

		body, ok := fake.get("ct/file.txt")
		require.True(t, ok)
		assert.Equal(t, "updated-file-content", string(body))
	})

	t.Run("string", func(t *testing.T) {
		require.NoError(t, str.Update(t.Context(), "s1", "ct/string.txt", "updated-string", &update.Update{}))

		body, ok := fake.get("ct/string.txt")
		require.True(t, ok)
		assert.Equal(t, "updated-string", string(body))
	})

	t.Run("bytes", func(t *testing.T) {
		raw := []byte{0x0a, 0x0b}

		require.NoError(t, str.Update(t.Context(), "b1", "ct/bytes.bin", raw, &update.Update{}))

		body, ok := fake.get("ct/bytes.bin")
		require.True(t, ok)
		assert.Equal(t, raw, body)
	})

	t.Run("struct-default", func(t *testing.T) {
		doc := &sampleDoc{ID: "d1", Name: "delta"}

		require.NoError(t, str.Update(t.Context(), "d1", "ct/struct.json", doc, &update.Update{}))

		body, ok := fake.get("ct/struct.json")
		require.True(t, ok)

		var got sampleDoc

		require.NoError(t, json.Unmarshal(body, &got))
		assert.Equal(t, *doc, got)
	})

	t.Run("file-read-error", func(t *testing.T) {
		dir, err := os.Open(t.TempDir())
		require.NoError(t, err)

		defer dir.Close()

		before := str.GetCounterUpdatedFailed().Value()

		err = str.Update(t.Context(), "e1", "ct/dir.txt", dir, &update.Update{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("marshal-error", func(t *testing.T) {
		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(t.Context(), "e2", "ct/chan.json", make(chan int), &update.Update{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterUpdatedFailed().Value())
	})

	t.Run("upload-error", func(t *testing.T) {
		fake.putStatus = http.StatusInternalServerError

		t.Cleanup(func() { fake.putStatus = 0 })

		before := str.GetCounterUpdatedFailed().Value()

		err := str.Update(t.Context(), "e3", "ct/fail.json", &sampleDoc{}, &update.Update{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterUpdatedFailed().Value())
	})
}

func TestS3_Update_OptionsAndHooks(t *testing.T) {
	str, _ := sharedTestS3(t)

	t.Run("option-error", func(t *testing.T) {
		err := str.Update(t.Context(), "id", "hooks/a.json", "x", &update.Update{}, storage.WithPreHook[*update.Update](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", "hooks/b.json", "x", &update.Update{}, storage.WithPreHook(hookRecorder[*update.Update](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("pre-hook-success", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", "hooks/c.json", "x", &update.Update{}, storage.WithPreHook(hookRecorder[*update.Update](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", "hooks/d.json", "x", &update.Update{}, storage.WithPostHook(hookRecorder[*update.Update](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-success", func(t *testing.T) {
		var called bool

		err := str.Update(t.Context(), "id", "hooks/e.json", "x", &update.Update{}, storage.WithPostHook(hookRecorder[*update.Update](&called, nil)))
		require.NoError(t, err)
		assert.True(t, called)
	})
}

//////
// Delete.
//////

func TestS3_Delete(t *testing.T) {
	str, fake := sharedTestS3(t)

	t.Run("empty-id", func(t *testing.T) {
		err := str.Delete(t.Context(), "", "k", &delete.Delete{})
		require.Error(t, err)
	})

	t.Run("target-missing", func(t *testing.T) {
		err := str.Delete(t.Context(), "id", "", &delete.Delete{})
		require.Error(t, err)
	})

	t.Run("option-error", func(t *testing.T) {
		err := str.Delete(t.Context(), "id", "k", &delete.Delete{}, storage.WithPreHook[*delete.Delete](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		err := str.Delete(t.Context(), "id", "k", &delete.Delete{}, storage.WithPreHook(hookRecorder[*delete.Delete](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		err := str.Delete(t.Context(), "id", "k", &delete.Delete{}, storage.WithPostHook(hookRecorder[*delete.Delete](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.deleteStatus = http.StatusInternalServerError

		t.Cleanup(func() { fake.deleteStatus = 0 })

		before := str.GetCounterDeletedFailed().Value()

		err := str.Delete(t.Context(), "id", "k", &delete.Delete{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterDeletedFailed().Value())
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		var preCalled, postCalled bool

		err := str.Delete(
			t.Context(), "id", "k", &delete.Delete{},
			storage.WithPreHook(hookRecorder[*delete.Delete](&preCalled, nil)),
			storage.WithPostHook(hookRecorder[*delete.Delete](&postCalled, nil)),
		)
		require.NoError(t, err)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
	})
}

//////
// Retrieve.
//////

func TestS3_Retrieve(t *testing.T) {
	str, fake := sharedTestS3(t)

	fake.put("k/ok.json", []byte(`{"id":"1","name":"alpha"}`))
	fake.put("k/bad.json", []byte(`not-json`))

	t.Run("empty-id", func(t *testing.T) {
		var v sampleDoc

		err := str.Retrieve(t.Context(), "", "k/ok.json", &v, &retrieve.Retrieve{})
		require.Error(t, err)
	})

	t.Run("target-missing", func(t *testing.T) {
		var v sampleDoc

		err := str.Retrieve(t.Context(), "id", "", &v, &retrieve.Retrieve{})
		require.Error(t, err)
	})

	t.Run("option-error", func(t *testing.T) {
		var v sampleDoc

		err := str.Retrieve(t.Context(), "id", "k/ok.json", &v, &retrieve.Retrieve{}, storage.WithPreHook[*retrieve.Retrieve](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var v sampleDoc

		var called bool

		err := str.Retrieve(t.Context(), "id", "k/ok.json", &v, &retrieve.Retrieve{}, storage.WithPreHook(hookRecorder[*retrieve.Retrieve](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("not-found-NoSuchKey", func(t *testing.T) {
		var v sampleDoc

		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(t.Context(), "id", "k/missing.json", &v, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterRetrievedFailed().Value())

		cErr, ok := customerror.To(err)
		require.True(t, ok)
		assert.Equal(t, http.StatusNotFound, cErr.StatusCode)
	})

	t.Run("not-found-alt-code", func(t *testing.T) {
		fake.setGetOverride("k/notfound-alt.json", http.StatusNotFound, "NotFound")

		t.Cleanup(func() { fake.clearGetOverride("k/notfound-alt.json") })

		var v sampleDoc

		err := str.Retrieve(t.Context(), "id", "k/notfound-alt.json", &v, &retrieve.Retrieve{})
		require.Error(t, err)

		cErr, ok := customerror.To(err)
		require.True(t, ok)
		assert.Equal(t, http.StatusNotFound, cErr.StatusCode)
	})

	t.Run("aws-error-generic", func(t *testing.T) {
		fake.setGetOverride("k/fail.json", http.StatusInternalServerError, "InternalError")

		t.Cleanup(func() { fake.clearGetOverride("k/fail.json") })

		var v sampleDoc

		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(t.Context(), "id", "k/fail.json", &v, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		var v sampleDoc

		before := str.GetCounterRetrievedFailed().Value()

		err := str.Retrieve(t.Context(), "id", "k/bad.json", &v, &retrieve.Retrieve{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterRetrievedFailed().Value())
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var v sampleDoc

		var called bool

		err := str.Retrieve(t.Context(), "id", "k/ok.json", &v, &retrieve.Retrieve{}, storage.WithPostHook(hookRecorder[*retrieve.Retrieve](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		var v sampleDoc

		var preCalled, postCalled bool

		err := str.Retrieve(
			t.Context(), "id", "k/ok.json", &v, &retrieve.Retrieve{},
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

func TestS3_Count(t *testing.T) {
	// Isolated: see the comment in TestS3_CRUD_HappyPath.
	fake := newS3Fake("bucket-count")
	str := newTestS3(t, fake)

	fake.put("c/one", []byte("a"))
	fake.put("c/two", []byte("b"))

	t.Run("happy-path", func(t *testing.T) {
		cnt, err := str.Count(t.Context(), "", &count.Count{Search: "*"})
		require.NoError(t, err)
		assert.EqualValues(t, 2, cnt)
	})

	t.Run("option-error", func(t *testing.T) {
		_, err := str.Count(t.Context(), "", &count.Count{}, storage.WithPreHook[*count.Count](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Count(t.Context(), "", &count.Count{}, storage.WithPreHook(hookRecorder[*count.Count](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var called bool

		_, err := str.Count(t.Context(), "", &count.Count{}, storage.WithPostHook(hookRecorder[*count.Count](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		var preCalled, postCalled bool

		cnt, err := str.Count(
			t.Context(), "", &count.Count{},
			storage.WithPreHook(hookRecorder[*count.Count](&preCalled, nil)),
			storage.WithPostHook(hookRecorder[*count.Count](&postCalled, nil)),
		)
		require.NoError(t, err)
		assert.EqualValues(t, 2, cnt)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.listStatus = http.StatusInternalServerError

		t.Cleanup(func() { fake.listStatus = 0 })

		before := str.GetCounterCountedFailed().Value()

		_, err := str.Count(t.Context(), "", &count.Count{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterCountedFailed().Value())
	})

	t.Run("empty", func(t *testing.T) {
		t.Cleanup(fake.withObjects(map[string][]byte{}))

		cnt, err := str.Count(t.Context(), "", &count.Count{})
		require.NoError(t, err)
		assert.EqualValues(t, 0, cnt)
	})

	t.Run("pagination", func(t *testing.T) {
		t.Cleanup(fake.withListPages([]string{
			s3ListObjectsV2XML([]string{"p/a", "p/b"}, true, "tok1"),
			s3ListObjectsV2XML([]string{"p/c"}, false, ""),
		}))

		cnt, err := str.Count(t.Context(), "", &count.Count{})
		require.NoError(t, err)
		assert.EqualValues(t, 3, cnt)
	})
}

//////
// List.
//////

func TestS3_List(t *testing.T) {
	// Isolated: see the comment in TestS3_CRUD_HappyPath.
	fake := newS3Fake("bucket-list")
	str := newTestS3(t, fake)

	fake.put("l/one", []byte("a"))
	fake.put("l/two", []byte("b"))

	t.Run("happy-path", func(t *testing.T) {
		var keys ResponseListKeys

		require.NoError(t, str.List(t.Context(), "", &keys, &list.List{Search: "*"}))
		assert.ElementsMatch(t, []string{"l/one", "l/two"}, keys.Keys)
	})

	t.Run("option-error", func(t *testing.T) {
		var keys ResponseListKeys

		err := str.List(t.Context(), "", &keys, &list.List{}, storage.WithPreHook[*list.List](nil))
		require.Error(t, err)
	})

	t.Run("pre-hook-error", func(t *testing.T) {
		var keys ResponseListKeys

		var called bool

		err := str.List(t.Context(), "", &keys, &list.List{}, storage.WithPreHook(hookRecorder[*list.List](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("post-hook-error", func(t *testing.T) {
		var keys ResponseListKeys

		var called bool

		err := str.List(t.Context(), "", &keys, &list.List{}, storage.WithPostHook(hookRecorder[*list.List](&called, errHook)))
		require.Error(t, err)
		assert.True(t, called)
	})

	t.Run("happy-path-with-hooks", func(t *testing.T) {
		var keys ResponseListKeys

		var preCalled, postCalled bool

		err := str.List(
			t.Context(), "", &keys, &list.List{},
			storage.WithPreHook(hookRecorder[*list.List](&preCalled, nil)),
			storage.WithPostHook(hookRecorder[*list.List](&postCalled, nil)),
		)
		require.NoError(t, err)
		assert.True(t, preCalled)
		assert.True(t, postCalled)
	})

	t.Run("aws-error", func(t *testing.T) {
		fake.listStatus = http.StatusInternalServerError

		t.Cleanup(func() { fake.listStatus = 0 })

		before := str.GetCounterListedFailed().Value()

		var keys ResponseListKeys

		err := str.List(t.Context(), "", &keys, &list.List{})
		require.Error(t, err)
		assert.EqualValues(t, before+1, str.GetCounterListedFailed().Value())
	})

	t.Run("empty", func(t *testing.T) {
		t.Cleanup(fake.withObjects(map[string][]byte{}))

		var keys ResponseListKeys

		require.NoError(t, str.List(t.Context(), "", &keys, &list.List{}))
		assert.Empty(t, keys.Keys)
	})

	t.Run("pagination", func(t *testing.T) {
		t.Cleanup(fake.withListPages([]string{
			s3ListObjectsV2XML([]string{"p/a", "p/b"}, true, "tok1"),
			s3ListObjectsV2XML([]string{"p/c"}, false, ""),
		}))

		var keys ResponseListKeys

		require.NoError(t, str.List(t.Context(), "", &keys, &list.List{}))
		assert.ElementsMatch(t, []string{"p/a", "p/b", "p/c"}, keys.Keys)
	})
}
