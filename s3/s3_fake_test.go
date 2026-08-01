package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

type s3BulkDocument struct {
	Value string `json:"value"`
}

func s3BulkItems(count int) []storage.BulkItem {
	items := make([]storage.BulkItem, count)
	for index := range items {
		items[index] = storage.BulkItem{
			ID:    fmt.Sprintf("bulk/item-%d.json", index),
			Value: s3BulkDocument{Value: fmt.Sprintf("value-%d", index)},
		}
	}

	return items
}

func TestS3BulkCreateHappyPathManyItems(t *testing.T) {
	fake := newS3Fake("bulk-happy-bucket")
	str := newTestS3(t, fake)
	items := s3BulkItems(20)

	result, err := str.BulkCreate(t.Context(), "unused", items, nil)
	require.NoError(t, err)
	require.Equal(t, len(items), len(result.Succeeded))
	require.Empty(t, result.Failures)
	require.False(t, result.Stopped)

	for index, item := range items {
		require.Equal(t, index, result.Succeeded[index])

		var got s3BulkDocument
		require.NoError(t, str.Retrieve(t.Context(), item.ID, item.ID, &got, nil))
		require.Equal(t, item.Value, got)
	}
}

func TestS3BulkCreateEmptyInput(t *testing.T) {
	fake := newS3Fake("bulk-empty-bucket")
	str := newTestS3(t, fake)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := str.BulkCreate(ctx, "unused", nil, nil)
	require.NoError(t, err)
	require.Equal(t, storage.BulkResult{}, result)
}

func TestS3BulkCreateSingleItem(t *testing.T) {
	fake := newS3Fake("bulk-single-bucket")
	str := newTestS3(t, fake)
	items := s3BulkItems(1)

	result, err := str.BulkCreate(t.Context(), "unused", items, nil)
	require.NoError(t, err)
	require.Equal(t, []int{0}, result.Succeeded)

	var got s3BulkDocument
	require.NoError(t, str.Retrieve(t.Context(), items[0].ID, items[0].ID, &got, nil))
	require.Equal(t, items[0].Value, got)
}

func TestS3BulkCreatePartialFailurePreservesOriginalIndices(t *testing.T) {
	fake := newS3Fake("bulk-partial-bucket")
	str := newTestS3(t, fake)
	items := s3BulkItems(20)
	rejected := errors.New("rejected by s3 bulk test")
	preHook := func(_ context.Context, _ storage.IStorage, id, _ string, _ any, _ *create.Create) error {
		if id == items[17].ID {
			return rejected
		}

		return nil
	}

	result, err := str.BulkCreate(t.Context(), "unused", items, nil, storage.WithPreHook(preHook))
	require.Error(t, err)
	require.False(t, result.Stopped)
	require.Len(t, result.Failures, 1)
	if failureIndices := []int{result.Failures[0].Index}; !reflect.DeepEqual(failureIndices, []int{17}) {
		t.Fatalf("failure indices = %v, want [17]", failureIndices)
	}
	require.ErrorIs(t, result.Failures[0].Err, rejected)

	for index, item := range items {
		_, found := fake.get(item.ID)
		if index == 17 {
			require.False(t, found)

			continue
		}

		require.True(t, found, "item %d", index)
	}
}

func TestS3BulkCreateContextCancellationMidBatch(t *testing.T) {
	fake := newS3Fake("bulk-cancel-bucket")
	str := newTestS3(t, fake)
	items := s3BulkItems(24)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	var completed atomic.Int32
	postHook := func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
		if completed.Add(1) == 3 {
			cancel()
		}

		return nil
	}

	result, err := str.BulkCreate(ctx, "unused", items, nil, storage.WithPostHook(postHook))
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, result.Stopped)
	require.NotEmpty(t, result.Succeeded)
	require.Equal(t, len(items), len(result.Succeeded)+len(result.Failures))

	succeeded := make(map[int]bool, len(result.Succeeded))
	for _, index := range result.Succeeded {
		succeeded[index] = true
	}
	for index, item := range items {
		_, found := fake.get(item.ID)
		require.Equal(t, found, succeeded[index], "persisted/report mismatch for item %d", index)
	}
	for _, failure := range result.Failures {
		require.ErrorIs(t, failure.Err, context.Canceled)
	}
}

//////
// A minimal, offline, in-memory fake of the S3 REST API. It's deliberately
// small: only what the s3 package's operations (HeadBucket, PutObject,
// GetObject, DeleteObject, ListObjectsV2) exercise.
//////

// s3GetOverride forces a specific error status+code for a GetObject key.
type s3GetOverride struct {
	status int
	code   string
}

// s3Fake is a stateful, concurrency-safe fake S3 server.
type s3Fake struct {
	bucket string

	mu      sync.Mutex
	objects map[string][]byte

	// Status overrides. Zero means "use the default successful status".
	headStatus   int
	headFailures int
	putStatus    int
	deleteStatus int
	listStatus   int

	// getOverrides lets a test force a specific status+error-code for a given
	// key on GetObject (e.g. a 404/NotFound instead of the default
	// 404/NoSuchKey, or a 500/InternalError). When a key has no override, a
	// stored object is served as 200/body and a missing one as
	// 404/NoSuchKey.
	getOverrides map[string]s3GetOverride

	// listPages, when non-empty, overrides the derived-from-objects page and
	// is used to test pagination: each call to ListObjectsV2 pops the next
	// entry (the last one repeats once exhausted).
	listPages []string
	listCall  int

	// requests records every request path+method seen, guarded by mu.
	requests []string
}

// newS3Fake creates a ready-to-use fake with no stored objects.
func newS3Fake(bucket string) *s3Fake {
	return &s3Fake{
		bucket:       bucket,
		objects:      map[string][]byte{},
		getOverrides: map[string]s3GetOverride{},
	}
}

// server starts (and the caller must Close) a real httptest server backing
// the fake. Only used when a test needs a genuinely dialable URL (e.g.
// following a presigned URL with a plain http.Get); everything that goes
// through the AWS SDK instead uses client(), which never opens a socket.
func (f *s3Fake) server() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(f.handle))
}

// localTransport is an http.RoundTripper that invokes an http.Handler
// in-process via httptest.NewRecorder, with no real socket and no background
// goroutines. Standing up a real httptest.Server per test/case is how the
// suite started, but under -race, a just-closed server's connection
// goroutines (net/http's persistConn readLoop) can still be unwinding when a
// *following* test's own allocations reuse the same freed memory address -
// the race detector then (correctly, from its point of view) flags the
// unsynchronized access. Routing AWS SDK calls in-process instead of over a
// real loopback connection sidesteps that whole goroutine-lifecycle class of
// flakiness, since RoundTrip runs synchronously on the calling goroutine.
type localTransport struct {
	handler http.Handler
}

func (rt *localTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()

	rt.handler.ServeHTTP(rec, req)

	resp := rec.Result()
	resp.Request = req

	return resp, nil
}

// client returns an http.Client that routes every request straight to the
// fake's handler in-process (see localTransport).
func (f *s3Fake) client() *http.Client {
	return &http.Client{Transport: &localTransport{handler: http.HandlerFunc(f.handle)}}
}

// put seeds an object directly, bypassing PutObject.
func (f *s3Fake) put(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.objects[key] = body
}

// get reads a stored object directly, bypassing GetObject.
func (f *s3Fake) get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	b, ok := f.objects[key]

	return b, ok
}

// setGetOverride forces the given status+code for a GetObject key.
func (f *s3Fake) setGetOverride(key string, status int, code string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getOverrides[key] = s3GetOverride{status: status, code: code}
}

// clearGetOverride removes a GetObject override (named to avoid colliding
// with the params/v2/delete import used by callers in this package's other
// test files).
func (f *s3Fake) clearGetOverride(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.getOverrides, key)
}

// withObjects temporarily replaces the fake's object set (e.g. to exercise
// an "empty bucket" case) and returns a restore func. Reusing a single
// server/client across such variations - rather than spinning up a fresh
// httptest.Server per case - avoids a class of net/http-internal false
// positives under -race, where a just-closed connection's background
// goroutines can still be unwinding when a new server/session's memory
// allocations reuse the same address.
func (f *s3Fake) withObjects(objects map[string][]byte) func() {
	f.mu.Lock()
	prev := f.objects
	f.objects = objects
	f.mu.Unlock()

	return func() {
		f.mu.Lock()
		f.objects = prev
		f.mu.Unlock()
	}
}

// withListPages temporarily overrides ListObjectsV2 pagination (see
// withObjects for why this mutates fake state in place instead of standing
// up a new server).
func (f *s3Fake) withListPages(pages []string) func() {
	f.mu.Lock()
	prevPages := f.listPages
	prevCall := f.listCall
	f.listPages = pages
	f.listCall = 0
	f.mu.Unlock()

	return func() {
		f.mu.Lock()
		f.listPages = prevPages
		f.listCall = prevCall
		f.mu.Unlock()
	}
}

func (f *s3Fake) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests = append(f.requests, r.Method+" "+r.URL.String())

	prefix := "/" + f.bucket

	switch {
	case r.Method == http.MethodHead && r.URL.Path == prefix:
		f.handleHeadBucket(w)

	case r.Method == http.MethodGet && r.URL.Path == prefix && r.URL.Query().Get("list-type") == "2":
		f.handleListObjectsV2(w)

	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, prefix+"/"):
		f.handlePutObject(w, r, strings.TrimPrefix(r.URL.Path, prefix+"/"))

	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, prefix+"/"):
		f.handleGetObject(w, strings.TrimPrefix(r.URL.Path, prefix+"/"))

	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, prefix+"/"):
		f.handleDeleteObject(w, strings.TrimPrefix(r.URL.Path, prefix+"/"))

	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func (f *s3Fake) handleHeadBucket(w http.ResponseWriter) {
	if f.headFailures > 0 {
		f.headFailures--
		writeS3XMLError(w, http.StatusInternalServerError, "InternalError", "simulated retryable head failure")

		return
	}

	if f.headStatus != 0 && f.headStatus != http.StatusOK {
		w.WriteHeader(f.headStatus)

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (f *s3Fake) requestCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := 0
	for _, request := range f.requests {
		if strings.HasPrefix(request, method+" ") {
			count++
		}
	}

	return count
}

func (f *s3Fake) handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	if f.putStatus != 0 && f.putStatus != http.StatusOK {
		writeS3XMLError(w, f.putStatus, "InternalError", "simulated put failure")

		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3XMLError(w, http.StatusInternalServerError, "InternalError", err.Error())

		return
	}

	f.objects[key] = body

	w.Header().Set("ETag", `"fake-etag"`)
	w.WriteHeader(http.StatusOK)
}

func (f *s3Fake) handleGetObject(w http.ResponseWriter, key string) {
	if ov, ok := f.getOverrides[key]; ok {
		writeS3XMLError(w, ov.status, ov.code, "simulated get failure")

		return
	}

	body, ok := f.objects[key]
	if !ok {
		writeS3XMLError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")

		return
	}

	w.WriteHeader(http.StatusOK)

	//nolint:errcheck
	w.Write(body)
}

func (f *s3Fake) handleDeleteObject(w http.ResponseWriter, key string) {
	if f.deleteStatus != 0 && f.deleteStatus != http.StatusNoContent {
		writeS3XMLError(w, f.deleteStatus, "InternalError", "simulated delete failure")

		return
	}

	delete(f.objects, key)

	w.WriteHeader(http.StatusNoContent)
}

func (f *s3Fake) handleListObjectsV2(w http.ResponseWriter) {
	if f.listStatus != 0 && f.listStatus != http.StatusOK {
		writeS3XMLError(w, f.listStatus, "InternalError", "simulated list failure")

		return
	}

	if len(f.listPages) > 0 {
		idx := f.listCall
		if idx >= len(f.listPages) {
			idx = len(f.listPages) - 1
		}

		f.listCall++

		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)

		//nolint:errcheck
		w.Write([]byte(f.listPages[idx]))

		return
	}

	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	//nolint:errcheck
	w.Write([]byte(s3ListObjectsV2XML(keys, false, "")))
}

//////
// XML builders.
//////

// writeS3XMLError writes a S3-shaped error response.
func writeS3XMLError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)

	fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message></Error>`, code, message)
}

// s3ListObjectsV2XML renders a ListObjectsV2 XML response for the given keys.
func s3ListObjectsV2XML(keys []string, truncated bool, nextToken string) string {
	var b strings.Builder

	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, "<KeyCount>%d</KeyCount>", len(keys))
	fmt.Fprintf(&b, "<IsTruncated>%t</IsTruncated>", truncated)

	if nextToken != "" {
		fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", nextToken)
	}

	for _, k := range keys {
		b.WriteString("<Contents>")
		fmt.Fprintf(&b, "<Key>%s</Key>", k)
		b.WriteString("<LastModified>2024-01-01T00:00:00.000Z</LastModified>")
		b.WriteString(`<ETag>"etag"</ETag>`)
		b.WriteString("<Size>1</Size>")
		b.WriteString("<StorageClass>STANDARD</StorageClass>")
		b.WriteString("</Contents>")
	}

	b.WriteString("</ListBucketResult>")

	return b.String()
}

//////
// Config helper.
//////

// s3TestConfig points the AWS SDK at endpoint using client. Endpoint only
// needs to be well-formed (not necessarily dialable) when client's transport
// doesn't actually hit the network, e.g. a *localTransport.
func s3TestConfig(endpoint string, client *http.Client) *Config {
	return &aws.Config{
		BaseEndpoint: aws.String(endpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
		HTTPClient:   client,
	}
}
