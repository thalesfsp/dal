package elasticsearch

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

//////
// Offline fake Elasticsearch server.
//
// go-elasticsearch v8 rejects any response that doesn't carry the
// `X-Elastic-Product: Elasticsearch` header (the "product check"), and `New`
// pings `GET /` expecting a `version.number` field. Every helper below sets
// that header, and any server that backs a successful `New`/`NewWithIndex`
// call must answer `GET /` (and `HEAD /`) with a valid info payload.
//////

// esInfoBody is a valid response for `GET /`, required for the product
// check and for `New`'s ping to succeed.
const esInfoBody = `{"name":"fake","version":{"number":"8.15.0"},"tagline":"You Know, for Search"}`

// capturedRequest is one HTTP request the fake server received.
type capturedRequest struct {
	Method string
	Path   string
	Query  string
	Body   string
}

// fakeServer is a minimal in-process stand-in for an Elasticsearch node. A
// route func supplied by the test inspects method+path and writes the
// response; fakeServer takes care of recording every request (so tests can
// assert on what was actually sent) and stamping the mandatory product
// header on every response.
type fakeServer struct {
	*httptest.Server

	mu   sync.Mutex
	reqs []capturedRequest
}

// newFakeServer starts an httptest server. Every response gets the
// `X-Elastic-Product` header; route is responsible for the rest, including
// `GET /` and `HEAD /` if the test needs `New`/`NewWithIndex` to succeed.
// The server is closed automatically via t.Cleanup.
func newFakeServer(t *testing.T, route func(w http.ResponseWriter, r *http.Request, body []byte)) *fakeServer {
	t.Helper()

	fs := &fakeServer{}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		fs.mu.Lock()
		fs.reqs = append(fs.reqs, capturedRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.RawQuery,
			Body:   string(body),
		})
		fs.mu.Unlock()

		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")

		if route == nil {
			writeJSON(w, http.StatusNotFound, `{"error":{"type":"not_found","reason":"no route configured"},"status":404}`)

			return
		}

		route(w, r, body)
	})

	fs.Server = httptest.NewServer(handler)
	t.Cleanup(fs.Close)

	return fs
}

// Requests returns a copy of the requests captured so far.
func (fs *fakeServer) Requests() []capturedRequest {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	out := make([]capturedRequest, len(fs.reqs))
	copy(out, fs.reqs)

	return out
}

// Last returns the most recently captured request, or the zero value if
// none was captured yet.
func (fs *fakeServer) Last() capturedRequest {
	reqs := fs.Requests()
	if len(reqs) == 0 {
		return capturedRequest{}
	}

	return reqs[len(reqs)-1]
}

// CountPath returns how many captured requests match method+path exactly.
func (fs *fakeServer) CountPath(method, path string) int {
	n := 0

	for _, r := range fs.Requests() {
		if r.Method == method && r.Path == path {
			n++
		}
	}

	return n
}

// writeJSON writes status and body. Headers (including the product header)
// must already be set by the caller before invoking this.
func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(json.RawMessage(body)); err != nil {
		return
	}
}

// infoRoute answers `GET /` and `HEAD /` with a valid info payload, and
// delegates everything else to next. Use it to compose a full route out of
// "New must succeed" plus test-specific behavior.
func infoRoute(next func(w http.ResponseWriter, r *http.Request, body []byte)) func(w http.ResponseWriter, r *http.Request, body []byte) {
	return func(w http.ResponseWriter, r *http.Request, body []byte) {
		if r.URL.Path == "/" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			writeJSON(w, http.StatusOK, esInfoBody)

			return
		}

		if next == nil {
			writeJSON(w, http.StatusNotFound, `{"error":{"type":"not_found","reason":"no route configured"},"status":404}`)

			return
		}

		next(w, r, body)
	}
}

//////
// In-memory document store, used to back full CRUD round-trips and
// hook-related tests without hand-writing a response for every scenario.
//////

// memStore is a tiny in-memory Elasticsearch emulator: index -> id ->
// source document. It implements just enough of the API surface the
// ElasticSearch adapter actually calls.
type memStore struct {
	mu        sync.Mutex
	docs      map[string]map[string]map[string]any
	rejectIDs map[string]int
}

// newMemStore returns an empty store.
func newMemStore() *memStore {
	return &memStore{
		docs:      map[string]map[string]map[string]any{},
		rejectIDs: map[string]int{},
	}
}

// route implements the fakeServer route signature, dispatching by
// method+path the same way a real ES node would.
func (m *memStore) route(w http.ResponseWriter, r *http.Request, body []byte) {
	if r.URL.Path == "/" && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		writeJSON(w, http.StatusOK, esInfoBody)

		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	switch {
	case r.Method == http.MethodPut && len(parts) == 1:
		writeJSON(w, http.StatusOK, `{"acknowledged":true}`)

	case r.Method == http.MethodDelete && len(parts) == 1:
		writeJSON(w, http.StatusOK, `{"acknowledged":true}`)

	case (r.Method == http.MethodPut || r.Method == http.MethodPost) && len(parts) == 3 && parts[1] == "_doc":
		m.indexDoc(w, parts[0], parts[2], body)

	case r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "_doc":
		// No document ID: ES would auto-generate one. Tests only assert on
		// the method/path used, so a fixed placeholder ID is fine here.
		m.indexDoc(w, parts[0], "auto-generated", body)

	case r.Method == http.MethodGet && len(parts) == 3 && parts[1] == "_doc":
		m.getDoc(w, parts[0], parts[2])

	case r.Method == http.MethodPost && len(parts) == 3 && parts[1] == "_update":
		m.updateDoc(w, parts[0], parts[2], body)

	case r.Method == http.MethodDelete && len(parts) == 3 && parts[1] == "_doc":
		m.deleteDoc(w, parts[0], parts[2])

	case r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "_search":
		m.search(w, parts[0])

	case r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "_bulk":
		m.bulk(w, parts[0], body)

	default:
		writeJSON(w, http.StatusNotFound, `{"error":{"type":"not_found","reason":"no route configured"},"status":404}`)
	}
}

func (m *memStore) bulk(w http.ResponseWriter, index string, body []byte) {
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	items := make([]map[string]any, 0, len(lines)/2)

	for lineIndex := 0; lineIndex+1 < len(lines); lineIndex += 2 {
		var metadata map[string]struct {
			ID string `json:"_id"`
		}
		var doc map[string]any

		if err := json.Unmarshal([]byte(lines[lineIndex]), &metadata); err != nil {
			writeJSON(w, http.StatusBadRequest, `{"error":{"type":"parse_exception","reason":"bad metadata"},"status":400}`)

			return
		}
		if err := json.Unmarshal([]byte(lines[lineIndex+1]), &doc); err != nil {
			writeJSON(w, http.StatusBadRequest, `{"error":{"type":"parse_exception","reason":"bad document"},"status":400}`)

			return
		}

		operation := metadata["index"]
		m.mu.Lock()
		status := m.rejectIDs[operation.ID]
		if status == 0 {
			status = http.StatusCreated
			if m.docs[index] == nil {
				m.docs[index] = map[string]map[string]any{}
			}
			m.docs[index][operation.ID] = doc
		}
		m.mu.Unlock()

		item := map[string]any{"status": status}
		if status < http.StatusOK || status >= http.StatusMultipleChoices {
			item["error"] = map[string]any{"type": "rejected_by_fake", "reason": "rejected " + operation.ID}
		}
		items = append(items, map[string]any{"index": item})
	}

	response, err := json.Marshal(map[string]any{"errors": true, "items": items})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, `{"error":{"type":"internal","reason":"marshal failed"},"status":500}`)

		return
	}
	writeJSON(w, http.StatusOK, string(response))
}

func (m *memStore) indexDoc(w http.ResponseWriter, index, id string, body []byte) {
	var doc map[string]any

	_ = json.Unmarshal(body, &doc)

	m.mu.Lock()
	if m.docs[index] == nil {
		m.docs[index] = map[string]map[string]any{}
	}

	m.docs[index][id] = doc
	m.mu.Unlock()

	writeJSON(w, http.StatusCreated, `{"result":"created"}`)
}

func (m *memStore) getDoc(w http.ResponseWriter, index, id string) {
	m.mu.Lock()
	doc, ok := m.docs[index][id]
	m.mu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, `{"_index":"`+index+`","_id":"`+id+`","found":false}`)

		return
	}

	srcJSON, err := json.Marshal(doc)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, `{"error":{"type":"internal","reason":"marshal failed"},"status":500}`)

		return
	}

	writeJSON(w, http.StatusOK, `{"_index":"`+index+`","_id":"`+id+`","found":true,"_source":`+string(srcJSON)+`}`)
}

func (m *memStore) updateDoc(w http.ResponseWriter, index, id string, body []byte) {
	var wrapper struct {
		Doc map[string]any `json:"doc"`
	}

	_ = json.Unmarshal(body, &wrapper)

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.docs[index][id]
	if !ok {
		writeJSON(w, http.StatusNotFound, `{"error":{"type":"document_missing_exception","reason":"document missing"},"status":404}`)

		return
	}

	maps.Copy(existing, wrapper.Doc)

	m.docs[index][id] = existing

	writeJSON(w, http.StatusOK, `{"result":"updated"}`)
}

func (m *memStore) deleteDoc(w http.ResponseWriter, index, id string) {
	m.mu.Lock()
	_, ok := m.docs[index][id]
	if ok {
		delete(m.docs[index], id)
	}
	m.mu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, `{"found":false}`)

		return
	}

	writeJSON(w, http.StatusOK, `{"result":"deleted"}`)
}

func (m *memStore) search(w http.ResponseWriter, index string) {
	m.mu.Lock()
	docs := m.docs[index]
	hits := make([]map[string]any, 0, len(docs))

	for _, d := range docs {
		hits = append(hits, map[string]any{"_source": d})
	}
	m.mu.Unlock()

	resp := map[string]any{
		"hits": map[string]any{
			"total": map[string]any{"value": len(hits), "relation": "eq"},
			"hits":  hits,
		},
	}

	b, err := json.Marshal(resp)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, `{"error":{"type":"internal","reason":"marshal failed"},"status":500}`)

		return
	}

	writeJSON(w, http.StatusOK, string(b))
}
