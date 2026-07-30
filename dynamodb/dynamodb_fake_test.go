package dynamodb

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

//////
// A minimal, offline, in-memory fake of the DynamoDB JSON RPC API. Every
// operation the dynamodb package's methods exercise (ListTables, GetItem,
// PutItem, DeleteItem, UpdateItem, Scan) is dispatched off the
// X-Amz-Target header, matching the real service's single-endpoint,
// JSON-RPC-style protocol.
//////

// dynamoOverride forces an AWS-shaped error for one target.
type dynamoOverride struct {
	status  int
	errType string // e.g. "ResourceNotFoundException" (goes in "__type").
	message string
}

// dynamoRequest records one inbound call for assertions (e.g. that a Scan
// carried Select:"COUNT", or that ExpressionAttributeNames sanitizes unsafe
// attribute names).
type dynamoRequest struct {
	target string
	body   []byte
}

// dynamoFake is a stateful, concurrency-safe fake DynamoDB server.
type dynamoFake struct {
	mu sync.Mutex

	// items is keyed by table name, then by the "id" attribute's string
	// value.
	items map[string]map[string]map[string]*dynamodb.AttributeValue

	// overrides forces a specific target (e.g. "Scan") to fail.
	overrides map[string]dynamoOverride

	// scanQueue, when non-empty, overrides Scan's response: each call pops
	// the next entry (the last one repeats once exhausted), ignoring items.
	// Used to test Count/List pagination deterministically.
	scanQueue []dynamodb.ScanOutput
	scanCall  int

	requests []dynamoRequest
}

// newDynamoFake creates a ready-to-use fake with no stored items.
func newDynamoFake() *dynamoFake {
	return &dynamoFake{
		items:     map[string]map[string]map[string]*dynamodb.AttributeValue{},
		overrides: map[string]dynamoOverride{},
	}
}

//////
// Transport (in-process; see s3's localTransport for why: it avoids a class
// of net/http-internal false positives under -race that showed up when many
// short-lived real httptest.Server instances were started/torn down in
// quick succession within the same test binary).
//////

type dynamoLocalTransport struct {
	handler http.Handler
}

func (rt *dynamoLocalTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()

	rt.handler.ServeHTTP(rec, req)

	resp := rec.Result()
	resp.Request = req

	return resp, nil
}

// client returns an http.Client that routes every request straight to the
// fake's handler in-process.
func (f *dynamoFake) client() *http.Client {
	return &http.Client{Transport: &dynamoLocalTransport{handler: http.HandlerFunc(f.handle)}}
}

// dynamoTestConfig points the AWS SDK at dynamoPlaceholderEndpoint using
// client. Every test in this package routes through the in-process
// transport (no real socket, so there's no dialable endpoint to vary here) -
// unlike s3, DynamoDB has no presigned-URL-style feature that would ever
// need a genuinely reachable one.
func dynamoTestConfig(client *http.Client) *Config {
	return &aws.Config{
		Endpoint:    aws.String(dynamoPlaceholderEndpoint),
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials("k", "s", ""),
		DisableSSL:  aws.Bool(true),
		HTTPClient:  client,
	}
}

//////
// State helpers.
//////

// putItem seeds an item directly, bypassing PutItem.
func (f *dynamoFake) putItem(table string, item map[string]*dynamodb.AttributeValue) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := attrString(item["id"])

	if f.items[table] == nil {
		f.items[table] = map[string]map[string]*dynamodb.AttributeValue{}
	}

	f.items[table][id] = item
}

// getItem reads an item directly, bypassing GetItem.
func (f *dynamoFake) getItem(table, id string) (map[string]*dynamodb.AttributeValue, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	item, ok := f.items[table][id]

	return item, ok
}

// setOverride forces the given target to fail with status/errType.
func (f *dynamoFake) setOverride(target string, status int, errType string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.overrides[target] = dynamoOverride{status: status, errType: errType, message: "boom"}
}

// clearOverride removes a target's forced failure.
func (f *dynamoFake) clearOverride(target string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.overrides, target)
}

// setScanQueue overrides Scan's responses (see scanQueue) and returns a
// restore func.
func (f *dynamoFake) setScanQueue(pages []dynamodb.ScanOutput) func() {
	f.mu.Lock()
	prevQueue := f.scanQueue
	prevCall := f.scanCall
	f.scanQueue = pages
	f.scanCall = 0
	f.mu.Unlock()

	return func() {
		f.mu.Lock()
		f.scanQueue = prevQueue
		f.scanCall = prevCall
		f.mu.Unlock()
	}
}

// scanRequests returns the captured Scan request bodies, in call order.
func (f *dynamoFake) scanRequests() []dynamoRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []dynamoRequest

	for _, r := range f.requests {
		if r.target == "Scan" {
			out = append(out, r)
		}
	}

	return out
}

// attrString extracts the string value of a DynamoDB S attribute; empty if
// av is nil or not an S.
func attrString(av *dynamodb.AttributeValue) string {
	if av == nil || av.S == nil {
		return ""
	}

	return *av.S
}

//////
// Dispatcher.
//////

func (f *dynamoFake) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	target := r.Header.Get("X-Amz-Target")

	idx := strings.LastIndex(target, ".")
	op := target

	if idx >= 0 {
		op = target[idx+1:]
	}

	body, _ := io.ReadAll(r.Body)

	f.requests = append(f.requests, dynamoRequest{target: op, body: body})

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")

	if ov, ok := f.overrides[op]; ok {
		writeDynamoError(w, ov.status, ov.errType, ov.message)

		return
	}

	switch op {
	case "ListTables":
		f.handleListTables(w)
	case "GetItem":
		f.handleGetItem(w, body)
	case "PutItem":
		f.handlePutItem(w, body)
	case "DeleteItem":
		f.handleDeleteItem(w, body)
	case "UpdateItem":
		f.handleUpdateItem(w, body)
	case "Scan":
		f.handleScan(w, body)
	default:
		writeDynamoError(w, http.StatusBadRequest, "UnknownOperationException", "operation not implemented by fake: "+op)
	}
}

func (f *dynamoFake) handleListTables(w http.ResponseWriter) {
	writeDynamoJSON(w, dynamodb.ListTablesOutput{TableNames: []*string{}})
}

func (f *dynamoFake) handleGetItem(w http.ResponseWriter, body []byte) {
	var in dynamodb.GetItemInput

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	table := aws.StringValue(in.TableName)
	id := attrString(in.Key["id"])

	item := f.items[table][id]

	writeDynamoJSON(w, dynamodb.GetItemOutput{Item: item})
}

func (f *dynamoFake) handlePutItem(w http.ResponseWriter, body []byte) {
	var in dynamodb.PutItemInput

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	table := aws.StringValue(in.TableName)
	id := attrString(in.Item["id"])

	if f.items[table] == nil {
		f.items[table] = map[string]map[string]*dynamodb.AttributeValue{}
	}

	f.items[table][id] = in.Item

	writeDynamoJSON(w, dynamodb.PutItemOutput{})
}

func (f *dynamoFake) handleDeleteItem(w http.ResponseWriter, body []byte) {
	var in dynamodb.DeleteItemInput

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	table := aws.StringValue(in.TableName)
	id := attrString(in.Key["id"])

	delete(f.items[table], id)

	writeDynamoJSON(w, dynamodb.DeleteItemOutput{})
}

func (f *dynamoFake) handleUpdateItem(w http.ResponseWriter, body []byte) {
	var in dynamodb.UpdateItemInput

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	table := aws.StringValue(in.TableName)
	id := attrString(in.Key["id"])

	if f.items[table] == nil {
		f.items[table] = map[string]map[string]*dynamodb.AttributeValue{}
	}

	item, ok := f.items[table][id]
	if !ok {
		item = map[string]*dynamodb.AttributeValue{"id": in.Key["id"]}
	}

	// Apply "SET #placeholder = :placeholder, ..." naively: for every
	// expression attribute name, look up the value sharing its placeholder
	// suffix and assign it under the real attribute name. Good enough for a
	// fake exercising the caller's code, not DynamoDB's expression engine.
	for namePlaceholder, realName := range in.ExpressionAttributeNames {
		suffix := strings.TrimPrefix(namePlaceholder, "#")
		if av, ok := in.ExpressionAttributeValues[":"+suffix]; ok {
			item[aws.StringValue(realName)] = av
		}
	}

	f.items[table][id] = item

	writeDynamoJSON(w, dynamodb.UpdateItemOutput{})
}

func (f *dynamoFake) handleScan(w http.ResponseWriter, body []byte) {
	if len(f.scanQueue) > 0 {
		idx := f.scanCall
		if idx >= len(f.scanQueue) {
			idx = len(f.scanQueue) - 1
		}

		f.scanCall++

		writeDynamoJSON(w, f.scanQueue[idx])

		return
	}

	var in dynamodb.ScanInput

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	table := aws.StringValue(in.TableName)

	items := make([]map[string]*dynamodb.AttributeValue, 0, len(f.items[table]))
	for _, item := range f.items[table] {
		items = append(items, item)
	}

	count := int64(len(items))

	out := dynamodb.ScanOutput{Count: &count}

	// Select=COUNT responses omit Items entirely, matching real DynamoDB
	// (this is exactly the behavior the fixed prod bug depended on).
	if aws.StringValue(in.Select) != dynamodb.SelectCount {
		out.Items = items
	}

	writeDynamoJSON(w, out)
}

//////
// Response writers.
//////

func writeDynamoJSON(w http.ResponseWriter, v any) {
	w.WriteHeader(http.StatusOK)

	// A ResponseRecorder-backed http.ResponseWriter (see dynamoLocalTransport)
	// never fails to buffer a write, and headers are already sent - there's
	// nothing a caller could do differently with this error.
	//nolint:errchkjson // v is always one of this file's own dynamodb.*Output structs, never user input.
	_ = json.NewEncoder(w).Encode(v)
}

func writeDynamoError(w http.ResponseWriter, status int, errType, message string) {
	w.WriteHeader(status)

	//nolint:errchkjson // the payload is two plain strings we build ourselves.
	_ = json.NewEncoder(w).Encode(map[string]string{
		"__type":  "com.amazonaws.dynamodb#" + errType,
		"message": message,
	})
}
