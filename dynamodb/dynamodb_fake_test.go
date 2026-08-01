package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	items map[string]map[string]map[string]types.AttributeValue

	// overrides forces a specific target (e.g. "Scan") to fail.
	overrides map[string]dynamoOverride

	listTablesFailures   int
	unprocessedRemaining map[string]int

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
		items:                map[string]map[string]map[string]types.AttributeValue{},
		overrides:            map[string]dynamoOverride{},
		unprocessedRemaining: map[string]int{},
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
		BaseEndpoint: aws.String(dynamoPlaceholderEndpoint),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
		HTTPClient:   client,
	}
}

//////
// State helpers.
//////

// putItem seeds an item directly, bypassing PutItem.
func (f *dynamoFake) putItem(table string, item map[string]types.AttributeValue) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := attrString(item["id"])

	if f.items[table] == nil {
		f.items[table] = map[string]map[string]types.AttributeValue{}
	}

	f.items[table][id] = item
}

// getItem reads an item directly, bypassing GetItem.
func (f *dynamoFake) getItem(table, id string) (map[string]types.AttributeValue, bool) {
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
func attrString(av types.AttributeValue) string {
	if value, ok := av.(*types.AttributeValueMemberS); ok {
		return value.Value
	}

	return ""
}

func stringAttribute(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
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

	if op == "ListTables" && f.listTablesFailures > 0 {
		f.listTablesFailures--
		writeDynamoError(w, http.StatusBadRequest, "RequestLimitExceeded", "simulated throttling")

		return
	}

	switch op {
	case "ListTables":
		f.handleListTables(w)
	case "GetItem":
		f.handleGetItem(w, body)
	case "PutItem":
		f.handlePutItem(w, body)
	case "BatchWriteItem":
		f.handleBatchWriteItem(w, body)
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

func (f *dynamoFake) handleBatchWriteItem(w http.ResponseWriter, body []byte) {
	var in struct {
		RequestItems map[string][]struct {
			PutRequest struct {
				Item map[string]json.RawMessage `json:"Item"`
			} `json:"PutRequest"`
		} `json:"RequestItems"`
	}

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	unprocessed := make(map[string][]map[string]any)
	for table, requests := range in.RequestItems {
		for _, request := range requests {
			item, err := decodeDynamoItem(request.PutRequest.Item)
			if err != nil {
				writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

				return
			}

			id := attrString(item["id"])
			if f.unprocessedRemaining[id] > 0 {
				f.unprocessedRemaining[id]--
				unprocessed[table] = append(unprocessed[table], map[string]any{
					"PutRequest": map[string]any{"Item": encodeDynamoItem(item)},
				})

				continue
			}

			if f.items[table] == nil {
				f.items[table] = map[string]map[string]types.AttributeValue{}
			}
			f.items[table][id] = item
		}
	}

	writeDynamoJSON(w, map[string]any{"UnprocessedItems": unprocessed})
}

func (f *dynamoFake) requestCount(target string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := 0
	for _, request := range f.requests {
		if request.target == target {
			count++
		}
	}

	return count
}

func (f *dynamoFake) handleListTables(w http.ResponseWriter) {
	writeDynamoJSON(w, map[string]any{"TableNames": []string{}})
}

func (f *dynamoFake) handleGetItem(w http.ResponseWriter, body []byte) {
	var in struct {
		TableName string                     `json:"TableName"`
		Key       map[string]json.RawMessage `json:"Key"`
	}

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	key, err := decodeDynamoItem(in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	id := attrString(key["id"])

	item := f.items[in.TableName][id]

	writeDynamoJSON(w, map[string]any{"Item": encodeDynamoItem(item)})
}

func (f *dynamoFake) handlePutItem(w http.ResponseWriter, body []byte) {
	var in struct {
		TableName string                     `json:"TableName"`
		Item      map[string]json.RawMessage `json:"Item"`
	}

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	item, err := decodeDynamoItem(in.Item)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	id := attrString(item["id"])

	if f.items[in.TableName] == nil {
		f.items[in.TableName] = map[string]map[string]types.AttributeValue{}
	}

	f.items[in.TableName][id] = item

	writeDynamoJSON(w, map[string]any{})
}

func (f *dynamoFake) handleDeleteItem(w http.ResponseWriter, body []byte) {
	var in struct {
		TableName string                     `json:"TableName"`
		Key       map[string]json.RawMessage `json:"Key"`
	}

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	key, err := decodeDynamoItem(in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	id := attrString(key["id"])

	delete(f.items[in.TableName], id)

	writeDynamoJSON(w, map[string]any{})
}

func (f *dynamoFake) handleUpdateItem(w http.ResponseWriter, body []byte) {
	var in struct {
		TableName                 string                     `json:"TableName"`
		Key                       map[string]json.RawMessage `json:"Key"`
		ExpressionAttributeNames  map[string]string          `json:"ExpressionAttributeNames"`
		ExpressionAttributeValues map[string]json.RawMessage `json:"ExpressionAttributeValues"`
	}

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	key, err := decodeDynamoItem(in.Key)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	values, err := decodeDynamoItem(in.ExpressionAttributeValues)
	if err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	id := attrString(key["id"])

	if f.items[in.TableName] == nil {
		f.items[in.TableName] = map[string]map[string]types.AttributeValue{}
	}

	item, ok := f.items[in.TableName][id]
	if !ok {
		item = map[string]types.AttributeValue{"id": key["id"]}
	}

	// Apply "SET #placeholder = :placeholder, ..." naively: for every
	// expression attribute name, look up the value sharing its placeholder
	// suffix and assign it under the real attribute name. Good enough for a
	// fake exercising the caller's code, not DynamoDB's expression engine.
	for namePlaceholder, realName := range in.ExpressionAttributeNames {
		suffix := strings.TrimPrefix(namePlaceholder, "#")
		if av, ok := values[":"+suffix]; ok {
			item[realName] = av
		}
	}

	f.items[in.TableName][id] = item

	writeDynamoJSON(w, map[string]any{})
}

func (f *dynamoFake) handleScan(w http.ResponseWriter, body []byte) {
	if len(f.scanQueue) > 0 {
		idx := f.scanCall
		if idx >= len(f.scanQueue) {
			idx = len(f.scanQueue) - 1
		}

		f.scanCall++

		writeDynamoScanOutput(w, f.scanQueue[idx])

		return
	}

	var in struct {
		TableName string       `json:"TableName"`
		Select    types.Select `json:"Select"`
	}

	if err := json.Unmarshal(body, &in); err != nil {
		writeDynamoError(w, http.StatusBadRequest, "SerializationException", err.Error())

		return
	}

	items := make([]map[string]types.AttributeValue, 0, len(f.items[in.TableName]))
	for _, item := range f.items[in.TableName] {
		items = append(items, item)
	}

	if len(items) > math.MaxInt32 {
		panic("fake DynamoDB item count exceeds int32")
	}

	// The upper bound is checked immediately above.
	out := dynamodb.ScanOutput{Count: int32(len(items))} //nolint:gosec

	// Select=COUNT responses omit Items entirely, matching real DynamoDB
	// (this is exactly the behavior the fixed prod bug depended on).
	if in.Select != types.SelectCount {
		out.Items = items
	}

	writeDynamoScanOutput(w, out)
}

func decodeDynamoItem(item map[string]json.RawMessage) (map[string]types.AttributeValue, error) {
	decoded := make(map[string]types.AttributeValue, len(item))

	for name, raw := range item {
		value, err := decodeDynamoAttribute(raw)
		if err != nil {
			return nil, err
		}

		decoded[name] = value
	}

	return decoded, nil
}

func decodeDynamoAttribute(raw json.RawMessage) (types.AttributeValue, error) {
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}

	for kind, encoded := range value {
		switch kind {
		case "S":
			var v string
			if err := json.Unmarshal(encoded, &v); err != nil {
				return nil, err
			}

			return stringAttribute(v), nil
		case "N":
			var v string
			if err := json.Unmarshal(encoded, &v); err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberN{Value: v}, nil
		case "BOOL":
			var v bool
			if err := json.Unmarshal(encoded, &v); err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberBOOL{Value: v}, nil
		case "NULL":
			var v bool
			if err := json.Unmarshal(encoded, &v); err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberNULL{Value: v}, nil
		case "B":
			var v string
			if err := json.Unmarshal(encoded, &v); err != nil {
				return nil, err
			}

			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberB{Value: decoded}, nil
		case "M":
			var v map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &v); err != nil {
				return nil, err
			}

			decoded, err := decodeDynamoItem(v)
			if err != nil {
				return nil, err
			}

			return &types.AttributeValueMemberM{Value: decoded}, nil
		case "L":
			var v []json.RawMessage
			if err := json.Unmarshal(encoded, &v); err != nil {
				return nil, err
			}

			decoded := make([]types.AttributeValue, 0, len(v))
			for _, element := range v {
				attribute, err := decodeDynamoAttribute(element)
				if err != nil {
					return nil, err
				}

				decoded = append(decoded, attribute)
			}

			return &types.AttributeValueMemberL{Value: decoded}, nil
		}
	}

	return nil, &json.UnsupportedValueError{Str: "unsupported DynamoDB attribute value"}
}

func encodeDynamoItem(item map[string]types.AttributeValue) map[string]any {
	encoded := make(map[string]any, len(item))
	for name, value := range item {
		encoded[name] = encodeDynamoAttribute(value)
	}

	return encoded
}

func encodeDynamoAttribute(value types.AttributeValue) map[string]any {
	switch value := value.(type) {
	case *types.AttributeValueMemberS:
		return map[string]any{"S": value.Value}
	case *types.AttributeValueMemberN:
		return map[string]any{"N": value.Value}
	case *types.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": value.Value}
	case *types.AttributeValueMemberNULL:
		return map[string]any{"NULL": value.Value}
	case *types.AttributeValueMemberB:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(value.Value)}
	case *types.AttributeValueMemberM:
		return map[string]any{"M": encodeDynamoItem(value.Value)}
	case *types.AttributeValueMemberL:
		encoded := make([]map[string]any, 0, len(value.Value))
		for _, element := range value.Value {
			encoded = append(encoded, encodeDynamoAttribute(element))
		}

		return map[string]any{"L": encoded}
	default:
		return map[string]any{}
	}
}

func writeDynamoScanOutput(w http.ResponseWriter, output dynamodb.ScanOutput) {
	items := make([]map[string]any, 0, len(output.Items))
	for _, item := range output.Items {
		items = append(items, encodeDynamoItem(item))
	}

	response := map[string]any{
		"Count": output.Count,
		"Items": items,
	}
	if len(output.LastEvaluatedKey) > 0 {
		response["LastEvaluatedKey"] = encodeDynamoItem(output.LastEvaluatedKey)
	}

	writeDynamoJSON(w, response)
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
