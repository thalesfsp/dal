package mongodb

// This file provides an in-memory, offline mock of the MongoDB wire protocol
// so the adapter can be exercised without Docker or network access. Driver v2
// exports the mock deployment used by its own tests, so this wrapper only adds
// disconnect tracking and the response builders needed by this package.

import (
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

const (
	mockFirstBatch = "firstBatch"
	mockNextBatch  = "nextBatch"
)

type mockCommandError struct {
	Code    int32
	Message string
	Name    string
}

type mockWriteError struct {
	Index   int
	Code    int
	Message string
}

// mockDeployment wraps the v2 driver's exported mock and records whether the
// adapter disconnected it after a constructor failure.
type mockDeployment struct {
	*drivertest.MockDeployment

	mu           sync.Mutex
	disconnected bool

	// disconnectErr, when set, is returned instead of delegating to the driver
	// mock — so a test can exercise Close's error path without a real failure.
	disconnectErr error
}

// Disconnect records the call and delegates to the driver mock exactly once.
func (md *mockDeployment) Disconnect(ctx context.Context) error {
	md.mu.Lock()
	defer md.mu.Unlock()

	if md.disconnected {
		return nil
	}

	md.disconnected = true

	// The call is recorded either way, so Disconnected() stays truthful even on
	// the injected-failure path.
	if md.disconnectErr != nil {
		return md.disconnectErr
	}

	return md.MockDeployment.Disconnect(ctx)
}

// Disconnected reports whether Disconnect was called.
func (md *mockDeployment) Disconnected() bool {
	md.mu.Lock()
	defer md.mu.Unlock()

	return md.disconnected
}

// addResponses queues responses to be returned, in order, one per command.
func (md *mockDeployment) addResponses(responses ...bson.D) {
	md.AddResponses(responses...)
}

// newMockDeployment returns a v2 mock driver deployment.
func newMockDeployment(responses ...bson.D) *mockDeployment {
	return &mockDeployment{MockDeployment: drivertest.NewMockDeployment(responses...)}
}

func mockCreateCursorResponse(cursorID int64, identifier string, batch ...bson.D) bson.D {
	batchArray := make(bson.A, 0, len(batch))
	for _, document := range batch {
		batchArray = append(batchArray, document)
	}

	return bson.D{
		{Key: "ok", Value: 1},
		{Key: "cursor", Value: bson.D{
			{Key: "id", Value: cursorID},
			{Key: "ns", Value: testNS},
			{Key: identifier, Value: batchArray},
		}},
	}
}

func mockCreateCommandErrorResponse(commandError mockCommandError) bson.D {
	return bson.D{
		{Key: "ok", Value: 0},
		{Key: "code", Value: commandError.Code},
		{Key: "errmsg", Value: commandError.Message},
		{Key: "codeName", Value: commandError.Name},
	}
}

func mockCreateWriteErrorsResponse(writeErrors ...mockWriteError) bson.D {
	errorsArray := make(bson.A, len(writeErrors))
	for index, writeError := range writeErrors {
		errorsArray[index] = bson.D{
			{Key: "index", Value: writeError.Index},
			{Key: "code", Value: writeError.Code},
			{Key: "errmsg", Value: writeError.Message},
		}
	}

	return bson.D{
		{Key: "ok", Value: 1},
		{Key: "writeErrors", Value: errorsArray},
	}
}

func mockCreateSuccessResponse(elements ...bson.E) bson.D {
	return append(bson.D{{Key: "ok", Value: 1}}, elements...)
}
