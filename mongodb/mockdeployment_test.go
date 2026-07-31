package mongodb

// This file provides an in-memory, offline mock of the MongoDB wire protocol
// so the adapter can be exercised without Docker or network access.
//
// It is a minimal adaptation of the (unexported) mock deployment used by the
// official driver's own test harness
// (go.mongodb.org/mongo-driver/mongo/integration/mtest, opmsg_deployment.go),
// rebuilt here on top of exported driver packages only. The mtest harness
// itself cannot be used because mtest.New unconditionally skips tests when
// `go test -short` is set — and this repo's offline test command uses -short.
// The mtest RESPONSE BUILDERS (mtest.CreateCursorResponse, etc.) are plain
// functions with no environment requirements, so tests still use those to
// build the queued replies.
//
// How it works: options.ClientOptions has an exported Deployment field. When
// set, the driver skips real topology discovery/handshakes and sends every
// command through the provided driver.Deployment. mockDeployment hands out a
// single mockConnection whose WriteWireMessage is a no-op and whose
// ReadWireMessage pops the next queued bson.D document and wraps it in an
// OP_MSG reply — exactly what a real server would return.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

const (
	mockServerAddress            = address.Address("127.0.0.1:27017")
	mockMaxDocumentSize   uint32 = 16777216
	mockMaxMessageSize    uint32 = 48000000
	mockMaxBatchCount     uint32 = 100000
	mockSessionTimeoutMin uint32 = 30
)

// errNoMockResponses is returned when a command is issued but no canned
// response was queued for it.
var errNoMockResponses = errors.New("mock connection: no responses remaining")

// mockSessionTimeoutMinInt64 mirrors mockSessionTimeoutMin for the fields that
// require a *int64.
var mockSessionTimeoutMinInt64 = int64(mockSessionTimeoutMin)

// mockServerDescription advertises a modern, sessions-capable standalone
// primary so the driver considers every feature the adapter uses supported.
var mockServerDescription = description.Server{
	CanonicalAddr:            mockServerAddress,
	MaxDocumentSize:          mockMaxDocumentSize,
	MaxMessageSize:           mockMaxMessageSize,
	MaxBatchCount:            mockMaxBatchCount,
	SessionTimeoutMinutes:    mockSessionTimeoutMin,
	SessionTimeoutMinutesPtr: &mockSessionTimeoutMinInt64,
	Kind:                     description.RSPrimary,
	WireVersion: &description.VersionRange{
		Max: topology.SupportedWireVersions.Max,
	},
}

// mockConnection implements driver.Connection, replying to every command with
// the next queued document encoded as an OP_MSG.
type mockConnection struct {
	mu        sync.Mutex
	responses []bson.D
}

var _ driver.Connection = (*mockConnection)(nil)

// WriteWireMessage discards the outgoing command.
func (c *mockConnection) WriteWireMessage(context.Context, []byte) error {
	return nil
}

// ReadWireMessage pops the next queued response and encodes it as an OP_MSG.
func (c *mockConnection) ReadWireMessage(context.Context) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.responses) == 0 {
		return nil, errNoMockResponses
	}

	next := c.responses[0]
	c.responses = c.responses[1:]

	var (
		wmindex int32
		dst     []byte
	)

	wmindex, dst = wiremessage.AppendHeaderStart(dst, wiremessage.NextRequestID(), 0, wiremessage.OpMsg)
	dst = wiremessage.AppendMsgFlags(dst, 0)
	dst = wiremessage.AppendMsgSectionType(dst, wiremessage.SingleDocument)

	resBytes, err := bson.Marshal(next)
	if err != nil {
		return nil, err
	}

	dst = append(dst, resBytes...)

	// Wire-message lengths are int32 on the wire. Test responses are a few
	// hundred bytes, so this can't overflow, but assert rather than assume.
	length := len(dst[wmindex:])
	if length > math.MaxInt32 {
		return nil, fmt.Errorf("mock response too large: %d bytes", length)
	}

	dst = bsoncore.UpdateLength(dst, wmindex, int32(length))

	return dst, nil
}

// Description returns the fixed mock server description.
func (c *mockConnection) Description() description.Server { return mockServerDescription }

// Close is a no-op.
func (*mockConnection) Close() error { return nil }

// ID returns a fixed identifier.
func (*mockConnection) ID() string { return "<mock_connection>" }

// DriverConnectionID returns a fixed pool identifier.
func (*mockConnection) DriverConnectionID() uint64 { return 0 }

// ServerConnectionID returns a fixed server-side identifier.
func (*mockConnection) ServerConnectionID() *int64 {
	id := int64(42)

	return &id
}

// Address returns the fixed mock address.
func (*mockConnection) Address() address.Address { return mockServerAddress }

// Stale reports the connection as always fresh.
func (*mockConnection) Stale() bool { return false }

// OIDCTokenGenID returns a fixed OIDC token generation ID.
func (*mockConnection) OIDCTokenGenID() uint64 { return 0 }

// SetOIDCTokenGenID is a no-op.
func (*mockConnection) SetOIDCTokenGenID(uint64) {}

// zeroRTTMonitor is a driver.RTTMonitor that always reports zero round-trip
// times (stand-in for the driver-internal csot.ZeroRTTMonitor).
type zeroRTTMonitor struct{}

var _ driver.RTTMonitor = (*zeroRTTMonitor)(nil)

// EWMA reports a zero moving-average RTT.
func (*zeroRTTMonitor) EWMA() time.Duration { return 0 }

// Min reports a zero minimum RTT.
func (*zeroRTTMonitor) Min() time.Duration { return 0 }

// P90 reports a zero 90th-percentile RTT.
func (*zeroRTTMonitor) P90() time.Duration { return 0 }

// Stats reports empty monitor stats.
func (*zeroRTTMonitor) Stats() string { return "" }

// mockDeployment implements driver.Deployment (plus the optional Connector,
// Disconnector, and Subscriber interfaces the client checks for) backed by a
// single mockConnection.
type mockDeployment struct {
	conn *mockConnection

	mu            sync.Mutex
	updates       chan description.Topology
	disconnected  bool
	disconnectErr error
}

var (
	_ driver.Deployment   = (*mockDeployment)(nil)
	_ driver.Server       = (*mockDeployment)(nil)
	_ driver.Connector    = (*mockDeployment)(nil)
	_ driver.Disconnector = (*mockDeployment)(nil)
	_ driver.Subscriber   = (*mockDeployment)(nil)
)

// SelectServer returns the deployment itself, which doubles as the only
// server.
func (md *mockDeployment) SelectServer(context.Context, description.ServerSelector) (driver.Server, error) {
	return md, nil
}

// Kind reports a single-server topology.
func (md *mockDeployment) Kind() description.TopologyKind { return description.Single }

// Connection hands out the shared mock connection.
func (md *mockDeployment) Connection(context.Context) (driver.Connection, error) {
	return md.conn, nil
}

// RTTMonitor returns a zero-valued RTT monitor.
func (md *mockDeployment) RTTMonitor() driver.RTTMonitor { return &zeroRTTMonitor{} }

// Connect is a no-op.
func (md *mockDeployment) Connect() error { return nil }

// Disconnect closes the topology updates channel exactly once.
func (md *mockDeployment) Disconnect(context.Context) error {
	md.mu.Lock()
	defer md.mu.Unlock()

	if !md.disconnected {
		md.disconnected = true

		if md.updates != nil {
			close(md.updates)
		}
	}

	return md.disconnectErr
}

// Disconnected reports whether Disconnect was called - used by tests to assert
// the constructor cleans up the client it created when it fails mid-way.
func (md *mockDeployment) Disconnected() bool {
	md.mu.Lock()
	defer md.mu.Unlock()

	return md.disconnected
}

// Subscribe returns a subscription seeded with the mock topology description.
func (md *mockDeployment) Subscribe() (*driver.Subscription, error) {
	md.mu.Lock()
	defer md.mu.Unlock()

	if md.updates == nil {
		md.updates = make(chan description.Topology, 1)

		md.updates <- description.Topology{
			SessionTimeoutMinutes:    mockSessionTimeoutMin,
			SessionTimeoutMinutesPtr: &mockSessionTimeoutMinInt64,
		}
	}

	return &driver.Subscription{Updates: md.updates}, nil
}

// Unsubscribe is a no-op.
func (md *mockDeployment) Unsubscribe(*driver.Subscription) error { return nil }

// addResponses queues responses to be returned, in order, one per command.
func (md *mockDeployment) addResponses(responses ...bson.D) {
	md.conn.mu.Lock()
	defer md.conn.mu.Unlock()

	md.conn.responses = append(md.conn.responses, responses...)
}

// newMockDeployment returns a mock driver.Deployment that responds to each
// command with the next queued OP_MSG response.
func newMockDeployment(responses ...bson.D) *mockDeployment {
	return &mockDeployment{conn: &mockConnection{responses: responses}}
}
