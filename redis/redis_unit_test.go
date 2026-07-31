package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

//////
// Offline (no ENVIRONMENT gating) tests exercising the real Redis adapter
// against an in-process fake RESP server. See fakeserver_test.go.
//////

// newOfflineStorage starts a fake server and connects the adapter to it.
func newOfflineStorage(t *testing.T) (*fakeRedis, *Redis) {
	t.Helper()

	f := startFakeRedis(t)

	str, err := New(t.Context(), &Config{Addr: f.Addr()})
	require.NoError(t, err)
	require.NotNil(t, str)

	return f, str
}

type closeErrorConn struct {
	net.Conn

	err error
}

func (c *closeErrorConn) Close() error {
	_ = c.Conn.Close()

	return c.err
}

// errHookBoom is returned by failing test hooks.
var errHookBoom = errors.New("hook boom")

func TestRedisOffline_Close(t *testing.T) {
	t.Run("closes client and is idempotent", func(t *testing.T) {
		_, str := newOfflineStorage(t)

		require.NoError(t, str.Close())
		require.NoError(t, str.Close())

		_, err := str.Client.Ping(t.Context()).Result()
		require.Error(t, err, "operation after Close must fail because client is closed")
	})

	t.Run("concurrent calls", func(t *testing.T) {
		_, str := newOfflineStorage(t)

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
	})

	t.Run("propagates client close error once", func(t *testing.T) {
		f := startFakeRedis(t)
		closeErr := errors.New("redis connection close failed")
		dialer := &net.Dialer{}
		cfg := &Config{
			Addr: f.Addr(),
			Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}

				return &closeErrorConn{Conn: conn, err: closeErr}, nil
			},
		}

		str, err := New(t.Context(), cfg)
		require.NoError(t, err)

		require.ErrorIs(t, str.Close(), closeErr)
		require.NoError(t, str.Close())
	})
}

// Happy path: full CRUD round-trip, plus Count/List with glob patterns.
func TestRedisOffline_CRUDRoundTrip(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	createdBefore := str.GetCounterCreated().Value()

	id, err := str.Create(ctx, shared.DocumentID, "", shared.TestDataWithID, &create.Create{})
	require.NoError(t, err)
	assert.Equal(t, shared.DocumentID, id)
	assert.Equal(t, createdBefore+1, str.GetCounterCreated().Value())

	var got shared.TestDataWithIDS

	require.NoError(t, str.Retrieve(ctx, id, "", &got, &retrieve.Retrieve{}))
	assert.Equal(t, *shared.TestDataWithID, got)

	require.NoError(t, str.Update(ctx, id, "", shared.UpdatedTestDataID, &update.Update{}))

	var updated shared.TestDataWithIDS

	require.NoError(t, str.Retrieve(ctx, id, "", &updated, &retrieve.Retrieve{}))
	assert.Equal(t, shared.DocumentNameUpdated, updated.Name)

	// Count: explicit glob, empty search (falls back to "*"), and no match.
	c, err := str.Count(ctx, "", &count.Count{Search: "VF*"})
	require.NoError(t, err)
	assert.EqualValues(t, 1, c)

	c, err = str.Count(ctx, "", &count.Count{Search: ""})
	require.NoError(t, err)
	assert.EqualValues(t, 1, c)

	c, err = str.Count(ctx, "", &count.Count{Search: "nomatch*"})
	require.NoError(t, err)
	assert.EqualValues(t, 0, c)

	// List: glob match, and no match.
	var keys ResponseListKeys

	require.NoError(t, str.List(ctx, "", &keys, &list.List{Search: "VF*"}))
	assert.Equal(t, []string{shared.DocumentID}, keys.Keys)

	var none ResponseListKeys

	require.NoError(t, str.List(ctx, "", &none, &list.List{Search: "zzz*"}))
	assert.Empty(t, none.Keys)

	// Delete, then the document must be gone.
	require.NoError(t, str.Delete(ctx, id, "", &delete.Delete{}))

	err = str.Retrieve(ctx, id, "", &got, &retrieve.Retrieve{})
	assert.Error(t, err, "retrieving a deleted document must fail")

	c, err = str.Count(ctx, "", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 0, c)
}

// Nil params are legal for every operation ("use defaults").
func TestRedisOffline_NilParams(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	id, err := str.Create(ctx, "nil-prm", "", shared.TestData, nil)
	require.NoError(t, err)
	assert.Equal(t, "nil-prm", id)

	var got shared.TestDataS

	require.NoError(t, str.Retrieve(ctx, "nil-prm", "", &got, nil))
	assert.Equal(t, *shared.TestData, got)

	c, err := str.Count(ctx, "", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, c)

	var keys ResponseListKeys

	require.NoError(t, str.List(ctx, "", &keys, nil))
	assert.Equal(t, []string{"nil-prm"}, keys.Keys)

	require.NoError(t, str.Update(ctx, "nil-prm", "", shared.UpdatedTestData, nil))
	require.NoError(t, str.Delete(ctx, "nil-prm", "", nil))
}

// Empty ids are rejected by Create, Retrieve, Update and Delete.
func TestRedisOffline_EmptyID(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	failedBefore := str.GetCounterCreatedFailed().Value()

	id, err := str.Create(ctx, "", "", shared.TestData, nil)
	assert.Error(t, err)
	assert.Empty(t, id)
	assert.Equal(t, failedBefore+1, str.GetCounterCreatedFailed().Value())

	var got shared.TestDataS

	assert.Error(t, str.Retrieve(ctx, "", "", &got, nil))
	assert.Error(t, str.Update(ctx, "", "", shared.TestData, nil))
	assert.Error(t, str.Delete(ctx, "", "", nil))
}

// Retrieve edge cases: missing key (404), empty payload (skips unmarshal) and
// a corrupt payload (unmarshal error).
func TestRedisOffline_RetrieveEdgeCases(t *testing.T) {
	ctx := t.Context()

	f, str := newOfflineStorage(t)

	f.seed(map[string]string{
		"empty-doc": "",
		"bad-doc":   "{not-json",
	})

	var got shared.TestDataS

	assert.Error(t, str.Retrieve(ctx, shared.DocumentIDInvalid, "", &got, nil), "missing key must fail")

	require.NoError(t, str.Retrieve(ctx, "empty-doc", "", &got, nil), "empty payload must not fail")
	assert.Empty(t, got.Name)

	assert.Error(t, str.Retrieve(ctx, "bad-doc", "", &got, nil), "corrupt payload must fail")
}

// Unmarshalable documents make Create and Update fail before hitting Redis.
func TestRedisOffline_MarshalErrors(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	unmarshalable := make(chan int)

	id, err := str.Create(ctx, "marshal-err", "", unmarshalable, nil)
	assert.Error(t, err)
	assert.Empty(t, id)

	assert.Error(t, str.Update(ctx, "marshal-err", "", unmarshalable, nil))
}

// List must fail when the destination cannot hold the response.
func TestRedisOffline_ListParseError(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	var notAPointer string

	assert.Error(t, str.List(ctx, "", notAPointer, nil))
}

// SCAN pagination: enough keys that the iterator walks multiple cursor pages.
func TestRedisOffline_ScanPagination(t *testing.T) {
	ctx := t.Context()

	f, str := newOfflineStorage(t)

	const total = 1050 // pageSize is 100, so 11 SCAN pages.

	kv := make(map[string]string, total)

	for i := range total {
		kv[fmt.Sprintf("bulk:%04d", i)] = `{"name":"test"}`
	}

	f.seed(kv)

	c, err := str.Count(ctx, "", &count.Count{Search: "bulk:*"})
	require.NoError(t, err)
	assert.EqualValues(t, total, c)

	var keys ResponseListKeys

	require.NoError(t, str.List(ctx, "", &keys, &list.List{Search: "bulk:*"}))
	assert.Len(t, keys.Keys, total)
}

// Options and hooks: invalid option, failing pre-hook, failing post-hook and
// successful hooks, for every operation.
func TestRedisOffline_Hooks(t *testing.T) {
	ctx := t.Context()

	_, str := newOfflineStorage(t)

	_, err := str.Create(ctx, "hook-doc", "", shared.TestData, nil)
	require.NoError(t, err)

	t.Run("create", func(t *testing.T) {
		_, err := str.Create(ctx, "hook-doc", "", shared.TestData, nil, storage.WithPreHook[*create.Create](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		_, err = str.Create(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		_, err = str.Create(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPostHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		_, err = str.Create(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*create.Create](func(context.Context, storage.IStorage, string, string, any, *create.Create) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("retrieve", func(t *testing.T) {
		var got shared.TestDataS

		err := str.Retrieve(ctx, "hook-doc", "", &got, nil, storage.WithPreHook[*retrieve.Retrieve](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.Retrieve(ctx, "hook-doc", "", &got, nil,
			storage.WithPreHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.Retrieve(ctx, "hook-doc", "", &got, nil,
			storage.WithPostHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.Retrieve(ctx, "hook-doc", "", &got, nil,
			storage.WithPreHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*retrieve.Retrieve](func(context.Context, storage.IStorage, string, string, any, *retrieve.Retrieve) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("update", func(t *testing.T) {
		err := str.Update(ctx, "hook-doc", "", shared.TestData, nil, storage.WithPreHook[*update.Update](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.Update(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.Update(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPostHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.Update(ctx, "hook-doc", "", shared.TestData, nil,
			storage.WithPreHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*update.Update](func(context.Context, storage.IStorage, string, string, any, *update.Update) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("count", func(t *testing.T) {
		_, err := str.Count(ctx, "", nil, storage.WithPreHook[*count.Count](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		_, err = str.Count(ctx, "", nil,
			storage.WithPreHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		_, err = str.Count(ctx, "", nil,
			storage.WithPostHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		_, err = str.Count(ctx, "", nil,
			storage.WithPreHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*count.Count](func(context.Context, storage.IStorage, string, string, any, *count.Count) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("list", func(t *testing.T) {
		var keys ResponseListKeys

		err := str.List(ctx, "", &keys, nil, storage.WithPreHook[*list.List](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.List(ctx, "", &keys, nil,
			storage.WithPreHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.List(ctx, "", &keys, nil,
			storage.WithPostHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.List(ctx, "", &keys, nil,
			storage.WithPreHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*list.List](func(context.Context, storage.IStorage, string, string, any, *list.List) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})

	t.Run("delete", func(t *testing.T) {
		err := str.Delete(ctx, "hook-doc", "", nil, storage.WithPreHook[*delete.Delete](nil))
		assert.Error(t, err, "nil pre-hook option must fail")

		err = str.Delete(ctx, "hook-doc", "", nil,
			storage.WithPreHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		err = str.Delete(ctx, "hook-doc", "", nil,
			storage.WithPostHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				return errHookBoom
			}))
		assert.ErrorIs(t, err, errHookBoom)

		var pre, post bool

		err = str.Delete(ctx, "hook-doc", "", nil,
			storage.WithPreHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				pre = true

				return nil
			}),
			storage.WithPostHook[*delete.Delete](func(context.Context, storage.IStorage, string, string, any, *delete.Delete) error {
				post = true

				return nil
			}))
		require.NoError(t, err)
		assert.True(t, pre)
		assert.True(t, post)
	})
}

// Every operation must surface server/connection errors.
func TestRedisOffline_ServerDown(t *testing.T) {
	ctx := t.Context()

	f := startFakeRedis(t)

	// MaxRetries -1 disables go-redis command retries, keeping the test fast.
	str, err := New(t.Context(), &Config{Addr: f.Addr(), MaxRetries: -1})
	require.NoError(t, err)

	f.Close()

	_, err = str.Count(ctx, "", nil)
	assert.Error(t, err)

	var keys ResponseListKeys

	assert.Error(t, str.List(ctx, "", &keys, nil))

	_, err = str.Create(ctx, "down", "", shared.TestData, nil)
	assert.Error(t, err)

	assert.Error(t, str.Update(ctx, "down", "", shared.TestData, nil))

	var got shared.TestDataS

	assert.Error(t, str.Retrieve(ctx, "down", "", &got, nil))
	assert.Error(t, str.Delete(ctx, "down", "", nil))
}

// New: success wires the client and the singleton; failure returns an error.
func TestRedisOffline_NewAndSingleton(t *testing.T) {
	_, str := newOfflineStorage(t)

	client, ok := str.GetClient().(interface{ Close() error })
	assert.True(t, ok)
	assert.NotNil(t, client)
	assert.Same(t, str.Client, str.GetClient())

	// New stores the instance in the singleton.
	assert.Same(t, str, Get())

	// Set replaces the singleton; Get panics when it is empty.
	Set(nil)

	assert.Panics(t, func() { Get() })

	Set(str)

	assert.Same(t, str, Get())
}

// New must fail fast when the server is unreachable.
func TestRedisOffline_NewFailure(t *testing.T) {
	// Grab a loopback port and close it so connections are refused.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := listener.Addr().String()

	require.NoError(t, listener.Close())

	// The retrier backs off for seconds between ping attempts; the deadline
	// cancels the sleep so the failure path stays fast.
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	str, err := New(ctx, &Config{Addr: addr, MaxRetries: -1})
	assert.Error(t, err)
	assert.Nil(t, str)
}
