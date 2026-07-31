package sftp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pkgsftp "github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"golang.org/x/crypto/ssh"
)

//////
// In-process SSH+SFTP server used by the offline tests. It listens on
// 127.0.0.1 only, serves the real filesystem (tests use absolute paths under
// t.TempDir()), and supports multiple sequential connections.
//////

const (
	offlineTestUser = "dal-offline-user"
	offlineTestPass = "dal-offline-pass"
)

// errHookBoom is returned by failing test hooks.
var errHookBoom = errors.New("hook boom")

// testDoc is the document shape used by the offline tests.
type testDoc struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type testSSHServer struct {
	ln net.Listener

	// rejectSubsystem makes the server refuse the "sftp" subsystem so
	// sftp.NewClient fails after a successful SSH handshake.
	rejectSubsystem bool

	mu    sync.Mutex
	conns []net.Conn

	wg sync.WaitGroup
}

// startTestSSHServer starts an in-process SSH server speaking the SFTP
// subsystem and returns its "host:port" address. Shutdown is registered via
// t.Cleanup.
func startTestSSHServer(t *testing.T, rejectSubsystem bool) string {
	t.Helper()

	_, hostKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	signer, err := ssh.NewSignerFromKey(hostKey)
	require.NoError(t, err)

	cfg := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == offlineTestUser && string(pass) == offlineTestPass {
				return &ssh.Permissions{}, nil
			}

			return nil, fmt.Errorf("invalid credentials for user %q", c.User())
		},
	}
	cfg.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &testSSHServer{ln: ln, rejectSubsystem: rejectSubsystem}

	srv.wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // Listener closed: shutdown.
			}

			srv.mu.Lock()
			srv.conns = append(srv.conns, conn)
			srv.mu.Unlock()

			srv.wg.Go(func() {
				srv.handleConn(conn, cfg)
			})
		}
	})

	t.Cleanup(func() {
		_ = ln.Close()

		srv.mu.Lock()

		for _, c := range srv.conns {
			_ = c.Close()
		}

		srv.mu.Unlock()

		srv.wg.Wait()
	})

	return ln.Addr().String()
}

func (s *testSSHServer) handleConn(nConn net.Conn, cfg *ssh.ServerConfig) {
	defer func() { _ = nConn.Close() }()

	sconn, chans, reqs, err := ssh.NewServerConn(nConn, cfg)
	if err != nil {
		return
	}

	defer func() { _ = sconn.Close() }()

	go ssh.DiscardRequests(reqs)

	for newCh := range chans {
		if newCh.ChannelType() != "session" {
			_ = newCh.Reject(ssh.UnknownChannelType, "only session channels are supported")

			continue
		}

		ch, requests, err := newCh.Accept()
		if err != nil {
			continue
		}

		go s.handleSession(ch, requests)
	}
}

func (s *testSSHServer) handleSession(ch ssh.Channel, requests <-chan *ssh.Request) {
	started := false

	for req := range requests {
		ok := req.Type == "subsystem" &&
			isSFTPSubsystemPayload(req.Payload) &&
			!s.rejectSubsystem

		_ = req.Reply(ok, nil)

		if ok && !started {
			started = true

			go func() {
				srv, err := pkgsftp.NewServer(ch)
				if err != nil {
					_ = ch.Close()

					return
				}

				_ = srv.Serve()
				_ = srv.Close()
			}()
		}
	}
}

// isSFTPSubsystemPayload reports whether an SSH subsystem request payload
// (a length-prefixed string) names the "sftp" subsystem.
func isSFTPSubsystemPayload(payload []byte) bool {
	if len(payload) < 4 {
		return false
	}

	n := binary.BigEndian.Uint32(payload)

	return len(payload) >= int(4+n) && string(payload[4:4+n]) == "sftp"
}

//////
// Test helpers.
//////

// offlineClientConfig is the SSH client config matching the in-process
// server's credentials.
func offlineClientConfig() *Config {
	return &ssh.ClientConfig{
		User:            offlineTestUser,
		Auth:            []ssh.AuthMethod{ssh.Password(offlineTestPass)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec
		Timeout:         5 * time.Second,
	}
}

// newOfflineStorage starts a server and connects a fresh adapter to it.
func newOfflineStorage(t *testing.T) *SFTP {
	t.Helper()

	addr := startTestSSHServer(t, false)

	str, err := New(t.Context(), addr, offlineClientConfig())
	require.NoError(t, err)
	require.NotNil(t, str)

	t.Cleanup(func() { _ = str.Close() })

	return str
}

// hookOptions builds a pre+post hook pair which fail on demand.
func hookOptions[T any](preErr, postErr bool) []storage.Func[T] {
	return []storage.Func[T]{
		storage.WithPreHook(func(context.Context, storage.IStorage, string, string, any, T) error {
			if preErr {
				return errHookBoom
			}

			return nil
		}),
		storage.WithPostHook(func(context.Context, storage.IStorage, string, string, any, T) error {
			if postErr {
				return errHookBoom
			}

			return nil
		}),
	}
}

// mustCreateFile creates a file with the given content through the adapter.
func mustCreateFile(t *testing.T, str *SFTP, trgt string, v any) {
	t.Helper()

	id, err := str.Create(t.Context(), "test-id", trgt, v, nil)
	require.NoError(t, err)
	assert.Equal(t, "test-id", id)
}

//////
// Tests.
//////

// New must work against a real (in-process) server with both documented addr
// forms, and must register the singleton.
func TestNew_Offline_AddrForms(t *testing.T) {
	ctx := t.Context()
	addr := startTestSSHServer(t, false)

	for _, form := range []string{addr, "sftp://" + addr} {
		str, err := New(ctx, form, offlineClientConfig())
		require.NoError(t, err, "New must connect using addr form %q", form)
		require.NotNil(t, str)

		client, ok := str.GetClient().(*pkgsftp.Client)
		require.True(t, ok, "GetClient must return the underlying *sftp.Client")
		require.NotNil(t, client)

		// New must have registered the new storage as the singleton.
		assert.Equal(t, storage.IStorage(str), Get())

		require.NoError(t, str.Client.Close())
	}
}

func TestNew_Offline_Errors(t *testing.T) {
	ctx := t.Context()

	t.Run("wrong password fails at dial", func(t *testing.T) {
		addr := startTestSSHServer(t, false)

		cfg := offlineClientConfig()
		cfg.Auth = []ssh.AuthMethod{ssh.Password("nope")}
		cfg.Timeout = 3 * time.Second

		str, err := New(ctx, addr, cfg)
		require.Error(t, err)
		assert.Nil(t, str)
		assert.Contains(t, err.Error(), "dial")
	})

	t.Run("invalid URL form", func(t *testing.T) {
		str, err := New(ctx, "sftp://[::1", offlineClientConfig())
		require.Error(t, err)
		assert.Nil(t, str)
		assert.Contains(t, err.Error(), "convert addr to URL")
	})

	t.Run("subsystem rejected fails at sftp connect", func(t *testing.T) {
		addr := startTestSSHServer(t, true)

		str, err := New(ctx, addr, offlineClientConfig())
		require.Error(t, err)
		assert.Nil(t, str)
		assert.Contains(t, err.Error(), "connect")
	})
}

func TestSFTP_Offline_CRUD(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)

	dir := t.TempDir()
	trgt := filepath.Join(dir, "doc.json")

	//////
	// Create (nil prm exercises the default-params branch).
	//////

	doc := &testDoc{ID: "1", Name: "original"}

	id, err := str.Create(ctx, "id-1", trgt, doc, nil)
	require.NoError(t, err)
	assert.Equal(t, "id-1", id)

	//////
	// Retrieve (nil prm).
	//////

	var got testDoc

	require.NoError(t, str.Retrieve(ctx, "id-1", trgt, &got, nil))
	assert.Equal(t, *doc, got)

	//////
	// Update (non-nil prm exercises the caller-params branch).
	//////

	updated := &testDoc{ID: "1", Name: "updated"}

	require.NoError(t, str.Update(ctx, "id-1", trgt, updated, &update.Update{}))

	var gotUpdated testDoc

	require.NoError(t, str.Retrieve(ctx, "id-1", trgt, &gotUpdated, &retrieve.Retrieve{}))
	assert.Equal(t, *updated, gotUpdated)

	//////
	// Delete (non-nil prm), then confirm the file is gone.
	//////

	require.NoError(t, str.Delete(ctx, "id-1", trgt, &delete.Delete{}))

	var gone testDoc

	require.Error(t, str.Retrieve(ctx, "id-1", trgt, &gone, nil))

	// Deleting a missing file is, as coded, not an error (ErrNotExist is
	// swallowed).
	require.NoError(t, str.Delete(ctx, "id-1", trgt, nil))

	//////
	// Metrics sanity.
	//////

	assert.Equal(t, int64(1), str.GetCounterCreated().Value())
	assert.Equal(t, int64(1), str.GetCounterUpdated().Value())
	assert.Equal(t, int64(2), str.GetCounterDeleted().Value())
	assert.Equal(t, int64(2), str.GetCounterRetrieved().Value())
	assert.Equal(t, int64(1), str.GetCounterRetrievedFailed().Value())
}

func TestSFTP_Offline_Create_Errors(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)
	dir := t.TempDir()

	t.Run("missing target", func(t *testing.T) {
		_, err := str.Create(ctx, "id", "", &testDoc{}, nil)
		require.Error(t, err)
	})

	t.Run("unmarshalable value", func(t *testing.T) {
		_, err := str.Create(ctx, "id", filepath.Join(dir, "x.json"), make(chan int), nil)
		require.Error(t, err)
	})

	t.Run("nonexistent directory yields nil file", func(t *testing.T) {
		_, err := str.Create(ctx, "id", filepath.Join(dir, "missing-dir", "x.json"), &testDoc{}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("target is a directory", func(t *testing.T) {
		_, err := str.Create(ctx, "id", dir, &testDoc{}, nil)
		require.Error(t, err)
	})
}

func TestSFTP_Offline_Update_Errors(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)
	dir := t.TempDir()

	t.Run("missing id", func(t *testing.T) {
		require.Error(t, str.Update(ctx, "", filepath.Join(dir, "x.json"), &testDoc{}, nil))
	})

	t.Run("missing target", func(t *testing.T) {
		require.Error(t, str.Update(ctx, "id", "", &testDoc{}, nil))
	})

	t.Run("unmarshalable value", func(t *testing.T) {
		require.Error(t, str.Update(ctx, "id", filepath.Join(dir, "x.json"), make(chan int), nil))
	})

	t.Run("nonexistent directory yields nil file", func(t *testing.T) {
		err := str.Update(ctx, "id", filepath.Join(dir, "missing-dir", "x.json"), &testDoc{}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil")
	})

	t.Run("target is a directory", func(t *testing.T) {
		require.Error(t, str.Update(ctx, "id", dir, &testDoc{}, nil))
	})
}

func TestSFTP_Offline_Retrieve_Errors(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)
	dir := t.TempDir()

	t.Run("missing id", func(t *testing.T) {
		var v testDoc

		require.Error(t, str.Retrieve(ctx, "", filepath.Join(dir, "x.json"), &v, nil))
	})

	t.Run("missing target", func(t *testing.T) {
		var v testDoc

		require.Error(t, str.Retrieve(ctx, "id", "", &v, nil))
	})

	t.Run("missing file", func(t *testing.T) {
		var v testDoc

		require.Error(t, str.Retrieve(ctx, "id", filepath.Join(dir, "nope.json"), &v, nil))
	})

	t.Run("invalid JSON content", func(t *testing.T) {
		trgt := filepath.Join(dir, "invalid.json")

		// Write raw, non-JSON bytes directly through the SFTP client.
		f, err := str.Client.Create(trgt)
		require.NoError(t, err)

		_, err = f.Write([]byte("this is not JSON"))
		require.NoError(t, err)
		require.NoError(t, f.Close())

		var v testDoc

		require.Error(t, str.Retrieve(ctx, "id", trgt, &v, nil))
	})
}

func TestSFTP_Offline_Delete_Errors(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)
	dir := t.TempDir()

	t.Run("missing id", func(t *testing.T) {
		require.Error(t, str.Delete(ctx, "", filepath.Join(dir, "x.json"), nil))
	})

	t.Run("missing target", func(t *testing.T) {
		require.Error(t, str.Delete(ctx, "id", "", nil))
	})

	t.Run("non-empty directory fails", func(t *testing.T) {
		sub := filepath.Join(dir, "sub")
		require.NoError(t, str.Client.Mkdir(sub))

		mustCreateFile(t, str, filepath.Join(sub, "inner.json"), &testDoc{ID: "9"})

		require.Error(t, str.Delete(ctx, "id", sub, nil))
	})
}

func TestSFTP_Offline_Count(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)
	dir := t.TempDir()

	// Three files and one subdirectory (which Count must skip).
	mustCreateFile(t, str, filepath.Join(dir, "a.json"), &testDoc{ID: "a"})
	mustCreateFile(t, str, filepath.Join(dir, "b.json"), &testDoc{ID: "b"})
	mustCreateFile(t, str, filepath.Join(dir, "notes.txt"), "notes")
	require.NoError(t, str.Client.Mkdir(filepath.Join(dir, "subdir")))
	mustCreateFile(t, str, filepath.Join(dir, "subdir", "inner.json"), &testDoc{ID: "i"})

	t.Run("nil params counts all files", func(t *testing.T) {
		n, err := str.Count(ctx, dir, nil)
		require.NoError(t, err)
		assert.EqualValues(t, 3, n)
	})

	t.Run("glob subset", func(t *testing.T) {
		n, err := str.Count(ctx, dir, &count.Count{Search: "*.json"})
		require.NoError(t, err)
		assert.EqualValues(t, 2, n)
	})

	t.Run("glob zero matches", func(t *testing.T) {
		n, err := str.Count(ctx, dir, &count.Count{Search: "zzz-*"})
		require.NoError(t, err)
		assert.EqualValues(t, 0, n)
	})

	t.Run("empty search defaults to match-all without mutating params", func(t *testing.T) {
		prm := &count.Count{Search: ""}

		n, err := str.Count(ctx, dir, prm)
		require.NoError(t, err)
		assert.EqualValues(t, 3, n)
		assert.Empty(t, prm.Search, "Count must not mutate the caller's params")
	})

	t.Run("bad pattern", func(t *testing.T) {
		_, err := str.Count(ctx, dir, &count.Count{Search: "["})
		require.Error(t, err)
	})

	t.Run("missing directory", func(t *testing.T) {
		_, err := str.Count(ctx, filepath.Join(dir, "nope"), nil)
		require.Error(t, err)
	})

	t.Run("missing target", func(t *testing.T) {
		_, err := str.Count(ctx, "", nil)
		require.Error(t, err)
	})
}

func TestSFTP_Offline_List(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)
	dir := t.TempDir()

	mustCreateFile(t, str, filepath.Join(dir, "a.json"), &testDoc{ID: "a"})
	mustCreateFile(t, str, filepath.Join(dir, "b.json"), &testDoc{ID: "b"})
	require.NoError(t, str.Client.Mkdir(filepath.Join(dir, "subdir")))

	t.Run("nil params lists files only", func(t *testing.T) {
		var keys ResponseListKeys

		require.NoError(t, str.List(ctx, dir, &keys, nil))
		assert.ElementsMatch(t, []string{"a.json", "b.json"}, keys.Keys)
	})

	t.Run("non-nil params", func(t *testing.T) {
		var keys ResponseListKeys

		require.NoError(t, str.List(ctx, dir, &keys, listParam))
		assert.ElementsMatch(t, []string{"a.json", "b.json"}, keys.Keys)
	})

	t.Run("empty directory", func(t *testing.T) {
		empty := t.TempDir()

		var keys ResponseListKeys

		require.NoError(t, str.List(ctx, empty, &keys, nil))
		assert.Empty(t, keys.Keys)
	})

	t.Run("missing directory", func(t *testing.T) {
		var keys ResponseListKeys

		require.Error(t, str.List(ctx, filepath.Join(dir, "nope"), &keys, nil))
	})

	t.Run("missing target", func(t *testing.T) {
		var keys ResponseListKeys

		require.Error(t, str.List(ctx, "", &keys, nil))
	})
}

// Hooks: every operation must call pre/post hooks, propagate their errors, and
// propagate option-application errors (e.g. a nil hook).
func TestSFTP_Offline_Hooks(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)
	dir := t.TempDir()
	trgt := filepath.Join(dir, "hooked.json")
	doc := &testDoc{ID: "h", Name: "hooked"}

	//////
	// Create.
	//////

	_, err := str.Create(ctx, "id", trgt, doc, nil, hookOptions[*create.Create](false, false)...)
	require.NoError(t, err)

	_, err = str.Create(ctx, "id", trgt, doc, nil, hookOptions[*create.Create](true, false)...)
	require.Error(t, err)

	_, err = str.Create(ctx, "id", trgt, doc, nil, hookOptions[*create.Create](false, true)...)
	require.Error(t, err)

	_, err = str.Create(ctx, "id", trgt, doc, nil, storage.WithPreHook[*create.Create](nil))
	require.Error(t, err)

	//////
	// Retrieve.
	//////

	var v testDoc

	require.NoError(t, str.Retrieve(ctx, "id", trgt, &v, nil, hookOptions[*retrieve.Retrieve](false, false)...))
	require.Error(t, str.Retrieve(ctx, "id", trgt, &v, nil, hookOptions[*retrieve.Retrieve](true, false)...))
	require.Error(t, str.Retrieve(ctx, "id", trgt, &v, nil, hookOptions[*retrieve.Retrieve](false, true)...))
	require.Error(t, str.Retrieve(ctx, "id", trgt, &v, nil, storage.WithPostHook[*retrieve.Retrieve](nil)))

	//////
	// Update.
	//////

	require.NoError(t, str.Update(ctx, "id", trgt, doc, nil, hookOptions[*update.Update](false, false)...))
	require.Error(t, str.Update(ctx, "id", trgt, doc, nil, hookOptions[*update.Update](true, false)...))
	require.Error(t, str.Update(ctx, "id", trgt, doc, nil, hookOptions[*update.Update](false, true)...))
	require.Error(t, str.Update(ctx, "id", trgt, doc, nil, storage.WithPreHook[*update.Update](nil)))

	//////
	// Count.
	//////

	_, err = str.Count(ctx, dir, nil, hookOptions[*count.Count](false, false)...)
	require.NoError(t, err)

	_, err = str.Count(ctx, dir, nil, hookOptions[*count.Count](true, false)...)
	require.Error(t, err)

	_, err = str.Count(ctx, dir, nil, hookOptions[*count.Count](false, true)...)
	require.Error(t, err)

	_, err = str.Count(ctx, dir, nil, storage.WithPreHook[*count.Count](nil))
	require.Error(t, err)

	//////
	// List.
	//////

	var keys ResponseListKeys

	require.NoError(t, str.List(ctx, dir, &keys, nil, hookOptions[*list.List](false, false)...))
	require.Error(t, str.List(ctx, dir, &keys, nil, hookOptions[*list.List](true, false)...))
	require.Error(t, str.List(ctx, dir, &keys, nil, hookOptions[*list.List](false, true)...))
	require.Error(t, str.List(ctx, dir, &keys, nil, storage.WithPostHook[*list.List](nil)))

	//////
	// Delete.
	//////

	require.Error(t, str.Delete(ctx, "id", trgt, nil, hookOptions[*delete.Delete](true, false)...))
	require.Error(t, str.Delete(ctx, "id", trgt, nil, hookOptions[*delete.Delete](false, true)...))
	require.Error(t, str.Delete(ctx, "id", trgt, nil, storage.WithPreHook[*delete.Delete](nil)))
	require.NoError(t, str.Delete(ctx, "id", trgt, nil, hookOptions[*delete.Delete](false, false)...))
}

func TestSFTP_Offline_Singleton(t *testing.T) {
	str := newOfflineStorage(t)

	// New registered str as the singleton.
	assert.Equal(t, storage.IStorage(str), Get())

	// Get must panic when the singleton is unset.
	Set(nil)

	assert.Panics(t, func() { Get() })

	// Set must restore it.
	Set(str)

	assert.Equal(t, storage.IStorage(str), Get())
}
