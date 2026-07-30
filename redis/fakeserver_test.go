package redis

import (
	"bufio"
	"errors"
	"io"
	"net"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

//////
// fakeRedis is a minimal, in-process RESP2 server used to test the adapter
// offline (no Docker, no network beyond loopback). It implements exactly the
// commands the adapter issues: PING, SET, GET, DEL and SCAN, plus the go-redis
// connection handshake (HELLO is answered with an error reply, which makes
// go-redis fall back to RESP2; CLIENT SETINFO is acknowledged).
//////

type fakeRedis struct {
	listener net.Listener

	mu    sync.Mutex
	data  map[string]string
	conns []net.Conn

	// pageSize is the number of keys returned per SCAN page, so the
	// client-side cursor loop iterates multiple times.
	pageSize int

	closeOnce sync.Once
}

// startFakeRedis starts the server on a random loopback port and registers a
// cleanup that shuts it down.
func startFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	f := &fakeRedis{
		listener: listener,
		data:     map[string]string{},
		pageSize: 100,
	}

	go f.serve()

	t.Cleanup(f.Close)

	return f
}

// Addr returns the host:port the server listens on.
func (f *fakeRedis) Addr() string {
	return f.listener.Addr().String()
}

// Close stops accepting and closes every open connection (idempotent).
func (f *fakeRedis) Close() {
	f.closeOnce.Do(func() {
		_ = f.listener.Close()

		f.mu.Lock()
		defer f.mu.Unlock()

		for _, conn := range f.conns {
			_ = conn.Close()
		}
	})
}

// seed inserts entries directly into the store, bypassing the protocol.
func (f *fakeRedis) seed(kv map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for k, v := range kv {
		f.data[k] = v
	}
}

func (f *fakeRedis) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}

		f.mu.Lock()
		f.conns = append(f.conns, conn)
		f.mu.Unlock()

		go f.handle(conn)
	}
}

func (f *fakeRedis) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}

		f.dispatch(writer, args)

		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func (f *fakeRedis) dispatch(w *bufio.Writer, args []string) {
	if len(args) == 0 {
		writeErrorReply(w, "ERR empty command")

		return
	}

	switch strings.ToUpper(args[0]) {
	case "HELLO":
		// Answering HELLO with an error reply makes go-redis fall back to
		// RESP2, exactly like a pre-6.0 redis-server.
		writeErrorReply(w, "ERR unknown command 'HELLO'")
	case "CLIENT":
		writeSimpleReply(w, "OK")
	case "PING":
		writeSimpleReply(w, "PONG")
	case "SET":
		if len(args) < 3 {
			writeErrorReply(w, "ERR wrong number of arguments for 'set'")

			return
		}

		f.mu.Lock()
		f.data[args[1]] = args[2]
		f.mu.Unlock()

		writeSimpleReply(w, "OK")
	case "GET":
		if len(args) < 2 {
			writeErrorReply(w, "ERR wrong number of arguments for 'get'")

			return
		}

		f.mu.Lock()
		v, ok := f.data[args[1]]
		f.mu.Unlock()

		if !ok {
			writeNilReply(w)

			return
		}

		writeBulkReply(w, v)
	case "DEL":
		deleted := 0

		f.mu.Lock()

		for _, k := range args[1:] {
			if _, ok := f.data[k]; ok {
				delete(f.data, k)

				deleted++
			}
		}

		f.mu.Unlock()

		writeIntReply(w, deleted)
	case "SCAN":
		f.scan(w, args)
	default:
		writeErrorReply(w, "ERR unknown command '"+args[0]+"'")
	}
}

// scan implements `SCAN cursor [MATCH pattern] [COUNT n]` with deterministic,
// index-based cursors over the sorted key set, paged by pageSize.
func (f *fakeRedis) scan(w *bufio.Writer, args []string) {
	if len(args) < 2 {
		writeErrorReply(w, "ERR wrong number of arguments for 'scan'")

		return
	}

	cursor, err := strconv.Atoi(args[1])
	if err != nil || cursor < 0 {
		writeErrorReply(w, "ERR invalid cursor")

		return
	}

	pattern := "*"

	for i := 2; i+1 < len(args); i += 2 {
		if strings.EqualFold(args[i], "MATCH") {
			pattern = args[i+1]
		}
	}

	f.mu.Lock()

	keys := make([]string, 0, len(f.data))

	for k := range f.data {
		if matched, mErr := path.Match(pattern, k); mErr == nil && matched {
			keys = append(keys, k)
		}
	}

	pageSize := f.pageSize

	f.mu.Unlock()

	sort.Strings(keys)

	if cursor > len(keys) {
		cursor = len(keys)
	}

	end := cursor + pageSize
	if end > len(keys) {
		end = len(keys)
	}

	next := end
	if next >= len(keys) {
		next = 0
	}

	page := keys[cursor:end]

	_, _ = w.WriteString("*2\r\n")

	writeBulkReply(w, strconv.Itoa(next))

	_, _ = w.WriteString("*" + strconv.Itoa(len(page)) + "\r\n")

	for _, k := range page {
		writeBulkReply(w, k)
	}
}

//////
// RESP2 protocol helpers.
//////

// readCommand reads one client command: an array of bulk strings.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[0] != '*' {
		return nil, errors.New("fakeredis: expected array header, got " + line)
	}

	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	args := make([]string, 0, n)

	for range n {
		header, err := readLine(r)
		if err != nil {
			return nil, err
		}

		if len(header) < 2 || header[0] != '$' {
			return nil, errors.New("fakeredis: expected bulk header, got " + header)
		}

		size, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, err
		}

		// Payload plus trailing CRLF.
		buf := make([]byte, size+2)

		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}

		args = append(args, string(buf[:size]))
	}

	return args, nil
}

func readLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimRight(s, "\r\n"), nil
}

func writeSimpleReply(w *bufio.Writer, s string) {
	_, _ = w.WriteString("+" + s + "\r\n")
}

func writeErrorReply(w *bufio.Writer, msg string) {
	_, _ = w.WriteString("-" + msg + "\r\n")
}

func writeIntReply(w *bufio.Writer, n int) {
	_, _ = w.WriteString(":" + strconv.Itoa(n) + "\r\n")
}

func writeBulkReply(w *bufio.Writer, s string) {
	_, _ = w.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

func writeNilReply(w *bufio.Writer) {
	_, _ = w.WriteString("$-1\r\n")
}
