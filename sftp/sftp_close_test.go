package sftp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/params/v2/list"
)

// Close must tear down both the SFTP channel and the SSH transport under it —
// closing only the client leaves the TCP+SSH session open for the lifetime of
// the process.
func TestSFTP_Close_ReleasesSSHTransport(t *testing.T) {
	addr := startTestSSHServer(t, false)

	str, err := New(t.Context(), addr, offlineClientConfig())
	require.NoError(t, err)
	require.NotNil(t, str.Conn, "the SSH transport must be retained so it can be closed")

	require.NoError(t, str.Close())

	// The transport is gone: opening a new session on it must fail.
	_, err = str.Conn.NewSession()
	assert.Error(t, err)

	// Close is safe to call on a zero-valued adapter (nothing to release).
	assert.NoError(t, (&SFTP{}).Close())
}

// List honors the documented Search glob (it used to return every file
// regardless of the pattern).
func TestSFTP_List_HonorsSearchGlob(t *testing.T) {
	ctx := t.Context()
	str := newOfflineStorage(t)

	dir := t.TempDir()

	for _, name := range []string{"a.json", "b.json", "c.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(`{}`), 0o600))
	}

	var jsonOnly ResponseList[map[string]any]

	require.NoError(t, str.List(ctx, dir, &jsonOnly, &list.List{Search: "*.json"}))
	assert.Len(t, jsonOnly.Items, 2)

	// Empty search means match-all, matching Count's behavior.
	var all ResponseList[map[string]any]

	require.NoError(t, str.List(ctx, dir, &all, &list.List{Search: ""}))
	assert.Len(t, all.Items, 3)

	// A malformed pattern surfaces as an error instead of an empty listing.
	var none ResponseList[map[string]any]

	assert.Error(t, str.List(ctx, dir, &none, &list.List{Search: "["}))
}
