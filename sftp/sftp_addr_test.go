package sftp

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// newUnreachableConfig is an SSH config for an address nothing listens on, so
// New fails fast at dial time — after addr parsing, which is what these tests
// exercise.
func newUnreachableConfig() *Config {
	return &ssh.ClientConfig{
		User:            "test",
		Auth:            []ssh.AuthMethod{ssh.Password("test")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec
		Timeout:         250 * time.Millisecond,
	}
}

// Regression: the documented "host:port" form used to be fed to url.Parse,
// which mangled it ("example.com:22" became scheme=example.com, host="") and
// every dial went to an empty address. Both documented forms must now reach
// the dialer with the right host.
func TestNew_AddrForms(t *testing.T) {
	ctx := t.Context()

	for _, addr := range []string{
		"127.0.0.1:1",        // documented host:port form
		"sftp://127.0.0.1:1", // URL form used by integration environments
	} {
		_, err := New(ctx, addr, newUnreachableConfig())
		require.Error(t, err, "nothing listens on port 1 — New must fail")

		assert.True(t, strings.Contains(err.Error(), "dial"),
			"failure for %q must happen at dial time (addr parsed correctly), got: %v", addr, err)
		assert.NotContains(t, err.Error(), "missing address",
			"addr %q must not be mangled into an empty host", addr)
	}
}
