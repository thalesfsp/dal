package shared

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateUUID(t *testing.T) {
	u1 := GenerateUUID()
	u2 := GenerateUUID()

	assert.Len(t, u1, 36)
	assert.NotEqual(t, u1, u2)
}

func TestGenerateID(t *testing.T) {
	// Content-based: same content, same ID — including surrounding whitespace
	// which is trimmed.
	assert.Equal(t, GenerateID("abc"), GenerateID(" abc \n"))
	assert.NotEqual(t, GenerateID("abc"), GenerateID("abd"))
	assert.Len(t, GenerateID("abc"), 64)
}

func TestSliceContains(t *testing.T) {
	assert.True(t, SliceContains([]string{"A", "b"}, "a"))
	assert.True(t, SliceContains([]string{"A", "b"}, "B"))
	assert.False(t, SliceContains([]string{"A", "b"}, "c"))
	assert.False(t, SliceContains(nil, "a"))
}

func TestMarshalUnmarshal(t *testing.T) {
	b, err := Marshal(map[string]string{"k": "v"})
	require.NoError(t, err)

	var out map[string]string

	require.NoError(t, Unmarshal(b, &out))
	assert.Equal(t, "v", out["k"])

	// Marshal failure: channels are not serializable.
	_, err = Marshal(make(chan int))
	assert.Error(t, err)

	// Unmarshal failure: invalid JSON.
	assert.Error(t, Unmarshal([]byte("{"), &out))

	// Regression: a non-pointer destination must fail loudly, not silently
	// decode into the interface box and drop the data.
	assert.Error(t, Unmarshal([]byte(`{"k":"v"}`), map[string]string(nil)))
}

func TestDecodeEncode(t *testing.T) {
	var out map[string]string

	require.NoError(t, Decode(strings.NewReader(`{"k":"v"}`), &out))
	assert.Equal(t, "v", out["k"])

	assert.Error(t, Decode(strings.NewReader("{"), &out))

	var buf bytes.Buffer

	require.NoError(t, Encode(&buf, map[string]string{"k": "v"}))
	assert.JSONEq(t, `{"k":"v"}`, buf.String())

	assert.Error(t, Encode(&buf, make(chan int)))
}

func TestReadAll(t *testing.T) {
	b, err := ReadAll(strings.NewReader("data"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), b)

	_, err = ReadAll(iotest.ErrReader(assert.AnError))
	assert.Error(t, err)
}

func TestTargetName(t *testing.T) {
	got, err := TargetName("explicit", "fallback")
	require.NoError(t, err)
	assert.Equal(t, "explicit", got)

	got, err = TargetName("", "fallback")
	require.NoError(t, err)
	assert.Equal(t, "fallback", got)

	_, err = TargetName("", "")
	assert.Error(t, err)
}

func TestHTTPMethodString(t *testing.T) {
	assert.Equal(t, http.MethodGet, MethodGet.String())
	assert.Equal(t, http.MethodDelete, MethodDelete.String())
}

func TestCreateHTTPTestServer_BadPaths(t *testing.T) {
	srv := CreateHTTPTestServer(
		http.StatusTeapot,
		map[string]string{"X-Want": "yes"},
		map[string]string{"q": "1"},
		"body",
	)
	defer srv.Close()

	// Missing required header → 400.
	resp, err := http.Get(srv.URL + "?q=1")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Header present, missing query param → 400.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("X-Want", "yes")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Both present → configured status.
	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"?q=1", nil)
	require.NoError(t, err)
	req.Header.Set("X-Want", "yes")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusTeapot, resp.StatusCode)
}
