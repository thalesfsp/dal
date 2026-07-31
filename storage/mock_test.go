package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This TEST exist just to ensure Mock match the IStorage interface.
func TestMock_match_interface(t *testing.T) {
	var iS IStorage = &Mock{}

	t.Log(iS)
}

func TestMock_Close(t *testing.T) {
	mock := &Mock{}

	require.NoError(t, mock.Close())
	assert.NoError(t, mock.Close())
}
