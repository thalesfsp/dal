package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeTestStorage struct {
	IStorage

	closeCalls int
	closeErr   error
	closed     bool
}

func (s *closeTestStorage) Close() error {
	s.closeCalls++
	if s.closed {
		return nil
	}

	s.closed = true

	return s.closeErr
}

type nonClosingTestStorage struct {
	IStorage
}

func TestClose(t *testing.T) {
	t.Run("closer is called and returns nil", func(t *testing.T) {
		strg := &closeTestStorage{}

		require.NoError(t, Close(strg))
		assert.Equal(t, 1, strg.closeCalls)
	})

	t.Run("closer error is propagated exactly", func(t *testing.T) {
		closeErr := errors.New("close failed")
		strg := &closeTestStorage{closeErr: closeErr}

		err := Close(strg)

		assert.Same(t, closeErr, err)
		assert.Equal(t, 1, strg.closeCalls)
	})

	t.Run("storage without closer returns nil", func(t *testing.T) {
		strg := &nonClosingTestStorage{}

		assert.NotPanics(t, func() {
			require.NoError(t, Close(strg))
		})
	})

	t.Run("closer implementation is idempotent", func(t *testing.T) {
		closeErr := errors.New("first close failed")
		strg := &closeTestStorage{closeErr: closeErr}

		assert.Same(t, closeErr, Close(strg))
		require.NoError(t, Close(strg))
		assert.Equal(t, 2, strg.closeCalls)
	})

	t.Run("Mock is safe", func(t *testing.T) {
		assert.NotPanics(t, func() {
			require.NoError(t, Close(&Mock{}))
		})
	})
}
