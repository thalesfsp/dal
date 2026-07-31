package storage

//////
// Const, vars, and types.
//////

// ICloser is an optional interface an adapter may implement to release the
// resources it acquired at construction time.
//
// It is deliberately NOT part of IStorage. Adding a method to IStorage would
// break every third-party implementation of it, and that is a v3 change. As a
// separate, single-method interface it is additive: callers that need to
// release resources type-assert for it, and callers that do not are unaffected.
//
// Every adapter in this repository implements it, so the assertion always
// succeeds for storages built here. It is still worth asserting rather than
// depending on the concrete type, because that is what keeps a caller holding
// an IStorage able to clean up:
//
//	if c, ok := strg.(storage.ICloser); ok {
//		if err := c.Close(); err != nil { ... }
//	}
//
// Or, more conveniently, via the Close helper below.
//
// Implementations must be safe to call more than once: a second Close returns
// nil rather than an error, so a deferred Close alongside an explicit one is
// not a bug.
//
// The in-process adapters (memory, file) hold no releasable resource and
// implement Close as a no-op purely so the assertion is uniform.
type ICloser interface {
	// Close releases the resources held by the storage. It is idempotent.
	Close() error
}

//////
// Exported functionality.
//////

// Close releases the resources held by s, if it holds any.
//
// It returns nil when s does not implement ICloser, which makes it safe to call
// against any IStorage — including a third-party implementation written before
// ICloser existed, and against storage.Mock.
//
// This matters most for the adapters whose leaked handle is more than a socket
// the OS reclaims at exit: bolt holds an exclusive file lock for as long as the
// database is open, so a storage that is never closed blocks every subsequent
// Open of the same path for the lifetime of the process.
func Close(s IStorage) error {
	if c, ok := s.(ICloser); ok {
		return c.Close()
	}

	return nil
}
