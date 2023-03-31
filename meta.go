package dal

import "github.com/thalesfsp/sypl"

// IMeta defines method(s) about the storage itself.
type IMeta interface {
	// GetLogger returns the logger.
	GetLogger() sypl.ISypl

	// GetName returns the storage name.
	GetName() string
}
