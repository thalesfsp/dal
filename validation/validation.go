package validation

import (
	"github.com/thalesfsp/configurer/util"
)

// Validate process the `default` -> `env` -> `validate` struct's fields
// tags.
//
// NOTE: Register here any additional validator.
func Validate(i interface{}) error {
	if err := util.Process(i); err != nil {
		return err
	}

	return nil
}
