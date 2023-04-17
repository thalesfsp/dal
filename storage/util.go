package storage

import (
	"github.com/thalesfsp/dal/internal/shared"
)

// ParseToStruct parses the given JSON (`from`) to struct (`to`).
func ParseToStruct(from, to any) error {
	pRBJ, err := shared.Marshal(from)
	if err != nil {
		return err
	}

	if err := shared.Unmarshal(pRBJ, to); err != nil {
		return err
	}

	return nil
}
