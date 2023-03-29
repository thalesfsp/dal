package storage

import (
	"github.com/thalesfsp/dal/shared"
)

// ParseToStruct parses the given JSON (`from`) to struct (`to`).
func ParseToStruct(from, to any) error {
	var parsedRespBodyAsJSON []byte

	pRBJ, err := shared.Marshal(from)
	if err != nil {
		return err
	}

	parsedRespBodyAsJSON = pRBJ

	if err := shared.Unmarshal(parsedRespBodyAsJSON, to); err != nil {
		return err
	}

	return nil
}
