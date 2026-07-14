package storage

import (
	"github.com/thalesfsp/dal/v2/internal/shared"
)

// ParseToStruct parses the given JSON (`from`) to struct (`to`).
func ParseToStruct(from, to any) error {
	pRBJ, err := shared.Marshal(from)
	if err != nil {
		return err
	}

	return shared.Unmarshal(pRBJ, to)
}

// Flatten2D takes a 2D slice and returns a 1D slice containing all the elements.
//
//nolint:staticcheck // S1011: explicit nested loop kept intentionally (gosimple merged into staticcheck in golangci-lint v2)
func Flatten2D[T any](data [][]T) []T {
	var result []T

	for _, outer := range data {
		for _, inner := range outer {
			result = append(result, inner)
		}
	}

	return result
}
