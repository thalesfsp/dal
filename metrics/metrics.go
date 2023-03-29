package metrics

import (
	"expvar"
	"fmt"
	"strings"

	"github.com/thalesfsp/dal/shared"
)

// NewInt creates and initializes a new expvar.Int. Name should be in the format of
// "{packageName}.{subject}.{type}", e.g. "{companyname}.api.rest.failed.counter".
//
// NOTE: All metrics are prefixed with the company name (e.g. companyname).
func NewInt(name string) *expvar.Int {
	counter := expvar.NewInt(
		fmt.Sprintf(
			"%s.%s",
			strings.ToLower(shared.CompanyName),
			name,
		),
	)

	counter.Set(0)

	return counter
}
