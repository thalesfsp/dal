package metrics

import (
	"expvar"
	"fmt"
	"os"

	"github.com/thalesfsp/dal/internal/logging"
)

// NewInt creates and initializes a new expvar.Int.
func NewInt(name string) *expvar.Int {
	prefix := os.Getenv("HTTPCLIENT_METRICS_PREFIX")

	if prefix == "" {
		logging.Get().Warnln("HTTPCLIENT_METRICS_PREFIX is not set. Using default (httpclient).")

		prefix = "httpclient"
	}

	counter := expvar.NewInt(
		fmt.Sprintf(
			"%s.%s",
			prefix,
			name,
		),
	)

	counter.Set(0)

	return counter
}
