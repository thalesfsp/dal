package metrics

import (
	"expvar"
	"os"
	"time"
)

// NewInt creates and initializes a new expvar.Int.
func NewInt(name string) *expvar.Int {
	prefix := os.Getenv("DAL_METRICS_PREFIX")

	finalName := name + "--" + time.Now().Format(time.RFC3339)

	if prefix != "" {
		finalName = prefix + "." + finalName
	}

	counter := expvar.NewInt(finalName)

	counter.Set(0)

	return counter
}
