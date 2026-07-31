package bolt

import (
	"testing"

	"github.com/thalesfsp/dal/v3/internal/paramtest"
	"github.com/thalesfsp/dal/v3/storage"
)

func TestBolt_MergesCallerParamsWithDefaults(t *testing.T) {
	base, err := storage.New(t.Context(), Name)
	if err != nil {
		t.Fatal(err)
	}

	paramtest.AssertMergedDefaults(t, &Bolt{Storage: base}, paramtest.Defaults{
		CountSearch: "*",
		ListSearch:  "*",
	})
}
