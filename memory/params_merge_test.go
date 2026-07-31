package memory

import (
	"sync"
	"testing"

	"github.com/thalesfsp/dal/v3/internal/paramtest"
	"github.com/thalesfsp/dal/v3/storage"
)

func TestMemory_MergesCallerParamsWithDefaults(t *testing.T) {
	base, err := storage.New(t.Context(), Name)
	if err != nil {
		t.Fatal(err)
	}

	strg := &Memory{Storage: base, client: &sync.Map{}}
	paramtest.AssertMergedDefaults(t, strg, paramtest.Defaults{
		CountSearch: "*",
		ListSearch:  "*",
	})
}
