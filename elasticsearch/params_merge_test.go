package elasticsearch

import (
	"testing"

	"github.com/thalesfsp/dal/v3/internal/paramtest"
	"github.com/thalesfsp/dal/v3/storage"
)

func TestElasticSearch_MergesCallerParamsWithDefaults(t *testing.T) {
	base, err := storage.New(t.Context(), Name)
	if err != nil {
		t.Fatal(err)
	}

	strg := &ElasticSearch{
		Storage: base,
		Target:  func() string { return "" },
	}
	paramtest.AssertMergedDefaults(t, strg, paramtest.Defaults{
		CountSearch: matchAllQuery,
		ListSearch:  matchAllQuery,
	})
}
