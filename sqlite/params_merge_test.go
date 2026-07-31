package sqlite

import (
	"testing"

	"github.com/thalesfsp/dal/v3/internal/paramtest"
	"github.com/thalesfsp/dal/v3/storage"
)

func TestSQLite_MergesCallerParamsWithDefaults(t *testing.T) {
	base, err := storage.New(t.Context(), Name)
	if err != nil {
		t.Fatal(err)
	}

	paramtest.AssertMergedDefaults(t, &SQLite{Storage: base}, paramtest.Defaults{
		CountSearch: "SELECT COUNT(*) FROM merge_target",
		ListSearch:  "SELECT * FROM merge_target",
	})
}
