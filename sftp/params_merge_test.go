package sftp

import (
	"testing"

	"github.com/thalesfsp/dal/v3/internal/paramtest"
	"github.com/thalesfsp/dal/v3/storage"
)

func TestSFTP_MergesCallerParamsWithDefaults(t *testing.T) {
	base, err := storage.New(t.Context(), Name)
	if err != nil {
		t.Fatal(err)
	}

	paramtest.AssertMergedDefaults(t, &SFTP{Storage: base}, paramtest.Defaults{
		CountSearch: "*",
		ListSearch:  "*",
	})
}
