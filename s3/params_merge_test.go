package s3

import (
	"testing"

	"github.com/thalesfsp/dal/v3/internal/paramtest"
	"github.com/thalesfsp/dal/v3/storage"
)

func TestS3_MergesCallerParamsWithDefaults(t *testing.T) {
	base, err := storage.New(t.Context(), Name)
	if err != nil {
		t.Fatal(err)
	}

	paramtest.AssertMergedDefaults(t, &S3{Storage: base}, paramtest.Defaults{
		CountSearch: "*",
		ListSearch:  "*",
	})
}
