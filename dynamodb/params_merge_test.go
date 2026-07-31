package dynamodb

import (
	"testing"

	"github.com/thalesfsp/dal/v3/internal/paramtest"
	"github.com/thalesfsp/dal/v3/storage"
)

func TestDynamoDB_MergesCallerParamsWithDefaults(t *testing.T) {
	base, err := storage.New(t.Context(), Name)
	if err != nil {
		t.Fatal(err)
	}

	strg := &DynamoDB{
		Storage: base,
		Target:  func() string { return "" },
	}
	paramtest.AssertMergedDefaults(t, strg, paramtest.Defaults{})
}
