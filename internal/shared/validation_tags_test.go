package shared

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/validation"
)

//////
// Guards the `required,structonly` convention every adapter uses on its
// client field. The validator descends into nested structs by default, which
// means walking a live driver client while its own goroutines mutate it — a
// data race, and pointless work besides. `structonly` stops the descent, and
// these assertions pin down that it does so without also dropping the nil
// check the tag is there for.
//////

// liveClient stands in for a driver client: mutex-guarded state the validator
// must not touch, plus a field it would otherwise demand be populated.
type liveClient struct {
	mu sync.Mutex //nolint:unused // Present to mirror a real client; never read.

	Inner string `validate:"required"`
}

type adapter struct {
	Client *liveClient `validate:"required,structonly"`
}

func TestStructOnly_KeepsRequiredButSkipsDescent(t *testing.T) {
	assert.Error(t, validation.Validate(&adapter{Client: nil}),
		"structonly must not weaken required: a nil client has to be rejected")

	assert.NoError(t, validation.Validate(&adapter{Client: &liveClient{}}),
		"structonly must stop the validator descending into the client's fields")
}
