package storage

import (
	"context"
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

// zeroTestData is an all-zero-valued struct: exactly the shape the fan-out
// helpers used to silently drop from their results.
type zeroTestData struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// newFullMock builds a Mock whose operations succeed with the given canned
// values. Getter funcs are wired so operations that consult them don't panic.
func newFullMock(name string, countResult int64) *Mock {
	return &Mock{
		MockCount: func(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error) {
			return countResult, nil
		},
		MockCreate: func(ctx context.Context, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (string, error) {
			return id, nil
		},
		MockDelete: func(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
			return nil
		},
		MockRetrieve: func(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error {
			if out, ok := v.(*zeroTestData); ok {
				*out = zeroTestData{}
			}

			return nil
		},
		MockList: func(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error {
			return nil
		},
		MockUpdate: func(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error {
			return nil
		},
		MockGetName: func() string { return name },
		MockGetType: func() string { return "mock" },
	}
}

// A storage legitimately counting 0 documents must still contribute its
// result — a zero is data, not absence of a result.
func TestCountFromMany_KeepsZeroCounts(t *testing.T) {
	m := Map{
		"empty": newFullMock("empty", 0),
		"full":  newFullMock("full", 7),
	}

	got, err := CountFromMany(t.Context(), m, "target", &count.Count{})
	require.NoError(t, err)

	assert.Len(t, got, 2, "the zero count must not be dropped")
	assert.ElementsMatch(t, []int64{0, 7}, got)
}

// A document whose fields are all zero-valued must still be returned by
// RetrieveFromMany.
func TestRetrieveFromMany_KeepsZeroValuedDocuments(t *testing.T) {
	m := Map{
		"a": newFullMock("a", 0),
	}

	got, err := RetrieveFromMany[zeroTestData](t.Context(), m, "id1", "target", &retrieve.Retrieve{})
	require.NoError(t, err)

	assert.Len(t, got, 1, "the zero-valued document must not be dropped")
}

// A backend returning an empty generated ID must still occupy its slot in
// CreateIntoMany's results.
func TestCreateIntoMany_KeepsEmptyIDs(t *testing.T) {
	m := Map{
		"a": newFullMock("a", 0),
	}

	got, err := CreateIntoMany(t.Context(), m, "", "target", zeroTestData{}, &create.Create{})
	require.NoError(t, err)

	assert.Len(t, got, 1, "the empty ID must not be dropped")
}

// RetrieveFromMany must aggregate ALL storage failures, not surface a
// nondeterministically-chosen single one.
func TestRetrieveFromMany_AggregatesAllErrors(t *testing.T) {
	failing := func(msg string) *Mock {
		m := newFullMock(msg, 0)
		m.MockRetrieve = func(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error {
			return customerror.NewFailedToError(msg)
		}

		return m
	}

	m := Map{
		"one": failing("boom-one"),
		"two": failing("boom-two"),
	}

	_, err := RetrieveFromMany[zeroTestData](t.Context(), m, "id1", "target", &retrieve.Retrieve{})
	require.Error(t, err)

	assert.Contains(t, err.Error(), "boom-one")
	assert.Contains(t, err.Error(), "boom-two")
}

// An unset Mock operation must yield an error — not a nil-func panic inside a
// concurrentloop worker goroutine, which would crash the whole process.
func TestMock_UnsetOperationsError(t *testing.T) {
	m := &Mock{}

	_, err := m.Count(t.Context(), "t", nil)
	assert.Error(t, err)

	err = m.Delete(t.Context(), "id", "t", nil)
	assert.Error(t, err)

	err = m.Retrieve(t.Context(), "id", "t", nil, nil)
	assert.Error(t, err)

	err = m.List(t.Context(), "t", nil, nil)
	assert.Error(t, err)

	_, err = m.Create(t.Context(), "id", "t", nil, nil)
	assert.Error(t, err)

	err = m.Update(t.Context(), "id", "t", nil, nil)
	assert.Error(t, err)
}

// Unset Mock getters return zero values instead of panicking.
func TestMock_UnsetGettersReturnZeroValues(t *testing.T) {
	m := &Mock{}

	assert.Equal(t, "", m.GetType())
	assert.Equal(t, "", m.GetName())
	assert.Nil(t, m.GetClient())
	assert.Nil(t, m.GetLogger())
	assert.Nil(t, m.GetCounterCounted())
	assert.Nil(t, m.GetCounterCreatedFailed())
}

// DeleteFromMany through a Map containing a fully-unset Mock must fail
// gracefully end-to-end (regression: this used to be a process crash).
func TestDeleteFromMany_UnsetMockFailsGracefully(t *testing.T) {
	m := Map{"broken": &Mock{}}

	_, err := DeleteFromMany(t.Context(), m, "id", "t", &delete.Delete{})
	assert.Error(t, err)
}

// Getter guards still delegate when set.
func TestMock_SetGettersDelegate(t *testing.T) {
	counter := &expvar.Int{}
	m := &Mock{
		MockGetName:           func() string { return "mocked" },
		MockGetCounterCounted: func() *expvar.Int { return counter },
	}

	assert.Equal(t, "mocked", m.GetName())
	assert.Same(t, counter, m.GetCounterCounted())
}
