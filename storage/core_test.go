package storage

import (
	"context"
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/sypl/v2"
)

//////
// Storage factory + IMeta getters.
//////

func TestNew_HappyPathAndGetters(t *testing.T) {
	s, err := New(t.Context(), "coretest")
	require.NoError(t, err)
	require.NotNil(t, s)

	assert.Equal(t, "coretest", s.GetName())
	assert.Equal(t, Type, s.GetType())
	assert.NotNil(t, s.GetLogger())

	// Every metric getter must return a live counter wired by New.
	counters := []*expvar.Int{
		s.GetCounterCounted(),
		s.GetCounterCountedFailed(),
		s.GetCounterCreated(),
		s.GetCounterCreatedFailed(),
		s.GetCounterDeleted(),
		s.GetCounterDeletedFailed(),
		s.GetCounterListed(),
		s.GetCounterListedFailed(),
		s.GetCounterPingFailed(),
		s.GetCounterRetrieved(),
		s.GetCounterRetrievedFailed(),
		s.GetCounterUpdated(),
		s.GetCounterUpdatedFailed(),
	}

	for i, c := range counters {
		require.NotNilf(t, c, "counter %d is nil", i)
	}
}

func TestNew_ValidationFailures(t *testing.T) {
	// Empty name violates `gte=1`.
	_, err := New(t.Context(), "")
	assert.Error(t, err)

	// Uppercase name violates `lowercase`.
	_, err = New(t.Context(), "NotLowercase")
	assert.Error(t, err)
}

//////
// Options.
//////

func TestOptions(t *testing.T) {
	o, err := NewOptions[*count.Count]()
	require.NoError(t, err)
	require.NotNil(t, o)

	hook := func(ctx context.Context, strg IStorage, id, target string, data any, param *count.Count) error {
		return nil
	}

	require.NoError(t, WithPreHook(hook)(o))
	assert.NotNil(t, o.PreHookFunc)

	require.NoError(t, WithPostHook(hook)(o))
	assert.NotNil(t, o.PostHookFunc)

	require.NoError(t, WithDatabase[*count.Count]("db1")(o))
	assert.Equal(t, "db1", o.Database)

	// Nil hooks are rejected with the dedicated errors.
	assert.ErrorIs(t, WithPreHook[*count.Count](nil)(o), ErrRequiredPreHook)
	assert.ErrorIs(t, WithPostHook[*count.Count](nil)(o), ErrRequiredPostHook)
}

//////
// Operation.
//////

func TestOperation_String(t *testing.T) {
	assert.Equal(t, "count", OperationCount.String())
	assert.Equal(t, "create", OperationCreate.String())
	assert.Equal(t, "delete", OperationDelete.String())
	assert.Equal(t, "list", OperationList.String())
	assert.Equal(t, "retrieve", OperationRetrieve.String())
	assert.Equal(t, "update", OperationUpdate.String())
}

//////
// Util.
//////

func TestParseToStruct(t *testing.T) {
	from := map[string]string{"name": "n1", "version": "v1"}

	var to zeroTestData

	require.NoError(t, ParseToStruct(from, &to))
	assert.Equal(t, "n1", to.Name)
	assert.Equal(t, "v1", to.Version)

	// Unmarshalable input (a channel) must surface the marshal error.
	assert.Error(t, ParseToStruct(make(chan int), &to))

	// Non-pointer destination must surface the unmarshal error.
	assert.Error(t, ParseToStruct(from, zeroTestData{}))
}

func TestFlatten2D(t *testing.T) {
	assert.Equal(t, []int{1, 2, 3, 4}, Flatten2D([][]int{{1, 2}, {3}, {}, {4}}))
	assert.Nil(t, Flatten2D[int](nil))
}

//////
// Map helpers.
//////

func TestMap_String(t *testing.T) {
	assert.Empty(t, Map{}.String())
	assert.Equal(t, "solo", Map{"solo": &Mock{}}.String())

	// Two entries: order is nondeterministic, but both names and the
	// separator must be present.
	s := Map{"a": &Mock{}, "b": &Mock{}}.String()
	assert.Contains(t, s, "a")
	assert.Contains(t, s, "b")
	assert.Contains(t, s, ", ")
}

//////
// Fan-out error paths: every helper must aggregate and return errors.
//////

// failingMock returns a Mock whose every operation fails.
func failingMock(name string) *Mock {
	return &Mock{
		MockCount: func(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error) {
			return 0, assert.AnError
		},
		MockCreate: func(ctx context.Context, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (string, error) {
			return "", assert.AnError
		},
		MockDelete: func(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
			return assert.AnError
		},
		MockRetrieve: func(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error {
			return assert.AnError
		},
		MockList: func(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error {
			return assert.AnError
		},
		MockUpdate: func(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error {
			return assert.AnError
		},
		MockGetName: func() string { return name },
		MockGetType: func() string { return "mock" },
	}
}

func TestFanOut_ErrorPaths(t *testing.T) {
	ctx := t.Context()
	m := Map{"bad": failingMock("bad")}

	_, err := CountFromMany(ctx, m, "tgt", &count.Count{})
	assert.Error(t, err)

	_, err = CreateIntoMany(ctx, m, "id", "tgt", &zeroTestData{}, &create.Create{})
	assert.Error(t, err)

	_, err = DeleteFromMany(ctx, m, "id", "tgt", &delete.Delete{})
	assert.Error(t, err)

	_, err = ListFromMany[zeroTestData](ctx, m, "tgt", &list.List{})
	assert.Error(t, err)

	_, err = RetrieveFromMany[zeroTestData](ctx, m, "id", "tgt", &retrieve.Retrieve{})
	assert.Error(t, err)

	_, err = UpdateIntoMany(ctx, m, "id", "tgt", &zeroTestData{}, &update.Update{})
	assert.Error(t, err)

	bad := failingMock("bad")

	_, err = CreateMany(ctx, bad, "tgt", &create.Create{}, map[string]zeroTestData{"k": {}})
	assert.Error(t, err)

	_, err = DeleteMany(ctx, bad, "tgt", &delete.Delete{}, "id-1")
	assert.Error(t, err)

	_, err = RetrieveMany[zeroTestData](ctx, bad, "tgt", &retrieve.Retrieve{}, "id-1")
	assert.Error(t, err)

	_, err = UpdateMany(ctx, bad, "tgt", &update.Update{}, map[string]zeroTestData{"k": {}})
	assert.Error(t, err)
}

// Happy paths of the N:1 helpers (the 1:N ones are covered elsewhere).
func TestFanOut_ManyAgainstOne_HappyPath(t *testing.T) {
	ctx := t.Context()
	ok := newFullMock("ok", 1)

	ids, err := CreateMany(ctx, ok, "tgt", &create.Create{}, map[string]zeroTestData{"id-1": {}, "id-2": {}})
	require.NoError(t, err)
	assert.Len(t, ids, 2)

	deleted, err := DeleteMany(ctx, ok, "tgt", &delete.Delete{}, "id-1", "id-2")
	require.NoError(t, err)
	assert.Equal(t, []bool{true, true}, deleted)

	got, err := RetrieveMany[zeroTestData](ctx, ok, "tgt", &retrieve.Retrieve{}, "id-1", "id-2")
	require.NoError(t, err)
	assert.Len(t, got, 2)

	updated, err := UpdateMany(ctx, ok, "tgt", &update.Update{}, map[string]zeroTestData{"id-1": {}})
	require.NoError(t, err)
	assert.Equal(t, []bool{true}, updated)
}

//////
// Interface-level generic wrappers: error branches.
//////

func TestGenericWrappers_ErrorBranches(t *testing.T) {
	ctx := t.Context()
	bad := failingMock("bad")

	_, err := Retrieve[zeroTestData](ctx, bad, "id", "tgt", &retrieve.Retrieve{})
	assert.Error(t, err)

	_, err = List[[]zeroTestData](ctx, bad, "tgt", &list.List{})
	assert.Error(t, err)

	_, err = Count(ctx, bad, "tgt", &count.Count{})
	assert.Error(t, err)

	_, err = Create(ctx, bad, "id", "tgt", &zeroTestData{}, &create.Create{})
	assert.Error(t, err)

	assert.Error(t, Update(ctx, bad, "id", "tgt", &zeroTestData{}, &update.Update{}))
	assert.Error(t, Delete(ctx, bad, "id", "tgt", &delete.Delete{}))
}

//////
// Mock getters: set variants must delegate; unset return zero values.
//////

func TestMock_AllGetters(t *testing.T) {
	c := expvar.NewInt("core_test.mock.counter")

	full := &Mock{
		MockGetType:                   func() string { return "mock" },
		MockGetClient:                 func() any { return 42 },
		MockGetLogger:                 func() sypl.ISypl { return nil },
		MockGetName:                   func() string { return "m" },
		MockGetCounterCounted:         func() *expvar.Int { return c },
		MockGetCounterCountedFailed:   func() *expvar.Int { return c },
		MockGetCounterCreated:         func() *expvar.Int { return c },
		MockGetCounterCreatedFailed:   func() *expvar.Int { return c },
		MockGetCounterDeleted:         func() *expvar.Int { return c },
		MockGetCounterDeletedFailed:   func() *expvar.Int { return c },
		MockGetCounterListed:          func() *expvar.Int { return c },
		MockGetCounterListedFailed:    func() *expvar.Int { return c },
		MockGetCounterPingFailed:      func() *expvar.Int { return c },
		MockGetCounterRetrieved:       func() *expvar.Int { return c },
		MockGetCounterRetrievedFailed: func() *expvar.Int { return c },
		MockGetCounterUpdated:         func() *expvar.Int { return c },
		MockGetCounterUpdatedFailed:   func() *expvar.Int { return c },
	}

	assert.Equal(t, "mock", full.GetType())
	assert.Equal(t, 42, full.GetClient())
	assert.Nil(t, full.GetLogger())
	assert.Equal(t, "m", full.GetName())
	assert.Same(t, c, full.GetCounterCounted())
	assert.Same(t, c, full.GetCounterCountedFailed())
	assert.Same(t, c, full.GetCounterCreated())
	assert.Same(t, c, full.GetCounterCreatedFailed())
	assert.Same(t, c, full.GetCounterDeleted())
	assert.Same(t, c, full.GetCounterDeletedFailed())
	assert.Same(t, c, full.GetCounterListed())
	assert.Same(t, c, full.GetCounterListedFailed())
	assert.Same(t, c, full.GetCounterPingFailed())
	assert.Same(t, c, full.GetCounterRetrieved())
	assert.Same(t, c, full.GetCounterRetrievedFailed())
	assert.Same(t, c, full.GetCounterUpdated())
	assert.Same(t, c, full.GetCounterUpdatedFailed())

	empty := &Mock{}
	assert.Empty(t, empty.GetType())
	assert.Nil(t, empty.GetClient())
	assert.Nil(t, empty.GetLogger())
	assert.Empty(t, empty.GetName())
	assert.Nil(t, empty.GetCounterCounted())
	assert.Nil(t, empty.GetCounterCountedFailed())
	assert.Nil(t, empty.GetCounterCreated())
	assert.Nil(t, empty.GetCounterCreatedFailed())
	assert.Nil(t, empty.GetCounterDeleted())
	assert.Nil(t, empty.GetCounterDeletedFailed())
	assert.Nil(t, empty.GetCounterListed())
	assert.Nil(t, empty.GetCounterListedFailed())
	assert.Nil(t, empty.GetCounterPingFailed())
	assert.Nil(t, empty.GetCounterRetrieved())
	assert.Nil(t, empty.GetCounterRetrievedFailed())
	assert.Nil(t, empty.GetCounterUpdated())
	assert.Nil(t, empty.GetCounterUpdatedFailed())
}
