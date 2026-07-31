package shared

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/field"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

func TestMergeParams_EveryField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "count Routing and Search",
			run: func(t *testing.T) {
				t.Helper()

				defaults := &count.Count{Routing: []string{"default-route"}, Search: "*"}
				overrides := &count.Count{Routing: []string{"caller-route"}}
				want := &count.Count{Routing: []string{"caller-route"}, Search: "*"}
				assertMerge(t, defaults, overrides, want)
			},
		},
		{
			name: "create Any ID Routing and TTL",
			run: func(t *testing.T) {
				t.Helper()

				defaults := &create.Create{Any: "default", ID: "default-id", Routing: "default-route", TTL: time.Minute}
				overrides := &create.Create{Any: "caller", ID: "caller-id", Routing: "caller-route", TTL: time.Hour}
				assertMerge(t, defaults, overrides, overrides)
			},
		},
		{
			name: "delete ID Routing Soft and Target",
			run: func(t *testing.T) {
				t.Helper()

				defaults := &delete.Delete{ID: "default-id", Routing: "default-route", Soft: true, Target: "default-target"}
				overrides := &delete.Delete{ID: "caller-id", Routing: "caller-route", Soft: false, Target: "caller-target"}
				assertMerge(t, defaults, overrides, overrides)
			},
		},
		{
			name: "list every field",
			run: func(t *testing.T) {
				t.Helper()

				defaults := &list.List{
					Any:       "default",
					Count:     7,
					Fields:    field.Fields{"default"},
					Limit:     10,
					Offset:    3,
					Routing:   []string{"default-route"},
					Search:    "default-search",
					SearchRaw: "default-raw",
					Sort:      customsort.SortSlice{{"default", customsort.Asc}},
				}
				overrides := &list.List{
					Any:       "caller",
					Count:     9,
					Fields:    field.Fields{"caller"},
					Limit:     20,
					Offset:    4,
					Routing:   []string{"caller-route"},
					Search:    "caller-search",
					SearchRaw: "caller-raw",
					Sort:      customsort.SortSlice{{"caller", customsort.Desc}},
				}
				assertMerge(t, defaults, overrides, overrides)
			},
		},
		{
			name: "retrieve Any ID Fields and Routing",
			run: func(t *testing.T) {
				t.Helper()

				defaults := &retrieve.Retrieve{
					Any: "default", ID: "default-id", Fields: field.Fields{"default"}, Routing: "default-route",
				}
				overrides := &retrieve.Retrieve{
					Any: "caller", ID: "caller-id", Fields: field.Fields{"caller"}, Routing: "caller-route",
				}
				assertMerge(t, defaults, overrides, overrides)
			},
		},
		{
			name: "update Any ID Routing and TTL",
			run: func(t *testing.T) {
				t.Helper()

				defaults := &update.Update{Any: "default", ID: "default-id", Routing: "default-route", TTL: time.Minute}
				overrides := &update.Update{Any: "caller", ID: "caller-id", Routing: "caller-route", TTL: time.Hour}
				assertMerge(t, defaults, overrides, overrides)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestMergeParams_ZeroRules(t *testing.T) {
	t.Parallel()

	t.Run("unset fields inherit defaults", func(t *testing.T) {
		t.Parallel()

		defaults := &list.List{
			Fields: field.Fields{"default"},
			Limit:  10,
			Search: "*",
			Sort:   customsort.SortSlice{{"id", customsort.Asc}},
		}
		overrides := &list.List{SearchRaw: "caller-raw"}
		want := &list.List{
			Fields:    field.Fields{"default"},
			Limit:     10,
			Search:    "*",
			SearchRaw: "caller-raw",
			Sort:      customsort.SortSlice{{"id", customsort.Asc}},
		}
		assertMerge(t, defaults, overrides, want)
	})

	t.Run("meaningful explicit zeros override defaults", func(t *testing.T) {
		t.Parallel()

		deleteDefaults := &delete.Delete{Soft: true}
		deleteOverrides := &delete.Delete{Soft: false}
		assertMerge(t, deleteDefaults, deleteOverrides, &delete.Delete{Soft: false})

		listDefaults := &list.List{Count: 8, Limit: 10, Offset: 6}
		listOverrides := &list.List{Count: 0, Limit: 0, Offset: 0}
		assertMerge(t, listDefaults, listOverrides, &list.List{Count: 0, Limit: 10, Offset: 0})
	})

	t.Run("nil params yield pure defaults", func(t *testing.T) {
		t.Parallel()

		defaults := &list.List{Limit: 10, Search: "*"}
		got := MergeParams(defaults, nil)
		if !reflect.DeepEqual(got, defaults) {
			t.Fatalf("MergeParams() = %#v, want pure defaults %#v", got, defaults)
		}
		if got == defaults {
			t.Fatal("MergeParams() returned the defaults pointer, want an independent struct")
		}
	})
}

func assertMerge[T Param](t *testing.T, defaults, overrides, want *T) {
	t.Helper()

	defaultsBefore := snapshotForComparison(t, defaults)
	overridesBefore := snapshotForComparison(t, overrides)

	got := MergeParams(defaults, overrides)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MergeParams() = %#v, want %#v", got, want)
	}
	if defaultsAfter := snapshotForComparison(t, defaults); !bytes.Equal(defaultsAfter, defaultsBefore) {
		t.Fatalf("MergeParams() mutated defaults: got %s, before %s", defaultsAfter, defaultsBefore)
	}
	if overridesAfter := snapshotForComparison(t, overrides); !bytes.Equal(overridesAfter, overridesBefore) {
		t.Fatalf("MergeParams() mutated caller params: got %s, before %s", overridesAfter, overridesBefore)
	}
}

func snapshotForComparison[T any](t *testing.T, value *T) []byte {
	t.Helper()

	snapshot, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}

	return snapshot
}
