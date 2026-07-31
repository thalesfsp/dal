package shared

import (
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

// Param is the set of operation params supported by MergeParams.
type Param interface {
	count.Count | create.Create | delete.Delete | list.List | retrieve.Retrieve | update.Update
}

// MergeParams returns a copy of defaults with fields set by overrides applied.
//
// The params structs do not track field presence, so zero handling has to be
// defined per field:
//
//   - count.Count: empty Routing is unset (`omitempty,gt=0`); empty Search is
//     unset because it is `required`.
//   - create.Create: nil Any and empty ID, Routing, and TTL are unset
//     (`omitempty,gt=0`).
//   - delete.Delete: empty ID is unset because it is `required`; empty Routing
//     is unset (`omitempty,gt=0`); false Soft is meaningful and is always
//     copied because the field has no `omitempty` validation; empty Target is
//     genuinely ambiguous because it has no validation tag, but is treated as
//     unset so a caller setting another field does not clear a default target.
//   - list.List: nil Any and empty Fields, Routing, Search, SearchRaw, and Sort
//     are unset (`omitempty,gt=0`); zero Limit is unset
//     (`omitempty,gt=0`) and list.New defaults it to 10, so zero cannot express
//     "no limit"; zero Count and Offset are meaningful and always copied
//     (`omitempty,gte=0`), representing an empty result and the first item.
//     Omitted and explicit zero are ambiguous for Count and Offset because the
//     params type has no presence bits; honoring their valid zero value wins.
//   - retrieve.Retrieve: nil Any and empty Fields and Routing are unset
//     (`omitempty,gt=0`); empty ID is unset because it is `required`.
//   - update.Update: nil Any and empty Routing and TTL are unset
//     (`omitempty,gt=0`); empty ID is unset because it is `required`.
//
// Neither input is mutated. The returned struct is a shallow copy; reference
// fields are assigned but never modified by this function.
func MergeParams[T Param](defaults, overrides *T) *T {
	merged := *defaults
	if overrides == nil {
		return &merged
	}

	switch dst := any(&merged).(type) {
	case *count.Count:
		if src, ok := any(overrides).(*count.Count); ok {
			mergeCountParams(dst, src)
		}
	case *create.Create:
		if src, ok := any(overrides).(*create.Create); ok {
			mergeCreateParams(dst, src)
		}
	case *delete.Delete:
		if src, ok := any(overrides).(*delete.Delete); ok {
			mergeDeleteParams(dst, src)
		}
	case *list.List:
		if src, ok := any(overrides).(*list.List); ok {
			mergeListParams(dst, src)
		}
	case *retrieve.Retrieve:
		if src, ok := any(overrides).(*retrieve.Retrieve); ok {
			mergeRetrieveParams(dst, src)
		}
	case *update.Update:
		if src, ok := any(overrides).(*update.Update); ok {
			mergeUpdateParams(dst, src)
		}
	}

	return &merged
}

func mergeCountParams(dst, src *count.Count) {
	if len(src.Routing) > 0 {
		dst.Routing = src.Routing
	}
	if src.Search != "" {
		dst.Search = src.Search
	}
}

func mergeCreateParams(dst, src *create.Create) {
	if src.Any != nil {
		dst.Any = src.Any
	}
	if src.ID != "" {
		dst.ID = src.ID
	}
	if src.Routing != "" {
		dst.Routing = src.Routing
	}
	if src.TTL != 0 {
		dst.TTL = src.TTL
	}
}

func mergeDeleteParams(dst, src *delete.Delete) {
	if src.ID != "" {
		dst.ID = src.ID
	}
	if src.Routing != "" {
		dst.Routing = src.Routing
	}
	dst.Soft = src.Soft
	if src.Target != "" {
		dst.Target = src.Target
	}
}

func mergeListParams(dst, src *list.List) {
	if src.Any != nil {
		dst.Any = src.Any
	}
	dst.Count = src.Count
	if len(src.Fields) > 0 {
		dst.Fields = src.Fields
	}
	if src.Limit != 0 {
		dst.Limit = src.Limit
	}
	dst.Offset = src.Offset
	if len(src.Routing) > 0 {
		dst.Routing = src.Routing
	}
	if src.Search != "" {
		dst.Search = src.Search
	}
	if src.SearchRaw != "" {
		dst.SearchRaw = src.SearchRaw
	}
	if len(src.Sort) > 0 {
		dst.Sort = src.Sort
	}
}

func mergeRetrieveParams(dst, src *retrieve.Retrieve) {
	if src.Any != nil {
		dst.Any = src.Any
	}
	if src.ID != "" {
		dst.ID = src.ID
	}
	if len(src.Fields) > 0 {
		dst.Fields = src.Fields
	}
	if src.Routing != "" {
		dst.Routing = src.Routing
	}
}

func mergeUpdateParams(dst, src *update.Update) {
	if src.Any != nil {
		dst.Any = src.Any
	}
	if src.ID != "" {
		dst.ID = src.ID
	}
	if src.Routing != "" {
		dst.Routing = src.Routing
	}
	if src.TTL != 0 {
		dst.TTL = src.TTL
	}
}
