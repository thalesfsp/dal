package shared

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/list"
)

//////
// Const, vars, and types.
//////

// typeRank orders JSON value kinds so that a comparison between two different
// kinds is still deterministic. Without it, sorting a field that is a string in
// one document and a number in another would depend on the input order.
const (
	rankNil = iota
	rankBool
	rankNumber
	rankString
	rankOther
)

//////
// Helpers.
//////

// rankOf returns the ordering rank of a decoded JSON value.
func rankOf(v any) int {
	switch v.(type) {
	case nil:
		return rankNil
	case bool:
		return rankBool
	case float64:
		return rankNumber
	case string:
		return rankString
	default:
		// Objects and arrays. They have no meaningful order, so they compare
		// equal to each other and sort last.
		return rankOther
	}
}

// compareValues returns -1, 0 or 1 ordering a against b.
//
// Values of different kinds are ordered by kind so the result is a total order
// and the sort is stable regardless of how the backend enumerated the records.
func compareValues(a, b any) int {
	rA, rB := rankOf(a), rankOf(b)

	if rA != rB {
		if rA < rB {
			return -1
		}

		return 1
	}

	switch av := a.(type) {
	case bool:
		bv, _ := b.(bool)

		switch {
		case av == bv:
			return 0
		case !av:
			return -1
		default:
			return 1
		}
	case float64:
		bv, _ := b.(float64)

		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case string:
		bv, _ := b.(string)

		return strings.Compare(av, bv)
	default:
		// nil == nil, and objects/arrays are treated as equal.
		return 0
	}
}

// lookup resolves a possibly dotted key (`a.b.c`) against a decoded document.
//
// A key that does not resolve yields nil, which sorts first in ascending order.
// That is deliberate: a document missing the sort field should not abort the
// whole List, it should just sort predictably.
func lookup(doc map[string]any, key string) any {
	if v, ok := doc[key]; ok {
		return v
	}

	parts := strings.Split(key, ".")
	if len(parts) == 1 {
		return nil
	}

	var current any = doc

	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}

		current, ok = m[part]
		if !ok {
			return nil
		}
	}

	return current
}

//////
// Exported functionality.
//////

// SortDocuments orders docs — each a marshalled JSON object — according to s.
//
// s is a `[][]string` of `[key, order]` pairs, applied left to right: the first
// pair is the primary key, the second breaks its ties, and so on. `order` is
// `asc` or `desc`, matching customsort; an unrecognised order is rejected
// rather than silently treated as ascending.
//
// The sort is stable, so documents equal on every requested key keep the order
// the backend produced them in.
func SortDocuments(docs [][]byte, s customsort.SortSlice) ([][]byte, error) {
	if len(s) == 0 || len(docs) == 0 {
		return docs, nil
	}

	// Decode once, up front. Decoding inside the comparator would re-parse the
	// same document O(n log n) times.
	decoded := make([]map[string]any, len(docs))

	for i, doc := range docs {
		var m map[string]any

		if err := json.Unmarshal(doc, &m); err != nil {
			return nil, customerror.NewFailedToError(
				"sort: unmarshal document",
				customerror.WithError(err),
			)
		}

		decoded[i] = m
	}

	// Validate the whole spec before sorting, so an invalid order is reported
	// instead of half-applied.
	keys := make([]string, 0, len(s))
	descs := make([]bool, 0, len(s))

	for _, pair := range s {
		if len(pair) != 2 {
			return nil, customerror.NewInvalidError(
				"sort spec, expected `[key, order]`",
			)
		}

		order := strings.ToLower(pair[1])
		if order != customsort.Asc && order != customsort.Desc {
			return nil, customerror.NewInvalidError(
				"sort order `" + pair[1] + "`, expected `" + customsort.Asc +
					"` or `" + customsort.Desc + "`",
			)
		}

		keys = append(keys, pair[0])
		descs = append(descs, order == customsort.Desc)
	}

	idx := make([]int, len(docs))
	for i := range idx {
		idx[i] = i
	}

	sort.SliceStable(idx, func(a, b int) bool {
		dA, dB := decoded[idx[a]], decoded[idx[b]]

		for k, key := range keys {
			c := compareValues(lookup(dA, key), lookup(dB, key))
			if c == 0 {
				continue
			}

			if descs[k] {
				return c > 0
			}

			return c < 0
		}

		return false
	})

	out := make([][]byte, len(docs))
	for i, j := range idx {
		out[i] = docs[j]
	}

	return out, nil
}

// PaginateDocuments applies offset then limit to docs.
//
// A non-positive limit means "no limit". Limit carries
// `validate:"omitempty,gt=0"`, while list.New() sets it to 10; callers that pass
// a value returned by list.New therefore receive at most 10 records.
//
// An offset past the end yields an empty slice rather than an error.
func PaginateDocuments(docs [][]byte, offset, limit int) [][]byte {
	if offset > 0 {
		if offset >= len(docs) {
			return [][]byte{}
		}

		docs = docs[offset:]
	}

	if limit > 0 && limit < len(docs) {
		docs = docs[:limit]
	}

	return docs
}

// ProjectDocuments reduces each document to the requested fields.
//
// An empty field list returns docs untouched. A field absent from a document is
// omitted rather than emitted as null, so projecting never invents keys.
func ProjectDocuments(docs [][]byte, fields []string) ([][]byte, error) {
	if len(fields) == 0 || len(docs) == 0 {
		return docs, nil
	}

	out := make([][]byte, 0, len(docs))

	for _, doc := range docs {
		var m map[string]any

		if err := json.Unmarshal(doc, &m); err != nil {
			return nil, customerror.NewFailedToError(
				"project: unmarshal document",
				customerror.WithError(err),
			)
		}

		projected := make(map[string]any, len(fields))

		for _, f := range fields {
			if v, ok := m[f]; ok {
				projected[f] = v
			}
		}

		b, err := json.Marshal(projected)
		if err != nil {
			return nil, customerror.NewFailedToError(
				"project: marshal document",
				customerror.WithError(err),
			)
		}

		out = append(out, b)
	}

	return out, nil
}

// ApplyListParams post-processes the documents an in-process adapter collected,
// honoring the List params that adapter cannot push down to its backend.
//
// Order matters and follows SQL: sort the full matched set, then paginate that
// ordering, then project. Projecting first would drop the sort key; paginating
// first would return the wrong page.
//
// It exists so the adapters that must materialize their results anyway —
// memory, file, sftp, bolt, redis — honor Sort, Limit, Offset and Fields
// identically instead of each ignoring a different subset. Backends that can
// push these down (the SQL adapters) should keep doing so; this is for the ones
// that cannot.
//
// A nil prm leaves docs untouched.
func ApplyListParams(docs [][]byte, prm *list.List) ([][]byte, error) {
	if prm == nil {
		return docs, nil
	}

	sorted, err := SortDocuments(docs, prm.Sort)
	if err != nil {
		return nil, err
	}

	paginated := PaginateDocuments(sorted, prm.Offset, prm.Limit)

	return ProjectDocuments(paginated, prm.Fields)
}

// AssembleItems wraps documents into the `{"items":[...]}` envelope the
// in-process adapters unmarshal into the caller's destination.
func AssembleItems(docs [][]byte) []byte {
	var b strings.Builder

	b.WriteString(`{"items":[`)

	for i, doc := range docs {
		if i > 0 {
			b.WriteByte(',')
		}

		b.Write(doc)
	}

	b.WriteString(`]}`)

	return []byte(b.String())
}
