package shared

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/list"
)

func byteDocuments(documents ...string) [][]byte {
	docs := make([][]byte, len(documents))
	for i, document := range documents {
		docs[i] = []byte(document)
	}

	return docs
}

func TestSortDocuments(t *testing.T) {
	tests := []struct {
		name string
		docs [][]byte
		sort customsort.SortSlice
		want [][]byte
	}{
		{
			name: "string ascending",
			docs: byteDocuments(`{"name":"charlie"}`, `{"name":"alice"}`, `{"name":"bob"}`),
			sort: customsort.SortSlice{{"name", "asc"}},
			want: byteDocuments(`{"name":"alice"}`, `{"name":"bob"}`, `{"name":"charlie"}`),
		},
		{
			name: "string descending",
			docs: byteDocuments(`{"name":"charlie"}`, `{"name":"alice"}`, `{"name":"bob"}`),
			sort: customsort.SortSlice{{"name", "desc"}},
			want: byteDocuments(`{"name":"charlie"}`, `{"name":"bob"}`, `{"name":"alice"}`),
		},
		{
			name: "number ascending",
			docs: byteDocuments(`{"value":2.5}`, `{"value":10}`, `{"value":-1}`),
			sort: customsort.SortSlice{{"value", "asc"}},
			want: byteDocuments(`{"value":-1}`, `{"value":2.5}`, `{"value":10}`),
		},
		{
			name: "number descending",
			docs: byteDocuments(`{"value":2.5}`, `{"value":10}`, `{"value":-1}`),
			sort: customsort.SortSlice{{"value", "desc"}},
			want: byteDocuments(`{"value":10}`, `{"value":2.5}`, `{"value":-1}`),
		},
		{
			name: "boolean false before true",
			docs: byteDocuments(`{"enabled":true,"id":"first"}`, `{"enabled":false}`, `{"enabled":true,"id":"second"}`),
			sort: customsort.SortSlice{{"enabled", "asc"}},
			want: byteDocuments(`{"enabled":false}`, `{"enabled":true,"id":"first"}`, `{"enabled":true,"id":"second"}`),
		},
		{
			name: "boolean descending",
			docs: byteDocuments(`{"enabled":false}`, `{"enabled":true}`),
			sort: customsort.SortSlice{{"enabled", "desc"}},
			want: byteDocuments(`{"enabled":true}`, `{"enabled":false}`),
		},
		{
			name: "multiple keys break primary key ties",
			docs: byteDocuments(
				`{"group":"b","score":1}`,
				`{"group":"a","score":1}`,
				`{"group":"a","score":3}`,
				`{"group":"a","score":2}`,
			),
			sort: customsort.SortSlice{{"group", "asc"}, {"score", "desc"}},
			want: byteDocuments(
				`{"group":"a","score":3}`,
				`{"group":"a","score":2}`,
				`{"group":"a","score":1}`,
				`{"group":"b","score":1}`,
			),
		},
		{
			name: "mixed kinds use deterministic rank order",
			docs: byteDocuments(
				`{"value":{"nested":true},"id":"object"}`,
				`{"value":"2","id":"string"}`,
				`{"value":2,"id":"number"}`,
				`{"value":true,"id":"true"}`,
				`{"value":null,"id":"nil"}`,
				`{"value":false,"id":"false"}`,
				`{"value":[],"id":"array"}`,
			),
			sort: customsort.SortSlice{{"value", "asc"}},
			want: byteDocuments(
				`{"value":null,"id":"nil"}`,
				`{"value":false,"id":"false"}`,
				`{"value":true,"id":"true"}`,
				`{"value":2,"id":"number"}`,
				`{"value":"2","id":"string"}`,
				`{"value":{"nested":true},"id":"object"}`,
				`{"value":[],"id":"array"}`,
			),
		},
		{
			name: "dotted path resolves nested objects",
			docs: byteDocuments(
				`{"a":{"b":{"c":"middle"}}}`,
				`{"a":{"b":{"c":"last"}}}`,
				`{"a":{"b":{"c":"first"}}}`,
			),
			sort: customsort.SortSlice{{"a.b.c", "asc"}},
			want: byteDocuments(
				`{"a":{"b":{"c":"first"}}}`,
				`{"a":{"b":{"c":"last"}}}`,
				`{"a":{"b":{"c":"middle"}}}`,
			),
		},
		{
			name: "missing key sorts first without error",
			docs: byteDocuments(`{"name":"bob"}`, `{"id":"missing"}`, `{"name":"alice"}`),
			sort: customsort.SortSlice{{"name", "asc"}},
			want: byteDocuments(`{"id":"missing"}`, `{"name":"alice"}`, `{"name":"bob"}`),
		},
		{
			name: "unresolved dotted path sorts first without error",
			docs: byteDocuments(
				`{"a":"not-an-object","id":"scalar"}`,
				`{"a":{},"id":"missing-child"}`,
				`{"a":{"b":"found"},"id":"resolved"}`,
			),
			sort: customsort.SortSlice{{"a.b", "asc"}},
			want: byteDocuments(
				`{"a":"not-an-object","id":"scalar"}`,
				`{"a":{},"id":"missing-child"}`,
				`{"a":{"b":"found"},"id":"resolved"}`,
			),
		},
		{
			name: "literal dotted top level key takes precedence",
			docs: byteDocuments(
				`{"a.b":"z","a":{"b":"a"}}`,
				`{"a.b":"a","a":{"b":"z"}}`,
			),
			sort: customsort.SortSlice{{"a.b", "asc"}},
			want: byteDocuments(
				`{"a.b":"a","a":{"b":"z"}}`,
				`{"a.b":"z","a":{"b":"a"}}`,
			),
		},
		{
			name: "empty sort leaves input untouched",
			docs: byteDocuments(`{"id":"first"}`, `{"id":"second"}`),
			sort: nil,
			want: byteDocuments(`{"id":"first"}`, `{"id":"second"}`),
		},
		{
			name: "empty documents leave input untouched",
			docs: nil,
			sort: customsort.SortSlice{{"name", "asc"}},
			want: nil,
		},
		{
			name: "stable for equal requested keys",
			docs: byteDocuments(
				`{"group":"same","score":1,"id":"first"}`,
				`{"group":"same","score":1,"id":"second"}`,
				`{"group":"same","score":1,"id":"third"}`,
			),
			sort: customsort.SortSlice{{"group", "asc"}, {"score", "asc"}},
			want: byteDocuments(
				`{"group":"same","score":1,"id":"first"}`,
				`{"group":"same","score":1,"id":"second"}`,
				`{"group":"same","score":1,"id":"third"}`,
			),
		},
		{
			name: "uppercase ASC is accepted",
			docs: byteDocuments(`{"value":2}`, `{"value":1}`),
			sort: customsort.SortSlice{{"value", "ASC"}},
			want: byteDocuments(`{"value":1}`, `{"value":2}`),
		},
		{
			name: "lowercase asc is accepted",
			docs: byteDocuments(`{"value":2}`, `{"value":1}`),
			sort: customsort.SortSlice{{"value", "asc"}},
			want: byteDocuments(`{"value":1}`, `{"value":2}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SortDocuments(tt.docs, tt.sort)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSortDocumentsErrors(t *testing.T) {
	tests := []struct {
		name string
		docs [][]byte
		sort customsort.SortSlice
	}{
		{
			name: "malformed JSON",
			docs: byteDocuments(`{"value":1}`, `{"value":`),
			sort: customsort.SortSlice{{"value", "asc"}},
		},
		{
			name: "sort pair has one element",
			docs: byteDocuments(`{"value":1}`),
			sort: customsort.SortSlice{{"value"}},
		},
		{
			name: "sort pair has three elements",
			docs: byteDocuments(`{"value":1}`),
			sort: customsort.SortSlice{{"value", "asc", "extra"}},
		},
		{
			name: "unrecognised word",
			docs: byteDocuments(`{"value":1}`),
			sort: customsort.SortSlice{{"value", "ascending"}},
		},
		{
			name: "order with trailing space",
			docs: byteDocuments(`{"value":1}`),
			sort: customsort.SortSlice{{"value", "ASC "}},
		},
		{
			name: "empty order",
			docs: byteDocuments(`{"value":1}`),
			sort: customsort.SortSlice{{"value", ""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SortDocuments(tt.docs, tt.sort)

			require.Error(t, err)
			assert.Nil(t, got)
		})
	}
}

func TestPaginateDocuments(t *testing.T) {
	all := byteDocuments("zero", "one", "two", "three")
	tests := []struct {
		name   string
		docs   [][]byte
		offset int
		limit  int
		want   [][]byte
	}{
		{name: "offset only", docs: all, offset: 2, want: byteDocuments("two", "three")},
		{name: "limit only", docs: all, limit: 2, want: byteDocuments("zero", "one")},
		{name: "offset and limit", docs: all, offset: 1, limit: 2, want: byteDocuments("one", "two")},
		{name: "offset equals length", docs: all, offset: 4, want: [][]byte{}},
		{name: "offset past end", docs: all, offset: 8, want: [][]byte{}},
		{name: "limit exceeds remainder", docs: all, offset: 2, limit: 10, want: byteDocuments("two", "three")},
		{name: "zero limit means no limit", docs: all, limit: 0, want: all},
		{name: "negative limit means no limit", docs: all, limit: -1, want: all},
		{name: "negative offset is ignored", docs: all, offset: -2, limit: 2, want: byteDocuments("zero", "one")},
		{name: "empty input", docs: nil, offset: 2, limit: 2, want: [][]byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, PaginateDocuments(tt.docs, tt.offset, tt.limit))
		})
	}
}

func TestProjectDocuments(t *testing.T) {
	tests := []struct {
		name   string
		docs   [][]byte
		fields []string
		want   [][]byte
	}{
		{
			name:   "subset of fields",
			docs:   byteDocuments(`{"id":"1","name":"alice","age":30}`),
			fields: []string{"id", "name"},
			want:   byteDocuments(`{"id":"1","name":"alice"}`),
		},
		{
			name:   "empty fields leave documents untouched",
			docs:   byteDocuments(`{"name": "preserve formatting"}`),
			fields: nil,
			want:   byteDocuments(`{"name": "preserve formatting"}`),
		},
		{
			name:   "absent requested field is omitted",
			docs:   byteDocuments(`{"id":"1","name":"alice"}`),
			fields: []string{"id", "missing"},
			want:   byteDocuments(`{"id":"1"}`),
		},
		{
			name:   "objects and arrays are preserved intact",
			docs:   byteDocuments(`{"object":{"nested":true},"array":[1,{"two":2}],"drop":"value"}`),
			fields: []string{"object", "array"},
			want:   byteDocuments(`{"array":[1,{"two":2}],"object":{"nested":true}}`),
		},
		{
			name:   "empty documents leave input untouched",
			docs:   nil,
			fields: []string{"id"},
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ProjectDocuments(tt.docs, tt.fields)

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestProjectDocumentsMalformedJSON(t *testing.T) {
	got, err := ProjectDocuments(byteDocuments(`{"valid":true}`, `{"invalid":`), []string{"valid"})

	require.Error(t, err)
	assert.Nil(t, got)
}

func TestApplyListParams(t *testing.T) {
	docs := byteDocuments(
		`{"id":"one","score":1,"drop":true}`,
		`{"id":"three","score":3,"drop":true}`,
		`{"id":"two","score":2,"drop":true}`,
	)

	t.Run("nil params leave documents untouched", func(t *testing.T) {
		got, err := ApplyListParams(docs, nil)

		require.NoError(t, err)
		assert.Equal(t, docs, got)
	})

	t.Run("sort then paginate then project", func(t *testing.T) {
		got, err := ApplyListParams(docs, &list.List{
			Sort:   customsort.SortSlice{{"score", "desc"}},
			Offset: 1,
			Limit:  1,
			Fields: []string{"id"},
		})

		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.JSONEq(t, `{"id":"two"}`, string(got[0]))
	})

	t.Run("sort error propagates with no result", func(t *testing.T) {
		got, err := ApplyListParams(docs, &list.List{
			Sort: customsort.SortSlice{{"score", "ascending"}},
		})

		require.Error(t, err)
		assert.Nil(t, got)
	})
}

func TestAssembleItems(t *testing.T) {
	tests := []struct {
		name string
		docs [][]byte
		want string
	}{
		{name: "zero documents", docs: nil, want: `{"items":[]}`},
		{name: "one document", docs: byteDocuments(`{"id":1}`), want: `{"items":[{"id":1}]}`},
		{
			name: "several documents",
			docs: byteDocuments(`{"id":1}`, `{"id":2}`, `{"id":3}`),
			want: `{"items":[{"id":1},{"id":2},{"id":3}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AssembleItems(tt.docs)
			assert.Equal(t, tt.want, string(got))

			var envelope struct {
				Items []json.RawMessage `json:"items"`
			}
			require.NoError(t, json.Unmarshal(got, &envelope))
			require.Len(t, envelope.Items, len(tt.docs))

			for i := range tt.docs {
				assert.JSONEq(t, string(tt.docs[i]), string(envelope.Items[i]))
			}
		})
	}
}
