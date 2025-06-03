package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

// TestDataS is a test data struct.
type TestDataS struct {
	K string `json:"k"`
}

var m1 = &Mock{
	MockCount: func(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error) {
		return 10, nil
	},

	MockDelete: func(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
		return nil
	},

	MockRetrieve: func(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error {
		result := `{"k": "mock1"}`

		return shared.Unmarshal([]byte(result), v)
	},

	MockList: func(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error {
		result := `["mock1", "mock2"]`

		return shared.Unmarshal([]byte(result), v)
	},

	MockCreate: func(ctx context.Context, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (string, error) {
		return "mock1", nil
	},

	MockUpdate: func(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error {
		result := `{"k": "mock1"}`

		return shared.Unmarshal([]byte(result), v)
	},

	MockGetName: func() string {
		return "m1"
	},
}

var m2 = &Mock{
	MockCount: func(ctx context.Context, target string, prm *count.Count, options ...Func[*count.Count]) (int64, error) {
		return 20, nil
	},

	MockDelete: func(ctx context.Context, id, target string, prm *delete.Delete, options ...Func[*delete.Delete]) error {
		return nil
	},

	MockRetrieve: func(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...Func[*retrieve.Retrieve]) error {
		result := `{"k": "mock2"}`

		return shared.Unmarshal([]byte(result), v)
	},

	MockList: func(ctx context.Context, target string, v any, prm *list.List, options ...Func[*list.List]) error {
		// var asd []string
		result := `["mock3", "mock4"]`

		return shared.Unmarshal([]byte(result), &v)
	},

	MockCreate: func(ctx context.Context, id, target string, v any, prm *create.Create, options ...Func[*create.Create]) (string, error) {
		return "mock2", nil
	},

	MockUpdate: func(ctx context.Context, id, target string, v any, prm *update.Update, options ...Func[*update.Update]) error {
		result := `{"k": "mock2"}`

		return shared.Unmarshal([]byte(result), v)
	},

	MockGetName: func() string {
		return "m2"
	},
}

func TestCreateIntoMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []string
		wantErr bool
	}{
		{
			name: "Should work",
			want: []string{"mock1", "mock2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			m := make(Map)
			m["m1"] = m1
			m["m2"] = m2

			got, err := CreateIntoMany(ctx, m, "id", "target", "value", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Contains(t, tt.want, got[0])
			assert.Contains(t, tt.want, got[1])
		})
	}
}

func TestDeleteFromMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []bool
		wantErr bool
	}{
		{
			name: "Should work",
			want: []bool{true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DeleteFromMany(context.Background(), Map{"m1": m1, "m2": m2}, "id", "target", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Contains(t, tt.want, got[0])
			assert.Contains(t, tt.want, got[1])
		})
	}
}

func TestListFromMany(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "Should work",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ListFromMany[string](context.Background(), Map{"m1": m1, "m2": m2}, "target", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Contains(t, got, "mock1")
			assert.Contains(t, got, "mock2")
			assert.Contains(t, got, "mock3")
			assert.Contains(t, got, "mock4")
		})
	}
}

func TestRetrieveFromMany(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "Should work",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RetrieveFromMany[*TestDataS](context.Background(), Map{"m1": m1, "m2": m2}, "id", "target", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("RetrieveMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got[0].K != "mock1" && got[0].K != "mock2" {
				t.Errorf("RetrieveMany() got = %v, want %v", got[0].K, "mock1 or mock2")
			}

			if got[1].K != "mock1" && got[1].K != "mock2" {
				t.Errorf("RetrieveMany() got = %v, want %v", got[1].K, "mock1 or mock2")
			}
		})
	}
}

func TestUpdateIntoMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []bool
		wantErr bool
	}{
		{
			name: "Should work",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UpdateIntoMany(context.Background(), Map{"m1": m1, "m2": m2}, "id", "target", "value", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Len(t, got, 2)
			assert.Contains(t, got, true)
		})
	}
}

func TestCountFromMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []int64
		wantErr bool
	}{
		{
			name: "Should work",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CountFromMany(context.Background(), Map{"m1": m1, "m2": m2}, "target", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("CountMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Len(t, got, 2)
			assert.Contains(t, got, int64(10))
			assert.Contains(t, got, int64(20))
		})
	}
}

func TestCreateMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []string
		wantErr bool
	}{
		{
			name: "Should work",
			want: []string{"mock1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			got, err := CreateMany(ctx, m1, "target", nil, map[string]string{"1": "content1", "2": "content2"})
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Contains(t, tt.want, got[0])
			assert.Contains(t, tt.want, got[1])
		})
	}
}

func TestDeleteMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []bool
		wantErr bool
	}{
		{
			name: "Should work",
			want: []bool{true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DeleteMany(context.Background(), m1, "target", nil, "1", "2")
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Contains(t, tt.want, got[0])
			assert.Contains(t, tt.want, got[1])
		})
	}
}

func TestRetrieveMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []TestDataS
		wantErr bool
	}{
		{
			name: "Should work",
			want: []TestDataS{
				{
					K: "mock1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			got, err := RetrieveMany[TestDataS](ctx, m1, "target", nil, "1", "2")
			if (err != nil) != tt.wantErr {
				t.Errorf("RetrieveMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			t.Logf("got: %+v", got)

			assert.Contains(t, tt.want, got[0])
			assert.Contains(t, tt.want, got[1])
		})
	}
}

func TestUpdateMany(t *testing.T) {
	tests := []struct {
		name    string
		want    []bool
		wantErr bool
	}{
		{
			name: "Should work",
			want: []bool{true, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UpdateMany(context.Background(), m1, "target", nil, map[string]string{"1": "content1", "2": "content2"})
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Contains(t, tt.want, got[0])
			assert.Contains(t, tt.want, got[1])
		})
	}
}
