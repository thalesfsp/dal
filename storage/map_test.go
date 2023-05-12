package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
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

func TestCreateMany(t *testing.T) {
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

			got, err := CreateMany(ctx, m, "id", "target", "value", nil)
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
			got, err := DeleteMany(context.Background(), Map{"m1": m1, "m2": m2}, "id", "target", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Contains(t, tt.want, got[0])
			assert.Contains(t, tt.want, got[1])
		})
	}
}

func TestListMany(t *testing.T) {
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
			got, err := ListMany[string](context.Background(), Map{"m1": m1, "m2": m2}, "target", nil)
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

func TestRetrieveMany(t *testing.T) {
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
			got, err := RetrieveMany[*TestDataS](context.Background(), Map{"m1": m1, "m2": m2}, "id", "target", nil)
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

func TestUpdateMany(t *testing.T) {
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
			got, err := UpdateMany(context.Background(), Map{"m1": m1, "m2": m2}, "id", "target", "value", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Len(t, got, 2)
			assert.Contains(t, got, true)
		})
	}
}

func TestCountMany(t *testing.T) {
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
			got, err := CountMany(context.Background(), Map{"m1": m1, "m2": m2}, "target", nil)
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
