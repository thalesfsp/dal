package elasticsearch

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/customsort"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
)

var listParam = &list.List{
	Limit:  10,
	Fields: []string{"id", "name", "version"},
	Offset: 0,
	Sort: customsort.SortMap{
		"id": customsort.Asc,
	},
	Search: `{"match_all" : {} }`,
}

func TestNew(t *testing.T) {
	if !shared.IsEnvironment(shared.Integration) {
		t.Skip("Skipping test. Not in e2e " + shared.Integration + "environment.")
	}

	t.Setenv("HTTPCLIENT_METRICS_PREFIX", "dal_"+Name+"_test")

	host := os.Getenv("ELASTICSEARCH_HOST")

	if host == "" {
		t.Fatal("ELASTICSEARCH_HOST is not set")
	}

	type args struct {
		ctx context.Context
		id  string
	}
	tests := []struct {
		name    string
		args    args
		want    any
		wantErr bool
	}{
		{
			name: "Shoud work - E2E",
			args: args{
				ctx: context.Background(),
				id:  shared.DocumentID,
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//////
			// Tear up.
			//////

			ctx, cancel := context.WithTimeout(tt.args.ctx, shared.DefaultTimeout)
			defer cancel()

			str, err := NewWithIndex(ctx, shared.DatabaseName, elasticsearch.Config{
				Addresses: []string{host},
			})
			assert.NoError(t, err)
			assert.NotNil(t, str)

			if str == nil || str.Client == nil {
				t.Fatal("str or str.Client is nil")
			}

			// Ensures that document will be deleted after the test even if it
			// fails.
			defer func() {
				assert.NoError(t, str.DeleteIndex(ctx, shared.DatabaseName))
			}()

			// Create index.
			assert.NoError(t, str.CreateIndex(ctx, shared.DatabaseName, `
			{
			  "mappings": {
				"properties": {
				  "id": {
					"type": "keyword",
					"doc_values": true
				  }
				}
			  }
			}`))

			//////
			// Should be able to insert doc.
			//////

			insertedItem := shared.TestDataWithID

			id, err := str.Create(ctx, tt.args.id, shared.DatabaseName, insertedItem, &create.Create{})

			assert.NotEmpty(t, id)
			assert.NoError(t, err)

			// Give enough time for the data to be inserted.
			time.Sleep(1 * time.Second)

			//////
			// Should be able to retrieve doc.
			//////

			var retrievedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, tt.args.id, shared.DatabaseName, &retrievedItem, &retrieve.Retrieve{}))
			assert.Equal(t, insertedItem, &retrievedItem)

			//////
			// Should be able to update doc.
			//////

			updatedItem := shared.UpdatedTestDataID
			assert.NoError(t, str.Update(ctx, tt.args.id, shared.DatabaseName, updatedItem, &update.Update{}))

			// Give enough time for the data to be updated.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm the doc is updated.
			//////

			var retrievedUpdatedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, tt.args.id, shared.DatabaseName, &retrievedUpdatedItem, &retrieve.Retrieve{}))
			assert.Equal(t, &retrievedUpdatedItem, updatedItem)

			//////
			// Should be able to count doc.
			//////

			// Copy list param.
			listParamCopy := *listParam
			listParamCopy.Limit = 0

			count, err := str.Count(ctx, shared.DatabaseName, &count.Count{
				Search: listParamCopy.Search,
			})

			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)

			//////
			// Should be able to list doc.
			//////

			var listItems []shared.TestDataWithIDS

			assert.NoError(t, str.List(ctx, shared.DatabaseName, &listItems, listParam))
			assert.NotNil(t, listItems)
			assert.NotEmpty(t, listItems)

			// Ensure that the test item is in the list.
			found := false

			for _, item := range listItems {
				if item.ID == tt.args.id {
					assert.Equal(t, retrievedUpdatedItem, item)

					found = true

					break
				}
			}

			assert.True(t, found)

			//////
			// Tear down.
			//////

			//////
			// Should be able to delete docs.
			//////

			assert.NoError(t, str.Delete(ctx, tt.args.id, shared.DatabaseName, &delete.Delete{}))

			// Give enough time for the data to be deleted.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm there's no docs.
			//////

			var emptyListItems []shared.TestDataWithIDS

			assert.NoError(t, str.List(ctx, shared.DatabaseName, &emptyListItems, listParam))
			assert.NotNil(t, emptyListItems)
			assert.Empty(t, emptyListItems)

			// Should check if the metrics are working.
			assert.Equal(t, int64(1), str.GetCounterCounted().Value())
			assert.Equal(t, int64(0), str.GetCounterCountedFailed().Value())
			assert.Equal(t, int64(1), str.GetCounterDeleted().Value())
			assert.Equal(t, int64(0), str.GetCounterDeletedFailed().Value())
			assert.Equal(t, int64(2), str.GetCounterRetrieved().Value())
			assert.Equal(t, int64(0), str.GetCounterRetrievedFailed().Value())
			assert.Equal(t, int64(2), str.GetCounterListed().Value())
			assert.Equal(t, int64(0), str.GetCounterListedFailed().Value())
			assert.Equal(t, int64(1), str.GetCounterCreated().Value())
			assert.Equal(t, int64(0), str.GetCounterCreatedFailed().Value())
			assert.Equal(t, int64(1), str.GetCounterUpdated().Value())
			assert.Equal(t, int64(0), str.GetCounterUpdatedFailed().Value())
		})
	}
}

func Test_buildQuery(t *testing.T) {
	type args struct {
		params *list.List
		addons []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Should work",
			args: args{
				params: listParam,
			},
			want:    `{"_source": ["id","name","version"],"query": {"match_all" : {} },"size": 10,"sort": [{"id": {"order": "asc"}}]}`,
			wantErr: false,
		},
		{
			name: "Should work - with addons",
			args: args{
				params: listParam,
				addons: []string{`"track_total_hits": true`},
			},
			want:    `{"_source": ["id","name","version"],"query": {"match_all" : {} },"size": 10,"sort": [{"id": {"order": "asc"}}], "track_total_hits": true}`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildQuery(tt.args.params, tt.args.addons...)

			if (err != nil) != tt.wantErr {
				t.Errorf("buildQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.JSONEq(t, tt.want, got)
		})
	}
}
