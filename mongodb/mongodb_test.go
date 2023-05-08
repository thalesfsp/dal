package mongodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/customsort"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var listParam = &list.List{
	Limit:  10,
	Fields: []string{"id", "name", "version"},
	Offset: 0,
	Sort: customsort.SortMap{
		"id": customsort.Asc,
	},
	Search: `{"version":"` + shared.DocumentVersion + `"}`,
}

func TestNew(t *testing.T) {
	// if !shared.IsEnvironment(shared.Integration) {
	// 	t.Skip("Skipping test. Not in e2e " + shared.Integration + "environment.")
	// }

	t.Setenv("HTTPCLIENT_METRICS_PREFIX", "dal_"+Name+"_test")

	host := os.Getenv("MONGODB_HOST")

	if host == "" {
		t.Fatal("MONGODB_HOST is not set")
	}

	type args struct {
		ctx context.Context
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

			str, err := New(ctx, shared.DatabaseName, options.Client().ApplyURI(host))
			assert.NoError(t, err)
			assert.NotNil(t, str)

			if str == nil || str.Client == nil {
				t.Fatal("str or str.Client is nil")
			}

			// Ensures the test collection will be clean after the test even if
			// it fails.
			defer func() {
				if str == nil || str.GetClient() == nil {
					t.Fatal(Name, "client is nil")
				}

				client, ok := str.GetClient().(*mongo.Client)
				if !ok {
					t.Fatal("Could not convert to *mongo.Client")
				}

				// Drop collection.
				assert.NoError(t, client.Database(shared.DatabaseName).Collection(shared.TableName).Drop(ctx))
			}()

			//////
			// Should be able to insert doc.
			//////

			insertedItem := shared.TestDataWithID

			objectID, err := str.Create(ctx, "", shared.TableName, insertedItem, &create.Create{})

			assert.NotEmpty(t, objectID)
			assert.NoError(t, err)

			// Give enough time for the data to be inserted.
			time.Sleep(1 * time.Second)

			//////
			// Should be able to retrieve doc.
			//////

			var retrievedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, objectID, shared.TableName, &retrievedItem, &retrieve.Retrieve{}))
			assert.Equal(t, insertedItem, &retrievedItem)

			//////
			// Should be able to update doc.
			//////

			updatedItem := shared.UpdatedTestDataID
			updatedItem.ID = objectID

			assert.NoError(t, str.Update(ctx, objectID, shared.TableName, updatedItem, &update.Update{}))

			// Give enough time for the data to be updated.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm the doc is updated.
			//////

			var retrievedUpdatedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, objectID, shared.TableName, &retrievedUpdatedItem, &retrieve.Retrieve{}))
			assert.Equal(t, &retrievedUpdatedItem, updatedItem)

			//////
			// Should be able to count doc.
			//////

			count, err := str.Count(ctx, shared.TableName, &count.Count{
				Search: listParam.Search,
			})

			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)

			//////
			// Should be able to list doc.
			//////

			var listItems []shared.TestDataWithIDS

			assert.NoError(t, str.List(ctx, shared.TableName, &listItems, listParam))
			assert.NotNil(t, listItems)
			assert.NotEmpty(t, listItems)

			// Ensure that the test item is in the list.
			found := false

			for _, item := range listItems {
				if item.ID == objectID {
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

			assert.NoError(t, str.Delete(ctx, objectID, shared.TableName, &delete.Delete{}))

			// Give enough time for the data to be deleted.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm there's no docs.
			//////

			var emptyListItems []shared.TestDataWithIDS

			assert.NoError(t, str.List(ctx, shared.TableName, &listItems, listParam))
			assert.Nil(t, emptyListItems)
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
