package s3

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

var listParam = &list.List{
	Limit:  10,
	Fields: []string{"id", "name", "version"},
	Offset: 0,
	Sort:   [][]string{{"id", customsort.Asc}},
	Search: "VF*",
}

func TestNew(t *testing.T) {
	if !shared.IsEnvironment(shared.Integration) {
		t.Skip("Skipping test. Not in e2e " + shared.Integration + "environment.")
	}

	t.Setenv("HTTPCLIENT_METRICS_PREFIX", "s3_test")

	bucket := os.Getenv("AWS_BUCKET")
	region := os.Getenv("AWS_REGION")

	if bucket == "" || region == "" {
		t.Fatal("Need to set AWS_BUCKET and AWS_REGION environment variables")
	}

	kiD := os.Getenv("AWS_ACCESS_KEY_ID")
	sAK := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if kiD == "" || sAK == "" {
		t.Skip("Skipping test. Missing creds")
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

			cfg := &Config{
				Region: aws.String(region),
			}

			str, err := New(ctx, bucket, cfg)
			assert.NoError(t, err)
			assert.NotNil(t, str)

			if str == nil || str.Client == nil {
				t.Fatal("str or str.Client is nil")
			}

			trgt := fmt.Sprintf("dal-s3-test-%s.json", tt.args.id)

			// Ensures that document will be deleted after the test even if it
			// fails.
			defer func() {
				assert.NoError(t, str.Delete(ctx, tt.args.id, trgt, &delete.Delete{}))
			}()

			//////
			// Should be able to insert doc.
			//////

			insertedItem := shared.TestDataWithID

			id, err := str.Create(ctx, tt.args.id, trgt, insertedItem, &create.Create{})
			assert.NotEmpty(t, id)
			assert.NoError(t, err)

			// Give enough time for the data to be inserted.
			time.Sleep(1 * time.Second)

			//////
			// Should be able to retrieve doc.
			//////

			var retrievedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, tt.args.id, trgt, &retrievedItem, &retrieve.Retrieve{}))
			assert.Equal(t, insertedItem, &retrievedItem)

			//////
			// Should be able to update doc.
			//////

			updatedItem := shared.UpdatedTestDataID
			assert.NoError(t, str.Update(ctx, tt.args.id, trgt, updatedItem, &update.Update{}))

			// Give enough time for the data to be updated.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm the doc is updated.
			//////

			var retrievedUpdatedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, tt.args.id, trgt, &retrievedUpdatedItem, &retrieve.Retrieve{}))
			assert.Equal(t, &retrievedUpdatedItem, updatedItem)

			//////
			// Should be able to count doc.
			//////

			count, err := str.Count(ctx, trgt, &count.Count{
				Search: listParam.Search,
			})

			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)

			//////
			// Should be able to list doc.
			//////

			var listItems ResponseListKeys

			assert.NoError(t, str.List(tt.args.ctx, trgt, &listItems, listParam))
			assert.NotNil(t, listItems)
			assert.NotEmpty(t, listItems.Keys)

			// Ensure that the test item is in the list.
			found := false

			for _, item := range listItems.Keys {
				if item == trgt {
					assert.Equal(t, item, trgt)

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

			assert.NoError(t, str.Delete(ctx, tt.args.id, trgt, &delete.Delete{}))

			// Give enough time for the data to be deleted.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm there's no docs.
			//////

			var emptyListItems ResponseListKeys

			assert.NoError(t, str.List(tt.args.ctx, trgt, &listItems, listParam))
			assert.NotNil(t, emptyListItems)
			assert.Empty(t, emptyListItems.Keys)

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

//nolint:gosec
func TestNew_RetrieveSigned(t *testing.T) {
	if !shared.IsEnvironment(shared.Integration) {
		t.Skip("Skipping test. Not in e2e " + shared.Integration + "environment.")
	}

	t.Setenv("HTTPCLIENT_METRICS_PREFIX", "s3_test")

	bucket := os.Getenv("AWS_BUCKET")
	region := os.Getenv("AWS_REGION")

	if bucket == "" || region == "" {
		t.Fatal("Need to set AWS_BUCKET and AWS_REGION environment variables")
	}

	kiD := os.Getenv("AWS_ACCESS_KEY_ID")
	sAK := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if kiD == "" || sAK == "" {
		t.Skip("Skipping test. Missing creds")
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
				id:  shared.DocumentID + "-signed",
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

			cfg := &Config{
				Region: aws.String(region),
			}

			str, err := New(ctx, bucket, cfg)
			assert.NoError(t, err)
			assert.NotNil(t, str)

			if str == nil || str.Client == nil {
				t.Fatal("str or str.Client is nil")
			}

			trgt := fmt.Sprintf("dal-s3-test-%s.json", tt.args.id)

			// Ensures that document will be deleted after the test even if it
			// fails.
			defer func() {
				assert.NoError(t, str.Delete(ctx, tt.args.id, trgt, &delete.Delete{}))
			}()

			//////
			// Should be able to insert doc.
			//////

			insertedItem := shared.TestDataWithID

			id, err := str.Create(ctx, tt.args.id, trgt, insertedItem, &create.Create{})
			assert.NotEmpty(t, id)
			assert.NoError(t, err)

			// Give enough time for the data to be inserted.
			time.Sleep(1 * time.Second)

			//////
			// Should be able to retrieve doc.
			//////

			url, err := RetrieveSigned(ctx, str, trgt, bucket, 3*time.Minute)
			assert.NoError(t, err)

			// Open the URL which is a json file and confirm.
			resp, err := http.Get(url)
			assert.NoError(t, err)

			defer resp.Body.Close()

			body, err := shared.ReadAll(resp.Body)
			assert.NoError(t, err)
			assert.Equal(t, `{"id":"VFzrpYMBXu5BQSZxo0qX","name":"test","version":"1.0.0"}`, string(body))
		})
	}
}
