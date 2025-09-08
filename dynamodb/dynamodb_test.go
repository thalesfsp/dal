package dynamodb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/dal/v2/internal/shared"
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
	Any:    map[string]interface{}{"version": shared.DocumentVersion},
}

func TestNew(t *testing.T) {
	// if !shared.IsEnvironment(shared.Integration) {
	// Ignored since it requires AWS credentials and DynamoDB setup.
	t.Skip("Skipping test. Not in e2e " + shared.Integration + " environment.")
	// }

	t.Setenv("HTTPCLIENT_METRICS_PREFIX", "dal_"+Name+"_test")

	// Check for DynamoDB endpoint (for local testing)
	endpoint := os.Getenv("DYNAMODB_ENDPOINT")
	region := os.Getenv("AWS_REGION")

	if region == "" {
		region = "us-east-1" // Default region
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
			name: "Should work - E2E",
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

			// Configure AWS config for testing
			cfg := &aws.Config{
				Region: aws.String(region),
			}

			// If testing locally with DynamoDB Local
			if endpoint != "" {
				cfg.Endpoint = aws.String(endpoint)
				cfg.Credentials = credentials.NewStaticCredentials("test", "test", "")
			}

			str, err := New(ctx, region, cfg)
			assert.NoError(t, err)

			// Create test table if it doesn't exist
			client, ok := str.GetClient().(*dynamodb.DynamoDB)
			assert.True(t, ok, "Failed to get DynamoDB client")

			tableName := shared.TableName
			createTableInput := &dynamodb.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("id"),
						KeyType:       aws.String(dynamodb.KeyTypeHash),
					},
				},
				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{
						AttributeName: aws.String("id"),
						AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
					},
				},
				BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
			}

			// Try to create table (ignore if it already exists)
			_, err = client.CreateTableWithContext(ctx, createTableInput)
			if err != nil {
				// Table might already exist, that's OK
				t.Logf("Table creation result: %v", err)
			} else {
				// Wait for table to be active
				err = client.WaitUntilTableExistsWithContext(ctx, &dynamodb.DescribeTableInput{
					TableName: aws.String(tableName),
				})
				assert.NoError(t, err)
			}

			// Ensures the test table will be clean after the test even if it fails.
			defer func() {
				// Delete all items from the table
				scanInput := &dynamodb.ScanInput{
					TableName: aws.String(tableName),
				}

				result, err := client.ScanWithContext(ctx, scanInput)
				if err == nil {
					for _, item := range result.Items {
						if idAttr, ok := item["id"]; ok {
							deleteInput := &dynamodb.DeleteItemInput{
								TableName: aws.String(tableName),
								Key: map[string]*dynamodb.AttributeValue{
									"id": idAttr,
								},
							}
							_, err := client.DeleteItemWithContext(ctx, deleteInput)
							assert.NoError(t, err, "Failed to delete item during cleanup")
						}
					}
				}
			}()

			//////
			// Should be able to insert doc.
			//////

			insertedID := shared.GenerateUUID()

			insertedItem := shared.TestDataWithID
			insertedItem.ID = insertedID

			id, err := str.Create(ctx, insertedID, tableName, insertedItem, &create.Create{})
			assert.NotEmpty(t, id)
			assert.NoError(t, err)

			// Give enough time for the data to be inserted.
			time.Sleep(1 * time.Second)

			//////
			// Should be able to retrieve doc.
			//////

			var retrievedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, insertedID, tableName, &retrievedItem, &retrieve.Retrieve{}))
			assert.Equal(t, insertedItem, &retrievedItem)

			//////
			// Should be able to update doc.
			//////

			updatedItem := shared.UpdatedTestDataID
			updatedItem.ID = id

			assert.NoError(t, str.Update(ctx, insertedID, tableName, updatedItem, &update.Update{}))

			// Give enough time for the data to be updated.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm the doc is updated.
			//////

			var retrievedUpdatedItem shared.TestDataWithIDS

			assert.NoError(t, str.Retrieve(ctx, insertedID, tableName, &retrievedUpdatedItem, &retrieve.Retrieve{}))
			assert.Equal(t, &retrievedUpdatedItem, updatedItem)

			//////
			// Should be able to count doc.
			//////

			count, err := str.Count(ctx, tableName, &count.Count{})

			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)

			//////
			// Should be able to list doc.
			//////

			var listItems []shared.TestDataWithIDS

			assert.NoError(t, str.List(ctx, tableName, &listItems, listParam))
			assert.NotNil(t, listItems)
			assert.NotEmpty(t, listItems)

			// Ensure that the test item is in the list.
			found := false

			for _, item := range listItems {
				if item.Name == retrievedUpdatedItem.Name {
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

			assert.NoError(t, str.Delete(ctx, insertedID, tableName, &delete.Delete{}))

			// Give enough time for the data to be deleted.
			time.Sleep(1 * time.Second)

			//////
			// Should confirm there's no docs.
			//////

			var emptyListItems []shared.TestDataWithIDS

			assert.NoError(t, str.List(ctx, tableName, &emptyListItems, listParam))
			assert.Nil(t, emptyListItems)
			assert.Empty(t, emptyListItems)
		})
	}
}
