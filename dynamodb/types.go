// Package dynamodb provides a DynamoDB implementation of the storage interface.
package dynamodb

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

//////
// Types.
//////

// Item represents a DynamoDB item.
type Item map[string]types.AttributeValue

// Items represents a slice of DynamoDB items.
type Items []Item

// TableDescription represents DynamoDB table description.
type TableDescription = types.TableDescription

// CreateTableInput represents DynamoDB create table input.
type CreateTableInput = dynamodb.CreateTableInput

// PutItemInput represents DynamoDB put item input.
type PutItemInput = dynamodb.PutItemInput

// GetItemInput represents DynamoDB get item input.
type GetItemInput = dynamodb.GetItemInput

// UpdateItemInput represents DynamoDB update item input.
type UpdateItemInput = dynamodb.UpdateItemInput

// DeleteItemInput represents DynamoDB delete item input.
type DeleteItemInput = dynamodb.DeleteItemInput

// ScanInput represents DynamoDB scan input.
type ScanInput = dynamodb.ScanInput

// QueryInput represents DynamoDB query input.
type QueryInput = dynamodb.QueryInput
