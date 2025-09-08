// Package dynamodb provides a DynamoDB implementation of the storage interface.
package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

//////
// Types.
//////

// Item represents a DynamoDB item.
type Item map[string]*dynamodb.AttributeValue

// Items represents a slice of DynamoDB items.
type Items []Item

// TableDescription represents DynamoDB table description.
type TableDescription = dynamodb.TableDescription

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
