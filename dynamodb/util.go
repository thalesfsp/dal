package dynamodb

import (
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/thalesfsp/customerror"
)

//////
// Utility functions for DynamoDB operations.
//////

// sanitizePlaceholder converts an attribute name into a string that is safe to
// use as a DynamoDB expression placeholder suffix (only [A-Za-z0-9_] is
// allowed). When characters are replaced, a short FNV-1a hash of the original
// name is appended so distinct names (e.g. "a-b" and "a.b") can't collide.
func sanitizePlaceholder(name string) string {
	safe := true

	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			safe = false

			break
		}
	}

	if safe {
		return name
	}

	var b strings.Builder

	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}

	h := fnv.New32a()

	// Write never fails on hash.Hash.

	h.Write([]byte(name))

	return fmt.Sprintf("%s_%x", b.String(), h.Sum32())
}

// IsNotFoundError checks if the error is a DynamoDB item not found error.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Check for AWS specific error
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		return awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException
	}

	// Check if it's a customerror not found type
	if customErr, ok := customerror.To(err); ok {
		return customErr.StatusCode == http.StatusNotFound
	}

	return false
}

// IsConditionalCheckFailedError checks if the error is a conditional check failed error.
func IsConditionalCheckFailedError(err error) bool {
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		return awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException
	}

	return false
}

// IsProvisionedThroughputExceededError checks if the error is a throughput exceeded error.
func IsProvisionedThroughputExceededError(err error) bool {
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		return awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException
	}

	return false
}

// BuildFilterExpression builds a DynamoDB filter expression from a map of conditions.
func BuildFilterExpression(conditions map[string]interface{}) (
	*string,
	map[string]*string,
	map[string]*dynamodb.AttributeValue,
	error,
) {
	if len(conditions) == 0 {
		return nil, nil, nil, nil
	}

	expressions := make([]string, 0, len(conditions))
	attributeNames := make(map[string]*string)
	attributeValues := make(map[string]*dynamodb.AttributeValue)

	for key, value := range conditions {
		placeholder := sanitizePlaceholder(key)

		attrName := fmt.Sprintf("#%s", placeholder)
		attrValue := fmt.Sprintf(":%s", placeholder)

		expressions = append(expressions, fmt.Sprintf("%s = %s", attrName, attrValue))
		attributeNames[attrName] = aws.String(key)

		av, err := dynamodbattribute.Marshal(value)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}
		attributeValues[attrValue] = av
	}

	if len(expressions) == 0 {
		return nil, nil, nil, nil
	}

	expression := fmt.Sprintf("(%s)", expressions[0])
	for i := 1; i < len(expressions); i++ {
		expression = fmt.Sprintf("%s AND (%s)", expression, expressions[i])
	}

	return aws.String(expression), attributeNames, attributeValues, nil
}

// BuildUpdateExpression builds a DynamoDB update expression from a map of fields to update.
func BuildUpdateExpression(updates map[string]interface{}, primaryKey string) (
	*string,
	map[string]*string,
	map[string]*dynamodb.AttributeValue,
	error,
) {
	if len(updates) == 0 {
		return nil, nil, nil, fmt.Errorf("no fields to update")
	}

	setExpressions := make([]string, 0, len(updates))
	attributeNames := make(map[string]*string)
	attributeValues := make(map[string]*dynamodb.AttributeValue)

	for key, value := range updates {
		// Skip the primary key in updates
		if key == primaryKey {
			continue
		}

		placeholder := sanitizePlaceholder(key)

		attrName := fmt.Sprintf("#%s", placeholder)
		attrValue := fmt.Sprintf(":%s", placeholder)

		setExpressions = append(setExpressions, fmt.Sprintf("%s = %s", attrName, attrValue))
		attributeNames[attrName] = aws.String(key)

		av, err := dynamodbattribute.Marshal(value)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to marshal value for key %s: %w", key, err)
		}
		attributeValues[attrValue] = av
	}

	if len(setExpressions) == 0 {
		return nil, nil, nil, fmt.Errorf("no valid fields to update (excluding primary key)")
	}

	expression := fmt.Sprintf("SET %s", setExpressions[0])
	for i := 1; i < len(setExpressions); i++ {
		expression = fmt.Sprintf("%s, %s", expression, setExpressions[i])
	}

	return aws.String(expression), attributeNames, attributeValues, nil
}

// BuildProjectionExpression builds a DynamoDB projection expression from a slice of field names.
func BuildProjectionExpression(fields []string) (*string, map[string]*string) {
	if len(fields) == 0 {
		return nil, nil
	}

	attributeNames := make(map[string]*string)
	projections := make([]string, 0, len(fields))

	for _, field := range fields {
		attrName := fmt.Sprintf("#%s", sanitizePlaceholder(field))
		projections = append(projections, attrName)
		attributeNames[attrName] = aws.String(field)
	}

	expression := projections[0]
	for i := 1; i < len(projections); i++ {
		expression = fmt.Sprintf("%s, %s", expression, projections[i])
	}

	return aws.String(expression), attributeNames
}

// MarshalItem marshals a Go value to a DynamoDB item.
func MarshalItem(v interface{}) (map[string]*dynamodb.AttributeValue, error) {
	return dynamodbattribute.MarshalMap(v)
}

// UnmarshalItem unmarshals a DynamoDB item to a Go value.
func UnmarshalItem(item map[string]*dynamodb.AttributeValue, v interface{}) error {
	return dynamodbattribute.UnmarshalMap(item, v)
}

// UnmarshalItems unmarshals a slice of DynamoDB items to a slice of Go values.
func UnmarshalItems(items []map[string]*dynamodb.AttributeValue, v interface{}) error {
	return dynamodbattribute.UnmarshalListOfMaps(items, v)
}

// MarshalKey marshals a key value for DynamoDB operations.
func MarshalKey(keyName string, keyValue interface{}) (map[string]*dynamodb.AttributeValue, error) {
	av, err := dynamodbattribute.Marshal(keyValue)
	if err != nil {
		return nil, err
	}

	return map[string]*dynamodb.AttributeValue{
		keyName: av,
	}, nil
}
