package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

const (
	dynamoBulkChunkLimit        = 25
	dynamoUnprocessedMaxRetries = 5
	dynamoUnprocessedBaseDelay  = 10 * time.Millisecond
)

var errDynamoUnprocessedRetriesExhausted = errors.New("dynamodb unprocessed item retries exhausted")

type dynamoBulkWrite struct {
	indexed storage.Indexed[storage.BulkItem]
	request types.WriteRequest
}

// BulkCreate writes at most 25 items per BatchWriteItem request and retries
// UnprocessedItems with bounded exponential backoff.
func (d *DynamoDB) BulkCreate(
	ctx context.Context,
	target string,
	items []storage.BulkItem,
	prm *create.Create,
	optionFuncs ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	if len(items) == 0 {
		return storage.BulkResult{}, nil
	}

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return storage.BulkResult{}, err
	}
	for _, optionFunc := range optionFuncs {
		if err := optionFunc(o); err != nil {
			return storage.BulkResult{}, err
		}
	}

	finalParam, err := create.New()
	if err != nil {
		return storage.BulkResult{}, err
	}
	finalParam = shared.MergeParams(finalParam, prm)

	trgt, err := shared.TargetName(target, d.Target())
	if err != nil {
		return storage.BulkResult{}, err
	}

	chunks, err := storage.ChunkIndexed(items, dynamoBulkChunkLimit)
	if err != nil {
		return storage.BulkResult{}, err
	}

	result := storage.BulkResult{Succeeded: make([]int, 0, len(items))}
	for chunkIndex, chunk := range chunks {
		if ctxErr := ctx.Err(); ctxErr != nil {
			dynamoStopBulk(&result, chunks[chunkIndex:], ctxErr)

			return result, result.Err()
		}

		prepared, stoppedAt := d.dynamoPrepareChunk(ctx, trgt, chunk, finalParam, o, &result)
		if stoppedAt >= 0 {
			remaining := append([][]storage.Indexed[storage.BulkItem]{chunk[stoppedAt:]}, chunks[chunkIndex+1:]...)
			dynamoStopBulk(&result, remaining, ctx.Err())

			return result, result.Err()
		}

		stopped := d.dynamoWriteChunk(ctx, trgt, prepared, finalParam, o, &result)
		if stopped {
			dynamoStopBulk(&result, chunks[chunkIndex+1:], ctx.Err())

			return result, result.Err()
		}
	}

	return result, result.Err()
}

func (d *DynamoDB) dynamoPrepareChunk(
	ctx context.Context,
	target string,
	chunk []storage.Indexed[storage.BulkItem],
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) ([]dynamoBulkWrite, int) {
	prepared := make([]dynamoBulkWrite, 0, len(chunk))
	for itemOffset, indexed := range chunk {
		if ctx.Err() != nil {
			return prepared, itemOffset
		}
		if indexed.Item.ID == "" {
			result.Failures = append(result.Failures, storage.BulkItemError{
				Index: indexed.Index,
				Err:   customerror.NewRequiredError("id"),
			})

			continue
		}
		if o.PreHookFunc != nil {
			if hookErr := o.PreHookFunc(ctx, d, indexed.Item.ID, target, indexed.Item.Value, prm); hookErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: hookErr})

				continue
			}
		}

		item, err := attributevalue.MarshalMap(indexed.Item.Value)
		if err != nil {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

			continue
		}
		primaryKey, err := attributevalue.Marshal(indexed.Item.ID)
		if err != nil {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

			continue
		}
		item[d.PrimaryKey] = primaryKey
		prepared = append(prepared, dynamoBulkWrite{
			indexed: indexed,
			request: types.WriteRequest{PutRequest: &types.PutRequest{Item: item}},
		})
	}

	return prepared, -1
}

func (d *DynamoDB) dynamoWriteChunk(
	ctx context.Context,
	target string,
	prepared []dynamoBulkWrite,
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) bool {
	pending := prepared
	for retry := 0; len(pending) > 0; retry++ {
		requests := make([]types.WriteRequest, len(pending))
		for i := range pending {
			requests[i] = pending[i].request
		}

		output, err := d.Client.BatchWriteItem(ctx, &awsdynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{target: requests},
		})
		if err != nil {
			dynamoFailPending(result, pending, err)

			return ctx.Err() != nil
		}

		unprocessed, matchErr := dynamoMatchUnprocessed(pending, output.UnprocessedItems[target])
		if matchErr != nil {
			dynamoFailPending(result, pending, matchErr)

			return false
		}

		dynamoRecordProcessed(ctx, d, target, prm, o, pending, unprocessed, result)
		if len(unprocessed) == 0 {
			return false
		}
		if retry == dynamoUnprocessedMaxRetries {
			dynamoFailPending(result, unprocessed, errDynamoUnprocessedRetriesExhausted)

			return false
		}

		delay := dynamoUnprocessedBaseDelay << retry
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			dynamoFailPending(result, unprocessed, ctx.Err())
			result.Stopped = true

			return true
		case <-timer.C:
		}

		pending = unprocessed
	}

	return false
}

func dynamoMatchUnprocessed(pending []dynamoBulkWrite, requests []types.WriteRequest) ([]dynamoBulkWrite, error) {
	unprocessed := make([]dynamoBulkWrite, 0, len(requests))
	used := make([]bool, len(pending))
	for _, request := range requests {
		matched := false
		for i := range pending {
			if !used[i] && reflect.DeepEqual(pending[i].request.PutRequest.Item, request.PutRequest.Item) {
				used[i] = true
				unprocessed = append(unprocessed, pending[i])
				matched = true

				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("dynamodb returned an unprocessed item that was not in the request")
		}
	}

	return unprocessed, nil
}

func dynamoRecordProcessed(
	ctx context.Context,
	d *DynamoDB,
	target string,
	prm *create.Create,
	o *storage.Options[*create.Create],
	pending []dynamoBulkWrite,
	unprocessed []dynamoBulkWrite,
	result *storage.BulkResult,
) {
	for _, write := range pending {
		isUnprocessed := false
		for _, retryWrite := range unprocessed {
			if write.indexed.Index == retryWrite.indexed.Index {
				isUnprocessed = true

				break
			}
		}
		if isUnprocessed {
			continue
		}

		if o.PostHookFunc != nil && ctx.Err() == nil {
			if hookErr := o.PostHookFunc(ctx, d, write.indexed.Item.ID, target, write.indexed.Item.Value, prm); hookErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: write.indexed.Index, Err: hookErr})

				continue
			}
		}
		result.Succeeded = append(result.Succeeded, write.indexed.Index)
	}
}

func dynamoFailPending(result *storage.BulkResult, pending []dynamoBulkWrite, err error) {
	for _, write := range pending {
		result.Failures = append(result.Failures, storage.BulkItemError{Index: write.indexed.Index, Err: err})
	}
}

func dynamoStopBulk(
	result *storage.BulkResult,
	chunks [][]storage.Indexed[storage.BulkItem],
	err error,
) {
	result.Stopped = true
	for _, chunk := range chunks {
		for _, indexed := range chunk {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})
		}
	}
}
