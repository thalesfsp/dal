package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

const elasticsearchBulkChunkLimit = 100

type elasticsearchBulkDocument struct {
	indexed storage.Indexed[storage.BulkItem]
	body    []byte
}

type elasticsearchBulkResponse struct {
	Items []map[string]struct {
		Status int             `json:"status"`
		Error  json.RawMessage `json:"error"`
	} `json:"items"`
}

// BulkCreate sends newline-delimited index operations to Elasticsearch's
// _bulk endpoint and inspects every per-item response status.
func (es *ElasticSearch) BulkCreate(
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

	trgt, err := shared.TargetName(target, es.Target())
	if err != nil {
		return storage.BulkResult{}, err
	}

	chunks, err := storage.ChunkIndexed(items, elasticsearchBulkChunkLimit)
	if err != nil {
		return storage.BulkResult{}, err
	}

	result := storage.BulkResult{Succeeded: make([]int, 0, len(items))}
	for chunkIndex, chunk := range chunks {
		if ctxErr := ctx.Err(); ctxErr != nil {
			elasticsearchStopBulk(&result, chunks[chunkIndex:], ctxErr)

			return result, result.Err()
		}

		prepared, stoppedAt := es.elasticsearchPrepareChunk(ctx, trgt, chunk, finalParam, o, &result)
		if stoppedAt >= 0 {
			remaining := append([][]storage.Indexed[storage.BulkItem]{chunk[stoppedAt:]}, chunks[chunkIndex+1:]...)
			elasticsearchStopBulk(&result, remaining, ctx.Err())

			return result, result.Err()
		}

		if es.elasticsearchSendChunk(ctx, trgt, prepared, finalParam, o, &result) {
			elasticsearchStopBulk(&result, chunks[chunkIndex+1:], ctx.Err())

			return result, result.Err()
		}
	}

	return result, result.Err()
}

func (es *ElasticSearch) elasticsearchPrepareChunk(
	ctx context.Context,
	target string,
	chunk []storage.Indexed[storage.BulkItem],
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) ([]elasticsearchBulkDocument, int) {
	prepared := make([]elasticsearchBulkDocument, 0, len(chunk))
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
			if hookErr := o.PreHookFunc(ctx, es, indexed.Item.ID, target, indexed.Item.Value, prm); hookErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: hookErr})

				continue
			}
		}

		body, err := shared.Marshal(indexed.Item.Value)
		if err != nil {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

			continue
		}
		prepared = append(prepared, elasticsearchBulkDocument{indexed: indexed, body: body})
	}

	return prepared, -1
}

func (es *ElasticSearch) elasticsearchSendChunk(
	ctx context.Context,
	target string,
	prepared []elasticsearchBulkDocument,
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) bool {
	if len(prepared) == 0 {
		return false
	}

	requestBody := elasticsearchBulkRequestBody(prepared)
	reqOpts := []func(*esapi.BulkRequest){
		es.Client.Bulk.WithContext(ctx),
		es.Client.Bulk.WithIndex(target),
	}
	if prm.Routing != "" {
		reqOpts = append(reqOpts, es.Client.Bulk.WithRouting(prm.Routing))
	}

	res, err := es.Client.Bulk(requestBody, reqOpts...)
	if err != nil {
		elasticsearchFailPrepared(result, prepared, err)

		return ctx.Err() != nil
	}

	if err := es.consumeBulkResponse(ctx, res, target, prepared, prm, o, result); err != nil {
		elasticsearchFailPrepared(result, prepared, err)
	}

	return false
}

func elasticsearchBulkRequestBody(prepared []elasticsearchBulkDocument) *bytes.Buffer {
	var requestBody bytes.Buffer
	for _, document := range prepared {
		metadata, err := json.Marshal(map[string]map[string]string{
			"index": {"_id": document.indexed.Item.ID},
		})
		if err != nil {
			panic("marshalling a string-only Elasticsearch bulk metadata map: " + err.Error())
		}
		requestBody.Write(metadata)
		requestBody.WriteByte('\n')
		requestBody.Write(document.body)
		requestBody.WriteByte('\n')
	}

	return &requestBody
}

func (es *ElasticSearch) consumeBulkResponse(
	ctx context.Context,
	res *esapi.Response,
	target string,
	prepared []elasticsearchBulkDocument,
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) error {
	defer res.Body.Close()

	if err := checkResponseIsError(res); err != nil {
		return err
	}

	var response elasticsearchBulkResponse
	if err := parseResponseBody(res.Body, &response); err != nil {
		return err
	}
	if len(response.Items) != len(prepared) {
		return fmt.Errorf("elasticsearch bulk response item count %d does not match request item count %d", len(response.Items), len(prepared))
	}

	for localIndex, operation := range response.Items {
		var itemResponse struct {
			Status int
			Error  json.RawMessage
		}
		if len(operation) != 1 {
			result.Failures = append(result.Failures, storage.BulkItemError{
				Index: prepared[localIndex].indexed.Index,
				Err:   fmt.Errorf("elasticsearch bulk item response contains %d operations", len(operation)),
			})

			continue
		}
		for _, responseItem := range operation {
			itemResponse.Status = responseItem.Status
			itemResponse.Error = responseItem.Error
		}

		if itemResponse.Status < 200 || itemResponse.Status >= 300 {
			result.Failures = append(result.Failures, storage.BulkItemError{
				Index: prepared[localIndex].indexed.Index,
				Err: fmt.Errorf(
					"elasticsearch bulk item returned status %d: %s",
					itemResponse.Status,
					elasticsearchBulkErrorText(itemResponse.Error),
				),
			})

			continue
		}

		document := prepared[localIndex]
		if o.PostHookFunc != nil && ctx.Err() == nil {
			if hookErr := o.PostHookFunc(ctx, es, document.indexed.Item.ID, target, document.indexed.Item.Value, prm); hookErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: document.indexed.Index, Err: hookErr})

				continue
			}
		}
		result.Succeeded = append(result.Succeeded, document.indexed.Index)
	}

	return nil
}

func elasticsearchBulkErrorText(raw json.RawMessage) string {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "unspecified item error"
	}

	return string(raw)
}

func elasticsearchFailPrepared(result *storage.BulkResult, prepared []elasticsearchBulkDocument, err error) {
	for _, document := range prepared {
		result.Failures = append(result.Failures, storage.BulkItemError{Index: document.indexed.Index, Err: err})
	}
}

func elasticsearchStopBulk(
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
