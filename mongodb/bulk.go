package mongodb

import (
	"context"
	"errors"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const mongoBulkChunkLimit = 100

type mongoBulkDocument struct {
	indexed storage.Indexed[storage.BulkItem]
	doc     bson.Raw
}

// BulkCreate inserts documents with unordered InsertMany calls so one write
// error does not prevent the other documents in the chunk from being tried.
func (m *MongoDB) BulkCreate(
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
	o.Database = m.Database
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

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return storage.BulkResult{}, err
	}

	chunks, err := storage.ChunkIndexed(items, mongoBulkChunkLimit)
	if err != nil {
		return storage.BulkResult{}, err
	}

	result := storage.BulkResult{Succeeded: make([]int, 0, len(items))}
	for chunkIndex, chunk := range chunks {
		if ctxErr := ctx.Err(); ctxErr != nil {
			mongoStopBulk(&result, chunks[chunkIndex:], ctxErr)

			return result, result.Err()
		}

		prepared := make([]mongoBulkDocument, 0, len(chunk))
		for itemOffset, indexed := range chunk {
			if ctxErr := ctx.Err(); ctxErr != nil {
				mongoStopBulk(&result, append([][]storage.Indexed[storage.BulkItem]{chunk[itemOffset:]}, chunks[chunkIndex+1:]...), ctxErr)

				return result, result.Err()
			}
			if indexed.Item.ID == "" {
				result.Failures = append(result.Failures, storage.BulkItemError{
					Index: indexed.Index,
					Err:   customerror.NewRequiredError("id"),
				})

				continue
			}
			if o.PreHookFunc != nil {
				if hookErr := o.PreHookFunc(ctx, m, indexed.Item.ID, trgt, indexed.Item.Value, finalParam); hookErr != nil {
					result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: hookErr})

					continue
				}
			}

			doc, marshalErr := bson.Marshal(indexed.Item.Value)
			if marshalErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: marshalErr})

				continue
			}
			prepared = append(prepared, mongoBulkDocument{indexed: indexed, doc: doc})
		}

		m.mongoInsertChunk(ctx, trgt, prepared, finalParam, o, &result)
	}

	return result, result.Err()
}

func (m *MongoDB) mongoInsertChunk(
	ctx context.Context,
	target string,
	prepared []mongoBulkDocument,
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) {
	if len(prepared) == 0 {
		return
	}

	documents := make([]any, len(prepared))
	for i := range prepared {
		documents[i] = prepared[i].doc
	}

	_, insertErr := m.Client.Database(o.Database).Collection(target).InsertMany(
		ctx,
		documents,
		options.InsertMany().SetOrdered(false),
	)
	failed := mongoInsertFailures(prepared, insertErr)

	for localIndex, document := range prepared {
		if itemErr, failedItem := failed[localIndex]; failedItem {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: document.indexed.Index, Err: itemErr})

			continue
		}

		if o.PostHookFunc != nil && ctx.Err() == nil {
			if hookErr := o.PostHookFunc(ctx, m, document.indexed.Item.ID, target, document.indexed.Item.Value, prm); hookErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: document.indexed.Index, Err: hookErr})

				continue
			}
		}
		result.Succeeded = append(result.Succeeded, document.indexed.Index)
	}
}

func mongoInsertFailures(prepared []mongoBulkDocument, insertErr error) map[int]error {
	failed := make(map[int]error)
	if insertErr == nil {
		return failed
	}

	var bulkException mongo.BulkWriteException
	if !errors.As(insertErr, &bulkException) || bulkException.WriteConcernError != nil || len(bulkException.WriteErrors) == 0 {
		return mongoFailEveryPrepared(prepared, insertErr)
	}

	for i := range bulkException.WriteErrors {
		writeError := &bulkException.WriteErrors[i]
		if writeError.Index < 0 || writeError.Index >= len(prepared) {
			return mongoFailEveryPrepared(prepared, insertErr)
		}
		failed[writeError.Index] = writeError
	}

	return failed
}

func mongoFailEveryPrepared(prepared []mongoBulkDocument, err error) map[int]error {
	failed := make(map[int]error, len(prepared))
	for i := range prepared {
		failed[i] = err
	}

	return failed
}

func mongoStopBulk(
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
