package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

const redisBulkChunkLimit = 100

type redisBulkCommand struct {
	indexed storage.Indexed[storage.BulkItem]
	value   []byte
	command *goredis.StatusCmd
}

// BulkCreate queues SET commands in non-transactional Redis pipelines and
// reads each command's error instead of treating Exec's aggregate error as the
// outcome of every item.
func (r *Redis) BulkCreate(
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
	finalParam.TTL = 0
	finalParam = shared.MergeParams(finalParam, prm)

	chunks, err := storage.ChunkIndexed(items, redisBulkChunkLimit)
	if err != nil {
		return storage.BulkResult{}, err
	}

	result := storage.BulkResult{Succeeded: make([]int, 0, len(items))}
	for chunkIndex, chunk := range chunks {
		if ctxErr := ctx.Err(); ctxErr != nil {
			redisStopBulk(&result, chunks[chunkIndex:], ctxErr)

			return result, result.Err()
		}

		prepared, stoppedAt := r.redisPrepareChunk(ctx, target, chunk, finalParam, o, &result)
		if stoppedAt >= 0 {
			remaining := append([][]storage.Indexed[storage.BulkItem]{chunk[stoppedAt:]}, chunks[chunkIndex+1:]...)
			redisStopBulk(&result, remaining, ctx.Err())

			return result, result.Err()
		}

		r.redisExecuteChunk(ctx, target, prepared, finalParam, o, &result)

		if ctxErr := ctx.Err(); ctxErr != nil {
			redisStopBulk(&result, chunks[chunkIndex+1:], ctxErr)

			return result, result.Err()
		}
	}

	return result, result.Err()
}

func (r *Redis) redisPrepareChunk(
	ctx context.Context,
	target string,
	chunk []storage.Indexed[storage.BulkItem],
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) ([]redisBulkCommand, int) {
	prepared := make([]redisBulkCommand, 0, len(chunk))
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
			if hookErr := o.PreHookFunc(ctx, r, indexed.Item.ID, target, indexed.Item.Value, prm); hookErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: hookErr})

				continue
			}
		}

		value, err := shared.Marshal(indexed.Item.Value)
		if err != nil {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

			continue
		}
		prepared = append(prepared, redisBulkCommand{indexed: indexed, value: value})
	}

	return prepared, -1
}

func (r *Redis) redisExecuteChunk(
	ctx context.Context,
	target string,
	prepared []redisBulkCommand,
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) {
	if len(prepared) == 0 {
		return
	}

	pipeline := r.Client.Pipeline()
	for i := range prepared {
		prepared[i].command = pipeline.Set(ctx, prepared[i].indexed.Item.ID, prepared[i].value, prm.TTL)
	}
	_, _ = pipeline.Exec(ctx)

	for _, queued := range prepared {
		if commandErr := queued.command.Err(); commandErr != nil {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: queued.indexed.Index, Err: commandErr})

			continue
		}
		if o.PostHookFunc != nil && ctx.Err() == nil {
			if hookErr := o.PostHookFunc(ctx, r, queued.indexed.Item.ID, target, queued.indexed.Item.Value, prm); hookErr != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: queued.indexed.Index, Err: hookErr})

				continue
			}
		}
		result.Succeeded = append(result.Succeeded, queued.indexed.Index)
	}
}

func redisStopBulk(
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
