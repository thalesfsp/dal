// Package sqlbulk implements the shared mechanics for native SQL bulk inserts.
package sqlbulk

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

// PlaceholderLimit is a conservative cap that is safe for MySQL, Postgres,
// and both legacy and current SQLite builds. Row chunks are derived from this
// parameter count rather than from a fixed number of rows.
const PlaceholderLimit = 999

// Execer is the subset of sqlx.DB used to execute a prepared bulk statement.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type insertOutcome struct {
	succeeded []storage.Indexed[storage.BulkItem]
	failures  []storage.BulkItemError
	stopped   bool
}

// Create inserts items with native multi-row statements. A failed statement
// is bisected until its database error can be assigned to exact input items.
func Create(
	ctx context.Context,
	adapter storage.IStorage,
	client Execer,
	dialect, requestedTarget, staticTarget string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	if len(items) == 0 {
		return storage.BulkResult{}, nil
	}
	target, err := shared.TargetName(requestedTarget, staticTarget)
	if err != nil {
		return allFailed(items, err)
	}

	o, err := storage.NewOptions[*create.Create]()
	if err == nil {
		for _, option := range options {
			if err = option(o); err != nil {
				break
			}
		}
	}
	if err != nil {
		return allFailed(items, err)
	}

	finalParam, err := create.New()
	if err != nil {
		return allFailed(items, err)
	}
	finalParam = shared.MergeParams(finalParam, prm)

	chunks, err := storage.ChunkIndexed(items, rowLimit(dialect, target, items))
	if err != nil {
		return storage.BulkResult{}, err
	}

	result := storage.BulkResult{Succeeded: make([]int, 0, len(items))}
	for chunkIndex, chunk := range chunks {
		accepted, canceled := prepareChunk(ctx, adapter, target, chunk, finalParam, o, &result)
		if canceled != nil {
			failChunks(&result, chunks, chunkIndex+1, canceled)

			return result, result.Err()
		}
		if len(accepted) == 0 {
			continue
		}

		outcome := insertRows(ctx, client, dialect, target, accepted)
		result.Failures = append(result.Failures, outcome.failures...)
		if finishWritten(ctx, adapter, target, outcome.succeeded, finalParam, o, &result) {
			failChunks(&result, chunks, chunkIndex+1, ctx.Err())

			return result, result.Err()
		}
		if outcome.stopped {
			failChunks(&result, chunks, chunkIndex+1, ctx.Err())

			return result, result.Err()
		}
	}

	return result, result.Err()
}

func prepareChunk(
	ctx context.Context,
	adapter storage.IStorage,
	target string,
	chunk []storage.Indexed[storage.BulkItem],
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) ([]storage.Indexed[storage.BulkItem], error) {
	accepted := make([]storage.Indexed[storage.BulkItem], 0, len(chunk))
	for itemIndex, indexed := range chunk {
		if err := ctx.Err(); err != nil {
			appendFailures(result, accepted, err)
			appendFailures(result, chunk[itemIndex:], err)

			return nil, err
		}
		if indexed.Item.ID == "" {
			result.Failures = append(result.Failures, storage.BulkItemError{
				Index: indexed.Index,
				Err:   customerror.NewRequiredError("id"),
			})

			continue
		}
		if o.PreHookFunc != nil {
			if err := o.PreHookFunc(ctx, adapter, indexed.Item.ID, target, indexed.Item.Value, prm); err != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

				continue
			}
		}
		accepted = append(accepted, indexed)
	}

	return accepted, nil
}

func finishWritten(
	ctx context.Context,
	adapter storage.IStorage,
	target string,
	written []storage.Indexed[storage.BulkItem],
	prm *create.Create,
	o *storage.Options[*create.Create],
	result *storage.BulkResult,
) bool {
	for writtenIndex, indexed := range written {
		if ctx.Err() != nil {
			appendSuccesses(result, adapter, written[writtenIndex:])

			return true
		}
		if o.PostHookFunc != nil {
			if err := o.PostHookFunc(ctx, adapter, indexed.Item.ID, target, indexed.Item.Value, prm); err != nil {
				result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})

				continue
			}
		}
		appendSuccesses(result, adapter, written[writtenIndex:writtenIndex+1])
	}

	return false
}

func insertRows(
	ctx context.Context,
	client Execer,
	dialect, target string,
	rows []storage.Indexed[storage.BulkItem],
) insertOutcome {
	if err := ctx.Err(); err != nil {
		return stoppedOutcome(rows, err)
	}

	values := make([]any, len(rows))
	for i := range rows {
		values[i] = rows[i].Item.Value
	}

	insertSQL, args, err := goqu.Dialect(dialect).Insert(target).Rows(values...).Prepared(true).ToSQL()
	if err == nil && len(args) > PlaceholderLimit {
		err = fmt.Errorf("bulk insert requires %d placeholders, limit is %d", len(args), PlaceholderLimit)
	}
	if err == nil {
		_, err = client.ExecContext(ctx, insertSQL, args...)
	}
	if err == nil {
		return insertOutcome{succeeded: rows}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return stoppedOutcome(rows, ctxErr)
	}
	if len(rows) == 1 {
		return failedOutcome(rows[0].Index, err)
	}

	middle := len(rows) / 2
	left := insertRows(ctx, client, dialect, target, rows[:middle])
	if left.stopped {
		left.failures = append(left.failures, stoppedOutcome(rows[middle:], ctx.Err()).failures...)

		return left
	}
	right := insertRows(ctx, client, dialect, target, rows[middle:])

	return insertOutcome{
		succeeded: append(left.succeeded, right.succeeded...),
		failures:  append(left.failures, right.failures...),
		stopped:   right.stopped,
	}
}

func failedOutcome(index int, err error) insertOutcome {
	return insertOutcome{failures: []storage.BulkItemError{{
		Index: index,
		Err: customerror.NewFailedToError(
			storage.OperationCreate.String(),
			customerror.WithError(err),
		),
	}}}
}

func stoppedOutcome(rows []storage.Indexed[storage.BulkItem], err error) insertOutcome {
	failures := make([]storage.BulkItemError, len(rows))
	for i := range rows {
		failures[i] = storage.BulkItemError{Index: rows[i].Index, Err: err}
	}

	return insertOutcome{failures: failures, stopped: true}
}

func rowLimit(dialect, target string, items []storage.BulkItem) int {
	maxPlaceholders := 1
	for i := range items {
		_, args, err := goqu.Dialect(dialect).Insert(target).Rows(items[i].Value).Prepared(true).ToSQL()
		if err == nil && len(args) > maxPlaceholders {
			maxPlaceholders = len(args)
		}
	}

	return max(1, PlaceholderLimit/maxPlaceholders)
}

func appendFailures(
	result *storage.BulkResult,
	items []storage.Indexed[storage.BulkItem],
	err error,
) {
	for _, indexed := range items {
		result.Failures = append(result.Failures, storage.BulkItemError{Index: indexed.Index, Err: err})
	}
}

func appendSuccesses(
	result *storage.BulkResult,
	adapter storage.IStorage,
	items []storage.Indexed[storage.BulkItem],
) {
	for _, indexed := range items {
		result.Succeeded = append(result.Succeeded, indexed.Index)
		adapter.GetCounterCreated().Add(1)
	}
}

func failChunks(
	result *storage.BulkResult,
	chunks [][]storage.Indexed[storage.BulkItem],
	start int,
	err error,
) {
	result.Stopped = true
	for _, chunk := range chunks[start:] {
		appendFailures(result, chunk, err)
	}
}

func allFailed(items []storage.BulkItem, err error) (storage.BulkResult, error) {
	result := storage.BulkResult{Failures: make([]storage.BulkItemError, len(items))}
	for i := range items {
		result.Failures[i] = storage.BulkItemError{Index: i, Err: err}
	}

	return result, result.Err()
}
