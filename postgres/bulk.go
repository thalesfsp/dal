package postgres

import (
	"context"

	"github.com/thalesfsp/dal/v3/internal/sqlbulk"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

// bulkPlaceholderLimit uses 999 parameters per statement, comfortably below
// Postgres's 65,535 bind-parameter ceiling and safe for conservative drivers.
const bulkPlaceholderLimit = sqlbulk.PlaceholderLimit

// BulkCreate creates data with parameterized, multi-row INSERT statements.
func (p *Postgres) BulkCreate(
	ctx context.Context,
	target string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	return sqlbulk.Create(ctx, p, p.Client, Name, target, p.Target, items, prm, options...)
}
