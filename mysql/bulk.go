package mysql

import (
	"context"

	"github.com/thalesfsp/dal/v3/internal/sqlbulk"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

// bulkPlaceholderLimit is deliberately capped at 999, below MySQL's 65,535
// protocol limit, so the same implementation is also safe with conservative
// drivers and the adapter's SQLite-backed offline test seam.
const bulkPlaceholderLimit = sqlbulk.PlaceholderLimit

// BulkCreate creates data in placeholder-bounded multi-row INSERT statements.
func (m *MySQL) BulkCreate(
	ctx context.Context,
	target string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	return sqlbulk.Create(ctx, m, m.Client, Name, target, m.Target, items, prm, options...)
}
