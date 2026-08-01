package sqlite

import (
	"context"

	"github.com/thalesfsp/dal/v3/internal/sqlbulk"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/create"
)

// bulkPlaceholderLimit is 999, the conservative SQLite variable limit that
// remains valid for legacy builds as well as the newer 32,766-variable builds.
const bulkPlaceholderLimit = sqlbulk.PlaceholderLimit

// BulkCreate creates data using parameterized, multi-row INSERT statements.
func (p *SQLite) BulkCreate(
	ctx context.Context,
	target string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	return sqlbulk.Create(ctx, p, p.Client, Name, target, p.Target, items, prm, options...)
}
