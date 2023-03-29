package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	// Import the postgres driver.
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"

	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/shared"
	"github.com/thalesfsp/dal/storage"
	"github.com/thalesfsp/dal/validation"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/customsort"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/get"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/set"
	"github.com/thalesfsp/params/update"
)

//////
// Const, vars, and types.
//////

const name = "postgres"

// Singleton.
var singleton *Postgres

// Config is the postgres configuration.
type Config struct {
	DataSourceName string `json:"dataSourceName" validate:"required"`
	DriverName     string `json:"driverName" validate:"required"`
}

// Postgres storage definition.
type Postgres struct {
	*storage.Storage

	// Client is the postgres client.
	Client *sqlx.DB `json:"-" validate:"required"`

	// Config is the postgres configuration.
	Config *Config `json:"-"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For Redis, it isn't used at all.
	Target string `json:"-" validate:"omitempty,gt=0"`
}

//////
// Helpers.
//////

// ToSQLString converts the `s` to a format that can be used in a SQL query.
//
// NOTE: Keywords in SQL are case-insensitive for the most popular DBMSs.
// The computer doesn't care whether you write SELECT, select, sELeCt, ASC, AsC,
// or asc.
func ToSQLString(s customsort.Sort) (string, error) {
	sqlFormatted, err := s.ToAnyString(" ", ", ")
	if err != nil {
		return "", err
	}

	return sqlFormatted, nil
}

func (p *Postgres) createDB(ctx context.Context, name string) error {
	if _, err := p.
		Client.
		ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(name))); err != nil {
		return customerror.NewFailedToError("createDB", customerror.WithError(err))
	}

	return nil
}

func (p *Postgres) deleteTable(ctx context.Context, name string) error {
	if _, err := p.
		Client.
		ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", name)); err != nil {
		return customerror.NewFailedToError("deleteTable", customerror.WithError(err))
	}

	return nil
}

func (p *Postgres) createTable(ctx context.Context, name, description string) error {
	sql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		%s
	)`, name, description)

	if _, err := p.
		Client.
		ExecContext(ctx, sql); err != nil {
		return customerror.NewFailedToError("createTable", customerror.WithError(err))
	}

	return nil
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (p *Postgres) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, err
	}

	// Application's default values.
	finalParam.Search = "1=1"

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return 0, err
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, "", trgt, finalParam); err != nil {
			return 0, err
		}
	}

	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s;`, trgt, finalParam.Search)

	var count int64
	if err = p.Client.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return 0, customerror.NewFailedToError(storage.OperationCount, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, "", trgt, finalParam); err != nil {
			return 0, err
		}
	}

	//////
	// Metrics.
	//////

	p.GetCounterDelete().Add(1)

	return count, nil
}

// Delete removes data.
func (p *Postgres) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return err
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return err
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return err
		}
	}

	ds := goqu.Delete(trgt).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	selectSQL, args, err := ds.ToSQL()
	if err != nil {
		return customerror.NewFailedToError(storage.OperationDelete, customerror.WithError(err))
	}

	if _, err := p.Client.ExecContext(ctx, selectSQL, args...); err != nil {
		return customerror.NewFailedToError(storage.OperationDelete, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	p.GetCounterDelete().Add(1)

	return nil
}

// Get data.
func (p *Postgres) Get(ctx context.Context, id, target string, v any, prm *get.Get, options ...storage.Func[*get.Get]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*get.Get]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := get.New()
	if err != nil {
		return err
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return err
	}

	//////
	// Get.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return err
		}
	}

	// Build the statement.
	ds := goqu.From(trgt).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	selectSQL, args, err := ds.ToSQL()
	if err != nil {
		return customerror.NewFailedToError(storage.OperationRetrieve, customerror.WithError(err))
	}

	// Execute the query.
	if err := p.Client.GetContext(ctx, v, selectSQL, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return customerror.NewHTTPError(http.StatusNotFound)
		}

		return customerror.NewFailedToError(storage.OperationRetrieve, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	p.GetCounterGet().Add(1)

	return nil
}

// List data.
//
// NOTE: It uses param.List.Search to query the data.
func (p *Postgres) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return err
	}

	if finalParam.Search == "" {
		finalParam.Search = "1=1"
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return err
	}

	//////
	// List.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, "", trgt, finalParam); err != nil {
			return err
		}
	}

	// Build SQL query
	sql := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		finalParam.Fields.String(),
		trgt,
		finalParam.Search,
	)

	if finalParam.Sort != nil {
		srt, err := ToSQLString(finalParam.Sort.ToSort())
		if err != nil {
			return err
		}

		sql += fmt.Sprintf(" ORDER BY %s", srt)
	}

	if finalParam.Limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", finalParam.Limit)
	}

	if finalParam.Offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", finalParam.Offset)
	}

	if err := p.Client.SelectContext(ctx, v, sql); err != nil {
		return customerror.NewFailedToError(storage.OperationList, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, "", trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	p.GetCounterList().Add(1)

	return nil
}

// Set data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (p *Postgres) Set(ctx context.Context, id, target string, v any, prm *set.Set, options ...storage.Func[*set.Set]) (string, error) {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*set.Set]()
	if err != nil {
		return id, err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return id, err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := set.New()
	if err != nil {
		return id, err
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return id, err
	}

	//////
	// Set.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return id, err
		}
	}

	// Build the statement.
	ds := goqu.
		Insert(trgt).
		Rows(v).
		Returning("id")

	// Convert the query to SQL, and arguments.
	insertSQL, args, err := ds.ToSQL()
	if err != nil {
		return "", customerror.NewFailedToError(storage.OperationCreate, customerror.WithError(err))
	}

	// Execute the query.
	var returnedID string
	if err := p.Client.QueryRowContext(ctx, insertSQL, args...).Scan(&returnedID); err != nil {
		return "", customerror.NewFailedToError(storage.OperationCreate, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return returnedID, err
		}
	}

	//////
	// Metrics.
	//////

	p.GetCounterSet().Add(1)

	return returnedID, nil
}

// Update data.
func (p *Postgres) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return err
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return err
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return err
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return err
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return err
		}
	}

	// Build the statement.
	ds := goqu.Update(trgt).Set(v).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	updateSQL, args, err := ds.ToSQL()
	if err != nil {
		return customerror.NewFailedToError(storage.OperationUpdate, customerror.WithError(err))
	}

	if _, err := p.Client.ExecContext(ctx, updateSQL, args...); err != nil {
		return customerror.NewFailedToError(storage.OperationUpdate, customerror.WithError(err))
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, finalParam); err != nil {
			return err
		}
	}

	//////
	// Metrics.
	//////

	p.GetCounterUpdate().Add(1)

	return nil
}

// GetClient returns the client.
func (p *Postgres) GetClient() any {
	return p.Client
}

//////
// Factory.
//////

// New creates a new postgres storage.
func New(ctx context.Context, dataSource string) (*Postgres, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*Postgres)(nil)

	cfg := &Config{
		DriverName:     name,
		DataSourceName: dataSource,
	}

	client, err := sqlx.Open(cfg.DriverName, cfg.DataSourceName)
	if err != nil {
		return nil, customerror.NewFailedToError(
			"creating "+name+" client",
			customerror.WithError(err),
		)
	}

	if err := client.Ping(); err != nil {
		return nil, customerror.NewFailedToError("ping "+name, customerror.WithError(err))
	}

	s, err := storage.New(name)
	if err != nil {
		return nil, err
	}

	storage := &Postgres{
		Storage: s,

		Client: client,
		Config: cfg,
	}

	if err := validation.Validate(storage); err != nil {
		return nil, err
	}

	singleton = storage

	return storage, nil
}

//////
// Exported functionalities.
//////

// Get returns a setup Postgres, or set it up.
func Get() *Postgres {
	if singleton == nil {
		panic(name + " storage not initialized")
	}

	return singleton
}
