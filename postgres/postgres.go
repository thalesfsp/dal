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
	"github.com/eapache/go-resiliency/retrier"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/internal/customapm"
	"github.com/thalesfsp/dal/internal/logging"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/dal/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/fields"
	"github.com/thalesfsp/sypl/level"
	"github.com/thalesfsp/validation"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "postgres"

// Singleton.
var singleton storage.IStorage

// Config is the postgres configuration.
type Config struct {
	DataSourceName string `json:"dataSourceName" validate:"required"`
	DriverName     string `json:"driverName"     validate:"required"`
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
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		p.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCountedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCountedFailed())
		}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return 0, customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCountedFailed())
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCountedFailed())
	}

	defaultSearch := "SELECT COUNT(*) FROM " + trgt

	if finalParam.Search == "" {
		finalParam.Search = defaultSearch
	}

	if prm != nil {
		if prm.Search != "" {
			prm.Search = defaultSearch
		}

		finalParam = prm
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, "", trgt, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCountedFailed())
		}
	}

	var count int64
	if err = p.Client.QueryRowContext(ctx, finalParam.Search).Scan(&count); err != nil {
		return 0, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCount.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterCountedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, "", trgt, count, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	p.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	p.GetCounterCounted().Add(1)

	return count, nil
}

// Delete removes data.
func (p *Postgres) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			p.GetLogger(),
			p.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		p.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterDeletedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterDeletedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterDeletedFailed())
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterDeletedFailed())
		}
	}

	ds := goqu.Delete(trgt).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	selectSQL, args, err := ds.ToSQL()
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationDelete.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterDeletedFailed(),
		)
	}

	if _, err := p.Client.ExecContext(ctx, selectSQL, args...); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationDelete.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterDeletedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	p.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	p.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (p *Postgres) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			p.GetLogger(),
			p.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		p.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterRetrievedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterRetrievedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterRetrievedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterRetrievedFailed())
		}
	}

	// Build the statement.
	ds := goqu.From(trgt).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	selectSQL, args, err := ds.ToSQL()
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterRetrievedFailed(),
		)
	}

	// Execute the query.
	if err := p.Client.GetContext(ctx, v, selectSQL, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return customapm.TraceError(
				ctx,
				customerror.NewHTTPError(http.StatusNotFound),
				p.GetLogger(),
				p.GetCounterRetrievedFailed(),
			)
		}

		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterRetrievedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	p.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	p.GetCounterRetrieved().Add(1)

	return nil
}

// List data.
//
// NOTE: It uses param.List.Search to query the data.
func (p *Postgres) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		p.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterListedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterListedFailed())
		}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterListedFailed())
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterListedFailed())
	}

	defaultSearch := "SELECT * FROM " + trgt

	if finalParam.Search == "" {
		finalParam.Search = defaultSearch
	}

	if prm != nil {
		if prm.Search != "" {
			prm.Search = defaultSearch
		}

		finalParam = prm
	}

	//////
	// List.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterListedFailed())
		}
	}

	if err := p.Client.SelectContext(ctx, v, finalParam.Search); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationList.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterListedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	p.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	p.GetCounterListed().Add(1)

	return nil
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (p *Postgres) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		p.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCreatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return "", customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCreatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterCreatedFailed())
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
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterCreatedFailed(),
		)
	}

	// Execute the query.
	var returnedID string
	if err := p.Client.QueryRowContext(ctx, insertSQL, args...).Scan(&returnedID); err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterCreatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, v, finalParam); err != nil {
			return "", err
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	p.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	p.GetCounterCreated().Add(1)

	return returnedID, nil
}

// Update data.
func (p *Postgres) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			p.GetLogger(),
			p.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		p.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterUpdatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterUpdatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, p.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterUpdatedFailed())
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, p, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterUpdatedFailed())
		}
	}

	// Build the statement.
	ds := goqu.Update(trgt).Set(v).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	updateSQL, args, err := ds.ToSQL()
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterUpdatedFailed(),
		)
	}

	if _, err := p.Client.ExecContext(ctx, updateSQL, args...); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			p.GetLogger(),
			p.GetCounterUpdatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, p, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, p.GetLogger(), p.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	p.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	p.GetCounterUpdated().Add(1)

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

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		DriverName:     Name,
		DataSourceName: dataSource,
	}

	client, err := sqlx.Open(cfg.DriverName, cfg.DataSourceName)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("connect", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	if err := r.Run(func() error {
		if err := client.Ping(); err != nil {
			cE := customerror.NewFailedToError(
				"ping",
				customerror.WithError(err),
				customerror.WithTag("retry"),
			)

			s.GetLogger().Errorln(cE)

			return cE
		}

		return nil
	}); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterPingFailed())
	}

	storage := &Postgres{
		Storage: s,

		Client: client,
		Config: cfg,
	}

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), nil)
	}

	singleton = storage

	return storage, nil
}

//////
// Exported functionalities.
//////

// Get returns a setup storage, or set it up.
func Get() storage.IStorage {
	if singleton == nil {
		panic(fmt.Sprintf("%s %s not %s", Name, storage.Type, status.Initialized))
	}

	return singleton
}

// Set sets the storage, primarily used for testing.
func Set(s storage.IStorage) {
	singleton = s
}
