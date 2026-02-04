package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	// Import the mysql driver.
	_ "github.com/go-sql-driver/mysql"

	// Import the mysql dialect for goqu.
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"

	"github.com/doug-martin/goqu/v9"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/jmoiron/sqlx"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v2/internal/customapm"
	"github.com/thalesfsp/dal/v2/internal/logging"
	"github.com/thalesfsp/dal/v2/internal/shared"
	"github.com/thalesfsp/dal/v2/storage"
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
const Name = "mysql"

// Singleton.
var singleton storage.IStorage

// Config is the MySQL configuration.
type Config struct {
	DataSourceName string `json:"dataSourceName" validate:"required"`
	DriverName     string `json:"driverName"     validate:"required"`
}

// MySQL storage definition.
type MySQL struct {
	*storage.Storage

	// Client is the MySQL client.
	Client *sqlx.DB `json:"-" validate:"required"`

	// Config is the MySQL configuration.
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

// QuoteIdentifier quotes an identifier (table, column name) for MySQL using
// backticks. Embedded backticks are escaped by doubling them.
func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

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

func (m *MySQL) createDB(ctx context.Context, name string) error {
	if _, err := m.
		Client.
		ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", QuoteIdentifier(name))); err != nil {
		return customerror.NewFailedToError("createDB", customerror.WithError(err))
	}

	return nil
}

func (m *MySQL) deleteTable(ctx context.Context, name string) error {
	if _, err := m.
		Client.
		ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", name)); err != nil {
		return customerror.NewFailedToError("deleteTable", customerror.WithError(err))
	}

	return nil
}

func (m *MySQL) createTable(ctx context.Context, name, description string) error {
	sql := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		%s
	)`, name, description)

	if _, err := m.
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
func (m *MySQL) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
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
		if err := o.PreHookFunc(ctx, m, "", trgt, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}
	}

	var count int64
	if err = m.Client.QueryRowContext(ctx, finalParam.Search).Scan(&count); err != nil {
		return 0, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCount.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterCountedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, "", trgt, count, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterCounted().Add(1)

	return count, nil
}

// Delete removes data.
func (m *MySQL) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			m.GetLogger(),
			m.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
		}
	}

	ds := goqu.Dialect(Name).Delete(trgt).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	selectSQL, args, err := ds.ToSQL()
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationDelete.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterDeletedFailed(),
		)
	}

	if _, err := m.Client.ExecContext(ctx, selectSQL, args...); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationDelete.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterDeletedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (m *MySQL) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
		}
	}

	// Build the statement.
	ds := goqu.Dialect(Name).From(trgt).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	selectSQL, args, err := ds.ToSQL()
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	// Execute the query.
	if err := m.Client.GetContext(ctx, v, selectSQL, args...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return customapm.TraceError(
				ctx,
				customerror.NewHTTPError(http.StatusNotFound),
				m.GetLogger(),
				m.GetCounterRetrievedFailed(),
			)
		}

		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterRetrieved().Add(1)

	return nil
}

// List data.
//
// NOTE: It uses param.List.Search to query the data.
func (m *MySQL) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
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
		if err := o.PreHookFunc(ctx, m, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	if err := m.Client.SelectContext(ctx, v, finalParam.Search); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationList.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterListedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterListed().Add(1)

	return nil
}

// Create data.
//
// NOTE: MySQL does not support RETURNING clause. This method uses
// LastInsertId() to retrieve the auto-generated ID. If you use non-numeric
// IDs (e.g., UUIDs), set the ID yourself before calling Create and it will
// be returned as-is.
func (m *MySQL) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
		}
	}

	// Build the statement. MySQL does not support RETURNING, so we use
	// ExecContext and retrieve the LastInsertId.
	ds := goqu.Dialect(Name).
		Insert(trgt).
		Rows(v)

	// Convert the query to SQL, and arguments.
	insertSQL, args, err := ds.ToSQL()
	if err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterCreatedFailed(),
		)
	}

	// Execute the query.
	result, err := m.Client.ExecContext(ctx, insertSQL, args...)
	if err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterCreatedFailed(),
		)
	}

	// If the caller provided an ID, return it as-is (non-auto-increment case).
	// Otherwise, retrieve the auto-generated ID.
	returnedID := id
	if returnedID == "" {
		lastID, err := result.LastInsertId()
		if err != nil {
			return "", customapm.TraceError(
				ctx,
				customerror.NewFailedToError("get last insert id", customerror.WithError(err)),
				m.GetLogger(),
				m.GetCounterCreatedFailed(),
			)
		}

		returnedID = strconv.FormatInt(lastID, 10)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return "", err
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterCreated().Add(1)

	return returnedID, nil
}

// Update data.
func (m *MySQL) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			m.GetLogger(),
			m.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	//////
	// Update.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
		}
	}

	// Build the statement.
	ds := goqu.Dialect(Name).Update(trgt).Set(v).Where(goqu.C("id").Eq(id))

	// Convert the query to SQL, and arguments.
	updateSQL, args, err := ds.ToSQL()
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterUpdatedFailed(),
		)
	}

	if _, err := m.Client.ExecContext(ctx, updateSQL, args...); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterUpdatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterUpdated().Add(1)

	return nil
}

// GetClient returns the client.
func (m *MySQL) GetClient() any {
	return m.Client
}

//////
// Factory.
//////

// New creates a new MySQL storage.
//
// The dataSource should be in the format:
//
//	user:password@tcp(host:port)/dbname?parseTime=true
//
// For MariaDB on AWS RDS, this would be:
//
//	admin:password@tcp(mydb.csrubqc9tdmh.us-east-1.rds.amazonaws.com:3306)/ringboost?parseTime=true
func New(ctx context.Context, dataSource string) (*MySQL, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*MySQL)(nil)

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

	storage := &MySQL{
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
