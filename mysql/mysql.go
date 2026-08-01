package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/doug-martin/goqu/v9"
	// Import the mysql dialect for goqu.
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	"github.com/eapache/go-resiliency/retrier"

	// Import the mysql driver.
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/envelope"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/validation"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "mysql"

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// driverName is the database/sql driver `New` opens the client with. It
// defaults to `Name` and exists as a test seam so the constructor's post-open
// error paths (which must close the client to avoid leaking the pool) can be
// exercised against an injected, controllable driver. Production never changes
// it.
var driverName = Name

// Config is the MySQL configuration.
type Config struct {
	DataSourceName string `json:"dataSourceName" validate:"required"`
	DriverName     string `json:"driverName"     validate:"required"`
}

// MySQL storage definition.
type MySQL struct {
	*storage.Storage

	// Client is the MySQL client.
	Client *sqlx.DB `json:"-" validate:"required,structonly"`

	// Config is the MySQL configuration.
	Config *Config `json:"-"`

	closeOnce sync.Once

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
	if err := shared.ValidateSQLIdentifier(name); err != nil {
		return err
	}

	if _, err := m.
		Client.
		ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", name)); err != nil {
		return customerror.NewFailedToError("deleteTable", customerror.WithError(err))
	}

	return nil
}

func (m *MySQL) createTable(ctx context.Context, name, description string) error {
	if err := shared.ValidateSQLIdentifier(name); err != nil {
		return err
	}

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
	var trgt string

	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage: m,
		Adapter: Name,
		Status:  status.Counted.String(),
		ResolveTarget: func() (string, error) {
			var err error

			trgt, err = shared.TargetName(target, m.Target)
			if err == nil && (prm == nil || prm.Search == "") {
				err = shared.ValidateSQLIdentifier(trgt)
			}

			return trgt, err
		},
		Param:   prm,
		Options: options,
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err == nil {
				defaultTarget := target
				if defaultTarget == "" {
					defaultTarget = m.Target
				}

				params.Search = "SELECT COUNT(*) FROM " + defaultTarget
			}

			return params, err
		},
		SuccessCounter: m.GetCounterCounted(),
		FailureCounter: m.GetCounterCountedFailed(),
	}, func(ctx context.Context, params *count.Count) (envelope.Result[int64], error) {
		var result int64
		if err := m.Client.QueryRowContext(ctx, params.Search).Scan(&result); err != nil {
			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[int64]{Value: result, PostHookData: result}, nil
	})
}

// Delete removes data.
func (m *MySQL) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	var trgt string

	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage: m,
		Adapter: Name,
		Status:  status.Deleted.String(),
		ID:      id,
		ResolveTarget: func() (string, error) {
			var err error

			trgt, err = shared.TargetName(target, m.Target)

			return trgt, err
		},
		Param:          prm,
		Options:        options,
		NewParams:      delete.New,
		RequiresID:     true,
		SuccessCounter: m.GetCounterDeleted(),
		FailureCounter: m.GetCounterDeletedFailed(),
	}, func(ctx context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		ds := goqu.Dialect(Name).Delete(trgt).Where(goqu.C("id").Eq(id))

		// Convert the query to SQL, and arguments.
		selectSQL, args, err := ds.ToSQL()
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			)
		}

		if _, err := m.Client.ExecContext(ctx, selectSQL, args...); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{}, nil
	})

	return err
}

// Retrieve data.
func (m *MySQL) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	var trgt string

	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:  m,
		Adapter:  Name,
		Status:   status.Retrieved.String(),
		ID:       id,
		HookData: v,
		ResolveTarget: func() (string, error) {
			var err error

			trgt, err = shared.TargetName(target, m.Target)

			return trgt, err
		},
		Param:          prm,
		Options:        options,
		NewParams:      retrieve.New,
		RequiresID:     true,
		SuccessCounter: m.GetCounterRetrieved(),
		FailureCounter: m.GetCounterRetrievedFailed(),
	}, func(ctx context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		// Build the statement.
		ds := goqu.Dialect(Name).From(trgt).Where(goqu.C("id").Eq(id))

		// Convert the query to SQL, and arguments.
		selectSQL, args, err := ds.ToSQL()
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}

		// Execute the query.
		if err := m.Client.GetContext(ctx, v, selectSQL, args...); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return envelope.Result[envelope.Void]{}, customerror.NewHTTPError(http.StatusNotFound)
			}

			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// Search is executed as the SQL query. Sort, Limit, Offset, and Fields are not
// translated into SQL; include them in Search when needed.
func (m *MySQL) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	var trgt string

	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:  m,
		Adapter:  Name,
		Status:   status.Listed.String(),
		HookData: v,
		ResolveTarget: func() (string, error) {
			var err error

			trgt, err = shared.TargetName(target, m.Target)
			if err == nil && (prm == nil || prm.Search == "") {
				err = shared.ValidateSQLIdentifier(trgt)
			}

			return trgt, err
		},
		Param:   prm,
		Options: options,
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err == nil {
				defaultTarget := target
				if defaultTarget == "" {
					defaultTarget = m.Target
				}

				params.Search = "SELECT * FROM " + defaultTarget
			}

			return params, err
		},
		SuccessCounter: m.GetCounterListed(),
		FailureCounter: m.GetCounterListedFailed(),
	}, func(ctx context.Context, params *list.List) (envelope.Result[envelope.Void], error) {
		if err := m.Client.SelectContext(ctx, v, params.Search); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// Create data.
//
// NOTE: MySQL does not support RETURNING clause. This method uses
// LastInsertId() to retrieve the auto-generated ID. If you use non-numeric
// IDs (e.g., UUIDs), set the ID yourself before calling Create and it will
// be returned as-is.
func (m *MySQL) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	var trgt string

	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:  m,
		Adapter:  Name,
		Status:   status.Created.String(),
		ID:       id,
		HookData: v,
		ResolveTarget: func() (string, error) {
			var err error

			trgt, err = shared.TargetName(target, m.Target)

			return trgt, err
		},
		Param:          prm,
		Options:        options,
		NewParams:      create.New,
		SuccessCounter: m.GetCounterCreated(),
		FailureCounter: m.GetCounterCreatedFailed(),
	}, func(ctx context.Context, _ *create.Create) (envelope.Result[string], error) {
		// Build the statement. MySQL does not support RETURNING, so we use
		// ExecContext and retrieve the LastInsertId.
		ds := goqu.Dialect(Name).
			Insert(trgt).
			Rows(v)

		// Convert the query to SQL, and arguments.
		insertSQL, args, err := ds.ToSQL()
		if err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		// Execute the query.
		result, err := m.Client.ExecContext(ctx, insertSQL, args...)
		if err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		// If the caller provided an ID, return it as-is (non-auto-increment case).
		// Otherwise, retrieve the auto-generated ID.
		returnedID := id
		if returnedID == "" {
			lastID, err := result.LastInsertId()
			if err != nil {
				return envelope.Result[string]{}, customerror.NewFailedToError(
					"get last insert id",
					customerror.WithError(err),
				)
			}

			returnedID = strconv.FormatInt(lastID, 10)
		}

		return envelope.Result[string]{Value: returnedID, PostHookData: v}, nil
	})
}

// Update data.
func (m *MySQL) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	var trgt string

	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:  m,
		Adapter:  Name,
		Status:   status.Updated.String(),
		ID:       id,
		HookData: v,
		ResolveTarget: func() (string, error) {
			var err error

			trgt, err = shared.TargetName(target, m.Target)

			return trgt, err
		},
		Param:          prm,
		Options:        options,
		NewParams:      update.New,
		RequiresID:     true,
		SuccessCounter: m.GetCounterUpdated(),
		FailureCounter: m.GetCounterUpdatedFailed(),
	}, func(ctx context.Context, _ *update.Update) (envelope.Result[envelope.Void], error) {
		// Build the statement.
		ds := goqu.Dialect(Name).Update(trgt).Set(v).Where(goqu.C("id").Eq(id))

		// Convert the query to SQL, and arguments.
		updateSQL, args, err := ds.ToSQL()
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}

		res, err := m.Client.ExecContext(ctx, updateSQL, args...)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}

		// Surface updates that matched nothing as 404, consistent with Retrieve.
		if rowsAffected, err := res.RowsAffected(); err == nil && rowsAffected == 0 {
			return envelope.Result[envelope.Void]{}, customerror.NewHTTPError(http.StatusNotFound)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// GetClient returns the client.
func (m *MySQL) GetClient() any {
	return m.Client
}

// Close releases the resources held by the storage.
func (m *MySQL) Close() error {
	var err error

	m.closeOnce.Do(func() {
		err = m.Client.Close()
	})

	return err
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
		DriverName:     driverName,
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

	// The client is open but not yet handed to the caller. Close it on every
	// error path below (ping failure, validation failure, or any future one)
	// so the underlying connection pool is never leaked. `success` flips true
	// only once the fully-built storage is about to be returned.
	success := false

	defer func() {
		if !success {
			_ = client.Close()
		}
	}()

	r := retrier.New(retrier.ExponentialBackoff(3, shared.TimeoutPing), nil)

	if err := r.RunCtx(ctx, func(ctx context.Context) error {
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

	singletonMutex.Lock()
	singleton = storage
	singletonMutex.Unlock()

	success = true

	return storage, nil
}

//////
// Exported functionalities.
//////

// Get returns a setup storage, or set it up.
func Get() storage.IStorage {
	singletonMutex.RLock()
	s := singleton
	singletonMutex.RUnlock()

	if s == nil {
		panic(fmt.Sprintf("%s %s not %s", Name, storage.Type, status.Initialized))
	}

	return s
}

// Set sets the storage, primarily used for testing.
func Set(s storage.IStorage) {
	singletonMutex.Lock()
	singleton = s
	singletonMutex.Unlock()
}
