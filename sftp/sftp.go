package sftp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/pkg/sftp"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/internal/customapm"
	"github.com/thalesfsp/dal/internal/logging"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/dal/storage"
	"github.com/thalesfsp/params/count"
	"github.com/thalesfsp/params/create"
	"github.com/thalesfsp/params/delete"
	"github.com/thalesfsp/params/list"
	"github.com/thalesfsp/params/retrieve"
	"github.com/thalesfsp/params/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/fields"
	"github.com/thalesfsp/sypl/level"
	"github.com/thalesfsp/validation"
	"golang.org/x/crypto/ssh"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "sftp"

// Singleton.
var singleton storage.IStorage

type (
	// Config is the SFTP configuration.
	Config = ssh.ClientConfig

	// Option is for the SFTP configuration.
	Option = sftp.ClientOption
)

// SFTP storage definition.
type SFTP struct {
	*storage.Storage

	Client *sftp.Client `json:"-" validate:"required"`

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
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (m *SFTP) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
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
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	if prm != nil {
		finalParam = prm
	}

	//////
	// Filter.
	//////

	if finalParam.Search != "" {
		finalParam.Search = "*"
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, "", trgt, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCountedFailed())
		}
	}

	files, err := m.Client.ReadDir(trgt)
	if err != nil {
		return 0, customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCount.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterCountedFailed(),
		)
	}

	count := 0

	for _, file := range files {
		if !file.IsDir() {
			count++
		}
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

	return int64(count), nil
}

// Delete removes data.
func (m *SFTP) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
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

	if err := m.Client.Remove(trgt); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return customapm.TraceError(
				ctx,
				customerror.NewFailedToError(
					storage.OperationDelete.String(),
					customerror.WithError(err),
				),
				m.GetLogger(),
				m.GetCounterDeletedFailed(),
			)
		}
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
func (m *SFTP) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
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

	srcFile, err := m.Client.Open(trgt)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterRetrievedFailed(),
		)
	}

	defer srcFile.Close()

	content, err := shared.ReadAll(srcFile)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterRetrievedFailed())
	}

	// Convert content to `v`.
	if err := shared.Unmarshal(content, v); err != nil {
		return customapm.TraceError(
			ctx,
			err,
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
// WARN: In general, projections that include non-indexed fields or fields
// that are part of a covered index (i.e., an index that includes all the
// projected fields) are less likely to impact performance.
//
// NOTE: It uses param.List.Search to query the data.
func (m *SFTP) List(ctx context.Context, target string, v any, prm *list.List, opts ...storage.Func[*list.List]) error {
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
	for _, option := range opts {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, m.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
	}

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, "", trgt, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
		}
	}

	files, err := m.Client.ReadDir(trgt)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationList.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterListedFailed(),
		)
	}

	keys := ResponseListKeys{[]string{}}

	for _, file := range files {
		if !file.IsDir() {
			keys.Keys = append(keys.Keys, file.Name())
		}
	}

	if err := storage.ParseToStruct(keys, v); err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterListedFailed())
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
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (m *SFTP) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
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

	b, err := shared.Marshal(v)
	if err != nil {
		return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
	}

	dstFile, err := m.Client.Create(trgt)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", customapm.TraceError(
				ctx,
				customerror.NewFailedToError(storage.OperationCreate.String(), customerror.WithError(err)),
				m.GetLogger(),
				m.GetCounterCreatedFailed(),
			)
		}
	}

	if dstFile == nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationCreate.String()+" file, it's nil"),
			m.GetLogger(),
			m.GetCounterCreatedFailed(),
		)
	}

	defer dstFile.Close()

	if _, err := dstFile.Write(b); err != nil {
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterCreatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterCreatedFailed())
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

	return id, nil
}

// Update data.
func (m *SFTP) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
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
	for _, option := range opts {
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

	b, err := shared.Marshal(v)
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterUpdatedFailed())
	}

	dstFile, err := m.Client.Create(trgt)
	if err != nil {
		if !os.IsNotExist(err) {
			return customapm.TraceError(
				ctx,
				customerror.NewFailedToError(storage.OperationUpdate.String(), customerror.WithError(err)),
				m.GetLogger(),
				m.GetCounterUpdatedFailed(),
			)
		}
	}

	if dstFile == nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationUpdate.String()+" file, it's nil"),
			m.GetLogger(),
			m.GetCounterUpdatedFailed(),
		)
	}

	defer dstFile.Close()

	if _, err := dstFile.Write(b); err != nil {
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
func (m *SFTP) GetClient() any {
	return m.Client
}

//////
// Factory.
//////

// New creates a new SFTP storage.
//
// NOTE: addr format is: host:port.
func New(ctx context.Context, addr string, cfg *Config, options ...Option) (*SFTP, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*SFTP)(nil)

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("convert addr to URL", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	conn, err := ssh.Dial("tcp", u.Host, cfg)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("dial", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	client, err := sftp.NewClient(conn, options...)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("connect", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	storage := &SFTP{
		Storage: s,

		Client: client,
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

// Get returns a setup SFTP, or set it up.
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
