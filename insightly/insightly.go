package insightly

import (
	"context"
	"fmt"
	"net/http"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/internal/customapm"
	"github.com/thalesfsp/dal/internal/logging"
	"github.com/thalesfsp/dal/internal/shared"
	"github.com/thalesfsp/dal/storage"
	"github.com/thalesfsp/httpclient"
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
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "insightly"

// Singleton.
var singleton storage.IStorage

// Insightly storage definition.
type Insightly struct {
	*storage.Storage

	// Client is the HTTP client.
	Client *httpclient.Client `json:"-" validate:"required"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For Redis, it isn't used at all.
	Target string `json:"-" validate:"omitempty,gt=0"`

	// Endpoint is the URL to the authentication endpoint.
	Endpoint string `json:"endpoint" validate:"required,url"`

	// APIKey is the API key to authenticate.
	APIKey string `json:"apiKey" validate:"required"`
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (str *Insightly) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		str.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	return 0, customapm.TraceError(
		ctx,
		customerror.NewHTTPError(http.StatusNotImplemented),
		str.GetLogger(),
		str.GetCounterCountedFailed(),
	)
}

// Delete removes data.
func (str *Insightly) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		str.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	return customapm.TraceError(
		ctx,
		customerror.NewHTTPError(http.StatusNotImplemented),
		str.GetLogger(),
		str.GetCounterDeletedFailed(),
	)
}

// Retrieve data.
func (str *Insightly) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			str.GetLogger(),
			str.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		str.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	return customapm.TraceError(
		ctx,
		customerror.NewHTTPError(http.StatusNotImplemented),
		str.GetLogger(),
		str.GetCounterRetrieved(),
	)
}

// List data.
//
// WARN: In general, projections that include non-indexed fields or fields
// that are part of a covered index (i.e., an index that includes all the
// projected fields) are less likely to impact performance.
//
// NOTE: It uses param.List.Search to query the data.
func (str *Insightly) List(ctx context.Context, target string, v any, prm *list.List, opts ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		str.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	return customapm.TraceError(
		ctx,
		customerror.NewHTTPError(http.StatusNotImplemented),
		str.GetLogger(),
		str.GetCounterListed(),
	)
}

// Create data.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set.
func (str *Insightly) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	if id == "" {
		return "", customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			str.GetLogger(),
			str.GetCounterCreatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		str.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, str.GetLogger(), str.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, str.GetLogger(), str.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, str.GetLogger(), str.GetCounterCreatedFailed())
	}

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, str.Target)
	if err != nil {
		return "", customapm.TraceError(ctx, err, str.GetLogger(), str.GetCounterCreatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, str, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, str.GetLogger(), str.GetCounterCreatedFailed())
		}
	}

	//////
	// Set contact data
	//////

	var cCR CreateContactResponse

	resp, err := httpclient.Get().Post(
		ctx,
		str.Endpoint,
		httpclient.WithBearerAuthToken(str.APIKey),
		httpclient.WithReqBody(v),
		httpclient.WithRespBody(&cCR),
	)
	if err != nil {
		return "", customapm.TraceError(ctx, err, str.GetLogger(), str.GetCounterCreatedFailed())
	}

	defer resp.Body.Close()

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, str, id, trgt, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, str.GetLogger(), str.GetCounterCreatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	str.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	str.GetCounterCreated().Add(1)

	return fmt.Sprintf("%d", cCR.ContactID), nil
}

// Update data.
func (str *Insightly) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			str.GetLogger(),
			str.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		str.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	return customapm.TraceError(
		ctx,
		customerror.NewHTTPError(http.StatusNotImplemented),
		str.GetLogger(),
		str.GetCounterUpdated(),
	)
}

// GetClient returns the client.
func (str *Insightly) GetClient() any {
	return str.Client
}

//////
// Factory.
//////

// New creates a new storage.
func New(ctx context.Context, endpoint, apiKey string) (*Insightly, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*Insightly)(nil)

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	client, err := httpclient.Initialize(httpclient.WithClientName(Name))
	if err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), nil)
	}

	storage := &Insightly{
		Client:  client,
		Storage: s,

		APIKey:   apiKey,
		Endpoint: endpoint,
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

// Get returns a setup, or set it up.
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
