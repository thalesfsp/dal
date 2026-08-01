package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v3/internal/customapm"
	"github.com/thalesfsp/dal/v3/internal/logging"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/fields"
	"github.com/thalesfsp/sypl/v2/level"
	"github.com/thalesfsp/validation"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "s3"

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

// Config is the S3 configuration.
type Config = aws.Config

// S3 storage definition.
type S3 struct {
	*storage.Storage

	Bucket string `json:"bucket" validate:"required,gt=0"`

	// Client is the S3 client.
	Client *awss3.Client `json:"-" validate:"required,structonly"`

	closeOnce sync.Once

	// Config is the S3 configuration.
	//
	// Skipped by the validator: it dives into untagged nested structs, and an
	// aws.Config holds live SDK state (credentials guarded by a mutex, which
	// background SDK goroutines write) that must not be read reflectively.
	Config *Config `json:"-" validate:"-"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For S3, it isn't used at all.
	Target string `json:"-" validate:"omitempty,gt=0"`

	// S3 transfer manager supports multipart uploads.
	uploader *transfermanager.Client
}

//////
// Implements the IStorage interface.
//////

// Count returns the number of items in the storage.
func (s *S3) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Counted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*count.Count]()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := count.New()
	if err != nil {
		return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	input := &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
	}

	var fileCount int

	paginator := awss3.NewListObjectsV2Paginator(s.Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return 0, customapm.TraceError(ctx, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			), s.GetLogger(), s.GetCounterCountedFailed())
		}

		fileCount += len(page.Contents)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, "", target, int64(fileCount), finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Counted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterCounted().Add(1)

	return int64(fileCount), nil
}

// Delete removes data.
func (s *S3) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			s.GetLogger(),
			s.GetCounterDeletedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Deleted.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*delete.Delete]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := delete.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
	}

	// Application's default values.
	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
	}

	//////
	// Delete.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
		}
	}

	input := &awss3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(trgt),
	}

	if _, err := s.Client.DeleteObject(ctx, input); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationDelete.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterDeletedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, nil, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Deleted.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterDeleted().Add(1)

	return nil
}

// Retrieve data.
func (s *S3) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Retrieved.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*retrieve.Retrieve]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := retrieve.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
	}

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	input := &awss3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(trgt),
	}

	output, err := s.Client.GetObject(ctx, input)
	if err != nil {
		var noSuchKey *types.NoSuchKey
		var responseError *smithyhttp.ResponseError

		if errors.As(err, &noSuchKey) ||
			(errors.As(err, &responseError) && responseError.HTTPStatusCode() == http.StatusNotFound) {
			return customapm.TraceError(
				ctx,
				customerror.NewHTTPError(http.StatusNotFound),
				s.GetLogger(),
				s.GetCounterRetrievedFailed())
		}

		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterRetrievedFailed())
	}

	// The body is an open HTTP response stream; it must always be closed.
	defer output.Body.Close()

	data, err := shared.ReadAll(output.Body)
	if err != nil {
		return customapm.TraceError(
			ctx,
			err,
			s.GetLogger(),
			s.GetCounterRetrievedFailed())
	}

	if err := shared.Unmarshal(data, v); err != nil {
		return customapm.TraceError(
			ctx,
			err,
			s.GetLogger(),
			s.GetCounterRetrievedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Retrieved.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterRetrieved().Add(1)

	return nil
}

// List data.
//
// List returns the keys in the configured bucket. Search, Sort, Limit, Offset,
// and Fields are not currently applied.
func (s *S3) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Listed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*list.List]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := list.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	// Application's default values.
	finalParam.Search = "*"

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	input := &awss3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
	}

	keys := ResponseListKeys{[]string{}}

	paginator := awss3.NewListObjectsV2Paginator(s.Client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return customapm.TraceError(
				ctx,
				customerror.NewFailedToError(storage.OperationList.String(),
					customerror.WithError(err),
				),
				s.GetLogger(), s.GetCounterListedFailed(),
			)
		}

		for _, object := range page.Contents {
			keys.Keys = append(keys.Keys, *object.Key)
		}
	}

	if err := storage.ParseToStruct(keys, v); err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Listed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterListed().Add(1)

	return nil
}

// BulkCreate creates data in bulk.
func (s *S3) BulkCreate(
	context.Context,
	string,
	[]storage.BulkItem,
	*create.Create,
	...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	// TODO(v3): real implementation lands in E2.
	return storage.BulkResult{}, storage.ErrBulkCreateNotImplemented
}

// Create data.
//
// NOTE: `v` can be a file, a string, or an struct.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (s *S3) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Created.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*create.Create]()
	if err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := create.New()
	if err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	finalParam.TTL = 0

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}
	}

	// Prepare the input for the S3 upload request.
	uploadInput := &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(trgt),
	}

	// Use a type switch to handle different types of content.
	switch content := v.(type) {
	case *os.File:
		// If the content is a file, ensure it gets closed after we're done with it.
		defer content.Close()

		// Read the entire content of the file into a byte slice.
		b, err := shared.ReadAll(content)
		if err != nil {
			// If an error occurred, log it and return.
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}

		// Set the body of the S3 upload request to the body reader.
		uploadInput.Body = bytes.NewReader(b)

	case string:
		// Set the body of the S3 upload request to the body reader.
		uploadInput.Body = bytes.NewReader([]byte(content))

	case []byte:
		// Raw bytes are uploaded as-is (mirrors Update; without this case they
		// would be JSON-marshaled into a base64 string).
		uploadInput.Body = bytes.NewReader(content)

	default:
		// If the content is neither a file nor a string, assume it's a struct and marshal it to JSON.
		jsonData, err := shared.Marshal(content)
		if err != nil {
			// If an error occurred, log it and return.
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}

		// Set the body and the content type of the S3 upload request.
		uploadInput.Body = bytes.NewReader(jsonData)
		uploadInput.ContentType = aws.String("application/json")
	}

	// Perform the S3 upload request.
	uO, err := s.uploader.UploadObject(ctx, uploadInput)
	if err != nil {
		// If an error occurred, log it and return.
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterCreatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return "", customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCreatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Created.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterCreated().Add(1)

	return aws.ToString(uO.Location), nil
}

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (s *S3) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	if id == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("id"),
			s.GetLogger(),
			s.GetCounterUpdatedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		s.GetType(),
		Name,
		status.Updated.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := storage.NewOptions[*update.Update]()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}
	}

	//////
	// Params initialization.
	//////

	finalParam, err := update.New()
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	finalParam.TTL = 0

	finalParam = shared.MergeParams(finalParam, prm)

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
	}

	//////
	// Create.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}
	}

	// Prepare the input for the S3 upload request.
	uploadInput := &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(trgt),
	}

	// Use a type switch to handle different types of content.
	switch content := v.(type) {
	case *os.File:
		// Read the entire content of the file into a byte slice.
		b, err := shared.ReadAll(content)
		if err != nil {
			// If an error occurred, log it and return.
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}

		// Set the body of the S3 upload request to the body reader.
		uploadInput.Body = bytes.NewReader(b)

	case string:
		// Set the body of the S3 upload request to the body reader.
		uploadInput.Body = bytes.NewReader([]byte(content))

	case []byte:
		uploadInput.Body = bytes.NewReader(content)

	default:
		// If the content is neither a file nor a string, assume it's a struct and marshal it to JSON.
		jsonData, err := shared.Marshal(content)
		if err != nil {
			// If an error occurred, log it and return.
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}

		// Set the body and the content type of the S3 upload request.
		uploadInput.Body = bytes.NewReader(jsonData)
		uploadInput.ContentType = aws.String("application/json")
	}

	// Perform the S3 upload request.
	if _, err := s.uploader.UploadObject(ctx, uploadInput); err != nil {
		// If an error occurred, log it and return.
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterUpdatedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterUpdatedFailed())
		}
	}

	//////
	// Logging
	//////

	// Correlates the transaction, span and log, and logs it.
	s.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Updated.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	s.GetCounterUpdated().Add(1)

	return nil
}

// GetClient returns the client.
func (s *S3) GetClient() any {
	return s.Client
}

// Close releases the resources held by the storage.
func (s *S3) Close() error {
	s.closeOnce.Do(func() {
		// An aws-sdk-go-v2 client holds no closable handle; its only retained
		// network resource is the HTTP client's idle keep-alive pool. The SDK's
		// aws.HTTPClient interface is just Do(*http.Request), so
		// CloseIdleConnections has to be probed for rather than called
		// directly — *http.Client and *http.Transport both provide it, and a
		// caller-supplied client that does not is simply left alone.
		if s.Config == nil {
			return
		}

		if closer, ok := s.Config.HTTPClient.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	})

	return nil
}

//////
// Factory.
//////

// New creates a new S3 storage.
func New(ctx context.Context, bucket string, cfg *Config) (*S3, error) {
	// Enforces IStorage interface implementation.
	var _ storage.IStorage = (*S3)(nil)

	//////
	// Storage.
	//////

	s, err := storage.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	//////
	// Client.
	//////

	var finalConfig Config
	if cfg == nil {
		finalConfig, err = awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, customapm.TraceError(
				ctx,
				customerror.NewFailedToError(
					"load AWS configuration",
					customerror.WithError(err),
				),
				s.GetLogger(),
				nil,
			)
		}
	} else {
		finalConfig = *cfg
	}

	client := awss3.NewFromConfig(finalConfig, func(options *awss3.Options) {
		options.UsePathStyle = finalConfig.BaseEndpoint != nil
	})

	//////
	// Ping.
	//////

	r := retrier.New(retrier.ExponentialBackoff(3, 10*time.Second), nil)

	if err := r.RunCtx(ctx, func(ctx context.Context) error {
		input := &awss3.HeadBucketInput{
			Bucket: aws.String(bucket),
		}

		if _, err := client.HeadBucket(ctx, input); err != nil {
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

	//////
	// Validation.
	//////

	storage := &S3{
		Storage: s,

		Bucket: bucket,
		Client: client,
		Config: &finalConfig,

		uploader: transfermanager.New(client),
	}

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), nil)
	}

	//////
	// Singleton.
	//////

	singletonMutex.Lock()
	singleton = storage
	singletonMutex.Unlock()

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

// RetrieveSigned generates a pre-signed URL for an S3 object.
//
// This function generates a pre-signed URL that allows anyone with the URL to
// retrieve the specified object from S3, even if they don't have AWS credentials.
// The URL is valid for the specified duration.
//
// Parameters:
// - ctx: The context in which the function is called. This is used for error tracing.
// - s: The S3 storage interface. This must return an AWS SDK v2 *s3.Client.
// - bucket: The name of the S3 bucket containing the object.
// - key: The key (path) of the object in the S3 bucket.
// - expire: The duration for which the pre-signed URL is valid.
//
// Returns:
// - The pre-signed URL as a string. This can be used to retrieve the object.
// - An error if the operation failed, or nil if the operation succeeded.
func RetrieveSigned(
	ctx context.Context,
	s storage.IStorage,
	target, bucket string,
	expire time.Duration,
) (string, error) {
	// Assert that the underlying type of s.GetClient() is *s3.Client.
	client, ok := s.GetClient().(*awss3.Client)
	if !ok {
		// If the assertion failed, return an error.
		//
		//nolint:err113 // goerr113 was renamed err113 in golangci-lint v2
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(errors.New("failed to retrieve client")),
			),
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}

	// Prepare the input for the S3 GetObject request.
	input := &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(target),
	}

	// Generate the pre-signed URL.
	output, err := awss3.NewPresignClient(client).PresignGetObject(
		ctx,
		input,
		awss3.WithPresignExpires(expire),
	)
	if err != nil {
		// If an error occurred, trace it and return.
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterRetrievedFailed(),
		)
	}

	// Return the pre-signed URL and no error.
	return output.URL, nil
}
