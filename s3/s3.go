package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/dal/v2/internal/customapm"
	"github.com/thalesfsp/dal/v2/internal/logging"
	"github.com/thalesfsp/dal/v2/internal/shared"
	"github.com/thalesfsp/dal/v2/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
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
const Name = "s3"

// Singleton.
var singleton storage.IStorage

// Config is the S3 configuration.
type Config = aws.Config

// S3 storage definition.
type S3 struct {
	*storage.Storage

	Bucket string `json:"bucket" validate:"required,gt=0"`

	// Client is the S3 client.
	Client *s3.S3 `json:"-" validate:"required"`

	// Config is the S3 configuration.
	Config *Config `json:"-"`

	// Target allows to set a static target. If it is empty, the target will be
	// dynamic - the one set at the operation (count, create, delete, etc) time.
	// Depending on the storage, target is a collection, a table, a bucket, etc.
	// For ElasticSearch, for example it doesn't have a concept of a database -
	// the target then is the index. Due to different cases of ElasticSearch
	// usage, the target can be static or dynamic - defined at the index time,
	// for example: log-{YYYY}-{MM}. For S3, it isn't used at all.
	Target string `json:"-" validate:"omitempty,gt=0"`

	// S3 manager is an specialized uploader for S3 which supports multipart
	// uploads.
	uploader *s3manager.Uploader
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

	if prm != nil {
		finalParam = prm
	}

	//////
	// Count.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, nil, finalParam); err != nil {
			return 0, customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterCountedFailed())
		}
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
	}

	var fileCount int

	if err := s.Client.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		fileCount += len(page.Contents)

		return true
	}); err != nil {
		return 0, customapm.TraceError(ctx, customerror.NewFailedToError(
			storage.OperationCount.String(),
			customerror.WithError(err),
		), s.GetLogger(), s.GetCounterCountedFailed())
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
	if prm != nil {
		finalParam = prm
	}

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

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(trgt),
	}

	if _, err := s.Client.DeleteObjectWithContext(ctx, input); err != nil {
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

	if prm != nil {
		finalParam = prm
	}

	//////
	// Target definition.
	//////

	trgt, err := shared.TargetName(target, s.Target)
	if err != nil {
		return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterDeletedFailed())
	}

	//////
	// Retrieve.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, id, target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterRetrievedFailed())
		}
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(trgt),
	}

	output, err := s.Client.GetObjectWithContext(ctx, input)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationRetrieve.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterRetrievedFailed())
	}

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
// NOTE: It uses params.List.Search to query the data.
//
// NOTE: S3 does not support the concept of "offset" and "limit" in the same
// way that a traditional SQL database does.
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

	if prm != nil {
		finalParam = prm
	}

	//////
	// Query.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, s, "", target, v, finalParam); err != nil {
			return customapm.TraceError(ctx, err, s.GetLogger(), s.GetCounterListedFailed())
		}
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s.Bucket),
	}

	keys := ResponseListKeys{[]string{}}

	if err := s.Client.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, object := range page.Contents {
			keys.Keys = append(keys.Keys, *object.Key)
		}

		return true
	}); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(storage.OperationList.String(),
				customerror.WithError(err),
			),
			s.GetLogger(), s.GetCounterListedFailed(),
		)
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

	if prm != nil {
		finalParam = prm
	}

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
	uploadInput := &s3manager.UploadInput{
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
	uO, err := s.uploader.Upload(uploadInput)
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

	return uO.Location, nil
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

	if prm != nil {
		finalParam = prm
	}

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
	uploadInput := &s3manager.UploadInput{
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
	if _, err := s.uploader.Upload(uploadInput); err != nil {
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

//////
// Factory.
//////

// New creates a new S3 storage.
func New(ctx context.Context, bucket string, cfg *Config) (*S3, error) {
	if singleton != nil {
		s3Storage, ok := singleton.(*S3)
		if !ok {
			return nil, customerror.NewFailedToError("retrieve client")
		}

		return s3Storage, nil
	}

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

	sess, err := session.NewSession(cfg)
	if err != nil {
		log.Fatal(err)
	}

	client := s3.New(sess)

	//////
	// Ping.
	//////

	r := retrier.New(retrier.ExponentialBackoff(3, 10*time.Second), nil)

	if err := r.Run(func() error {
		input := &s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		}

		if _, err := client.HeadBucketWithContext(ctx, input); err != nil {
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
		Config: cfg,

		uploader: s3manager.NewUploader(sess),
	}

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), nil)
	}

	//////
	// Singleton.
	//////

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

// RetrieveSigned generates a pre-signed URL for an S3 object.
//
// This function generates a pre-signed URL that allows anyone with the URL to
// retrieve the specified object from S3, even if they don't have AWS credentials.
// The URL is valid for the specified duration.
//
// Parameters:
// - ctx: The context in which the function is called. This is used for error tracing.
// - s: The S3 storage interface. This must have an underlying type of *s3.S3.
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
	// Assert that the underlying type of s.GetClient() is *s3.S3.
	client, ok := s.GetClient().(*s3.S3)
	if !ok {
		// If the assertion failed, return an error.
		//
		//nolint:goerr113
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(errors.New("failed to retrieve client")),
			),
			s.GetLogger(),
			s.GetCounterCreatedFailed(),
		)
	}

	// Prepare the input for the S3 GetObject request.
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(target),
	}

	// Generate the GetObject request.
	req, _ := client.GetObjectRequest(input)

	// Generate the pre-signed URL.
	url, err := req.Presign(expire)
	if err != nil {
		// If an error occurred, trace it and return.
		return "", customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			),
			s.GetLogger(),
			s.GetCounterCreatedFailed(),
		)
	}

	// Return the pre-signed URL and no error.
	return url, nil
}
