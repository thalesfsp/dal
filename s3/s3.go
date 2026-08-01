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
	"github.com/thalesfsp/dal/v3/internal/envelope"
	"github.com/thalesfsp/dal/v3/internal/shared"
	"github.com/thalesfsp/dal/v3/storage"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
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
	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         target,
		Param:          prm,
		Options:        options,
		SuccessCounter: s.GetCounterCounted(),
		FailureCounter: s.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err == nil {
				params.Search = "*"
			}

			return params, err
		},
	}, func(runCtx context.Context, _ *count.Count) (envelope.Result[int64], error) {
		input := &awss3.ListObjectsV2Input{
			Bucket: aws.String(s.Bucket),
		}

		var fileCount int

		paginator := awss3.NewListObjectsV2Paginator(s.Client, input)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(runCtx)
			if err != nil {
				return envelope.Result[int64]{}, customerror.NewFailedToError(
					storage.OperationCount.String(),
					customerror.WithError(err),
				)
			}

			fileCount += len(page.Contents)
		}

		result := int64(fileCount)

		return envelope.Result[int64]{Value: result, PostHookData: result}, nil
	})
}

// Delete removes data.
func (s *S3) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	var trgt string

	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         target,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterDeleted(),
		FailureCounter: s.GetCounterDeletedFailed(),
		NewParams: func() (*delete.Delete, error) {
			params, err := delete.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(runCtx context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		input := &awss3.DeleteObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(trgt),
		}

		if _, err := s.Client.DeleteObject(runCtx, input); err != nil {
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
func (s *S3) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	var trgt string

	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterRetrieved(),
		FailureCounter: s.GetCounterRetrievedFailed(),
		NewParams: func() (*retrieve.Retrieve, error) {
			params, err := retrieve.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(runCtx context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		input := &awss3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(trgt),
		}

		output, err := s.Client.GetObject(runCtx, input)
		if err != nil {
			var noSuchKey *types.NoSuchKey
			var responseError *smithyhttp.ResponseError

			if errors.As(err, &noSuchKey) ||
				(errors.As(err, &responseError) && responseError.HTTPStatusCode() == http.StatusNotFound) {
				return envelope.Result[envelope.Void]{}, customerror.NewHTTPError(http.StatusNotFound)
			}

			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}

		// The body is an open HTTP response stream; it must always be closed.
		defer output.Body.Close()

		data, err := shared.ReadAll(output.Body)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		if err := shared.Unmarshal(data, v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// List returns the keys in the configured bucket. Search, Sort, Limit, Offset,
// and Fields are not currently applied.
func (s *S3) List(ctx context.Context, target string, v any, prm *list.List, options ...storage.Func[*list.List]) error {
	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         target,
		HookData:       v,
		Param:          prm,
		Options:        options,
		SuccessCounter: s.GetCounterListed(),
		FailureCounter: s.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err == nil {
				params.Search = "*"
			}

			return params, err
		},
	}, func(runCtx context.Context, _ *list.List) (envelope.Result[envelope.Void], error) {
		input := &awss3.ListObjectsV2Input{
			Bucket: aws.String(s.Bucket),
		}

		keys := ResponseListKeys{[]string{}}

		paginator := awss3.NewListObjectsV2Paginator(s.Client, input)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(runCtx)
			if err != nil {
				return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
					storage.OperationList.String(),
					customerror.WithError(err),
				)
			}

			for _, object := range page.Contents {
				keys.Keys = append(keys.Keys, *object.Key)
			}
		}

		if err := storage.ParseToStruct(keys, v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// BulkCreate creates data with at most eight concurrent PUTs. Eight provides
// useful I/O parallelism without creating a goroutine for every object.
func (s *S3) BulkCreate(
	ctx context.Context,
	_ string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	const concurrencyLimit = 8

	return s.bulkCreate(ctx, items, prm, concurrencyLimit, options...)
}

func (s *S3) bulkCreate(
	ctx context.Context,
	items []storage.BulkItem,
	prm *create.Create,
	concurrencyLimit int,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	if len(items) == 0 {
		return storage.BulkResult{}, nil
	}

	chunks, err := storage.ChunkIndexed(items, concurrencyLimit)
	if err != nil {
		return storage.BulkResult{}, err
	}

	outcomes := make([]error, len(items))
	stopped := false

	for chunkIndex, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			stopped = true
			markS3BulkFailures(chunks[chunkIndex:], err, outcomes)

			break
		}

		s.runBulkCreateChunk(ctx, chunk, prm, outcomes, options...)

		if err := ctx.Err(); err != nil {
			stopped = stopped || s3BulkChunkStopped(chunk, outcomes, err)

			if chunkIndex+1 >= len(chunks) {
				continue
			}

			stopped = true
			markS3BulkFailures(chunks[chunkIndex+1:], err, outcomes)

			break
		}
	}

	return assembleS3BulkResult(items, outcomes, stopped)
}

func (s *S3) runBulkCreateChunk(
	ctx context.Context,
	chunk []storage.Indexed[storage.BulkItem],
	prm *create.Create,
	outcomes []error,
	options ...storage.Func[*create.Create],
) {
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(chunk))

	for _, indexed := range chunk {
		go func() {
			defer waitGroup.Done()

			if err := ctx.Err(); err != nil {
				outcomes[indexed.Index] = err

				return
			}

			_, outcomes[indexed.Index] = s.Create(
				ctx,
				indexed.Item.ID,
				indexed.Item.ID,
				indexed.Item.Value,
				prm,
				options...,
			)
		}()
	}

	waitGroup.Wait()
}

func markS3BulkFailures(chunks [][]storage.Indexed[storage.BulkItem], err error, outcomes []error) {
	for _, chunk := range chunks {
		for _, indexed := range chunk {
			outcomes[indexed.Index] = err
		}
	}
}

func s3BulkChunkStopped(chunk []storage.Indexed[storage.BulkItem], outcomes []error, stopErr error) bool {
	for _, indexed := range chunk {
		if errors.Is(outcomes[indexed.Index], stopErr) {
			return true
		}
	}

	return false
}

func assembleS3BulkResult(
	items []storage.BulkItem,
	outcomes []error,
	stopped bool,
) (storage.BulkResult, error) {
	result := storage.BulkResult{
		Succeeded: make([]int, 0, len(items)),
		Stopped:   stopped,
	}

	for index, err := range outcomes {
		if err != nil {
			result.Failures = append(result.Failures, storage.BulkItemError{Index: index, Err: err})

			continue
		}

		result.Succeeded = append(result.Succeeded, index)
	}

	return result, result.Err()
}

// Create data.
//
// NOTE: `v` can be a file, a string, or an struct.
//
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (s *S3) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	var (
		trgt        string
		contentFile *os.File
	)

	defer func() {
		if contentFile != nil {
			_ = contentFile.Close()
		}
	}()

	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        options,
		SuccessCounter: s.GetCounterCreated(),
		FailureCounter: s.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err != nil {
				return nil, err
			}

			params.TTL = 0
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(runCtx context.Context, _ *create.Create) (envelope.Result[string], error) {
		// Prepare the input for the S3 upload request.
		uploadInput := &transfermanager.UploadObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(trgt),
		}

		// Use a type switch to handle different types of content.
		switch content := v.(type) {
		case *os.File:
			contentFile = content

			// Read the entire content of the file into a byte slice.
			b, err := shared.ReadAll(content)
			if err != nil {
				return envelope.Result[string]{}, err
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
				return envelope.Result[string]{}, err
			}

			// Set the body and the content type of the S3 upload request.
			uploadInput.Body = bytes.NewReader(jsonData)
			uploadInput.ContentType = aws.String("application/json")
		}

		// Perform the S3 upload request.
		uO, err := s.uploader.UploadObject(runCtx, uploadInput)
		if err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[string]{Value: aws.ToString(uO.Location), PostHookData: v}, nil
	})
}

// Update data.
//
// NOTE: Not truly an update, it's an insert.
func (s *S3) Update(ctx context.Context, id, target string, v any, prm *update.Update, options ...storage.Func[*update.Update]) error {
	var trgt string

	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         target,
		HookData:       v,
		Options:        options,
		RequiresID:     true,
		SuccessCounter: s.GetCounterUpdated(),
		FailureCounter: s.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err != nil {
				return nil, err
			}

			params.TTL = 0
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(runCtx context.Context, _ *update.Update) (envelope.Result[envelope.Void], error) {
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
				return envelope.Result[envelope.Void]{}, err
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
				return envelope.Result[envelope.Void]{}, err
			}

			// Set the body and the content type of the S3 upload request.
			uploadInput.Body = bytes.NewReader(jsonData)
			uploadInput.ContentType = aws.String("application/json")
		}

		// Perform the S3 upload request.
		if _, err := s.uploader.UploadObject(runCtx, uploadInput); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
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
