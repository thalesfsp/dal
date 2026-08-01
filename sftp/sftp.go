package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pkg/sftp"
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
	"golang.org/x/crypto/ssh"
)

//////
// Const, vars, and types.
//////

// Name of the storage.
const Name = "sftp"

// Singleton.
var (
	singleton      storage.IStorage
	singletonMutex sync.RWMutex
)

type (
	// Config is the SFTP configuration.
	Config = ssh.ClientConfig

	// Option is for the SFTP configuration.
	Option = sftp.ClientOption
)

func readFile(client *sftp.Client, name string) ([]byte, error) {
	file, err := client.Open(name)
	if err != nil {
		return nil, err
	}

	doc, readErr := io.ReadAll(file)
	closeErr := file.Close()

	if readErr != nil {
		return nil, readErr
	}

	if closeErr != nil {
		return nil, closeErr
	}

	return doc, nil
}

// SFTP storage definition.
type SFTP struct {
	*storage.Storage

	closeOnce sync.Once

	Client *sftp.Client `json:"-" validate:"required,structonly"`

	// Conn is the underlying SSH transport carrying the SFTP subsystem.
	// Client.Close only tears down the SFTP channel, so callers need this to
	// release the TCP+SSH session — see Close.
	Conn *ssh.Client `json:"-" validate:"required,structonly"`

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
func (s *SFTP) Count(ctx context.Context, target string, prm *count.Count, options ...storage.Func[*count.Count]) (int64, error) {
	hookTarget := target
	if hookTarget == "" {
		hookTarget = s.Target
	}

	var trgt string

	return envelope.Run[count.Count, int64](ctx, envelope.Config[count.Count]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Counted.String(),
		Target:         hookTarget,
		Options:        options,
		SuccessCounter: s.GetCounterCounted(),
		FailureCounter: s.GetCounterCountedFailed(),
		NewParams: func() (*count.Count, error) {
			params, err := count.New()
			if err != nil {
				return nil, err
			}

			params.Search = "*"
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, params *count.Count) (envelope.Result[int64], error) {
		// Don't write into the caller's prm — copy the pattern, defaulting an
		// empty one to match-all.
		pattern := params.Search
		if pattern == "" {
			pattern = "*"
		}

		files, err := s.Client.ReadDir(trgt)
		if err != nil {
			return envelope.Result[int64]{}, customerror.NewFailedToError(
				storage.OperationCount.String(),
				customerror.WithError(err),
			)
		}

		fileCount := 0

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			matched, err := path.Match(pattern, file.Name())
			if err != nil {
				return envelope.Result[int64]{}, customerror.NewInvalidError(
					"search pattern",
					customerror.WithError(err),
				)
			}

			if matched {
				fileCount++
			}
		}

		result := int64(fileCount)

		return envelope.Result[int64]{Value: result, PostHookData: fileCount}, nil
	})
}

// Delete removes data.
func (s *SFTP) Delete(ctx context.Context, id, target string, prm *delete.Delete, options ...storage.Func[*delete.Delete]) error {
	hookTarget := target
	if hookTarget == "" {
		hookTarget = s.Target
	}

	var trgt string

	_, err := envelope.Run[delete.Delete, envelope.Void](ctx, envelope.Config[delete.Delete]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Deleted.String(),
		ID:             id,
		Target:         hookTarget,
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
	}, func(_ context.Context, _ *delete.Delete) (envelope.Result[envelope.Void], error) {
		if err := s.Client.Remove(trgt); err != nil && !errors.Is(err, os.ErrNotExist) {
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
func (s *SFTP) Retrieve(ctx context.Context, id, target string, v any, prm *retrieve.Retrieve, options ...storage.Func[*retrieve.Retrieve]) error {
	hookTarget := target
	if hookTarget == "" {
		hookTarget = s.Target
	}

	var trgt string

	_, err := envelope.Run[retrieve.Retrieve, envelope.Void](ctx, envelope.Config[retrieve.Retrieve]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Retrieved.String(),
		ID:             id,
		Target:         hookTarget,
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
	}, func(_ context.Context, _ *retrieve.Retrieve) (envelope.Result[envelope.Void], error) {
		srcFile, err := s.Client.Open(trgt)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationRetrieve.String(),
				customerror.WithError(err),
			)
		}

		defer srcFile.Close()

		content, err := shared.ReadAll(srcFile)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		// Convert content to `v`.
		if err := shared.Unmarshal(content, v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// List data.
//
// Search is a glob matched against filenames. Sort, Limit, Offset, and Fields
// are applied to the decoded documents after matching.
func (s *SFTP) List(ctx context.Context, target string, v any, prm *list.List, opts ...storage.Func[*list.List]) error {
	hookTarget := target
	if hookTarget == "" {
		hookTarget = s.Target
	}

	var trgt string

	_, err := envelope.Run[list.List, envelope.Void](ctx, envelope.Config[list.List]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Listed.String(),
		Target:         hookTarget,
		HookData:       v,
		Options:        opts,
		SuccessCounter: s.GetCounterListed(),
		FailureCounter: s.GetCounterListedFailed(),
		NewParams: func() (*list.List, error) {
			params, err := list.New()
			if err != nil {
				return nil, err
			}

			params.Search = "*"
			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, params *list.List) (envelope.Result[envelope.Void], error) {
		files, err := s.Client.ReadDir(trgt)
		if err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationList.String(),
				customerror.WithError(err),
			)
		}

		docs := make([][]byte, 0, len(files))

		// The documented contract is that Search filters the listing (Count
		// already honors it). An empty search means match-all.
		pattern := params.Search
		if pattern == "" {
			pattern = "*"
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			matched, err := path.Match(pattern, file.Name())
			if err != nil {
				return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
					storage.OperationList.String(),
					customerror.WithError(err),
				)
			}

			if matched {
				doc, err := readFile(s.Client, path.Join(trgt, file.Name()))
				if err != nil {
					return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
						storage.OperationList.String(),
						customerror.WithError(err),
					)
				}

				docs = append(docs, doc)
			}
		}

		if prm == nil {
			params = nil
		}

		docs, err = shared.ApplyListParams(docs, params)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		if err := shared.Unmarshal(shared.AssembleItems(docs), v); err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// BulkCreate creates data with at most eight concurrent SFTP writes. Eight
// keeps network round trips moving without flooding one SSH connection.
func (s *SFTP) BulkCreate(
	ctx context.Context,
	_ string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	const concurrencyLimit = 8

	return s.bulkCreate(ctx, items, prm, concurrencyLimit, options...)
}

func (s *SFTP) bulkCreate(
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
			markSFTPBulkFailures(chunks[chunkIndex:], err, outcomes)

			break
		}

		s.runBulkCreateChunk(ctx, chunk, prm, outcomes, options...)

		if err := ctx.Err(); err != nil {
			stopped = stopped || sftpBulkChunkStopped(chunk, outcomes, err)

			if chunkIndex+1 >= len(chunks) {
				continue
			}

			stopped = true
			markSFTPBulkFailures(chunks[chunkIndex+1:], err, outcomes)

			break
		}
	}

	return assembleSFTPBulkResult(items, outcomes, stopped)
}

func (s *SFTP) runBulkCreateChunk(
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

func markSFTPBulkFailures(chunks [][]storage.Indexed[storage.BulkItem], err error, outcomes []error) {
	for _, chunk := range chunks {
		for _, indexed := range chunk {
			outcomes[indexed.Index] = err
		}
	}
}

func sftpBulkChunkStopped(chunk []storage.Indexed[storage.BulkItem], outcomes []error, stopErr error) bool {
	for _, indexed := range chunk {
		if errors.Is(outcomes[indexed.Index], stopErr) {
			return true
		}
	}

	return false
}

func assembleSFTPBulkResult(
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
// NOTE: Not all storages returns the ID, neither all storages requires `id` to
// be set. You are better off setting the ID yourself.
func (s *SFTP) Create(ctx context.Context, id, target string, v any, prm *create.Create, options ...storage.Func[*create.Create]) (string, error) {
	hookTarget := target
	if hookTarget == "" {
		hookTarget = s.Target
	}

	var trgt string

	return envelope.Run[create.Create, string](ctx, envelope.Config[create.Create]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Created.String(),
		ID:             id,
		Target:         hookTarget,
		HookData:       v,
		Options:        options,
		SuccessCounter: s.GetCounterCreated(),
		FailureCounter: s.GetCounterCreatedFailed(),
		NewParams: func() (*create.Create, error) {
			params, err := create.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, _ *create.Create) (envelope.Result[string], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[string]{}, err
		}

		dstFile, err := s.Client.Create(trgt)
		if err != nil && !os.IsNotExist(err) {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		if dstFile == nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String() + " file, it's nil",
			)
		}

		defer dstFile.Close()

		if _, err := dstFile.Write(b); err != nil {
			return envelope.Result[string]{}, customerror.NewFailedToError(
				storage.OperationCreate.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[string]{Value: id, PostHookData: v}, nil
	})
}

// Update data.
func (s *SFTP) Update(ctx context.Context, id, target string, v any, prm *update.Update, opts ...storage.Func[*update.Update]) error {
	hookTarget := target
	if hookTarget == "" {
		hookTarget = s.Target
	}

	var trgt string

	_, err := envelope.Run[update.Update, envelope.Void](ctx, envelope.Config[update.Update]{
		Storage:        s,
		Adapter:        Name,
		Status:         status.Updated.String(),
		ID:             id,
		Target:         hookTarget,
		HookData:       v,
		Options:        opts,
		RequiresID:     true,
		SuccessCounter: s.GetCounterUpdated(),
		FailureCounter: s.GetCounterUpdatedFailed(),
		NewParams: func() (*update.Update, error) {
			params, err := update.New()
			if err != nil {
				return nil, err
			}

			params = shared.MergeParams(params, prm)
			trgt, err = shared.TargetName(target, s.Target)

			return params, err
		},
	}, func(_ context.Context, _ *update.Update) (envelope.Result[envelope.Void], error) {
		b, err := shared.Marshal(v)
		if err != nil {
			return envelope.Result[envelope.Void]{}, err
		}

		dstFile, err := s.Client.Create(trgt)
		if err != nil && !os.IsNotExist(err) {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}

		if dstFile == nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String() + " file, it's nil",
			)
		}

		defer dstFile.Close()

		if _, err := dstFile.Write(b); err != nil {
			return envelope.Result[envelope.Void]{}, customerror.NewFailedToError(
				storage.OperationUpdate.String(),
				customerror.WithError(err),
			)
		}

		return envelope.Result[envelope.Void]{PostHookData: v}, nil
	})

	return err
}

// GetClient returns the client.
func (s *SFTP) GetClient() any {
	return s.Client
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

	// Accept both the documented "host:port" form and a full
	// "sftp://host:port" URL.
	host := addr

	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, customapm.TraceError(
				ctx,
				customerror.NewFailedToError("convert addr to URL", customerror.WithError(err)),
				s.GetLogger(),
				s.GetCounterPingFailed(),
			)
		}

		host = u.Host
	}

	conn, err := ssh.Dial("tcp", host, cfg)
	if err != nil {
		return nil, customapm.TraceError(
			ctx,
			customerror.NewFailedToError("dial", customerror.WithError(err)),
			s.GetLogger(),
			s.GetCounterPingFailed(),
		)
	}

	// Close the SSH connection if New fails past this point, otherwise the
	// TCP+SSH session leaks.
	success := false

	defer func() {
		if !success {
			conn.Close()
		}
	}()

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
		Conn:   conn,
	}

	if err := validation.Validate(storage); err != nil {
		return nil, customapm.TraceError(ctx, err, s.GetLogger(), nil)
	}

	success = true

	singletonMutex.Lock()
	singleton = storage
	singletonMutex.Unlock()

	return storage, nil
}

// Close releases the SFTP client and the SSH transport underneath it.
// Closing only the client would leave the TCP+SSH session open for the
// lifetime of the process.
func (s *SFTP) Close() error {
	var closeErr error

	s.closeOnce.Do(func() {
		var errs []error

		if s.Client != nil {
			if err := s.Client.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		if s.Conn != nil {
			if err := s.Conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		if len(errs) > 0 {
			closeErr = customerror.NewFailedToError("close", customerror.WithError(errors.Join(errs...)))
		}
	})

	return closeErr
}

//////
// Exported functionalities.
//////

// Get returns a setup SFTP, or set it up.
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
