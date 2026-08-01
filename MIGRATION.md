# Migrating from v2 to v3

This guide covers the caller-visible changes between v2.2.3 and v3.0.0.

## 1. `List` applies pagination, sorting, and projection

Existing calls can keep compiling while returning fewer records. `bolt`, `file`, `memory`, `redis`, and `sftp` now apply `Sort`, `Limit`, `Offset`, and `Fields`; `list.New()` supplies a default `Limit` of 10. If code used `list.New()` and relied on one of these adapters returning every match, it now receives at most ten.

Before, these adapters ignored most or all of the structured fields:

```go
prm, err := list.New()
if err != nil {
	return err
}

// This could return every matching record even though prm.Limit == 10.
err = strg.List(ctx, "books", &out, prm)
```

After, choose a deliberate page size and paginate until done:

```go
prm, err := list.New()
if err != nil {
	return err
}
prm.Limit = 100
prm.Offset = 0
prm.Sort = customsort.SortSlice{{"id", customsort.Asc}}

err = strg.List(ctx, "books", &out, prm)
```

There is no portable “unlimited” value: zero is the unset marker and merges back to the default. Set a positive `Limit`. Also set `Sort` whenever result order matters; `memory.List` ranges a `sync.Map` and has no defined order without it.

The result shape also changed for `file`, `redis`, and `sftp`: `List` now returns decoded documents rather than key names.

Before:

```go
var out file.ResponseListKeys
err := strg.List(ctx, target, &out, prm)
for _, key := range out.Keys {
	_ = key
}
```

After:

```go
var out file.ResponseList[Document]
err := strg.List(ctx, target, &out, prm)
for _, document := range out.Items {
	_ = document
}
```

Make the equivalent `ResponseList[T]` change for `redis` and `sftp`.

## 2. Caller params merge over defaults

Operations now construct a merged params value by copying caller-supplied fields over validated defaults. Previously, passing a params struct replaced the defaults wholesale, so setting one field silently erased all other defaults. The merged value is passed to hooks and operation bodies, and the inputs are not mutated.

Before, a zero field could erase a default as a side effect of setting another field:

```go
prm := &list.List{
	Sort: customsort.SortSlice{{"title", customsort.Asc}},
	// Limit was zero because this struct replaced the defaults.
}
```

After, set only the fields you intend to override; the five in-process list implementations use the merged defaults:

```go
prm := &list.List{
	Sort: customsort.SortSlice{{"title", customsort.Asc}},
}
```

Audit code that depended on zero values clearing defaults. Most zero values mean “unset.” The intentional exceptions are `delete.Soft == false`, `list.Count == 0`, and `list.Offset == 0`, which remain meaningful values.

`bolt`, `file`, `memory`, `redis`, and `sftp` pass the same merged params to hooks and structured `List` post-processing. A literal such as `&list.List{Sort: ...}` therefore acquires the default `Limit` of 10 for sorting, pagination, and projection. A nil params pointer still applies none of those fields.

## 3. The module uses the v3 import path

Every package import must change its major-version suffix from `/v2` to `/v3`, and `go.mod` must require v3. The unchanged prefix is abbreviated below so this guide does not leave a copyable stale import.

Before:

```go
import "…/v2/storage"
```

After:

```go
import "github.com/thalesfsp/dal/v3/storage"
```

Run:

```sh
go get github.com/thalesfsp/dal/v3/storage
go mod tidy
```

## 4. `IStorage` gained lifecycle and bulk methods

`Close() error` is now required by `storage.IStorage`. The optional `storage.ICloser` interface and `storage.Close(IStorage)` helper were removed because every storage is now closable directly. All shipped adapters implement an idempotent `Close`; call it for every constructed storage.

Before:

```go
strg, err := memory.New(ctx)
if err != nil {
	return err
}
defer storage.Close(strg)
```

After:

```go
strg, err := memory.New(ctx)
if err != nil {
	return err
}
defer strg.Close()
```

Third-party implementations must add `Close() error`. A no-op implementation is appropriate only when the implementation owns no releasable resource:

```go
func (s *MyStorage) Close() error {
	return nil
}
```

`BulkCreate` was also added to `IStorage`, so third-party implementations must add that method too. This interface break was introduced by the bulk contract even though its commit did not carry a `BREAKING CHANGE` footer.

Before:

```go
var _ storage.IStorage = (*MyStorage)(nil) // no bulk method required
```

After:

```go
func (s *MyStorage) BulkCreate(
	ctx context.Context,
	target string,
	items []storage.BulkItem,
	prm *create.Create,
	options ...storage.Func[*create.Create],
) (storage.BulkResult, error) {
	return storage.BulkResult{}, storage.ErrBulkCreateNotImplemented
}
```

The sentinel above is for *your* implementation if you do not need bulk writes. All twelve shipped adapters implement the operation for real — natively where the backend has a batch primitive, and by looping internally where it does not.

## 5. AWS SDK v1 types became AWS SDK v2 types

The `s3.Config` and `dynamodb.Config` types still alias `aws.Config`, but now that alias comes from `github.com/aws/aws-sdk-go-v2/aws`. Pointer-valued v1 fields and session construction do not carry over: load or construct an SDK v2 config and pass its address.

Before:

```go
cfg := &s3.Config{
	Region:      aws.String("us-east-1"),
	Endpoint:    aws.String("http://localhost:9000"),
	Credentials: credentials.NewStaticCredentials("key", "secret", ""),
}
strg, err := s3.New(ctx, "books", cfg)
```

After:

```go
cfg, err := config.LoadDefaultConfig(ctx,
	config.WithRegion("us-east-1"),
	config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider("key", "secret", ""),
	),
)
if err != nil {
	return err
}
cfg.BaseEndpoint = aws.String("http://localhost:9000")

strg, err := s3.New(ctx, "books", &cfg)
```

Use the corresponding v2 packages for DynamoDB. Public DynamoDB helper types changed with the SDK as well: `Item` and the marshal/unmarshal helpers now use `map[string]types.AttributeValue`, `TableDescription` aliases `service/dynamodb/types.TableDescription`, and `BuildProjectionExpression` returns `map[string]string` rather than `map[string]*string`. Update imports and remove the old pointer-to-attribute-value and pointer-to-string assumptions.

The migration removes deprecated `aws-sdk-go` v1 and its S3 Crypto advisories `GO-2022-0646` and `GO-2022-0635` from the dependency graph.

## 6. MongoDB uses mongo-driver v2

`mongodb.Config` remains an alias of `options.ClientOptions`, but it now aliases `go.mongodb.org/mongo-driver/v2/mongo/options.ClientOptions`. MongoDB driver values are not interchangeable across major versions; update every driver import, including `bson`, `mongo`, and `mongo/options`.

Before:

```go
import "go.mongodb.org/mongo-driver/mongo/options"

cfg := options.Client().ApplyURI(uri)
strg, err := mongodb.New(ctx, "app", cfg)
```

After:

```go
import "go.mongodb.org/mongo-driver/v2/mongo/options"

cfg := options.Client().ApplyURI(uri)
strg, err := mongodb.New(ctx, "app", cfg)
```

Rebuild any code that directly uses the aliased config or the concrete client returned by `GetClient`; those values now have v2 SDK shapes and method signatures.

## 7. `dynamodb.New` accepts functional options

`dynamodb.New`, `NewWithTable`, and `NewWithDynamicTable` now take variadic `func(*DynamoDB)` options. Options run before validation, in argument order, so an option can satisfy a required field and the last option wins.

Before, exported options had to be applied after construction:

```go
strg, err := dynamodb.New(ctx, region, cfg)
if err != nil {
	return err
}
dynamodb.WithPrimaryKey("pk")(strg)
```

After, pass them to the constructor:

```go
strg, err := dynamodb.New(
	ctx,
	region,
	cfg,
	dynamodb.WithPrimaryKey("pk"),
)
```

Calls with no options still compile because the new parameter is variadic.

## 8. Redis empty payloads return Not Found

`redis.Retrieve` used to return `nil` for a stored zero-byte value and leave the destination untouched. It now returns a 404-style not-found error. Non-empty JSON such as `{}`, `[]`, `""`, and `null` still follows normal decoding.

Before:

```go
var doc Document
if err := strg.Retrieve(ctx, id, "", &doc, nil); err != nil {
	return err
}
// A zero-byte value silently left doc unchanged.
```

After, handle the empty payload like a missing key:

```go
var doc Document
err := strg.Retrieve(ctx, id, "", &doc, nil)
if customerror.IsHTTPStatus(err, http.StatusNotFound) {
	// The key is missing or its stored payload has zero bytes.
} else if err != nil {
	return err
}
```
