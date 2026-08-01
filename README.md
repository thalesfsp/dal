# dal

`dal` defines a common Go storage interface for CRUD, list, count, and bulk-create operations. Twelve adapters implement that interface for embedded, filesystem, database, cache, search, and cloud backends; callers can use a concrete adapter or program against `storage.IStorage`. The package also provides concurrent single- and multi-storage helpers, operation hooks, counters, logging, and Elastic APM instrumentation.

## Install

```sh
go get github.com/thalesfsp/dal/v3/storage
```

The module path in `go.mod` is `github.com/thalesfsp/dal/v3`.

## Adapters

| Package | Backend and use |
| --- | --- |
| `bolt` | bbolt: an embedded, persistent key/value database in one file. |
| `dynamodb` | AWS DynamoDB tables. |
| `elasticsearch` | Elasticsearch indices. |
| `file` | The local filesystem, with one file per document. |
| `memory` | A volatile, in-process `sync.Map`. |
| `mongodb` | MongoDB databases and collections. |
| `mysql` | MySQL tables through `database/sql` and `sqlx`. |
| `postgres` | PostgreSQL tables through `database/sql` and `sqlx`. |
| `redis` | Redis keys and values. |
| `s3` | Objects in an AWS S3 bucket. |
| `sftp` | Files on a remote SFTP server. |
| `sqlite` | SQLite tables through `database/sql` and `sqlx`. |

## Example

This example uses the in-process `memory` adapter and performs Create → Retrieve → List → Count → Update → Delete. It follows the runnable examples in `memory/example_test.go` and needs no external service.

```go
package main

import (
	"context"
	"fmt"

	"github.com/thalesfsp/dal/v3/memory"
	"github.com/thalesfsp/params/v2/count"
	"github.com/thalesfsp/params/v2/create"
	"github.com/thalesfsp/params/v2/customsort"
	"github.com/thalesfsp/params/v2/delete"
	"github.com/thalesfsp/params/v2/list"
	"github.com/thalesfsp/params/v2/retrieve"
	"github.com/thalesfsp/params/v2/update"
)

type book struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ctx := context.Background()

	strg, err := memory.New(ctx)
	must(err)
	defer func() { must(strg.Close()) }()

	id, err := strg.Create(ctx, "book-dune", "", &book{
		ID: "book-dune", Title: "Dune",
	}, &create.Create{})
	must(err)
	fmt.Println("created", id)

	var got book
	must(strg.Retrieve(ctx, id, "", &got, &retrieve.Retrieve{}))
	fmt.Println("retrieved", got.Title)

	var books memory.ResponseList[book]
	must(strg.List(ctx, "", &books, &list.List{
		Search: "book-*",
		Sort:   customsort.SortSlice{{"id", customsort.Asc}},
	}))
	fmt.Println("listed", len(books.Items))

	n, err := strg.Count(ctx, "", &count.Count{Search: "book-*"})
	must(err)
	fmt.Println("counted", n)

	must(strg.Update(ctx, id, "", &book{
		ID: "book-dune", Title: "Dune Messiah",
	}, &update.Update{}))
	fmt.Println("updated", id)

	must(strg.Delete(ctx, id, "", &delete.Delete{}))
	fmt.Println("deleted", id)
}
```

## Lifecycle

`Close() error` is part of `storage.IStorage`, and every shipped adapter implements it. `Close` is idempotent: the first call attempts to release the resources the adapter owns, and later calls return `nil`.

Closing `bolt` is a correctness requirement. bbolt holds an exclusive lock on its database file while it is open, so an unclosed storage blocks every later `Open` of that path for the process lifetime.

## List parameters

`list.List` exposes `Sort`, `Limit`, `Offset`, and `Fields`. `list.New()` sets **`Limit` to 10 by default**. `bolt`, `file`, `memory`, `redis`, and `sftp` merge caller params over those defaults, then apply the merged values in the order sort, paginate, then project. A literal such as `&list.List{Sort: ...}` therefore inherits the default limit; a nil params pointer applies none of these four fields. `memory.List` has no defined order unless `Sort` is set because it ranges a `sync.Map`.

`elasticsearch` and `mongodb` translate all four fields. `dynamodb` sends `Limit` and a `Fields` projection to `Scan`, skips `Offset` items client-side while consuming paginated results, and does not use `Sort`. The SQL adapters execute `Search` as the query and do not translate the four structured fields. `s3.List` does not apply them.

## Bulk create

`BulkCreate` is part of `storage.IStorage`, and all twelve adapters implement it. The implementations use multi-row `INSERT` for the SQL adapters, bounded bbolt write transactions for `bolt`, unordered `InsertMany` for `mongodb`, `_bulk` for `elasticsearch`, `BatchWriteItem` for `dynamodb`, and pipelining for `redis`. `file` and `memory` loop over single writes; `s3` and `sftp` do the same with bounded concurrency. All use the same result contract.

Its contract is explicit:

- It is **not atomic**. Successful writes remain committed when other items fail, and a post-hook failure does not roll back the write.
- Processing order and result-slice order are **not guaranteed**. Backends may chunk, parallelize, or reorder work.
- Every input is reported exactly once by its original zero-based input index in either `BulkResult.Succeeded` or `BulkResult.Failures`; failures retain their underlying errors.
- Any unsuccessful item produces an inspectable `*storage.BulkError`. If processing stops, unattempted items are failures carrying the stopping error and `BulkResult.Stopped` is true.
- Empty input succeeds with an empty result, even with an already-cancelled context. IDs must be non-empty. For duplicate IDs, the value that remains is unspecified.

Inspect the result before retrying so already-successful writes are not repeated.

## Contributing

1. Fork
2. Clone
3. Create a branch
4. Make changes following the same standards as the project
5. Run `make ci`
6. Create a merge request
