# Suggestions

Findings from a full read of the codebase that are **out of scope for a
test-and-fix pass** — each one is either a breaking change, a new dependency,
or a design decision that belongs to the maintainer. Ordered by impact.

## 1. `IStorage` has no lifecycle method

Every networked adapter (`redis`, `mongodb`, `elasticsearch`, `mysql`,
`postgres`, `sqlite`, `sftp`, `s3`, `dynamodb`) opens a long-lived client in
`New`, and none of them can be closed through the interface. A caller holding
an `IStorage` has no way to release a connection pool, so building a storage
per tenant/request leaks sockets and goroutines until the process exits.

`sftp` now has a concrete `Close` (it needed one — closing the SFTP channel
left the SSH transport open), but that only helps callers holding the
concrete type.

**Suggested change:** add `Close() error` to `IStorage` and implement it per
adapter (no-op for `memory`/`file`). This is a breaking interface change, so
it belongs in a v3 — or add it as an optional interface
(`interface{ Close() error }`) that callers can type-assert for, which is
backwards compatible and can land now.

## 2. User-supplied params bypass their constructors

Every operation in every adapter does this:

```go
finalParam, err := count.New()   // validated defaults
finalParam.Search = "*"          // application default
if prm != nil { finalParam = prm }  // ...then thrown away wholesale
```

When the caller passes params, the validated defaults are discarded rather
than merged. Two consequences: (a) an application default like
`Search = "*"` silently becomes `""` — adapters paper over this with ad-hoc
`if pattern == "" { pattern = "*" }` checks in some operations but not
others; (b) caller-supplied params never go through the `params/v2`
constructor's validation. It also allocates a params struct per call that is
immediately garbage in the common path.

**Suggested change:** merge instead of replace (copy non-zero fields from
`prm` onto `finalParam`), or validate `prm` explicitly when it is non-nil.
Either way, do it once in a shared helper rather than 60+ times.

## 3. Deprecated upstream SDKs

- `github.com/aws/aws-sdk-go` v1 is in maintenance mode; v2
  (`github.com/aws/aws-sdk-go-v2`) is the supported line. Affects `s3` and
  `dynamodb`. The `.golangci.yml` currently carries an `SA1019` exclusion to
  silence the deprecation warnings.
- `go.mongodb.org/mongo-driver` v1 is superseded by v2.

Both are breaking migrations (public `Config`/`Option` types are aliases of
SDK types), so they want their own PR — likely a v3.

## 4. `List` ignores most of its params

`list.List` carries `Search`, `Limit`, `Offset`, `Sort` and `Fields`, but
adapter support is uneven:

| Adapter | Search | Limit/Offset | Sort | Fields |
|---|---|---|---|---|
| memory, file, sftp | yes | no | no | no |
| redis | via SCAN MATCH | no | no | no |
| sql (mysql/postgres/sqlite) | yes | yes | yes | yes |
| elasticsearch, mongodb, dynamodb | yes | partial | partial | partial |

The doc comments do not distinguish the supported from the ignored, so a
caller setting `Limit` on `memory` gets everything back with no error.

**Suggested change:** either honor them uniformly (they are all cheap to
implement for the in-process adapters) or return an explicit
"unsupported param" error rather than ignoring them silently. At minimum,
document per-adapter support.

## 5. Empty-value ambiguity on `Retrieve`

`redis.Retrieve` of a key holding an empty string returns `nil` and leaves
the destination untouched, so the caller cannot distinguish "the record is
empty" from "nothing was decoded". Consider returning 404 for an empty
payload, consistent with a missing key.

## 6. Bulk operations

Already on the roadmap in `CHANGELOG.md`: `Create` and `Update` accept a
single object. `storage.CreateMany`/`UpdateMany` fan out one call per item
concurrently, which is correct but leaves per-backend batching on the table
(`mongodb.InsertMany`, ES `_bulk`, DynamoDB `BatchWriteItem`, SQL multi-row
`INSERT`, Redis pipelining). For large writes this is the difference between
N round-trips and one.

## 7. Candidate new adapters

Ranked by how much they'd add relative to what already exists:

**High value — new capability:**
- **BadgerDB or bbolt** — an embedded, *persistent* key-value store. Today
  the only local options are `memory` (volatile) and `file` (one document per
  file). Zero external infrastructure, so it can be fully tested in CI.
- **OpenSearch** — the ES fork, API-compatible enough to be a thin variant of
  the existing `elasticsearch` adapter, and a common requirement for AWS
  users (`opensearchproject/opensearch-go`).
- **Google Cloud Storage** and **Azure Blob Storage** — the object-store
  surface is already defined by `s3`; these complete the cloud triad.
  (MinIO and Cloudflare R2 already work through the existing `s3` adapter by
  pointing `Endpoint` at them — worth documenting rather than implementing.)

**Moderate value:**
- **Cassandra/ScyllaDB** (`gocql`) — the wide-column shape maps cleanly onto
  the `target`/`id` model.
- **ClickHouse** — analytics workloads; `database/sql` driver means it could
  reuse most of the SQL adapter scaffolding.
- **etcd** or **Consul KV** — config/coordination stores with the same
  key-value shape as `redis`.
- **Firestore** — document store, maps to the `mongodb` shape.

**Notes on things that look like adapters but aren't:**
- **CockroachDB** and **Amazon Aurora/RDS Postgres** already work through the
  `postgres` adapter; **Valkey** works through `redis`. Document, don't
  implement.
- **Kafka/NATS** are message transports, not CRUD-by-id stores; forcing them
  into `IStorage` would misrepresent their semantics. NATS **JetStream KV**
  is the exception and would fit.

## 8. `dynamodb`'s functional options are not wired to its constructors

`WithPrimaryKey`, `WithTarget` and `WithDynamicTarget` are exported and each
returns a `func(*DynamoDB)`, but none of `New`, `NewWithTable` or
`NewWithDynamicTable` takes a variadic options parameter. They can only be
used by constructing the storage first and then applying the option to the
returned instance by hand, which is not how a `With…` API reads.

`PrimaryKey` in particular is hardcoded to `"id"` by the constructors, so a
table keyed on anything else needs the post-hoc call to be usable at all.

**Suggested change:** accept `...func(*DynamoDB)` on the three constructors
and apply the options before `validation.Validate`, so an alternate primary
key is set before the struct is checked. That is backwards compatible —
existing calls keep working — and makes the exported helpers do what their
names promise.

## 9. Reduce the per-adapter boilerplate

Each of the six operations in each of the eleven adapters repeats the same
~40-line preamble: start an APM span, build options, apply option funcs,
build params, resolve the target, run the pre-hook — then ~10 lines of actual
backend work, then the post-hook, log line and counter. That is roughly 3,000
lines of duplicated control flow, and it is where the bugs found in this and
the previous pass lived (a counter attributed to the wrong operation, an
early `return` that skipped the post-hook, an operation label copy-pasted from
its neighbor).

**Suggested change:** extract the preamble/postamble into a generic helper,
e.g.

```go
func run[P any, R any](
    ctx context.Context, s storage.IStorage, op storage.Operation,
    id, target string, prm P, opts []storage.Func[P],
    fn func(ctx context.Context, trgt string, prm P) (R, error),
) (R, error)
```

so each adapter method shrinks to the backend call. It would have made all
four of those bugs impossible to write, and it makes adding an adapter a
matter of six small functions.
