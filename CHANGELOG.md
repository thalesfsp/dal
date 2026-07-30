# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Project Roadmap

- [ ] Create and Update should allow to pass more than one object per time (Bulk Create and Update)

## [Unreleased]
### Fixed
- Every adapter's `New` handed its whole storage struct to the validator,
  which descends into nested structs by default — so construction walked the
  live driver client (a `mongo.Client`, a `redis.Client`, a `sqlx.DB` pool,
  an `ssh.Client`, an `aws.Config`) reading fields the driver's own
  background goroutines write. The race detector caught it on `s3`, where the
  SDK's credential `singleflight` goroutine mutates state validation was
  reading. Client fields are now `required,structonly` and the config fields
  holding live state are skipped, so the tag means what it always intended:
  must not be nil.
- `dynamodb.List` set `Select: COUNT` on its Scan input, which makes DynamoDB
  omit `Items` entirely — the operation could never return data, and the
  setting is invalid alongside a projection. Its pagination loop compared
  `LastEvaluatedKey` against nil rather than checking length.
- `elasticsearch.Count` decoded the search response without first checking it
  for an ES-side error, as every other operation does. An error response
  carries an `error` envelope and no `hits`, so a rejected query came back as
  a successful count of zero.
- `elasticsearch.List` asserted `hits.hits` in one chained type assertion, so
  a response missing `hits`, or carrying a non-object there, panicked instead
  of returning the "unexpected type" error already written for that case.
- `mysql.Create` and `postgres.Create` returned a failing post-hook's error
  raw instead of routing it through `customapm.TraceError`, so a failed
  create skipped APM's span-failure marking and left the created-failed
  counter untouched — it looked like a success in metrics. `sqlite.Create`
  had the same bug.
- `mongodb.New` reported ping failures as a bare `context deadline exceeded`:
  `retrier.RunCtx` returns the context's error when the deadline fires during
  a backoff sleep, discarding the attempt's cause. The last one is now joined
  with it.
- `s3.Create` had no `[]byte` case, so raw bytes fell through to the JSON
  branch and were stored base64-encoded. `Update` already handled them.
- `internal/shared.Unmarshal` passed `&v` (the address of its `any`
  parameter) to `json.Unmarshal`, so a non-pointer destination decoded into
  the interface box and returned success while the caller's value stayed
  untouched. It now passes `v` through, and such calls fail loudly.
- `file.Delete` returned early when the file was already gone, skipping the
  post-hook, the debug log and the success counter — an idempotent delete now
  runs the whole path.
- `file.Count` and `file.List` returned `0`/`[]` for a target directory that
  does not exist (`fs.Glob` yields no matches and no error). Both now stat the
  target first and surface the failure.
- `sftp.List` ignored `list.List.Search` despite documenting it — it now
  applies the glob, matching `sftp.Count`.
- `sftp.Create` reported write failures under the *update* operation label
  (copy-paste from `Update`).

### Added
- `bolt` storage, backed by bbolt: an embedded, persistent key-value store
  needing no external service. It fills the gap between `memory` (volatile)
  and `file` (one file per document). Buckets map to targets and are created
  on write; a missing bucket is an error on read rather than an empty result,
  so a typo in a target cannot masquerade as an empty store. `Delete` stays
  idempotent on a missing bucket, matching `file`, `sftp` and `memory`.
  `List` walks bbolt's cursor, which yields keys in byte order, making
  `Limit` meaningful — most non-SQL adapters ignore it.
  `Close` matters more here than elsewhere: bbolt holds an exclusive lock on
  the database file, so a leaked handle blocks every later `Open` of the same
  path.
- Runnable examples for `bolt` and `memory`, checked by `go test` against
  their declared output. These are the only two storages that need no live
  backend, and so the only two where an example can execute.
- `sftp.Close` releases the SSH transport as well as the SFTP channel; the
  adapter retains it as `SFTP.Conn`. Closing only `Client` left the TCP+SSH
  session open for the lifetime of the process.
- Offline test suites for every package — no Docker, no network, no new
  dependencies. Every package is now at or above 90% statement coverage
  (previously as low as 0%), all running under `-race`:

  | Package | Before | After |
  |---|---|---|
  | `storage` | 50.2% | 99.5% |
  | `internal/logging` | 0% | 100% |
  | `internal/shared` | 39.4% | 95.7% |
  | `internal/customapm` | 69.8% | 95.3% |
  | `elasticsearch` | 14.5% | 94.7% |
  | `sqlite` | 64.2% | 93.8% |
  | `redis` | 0% | 93.5% |
  | `postgres` | 6.6% | 93.4% |
  | `mysql` | 6.4% | 93.2% |
  | `s3` | 0% | 93.1% |
  | `dynamodb` | 18.4% | 92.4% |
  | `sftp` | 4.3% | 92.0% |
  | `file` | 63.5% | 91.9% |
  | `memory` | 73.3% | 91.7% |
  | `mongodb` | 0% | 90.7% |

  The backends are faked in-process: an SSH server with the SFTP subsystem
  attached, a RESP2 server implementing exactly the commands the redis
  adapter sends, a mongo mock deployment driving the real driver, an
  Elasticsearch server with an in-memory index emulator, S3 and DynamoDB
  served through a custom transport that never opens a socket, and the
  mysql/postgres `driverName` seams pointed at sqlite.
- `SUGGESTIONS.md` — improvements, candidate new adapters, and the design
  changes found during the audit that are out of scope for a fix pass.

### Changed
- `memory.List` builds its JSON payload with a `strings.Builder` instead of
  repeated string concatenation (was O(n²) in the number of matched keys).

## [2.2.0] - 2026-07-05
### Changed
- All dependencies upgraded to their latest Go 1.24-compatible releases
  (testify v1.11.1, go-redis v9.21.0, go-elasticsearch v8.19.6, go-sqlite3
  v1.14.47, mongo-driver v1.17.9, mysql v1.10.0, pq v1.12.3, sftp v1.13.10,
  aws-sdk-go v1.55.8, x/crypto v0.48.0, x/text v0.34.0, and all transitive
  modules). The `go` directive moved from 1.23 to 1.24 (CI updated to match).
  Note: `aws-sdk-go` v1 and `mongo-driver` v1 are deprecated upstream in
  favor of their v2 modules — migrating is a separate, breaking effort.

### Fixed
- **Races/leaks**
  - The package-level `singleton` in every adapter is now guarded by a
    mutex — concurrent `New`/`Get`/`Set` no longer race.
  - APM transactions implicitly started by operations (when the incoming
    context carried none) are now ended, so they are reported instead of
    leaking pooled objects.
  - `s3.Retrieve` closes the response body; `s3.Create`/`Update` honor the
    context (`UploadWithContext`).
  - `redis.New`, `mongodb.New` and `sftp.New` close the client/connection
    on ping/validation failure instead of leaking pools, topology
    goroutines, or SSH sessions.
  - `file.Create` no longer leaks a file descriptor per `CreateIfNotExist`
    call, and write paths surface `Close` errors instead of dropping them.
  - Connection retries (`retrier.RunCtx`) now honor context cancellation
    instead of sleeping through the full backoff schedule.
- **Correctness**
  - Fan-out helpers (`CountFromMany`, `RetrieveFromMany`, `CreateIntoMany`,
    `CreateMany`, `RetrieveMany`, …) no longer silently drop zero-valued
    results (a legitimate count of `0`, an all-zero document, an empty ID).
  - `RetrieveFromMany` returns all aggregated errors, not a
    nondeterministically-chosen single one.
  - SQL adapters (postgres/sqlite/mysql): the `target` (table name) is now
    validated before being interpolated into the default `Count`/`List`
    queries — closing a SQL-injection hole; `Update` of a nonexistent row
    returns 404 (was silent success), matching `Retrieve`; postgres uses
    its registered goqu dialect.
  - `mongodb.List` no longer panics on nil params; `mongodb.Update` returns
    404 when nothing matched.
  - `elasticsearch.Count` emits `track_total_hits` again (counts were
    silently capped at 10k); routing params are honored (condition was
    inverted); double error-counting in `Query` removed.
  - `dynamodb.Count` paginates over `LastEvaluatedKey` (was undercounting
    tables larger than one scan page) and uses `Select: COUNT`; expression
    placeholders are sanitized so attribute names with dashes/dots work;
    `List` no longer sends an invalid `Limit: 0`; `New` no longer discards
    the `region` argument when a custom config lacks one.
  - `s3.New` returns an error instead of `log.Fatal`-ing the host process,
    and no longer silently returns a singleton configured for a different
    bucket; `s3.Retrieve` maps missing objects to 404.
  - `sftp.New` accepts the documented `host:port` address form; `sftp.Count`
    honors the search glob and no longer mutates the caller's params.
  - `redis.Count` uses non-blocking `SCAN` instead of `KEYS` and treats an
    empty search as match-all; `redis.Update` failures increment the update
    (not count) metric.
  - `memory.Count`/`List` honor the documented `Search` glob; `memory.Retrieve`
    errors on non-`[]byte` stored values instead of silently succeeding.
  - `memory.Create`/`redis.Create` reject empty IDs (which would have
    created unaddressable records).
  - `file.Create` no longer panics on nil params and resolves the
    storage-level `Target` fallback in `CreateIfNotExist`; `file.Retrieve`
    failures increment the retrieve (not delete) metric.
  - `storage.Mock` methods return an error when unset instead of panicking
    inside fan-out goroutines (which crashed the whole process).
  - `customapm.TraceError` is nil-safe.

### Added
- Offline unit/e2e test suites (happy, bad, and edge paths) for the memory,
  file, sqlite (real database), storage fan-out, customapm, dynamodb
  expression-builder, and elasticsearch query-builder surfaces — all running
  under `-race` with no external infrastructure.

## [2.1.1] - 2026-05-21
### Fixed
- `elasticsearch.parseResponseBodyError` now surfaces the full `caused_by`
  chain and the first `root_cause` entry in addition to the top-level
  `reason`. Previously, errors like `search_phase_execution_exception`
  surfaced only the misleading `"all shards failed"` text while the
  actionable detail (e.g. `"Result window is too large..."`) was dropped.
  The top-level reason still comes first so existing substring matchers
  remain compatible.
- `ResponseError.Status` is now populated from the ES response (was a
  latent bug — silently zero before).

### Added
- `ResponseErrorFromESReason` gained typed `Type`, `CausedBy` and
  `RootCause` fields so callers can inspect the chain programmatically
  in addition to the enriched `Reason` string.

## [1.0.0] - 2023-02-08
### Added
- First release.
