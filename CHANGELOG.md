# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Project Roadmap

- [ ] Create and Update should allow to pass more than one object per time (Bulk Create and Update)

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
