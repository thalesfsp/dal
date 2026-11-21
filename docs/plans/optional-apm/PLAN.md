# Plan: make Elastic APM optional in dal v3

## What is wrong today (v3.0.0, measured 2026-11-20)

- Every adapter (storage, memory, file, sqlite, postgres, mysql, mongodb, redis,
  s3, sftp, bolt, dynamodb, elasticsearch) imports `internal/customapm` and
  `internal/logging`, which import `go.elastic.co/apm`. `go list -deps` of each
  core package shows 18 `go.elastic.co/apm` packages plus 8 `elastic/go-sysinfo`
  packages.
- Importing `go.elastic.co/apm` runs its `init()`: it builds `apm.DefaultTracer`
  from `ELASTIC_APM_*` environment variables and starts three goroutines
  (`Tracer.loop`, its helper, and `HTTPTransport.WatchConfig`). One second after
  start the process sends `GET /config/v1/agents?service.name=<binary>` to the
  APM server (`http://localhost:8200` when nothing is configured) and keeps
  polling. A scratch program whose only import is `dal/v3/storage` did exactly
  that.
- Every dal operation opened an APM transaction and span on that tracer, and
  every failure was sent to it, whether or not the application wanted APM.
- `go-elasticsearch` is NOT pulled into the core packages: only dal's own
  `elasticsearch` package imports it. dal has no Elasticsearch log output; its
  only log-side Elastic code is the `trace.id` / `transaction.id` / `span.id`
  correlation fields added to log lines.

## Options weighed

| Option | What a consumer pays |
|---|---|
| A. Tracing hook in core, Elastic implementation as an opt-in package in the same module | Nothing to compile. A consumer that wants dal spans in Elastic adds one call at start-up. |
| B. Same as A, but the Elastic package in its own nested Go module | Same as A, plus a second module to tag, version and test, and go.mod still lists Elastic because the `elasticsearch` adapter needs its client. |
| C. Build tags (`-tags dalapm`) | Every consumer wanting APM changes its build command in every CI and Dockerfile; easy to forget silently. |
| D. Keep the import, only make it inert | Does not meet the goal: the `init()` above still runs, because it fires on import. |

## Recommendation: A

- New core package `github.com/thalesfsp/dal/v3/tracing`: a three-method
  `Tracer` interface (start a span, record an error, add log-correlation
  fields) with a do-nothing default. No dependencies beyond the standard library.
- New opt-in package `github.com/thalesfsp/dal/v3/elasticapm`: the previous
  behaviour, moved verbatim. Only code that imports it links Elastic APM.
- No exported identifier is removed or changed: `customapm` and `logging` were
  `internal`, so no consumer could import them.

## Compatibility and version

- Code compatibility: full. Every v3.0.0 consumer compiles unchanged.
- Behaviour change: after upgrading, dal no longer emits APM spans, transactions
  or errors unless the application opts in. Nothing else changes (counters,
  hooks, logs, errors are identical).
- Version: **v3.1.0**. Semver minor: new packages, no API removed. A v4 would
  force all ~18 consumers to rewrite every import path for a change no code
  depends on to compile; telemetry output is not part of dal's API contract.
  The behaviour change is listed first under "Changed" in the CHANGELOG.

## What a consumer does after the change

- Does NOT want Elastic (e.g. EmAI): bump to v3.1.0. Nothing else. Imports stay
  `github.com/thalesfsp/dal/v3/storage` and `github.com/thalesfsp/params/v2/...`.
- WANTS Elastic APM spans from dal: bump to v3.1.0 and, once at start-up:

  ```go
  import "github.com/thalesfsp/dal/v3/elasticapm"

  func main() {
      elasticapm.Enable() // uses apm.DefaultTracer, configured by ELASTIC_APM_* as before
      // or: tracing.Set(elasticapm.New(myTracer))
  }
  ```

- Wants another backend (OpenTelemetry, ...): implement `tracing.Tracer` and call
  `tracing.Set` once.

## Guard rails shipped with the change

- `internal/coreguard`: a test that runs `go list -deps` on every core package
  and fails if any Elastic package appears; a test that, with all core packages
  linked, no goroutine belongs to Elastic code and the default tracer is the
  no-op.
- `elasticapm` tests: the opt-in path end to end (spans, implicit transaction,
  error capture, log-correlation fields) on a recording tracer. The existing
  per-adapter envelope tests keep asserting one span and one transaction per
  operation, now through `elasticapm`.
