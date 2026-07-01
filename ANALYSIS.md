# DAL `List` / `Count` Search-Overwrite Bug — Analysis

Repo: `github.com/thalesfsp/dal/v2` — analyzed at local `main` HEAD
`cf34d8ac40259db8268a4a86e7b1a80148345aee` (module tag v2.1.1).

## 1. Exact defect + root cause (file:line)

The SQL storages resolve the query to run into `finalParam.Search`. The intent
is: **if the caller supplies a `Search`, use it; otherwise default to
`SELECT * FROM <target>`.** The implemented logic is inverted and overwrites the
caller's `Search` with the default in exactly the case it should be honored.

### mysql/mysql.go — `List` (pre-fix lines 545-557)

```go
defaultSearch := "SELECT * FROM " + trgt

if finalParam.Search == "" {          // finalParam is fresh from list.New(); Search is ALWAYS ""
    finalParam.Search = defaultSearch  //   -> so this always sets the default
}

if prm != nil {
    if prm.Search != "" {              // BUG: when the caller DID provide a Search...
        prm.Search = defaultSearch     //   ...OVERWRITE it with the default (SELECT * FROM target)
    }
    finalParam = prm
}
```

Then (pre-fix line 569):

```go
m.Client.SelectContext(ctx, v, finalParam.Search)   // no bind args at all
```

**Root cause:** the `if prm.Search != ""` guard has inverted intent. It clobbers
a non-empty caller `Search` with the default. The first `if finalParam.Search ==
""` block operates on the fresh `list.New()` value (always empty) and is dead
with respect to the caller. Net effect: **`List` can never filter** — every call
returns `SELECT * FROM target` regardless of the caller's `Search`.

Second, latent failure: when the caller passes a **non-nil** params struct with
an **empty** `Search` (`&list.List{Search: ""}`), `finalParam = prm` leaves
`Search == ""`, and `SelectContext(ctx, v, "")` is executed with an empty
statement — which **hangs** the `mattn/go-sqlite3` driver (observed in tests).

### Identical defect, all six sites

The same inverted block exists in **`Count`** (with `SELECT COUNT(*) FROM ...`)
and is duplicated verbatim across all three SQL storages:

| Storage   | `List`        | `Count`       |
|-----------|---------------|---------------|
| mysql     | ~L545-557     | ~L186-198     |
| postgres  | ~L536-548     | ~L177-189     |
| sqlite    | ~L536-548     | ~L177-189     |

## 2. Blast radius — which storages are affected

**Affected: mysql, postgres, sqlite** (all three share the same copy-pasted
block and the same `params/v2/list` + `params/v2/count` contract).

**Not affected:** `elasticsearch`, `redis`, `mongodb`, `s3`, `file`, `sftp`,
`memory`, `dynamodb` — they have their own `List`/`Count` implementations that do
not contain this inverted `prm.Search` overwrite. (`elasticsearch` uses
`finalParam.Search` as an ES query and honors a caller `Search`.)

## 3. Impact

- **Correctness (primary):** `List`/`Count` with a caller `Search`/filter is
  silently ignored on SQL storages — you always get the full table (or full
  count). Callers relying on `Search` to filter get wrong (unfiltered) results
  with no error. `LIMIT`/`OFFSET`/`ORDER BY` expressed via `Search` are likewise
  discarded.
- **Availability:** a non-nil params struct with an empty `Search` produces an
  empty SQL string, which hangs the SQLite driver (and is malformed for others).
- **Injection angle:** the documented contract is that `List`/`Count` "uses
  `param.List.Search` to query the data" — i.e. `Search` is a **caller-supplied,
  trusted query template**, not untrusted end-user input. The storage layer does
  **not** concatenate any untrusted value into the query; the only interpolation
  is `target` in the default `SELECT ... FROM <target>` path. NOTE: `target`
  resolves via `shared.TargetName` from the caller-supplied target argument (or
  the storage's configured `m.Target`) — it is caller/operator-controlled, NOT
  strictly server-only, and this is unchanged by the fix (pre-existing). It never
  comes from `prm.Search`. The fix preserves this: the caller's `Search` is
  passed **verbatim** to `SelectContext` (proved by a quote-in-value test
  round-trip), and no new value concatenation is introduced. Callers remain
  responsible for parameterizing untrusted values inside their own `Search` (the
  public interface exposes no separate bind-args slot — a pre-existing API shape,
  unchanged here).

## 4. Consumers — proj-ringboost-vendor (UVS) backward-compatibility

UVS (`github.com/WreckingBallStudioLabs/proj-ringboost-vendor`, on
`dal/v2 v2.1.0`) uses these dal storages: **elasticsearch (60 imports), redis
(40), postgres (5, wired in `cmd/storage.go`)**.

- Every `List`/`Count` consumer that passes a non-empty `Search` reads
  `storages[elasticsearch.Name]` (15 call sites — pricer, areacode, localizer,
  phonenumber, etc.). The pricer (`internal/pricer/pricing.go:260`,
  `util.go:670`) lists against **Elasticsearch** with an ES query-DSL `Search`
  (`{"bool":{"must":[...]}}`). **Elasticsearch is not affected by this bug.**
- The **postgres** storage is initialized and registered but **never has `.List`
  or `.Count` invoked on it** anywhere in UVS (verified by grep across the repo).
  It is used for other operations, not filtered listing.

**Verdict — fully backward-compatible:**
- UVS never exercised the buggy SQL `Search` path, so the current bug does **not**
  mask anything in UVS, and the fix does **not** change any behavior UVS relies
  on today.
- For the only SQL-List shape UVS could hit (nil params / no custom `Search`),
  the fix is behavior-preserving: it still defaults to `SELECT * FROM target`.
- The fix is purely additive to correctness: previously-ignored caller `Search`
  filters now work; the default (no-Search) path is byte-for-byte the same query.
  No public interface signature changes.

## 5. Fix

Replace the inverted block at all six sites with straightforward precedence.
`prm` is shallow-COPIED before defaulting so the caller-owned params struct is
never mutated (previously, defaulting an empty `Search` would have written the
default query back into the caller's struct):

```go
// Honor a caller-provided Search; only default when none was supplied.
// Copy prm so defaulting never mutates the caller-owned params struct.
if prm != nil {
    prmCopy := *prm
    finalParam = &prmCopy
}

if finalParam.Search == "" {
    finalParam.Search = "SELECT * FROM " + trgt   // or "SELECT COUNT(*) FROM " for Count
}
```

- Caller `Search` non-empty  -> used verbatim (filtering now works).
- Caller `Search` empty / nil params -> defaults to `SELECT * FROM target`
  (unchanged behavior; also fixes the empty-string hang).
- No untrusted-value concatenation is introduced; the only interpolation remains
  `target` (caller/operator-controlled via `shared.TargetName`, not `prm.Search`).
- `prm` is shallow-COPIED before defaulting so the caller-owned params struct is
  never mutated by DAL's defaulting (previously an empty `Search` would have had
  the default query written back into it).

## 6. Tests

`sqlite/sqlite_list_test.go` (new) exercises the **real** `List`/`Count` code
path fully offline against an isolated file-backed SQLite DB (no docker, no
`ENVIRONMENT` gate), so it runs under `make test`:

- **happy:** nil params -> all rows / full count; `Search` filter -> correct
  subset / filtered count.
- **edge:** `LIMIT`/`OFFSET` pagination honored; `ORDER BY` honored; empty
  `Search` falls back to default AND does not mutate the caller-owned params
  struct (`prm.Search` stays `""` after the call) — for both List and Count;
  quote-in-value (`O'Brien`) round-trips verbatim against a two-row table so the
  WHERE result (1 row) differs from the default (2 rows), proving the caller
  query actually ran; default path builds `SELECT * FROM <target>` from the
  target arg, never from `prm.Search`.
- **bad:** malformed `Search` surfaces an error (does not silently succeed).

The filter / LIMIT / ORDER BY / count-filter assertions are the **negative
control**: verified to FAIL on the pre-fix code for the right reason
("should have 1 item(s), but has 3", count "expected 2 got 3") and PASS after
the fix.

`mysql` and `postgres` share the identical fixed block; no live MySQL/Postgres
instance was available in this environment to run their integration tests, so
those storages are covered by (a) the shared-code equivalence with sqlite and
(b) `go build` + `go vet`. See the PR description for exactly what was run vs
skipped.
