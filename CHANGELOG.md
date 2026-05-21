# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Project Roadmap

- [ ] Create and Update should allow to pass more than one object per time (Bulk Create and Update)

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
