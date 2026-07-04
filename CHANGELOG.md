# Changelog

## [0.6.1] - 2026-07-04

### Added
- Byte-level cache progress reporting in `hugrsctl repo list` and `hugrsctl repo show`

### Fixed
- Cached chunk chunk_size validation prevents serving truncated data
- Fetch chunk size validation rejects incomplete upstream responses
- Session table atomic entry prevents duplicate concurrent chunk downloads
- Per-sha256 write serialization prevents TOCTOU on chunk storage
- Chunk replacement in metadata correctly updates sha256 and ref_count on re-fetch
- Download concurrency semaphore scoped correctly across all chunk downloads

### CI
- GitHub Release body is now extracted from the matching changelog section during publishing

## [0.6.0] - 2026-07-04

### Added
- Reconsile repair and batched GC (`hugrsctl service reconsile` + `hugrsctl service gc`)
- File download completion status reporting in `hugrsctl files show`
- File download progress display in `hugrsctl files show`
- Homebrew install documentation

### Changed
- Refactored: transactional chunk persistence and internal GC batch size
- Refactored: unify runtime data paths
- Refactored: evict_if_needed tries GC first before evicting a repo; removed unused server-side `gc()` loop
- Optimized: GC chunk reclamation batching
- Split security scan workflows from CI
- Updated quality gate documentation

### Fixed
- Exclude orphan chunks from stored bytes stats
- Reconcile metadata before reusing cache

### Tests
- Cover delete rollback transaction

### Docs
- Added poster image to README
- Clarified Rust lint policy in docs
- Removed unused env in client usage docs
- Added packaging and paths plan

## [0.5.0] - 2026-07-01

### Added
- ETag validation on HEAD and GET cache hits, with If-None-Match (304) support
- Transitional startup backfill for NULL etag/content_type in cached metadata
- Daemon CLI config overrides via figment (CLI args merged into config hierarchy)
- Streaming tests, config tests, and daemon CLI tests
- Dependabot configuration for automated dependency updates

### Changed
- Refactored: deleted `upload()` abstraction; chunk storage is now inline in download paths
- Refactored: unified HTTP chunk reads through file download sessions
- Tightened clippy lint policy with 9 additional deny-level lints

### Fixed
- Forward upstream error responses for API and file proxy paths
- SHA operation error caused by sha2 version bump (input length mismatch)

### CI
- Merged cargo-audit and trivy scan into unified CI workflow
- Added GNU Linux release package (alongside musl)

## [0.4.0] - 2026-06-28

### Added
- `hugrsctl` management client as a separate binary
- Control-plane admin API under `/_hugrs/...`
- Admin token generation and control-plane authentication
- Repo/file management commands with source-aware filtering and delete semantics
- GC dry-run and batched orphan chunk reclamation
- Orphan chunk tracking via `orphaned_at`
- Dedicated CLI documentation in `docs/CLI.md` and `docs/CLI_zh.md`

### Changed
- `hugrs` is now a zero-argument daemon entrypoint
- Removed the old in-process management CLI from `hugrs`
- Default management flow now uses `hugrsctl service|repo|file`
- Human-readable default output for `hugrsctl`, with aligned text formatting for service status/stats/GC
- Release packaging now includes both `hugrs` and `hugrsctl`
- Docker image now ships both binaries and starts `hugrs` directly

### Fixed
- Eliminate multi-writer metadata inconsistencies by moving management operations behind the serving process
- Delete operations now mark zero-ref chunks as orphaned instead of deleting backend data immediately
- Release workflow updated for the current two-binary layout

### Docs
- Reworked README for user-facing startup and management usage
- Separated CLI usage from configuration documentation
- Clarified platform-specific cache paths, including macOS defaults

### CI
- Removed the unused coverage workflow

## [0.3.1] - 2026-06-26

### Added
- Git/LFS proxy (info/refs, upload-pack, lfs batch)
- Modelscope proxy and configuration support
- SQL indexes for repo and ref_count to improve query performance
- Compatible with old trunks directory for seamless upgrades
- Code coverage setup

### Changed
- Rename `trunk` to `chunk` throughout codebase, switch to `rusqlite_migration`
- Extract migration SQL to files, drop `http_cache`
- Move schema evolution into SQL migrations
- Session decoupled from metadata database
- Default server host changed to `0.0.0.0`

### Fixed
- E2E test fixes
- Avoid redundant upstream chunk downloads
- Concurrent download deduplication

### Docs
- Git clone usage guide and Git/LFS API endpoints documentation
- Git/LFS proxy design and implementation specs

## [0.2.0] - 2026-06-25

### Added
- SessionTable, FileSessionManager, FileDownloadSession — new session-based download architecture
- In-flight budget configuration to control concurrent upstream requests
- hfd.sh script support for HuggingFace dataset downloads
- Trunk-level timing logs and slow trunk warnings
- Build stream client without request timeout
- MIT LICENSE

### Changed
- Sequential prefetch for active cursors
- Resign FileSessionManager to prevent session blocking
- Clean up unused dependencies (tokio-util, thiserror), move tempfile to dev-deps, add dashmap

### Docs
- Server refactor design spec (Chinese)
- Benchmark documentation
- Issue templates

## [0.1.1] - 2026-06-24

### Added
- Stats API for monitoring cache and bandwidth metrics
- Multi-arch Docker build (amd64 + arm64)
- Docker usage documentation in README

### Fixed
- Download same file multiple times (concurrent request deduplication)
- Remove unused doc file

### CI
- Add contents:write permission to release binary job
- Issue template and highlighted README
