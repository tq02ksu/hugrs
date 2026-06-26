# Changelog

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
