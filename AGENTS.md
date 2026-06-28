# AGENTS.md

## Project Overview

HugRS is a transparent caching proxy for HuggingFace and ModelScope model files. Files are split into 4MB chunks, each keyed by SHA256. The project now provides:

- `hugrs`: zero-argument daemon
- `hugrsctl`: management client for service, repo, and file operations
- control-plane admin API under `/_hugrs/...`

### Core Design Principles

- **Transparent cache**: upstream responses are forwarded as-is. Do NOT modify content-type, headers, or response body.
- **Proxy follows redirects**: upstream HuggingFace uses 302→xet-bridge redirect chains. The proxy MUST follow redirects internally and return the final response to clients. Clients should never see 30x from upstream.
- **Redirect transparency**: 302 responses are followed internally. Headers from the 302 (X-Repo-Commit, X-Linked-Size, X-Linked-ETag) and final 200 (Content-Length, ETag, Content-Type) are merged. The client always receives 200 with the combined metadata.
- **Metadata first**: HEAD requests cache file metadata (size, etag, x-repo-commit) in the `files` table without downloading content. Subsequent GET/POST uses cached metadata for Range/Content-Length.
- **No guessing**: never invent content-type, filenames, or other response metadata. Take it from upstream or don't include it. There is no fallback default for content-type like `application/octet-stream` — every byte of response metadata must trace back to an upstream source.
- **Partial downloads resume**: interrupted GET downloads restart from the last completed chunk. `file_chunks` tracks which chunks are cached.
- **Immutable chunks**: chunk data is keyed by SHA256 and never modified. Same chunk (same hash) is reused across multiple files.

## Tech Stack

- **Language**: Rust (stable)
- **Runtime**: tokio (async)
- **HTTP Framework**: axum
- **SQLite**: rusqlite (bundled, WAL mode)
- **S3**: aws-sdk-s3
- **CLI**: clap (derive)
- **HTTP Client**: reqwest
- **Error Handling**: anyhow + thiserror

## Build & Run Commands

```bash
# Build
cargo build

# Run daemon
cargo run

# Run management client
cargo run --bin hugrsctl -- service

# CLI help
cargo run --bin hugrsctl -- --help

# Tests
cargo test

# Lint
cargo clippy -- -D warnings

# Format
cargo fmt -- --check

# Release build
cargo build --release
```

## Code Conventions

- Use `anyhow::Result<T>` for application-level errors, `thiserror` for library errors
- Async functions return `Result<T>` from anyhow
- SQLite accessed via `rusqlite::Connection` with WAL pragma enabled at startup
- Storage backends implement the `StorageBackend` trait
- CLI uses clap derive macros
- Follow standard Rust naming conventions (snake_case, CamelCase)
- Keep modules focused: one module = one responsibility
- No comments unless code is genuinely non-obvious

## Documentation

- **Bilingual docs must stay in sync**:
  - `README.md` ↔ `README_zh.md`
  - `docs/CONFIG.md` ↔ `docs/CONFIG_zh.md`
  - `docs/CLI.md` ↔ `docs/CLI_zh.md`
  When modifying either file in a pair, sync the same change to its counterpart.

## Release Notes

- Release version currently follows git tags like `v0.4.0`
- `.github/workflows/release.yml` is tag-driven: pushing `v*` triggers binary and Docker release jobs
- Release artifacts must include both `hugrs` and `hugrsctl`
- Docker image must continue to ship both binaries, with `hugrs` as the entrypoint
- When cutting a release, update:
  - `Cargo.toml` version
  - `Cargo.lock`
  - Docker image tags in `README.md` and `README_zh.md`
  - any release-facing docs that mention the current version
- Before tagging a release, verify at minimum:
  - `cargo fmt -- --check`
  - `cargo clippy -- -D warnings`
  - `cargo test`

## Project Structure

```
src/
├── main.rs            # `hugrs` daemon entry point
├── bin/hugrsctl.rs    # `hugrsctl` binary entry point
├── hugrsctl_cli.rs    # Management CLI command definitions and formatting
├── admin_client.rs    # Client for the control-plane admin API
├── control.rs         # Control-plane request/response types
├── config.rs          # Configuration
├── server.rs          # HTTP server (axum)
├── service.rs         # Business logic
├── session.rs         # Download session and prefetch coordination
├── chunker.rs         # File split/assemble
├── metadata.rs        # SQLite operations
├── git.rs             # Git/LFS proxy support
├── storage/
│   ├── mod.rs         # StorageBackend trait
│   ├── local.rs       # Local FS backend
│   └── s3.rs          # S3 backend
├── migrations/        # SQL migrations
└── hf.rs              # HuggingFace Hub integration
```
