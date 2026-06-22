# AGENTS.md

## Project Overview

HugRS is a transparent caching proxy for HuggingFace model files. Files are split into 4MB trunks, each keyed by SHA256. Provides CLI management and HTTP API access.

### Core Design Principles

- **Transparent cache**: upstream responses are forwarded as-is. Do NOT modify content-type, headers, or response body.
- **Proxy follows redirects**: upstream HuggingFace uses 302→xet-bridge redirect chains. The proxy MUST follow redirects internally and return the final response to clients. Clients should never see 30x from upstream.
- **Redirect transparency**: 302 responses are followed internally. Headers from the 302 (X-Repo-Commit, X-Linked-Size, X-Linked-ETag) and final 200 (Content-Length, ETag, Content-Type) are merged. The client always receives 200 with the combined metadata.
- **Metadata first**: HEAD requests cache file metadata (size, etag, x-repo-commit) in the `files` table without downloading content. Subsequent GET/POST uses cached metadata for Range/Content-Length.
- **No guessing**: never invent content-type, filenames, or other response metadata. Take it from upstream or don't include it. There is no fallback default for content-type like `application/octet-stream` — every byte of response metadata must trace back to an upstream source.
- **Partial downloads resume**: interrupted GET downloads restart from the last completed trunk. `file_trunks` table tracks which chunks are cached.
- **Immutable trunks**: trunk data is keyed by SHA256 and never modified. Same trunk (same hash) used across multiple files.

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

# Run (HTTP server)
cargo run -- serve

# CLI help
cargo run -- --help

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

## Project Structure

```
src/
├── main.rs        # Entry point
├── cli.rs         # CLI command definitions
├── config.rs      # Configuration
├── server.rs      # HTTP server (axum)
├── service.rs     # Business logic
├── chunker.rs     # File split/assemble
├── metadata.rs    # SQLite operations
├── storage/
│   ├── mod.rs     # StorageBackend trait
│   ├── local.rs   # Local FS backend
│   └── s3.rs      # S3 backend
└── hf.rs          # HuggingFace Hub integration
```
