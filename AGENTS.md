# AGENTS.md

## Project Overview

HugRS is a content-addressed caching service for HuggingFace model files. Files are split into 4MB trunks, each keyed by SHA256. Provides CLI management and HTTP API access.

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
