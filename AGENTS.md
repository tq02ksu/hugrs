# AGENTS.md

## Project Overview

HugRS is a transparent caching proxy for HuggingFace and ModelScope model files. Files are split into 4MB chunks, each keyed by SHA256. The project now provides:

- `hugrs`: daemon process with config file, environment variable, and CLI override support
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
cargo clippy --all-features

# Format
cargo fmt

# Release build
cargo build --release
```

## Code Conventions

- Use `anyhow::Result<T>` for application-level errors, `thiserror` for library errors
- Async functions return `Result<T>` from anyhow
- SQLite accessed via `rusqlite::Connection` with WAL pragma enabled at startup
- Storage backends implement the `StorageBackend` trait
- CLI uses clap derive macros
- Clippy lint levels are centralized in `Cargo.toml` under `[lints.clippy]`
- Keep CI lint execution thin: it should run Cargo/Clippy, not redefine lint policy in workflow flags
- Prefer curated project lints over crate-level `#![forbid(...)]` attributes unless a rule truly must not be overridden
- Follow standard Rust naming conventions (snake_case, CamelCase)
- Keep modules focused: one module = one responsibility
- No comments unless code is genuinely non-obvious

## Documentation

- **Bilingual docs must stay in sync**:
  - `README.md` ↔ `README_zh.md`
  - `docs/CONFIG.md` ↔ `docs/CONFIG_zh.md`
  - `docs/CLI.md` ↔ `docs/CLI_zh.md`
  When modifying either file in a pair, sync the same change to its counterpart.
- **Protocol and interface design reference**:
  - When designing or reviewing upstream-facing APIs, file routes, metadata behavior, redirects, auth/header forwarding, or client compatibility logic, consult `docs/protocol/` first.
  - Treat the provider notes in `docs/protocol/huggingface.md` and `docs/protocol/modelscope.md` as the protocol baseline unless the code, trace evidence, or changelog proves they need to be updated.
  - If a feature change modifies upstream protocol behavior, external interfaces, or client usage modes, update the relevant file under `docs/protocol/` as needed and keep the document content consistent with the implementation.
  - Do not land an implementation that conflicts with the documented client-visible behavior in `docs/protocol/huggingface.md` or `docs/protocol/modelscope.md` unless the same change also updates those documents.
- **Config responsibility split**:
  - `clap` parses daemon and management CLI arguments.
  - `dotenvy` loads `.env` into the process environment.
  - `figment` merges defaults, config file values, environment variables, and CLI overrides into `Config`.
  Keep this separation explicit in code and docs. Do not describe `clap` or `dotenvy` as config merge solutions.

## Commit Checklist

- Before creating any commit, run the relevant verification commands for the change and confirm they pass.
- For Rust code changes, the default pre-commit quality gates are:
  - `cargo fmt`
  - `cargo clippy --all-features`
  - `cargo test`
- During iteration, targeted tests are acceptable for faster feedback, but do not commit until the full required checks for the touched area have passed.
- Do not commit code that is known to fail formatting, clippy, or tests.

## Release Checklist

- Release tags use the `v*` pattern. Pushing a tag like `v0.4.0` triggers `.github/workflows/release.yml`.
- Release artifacts and Docker image must include both `hugrs` and `hugrsctl`.
- Before cutting a release, follow this order:
  1. Summarize the changes since the previous version and write them into `CHANGELOG.md`.
  2. Re-review the implementation against the changelog and fix any incomplete design or release details first.
  3. If nothing else is missing, run the quality gates:
     - `cargo fmt`
     - `cargo clippy --all-features`
     - `cargo test`
  4. Only after the checks pass, bump the version in `Cargo.toml`.
  5. Update `Cargo.lock`, Docker image tags in `README.md` / `README_zh.md`, and any user-facing docs that mention the current release version.
  6. Commit the release changes, create the git tag, and push both commit and tag.
  7. **Update Homebrew tap formula** — the tap repo `homebrew-tap/Formula/hugrs.rb`
      needs version and SHA256 updated for all 4 platform binaries (aarch64/x86_64 × macOS/Linux).
      The tap repo lives at `../homebrew-tap/` (same level as this repo). Alternatively,
      use `gh repo clone tq02ksu/homebrew-tap` or `git clone` to access it. After updating,
      commit and push the tap repo as well.

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
