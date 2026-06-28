# CLI Admin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current local-state CLI with a service-owned control plane and a separate `hugrsctl` client, while simplifying the user model to `service`, `repos`, and `files`.

**Architecture:** Keep `metadata.rs` as the persistent truth, keep `session.rs` as the read/session data plane, and route all management mutations through `hugrs` control APIs under `/_hugrs`. Split the binaries so `hugrs` defaults to serving and `hugrsctl` becomes the only management CLI, with deletion updating file-to-chunk references and GC performing orphan chunk reclamation in batches.

**Tech Stack:** Rust (stable), clap, axum, tokio, rusqlite, serde, reqwest

---

## File Structure

- Modify: `Cargo.toml`
  - Add a `hugrsctl` binary target if the crate is currently only building `hugrs`.
- Modify: `src/main.rs`
  - Simplify `hugrs` entry so default invocation serves; keep `serve` as an alias if retained.
- Create: `src/bin/hugrsctl.rs`
  - New control-plane client CLI.
- Modify: `src/cli.rs`
  - Remove or reduce management subcommands from the server binary path.
- Modify: `src/config.rs`
  - Add admin token discovery defaults and control endpoint discovery fields/helpers.
- Modify: `src/metadata.rs`
  - Add orphan lifecycle metadata and repo/file query helpers for control APIs.
- Modify: `src/migrations/*.sql`
  - Add migration for orphan metadata if needed.
- Modify: `src/service.rs`
  - Add service-owned repo/file delete flows and GC dry-run/execute flows.
- Modify: `src/server.rs`
  - Add `/_hugrs/...` routes, auth middleware, and JSON response types.
- Create: `src/control.rs`
  - Shared control-plane request/response structs used by both server and `hugrsctl`.
- Create: `src/admin_client.rs`
  - Thin reqwest client for `hugrsctl`.
- Modify: `src/lib.rs`
  - Export `control` and `admin_client`.
- Modify: `tests/service_tests.rs`
  - Cover delete semantics, orphan marking, and GC behavior.
- Modify: `tests/e2e_tests.rs`
  - Cover control API auth and `/_hugrs` routes.
- Create or modify: `tests/control_tests.rs`
  - Cover `hugrsctl` command parsing or client behavior if appropriate.
- Modify: `README.md`
- Modify: `README_zh.md`
- Modify: `docs/CONFIG.md`
- Modify: `docs/CONFIG_zh.md`

## Global Constraints

- Transparent cache behavior for upstream file responses must remain unchanged.
- `chunk` stays an internal implementation detail and must not become a user-facing resource.
- Delete means removing file-cache references; physical chunk deletion is GC-only.
- `source` remains an optional filter; without it, views aggregate and deletes apply to all sources.
- API responses return raw machine values; `hugrsctl` handles human-readable formatting.

---

### Task 1: Add persistent orphan semantics and delete/GC service flows

**Files:**
- Modify: `src/metadata.rs`
- Modify: `src/service.rs`
- Modify: `src/migrations/005_cleanup_headerless_files.sql` or add a new migration file
- Modify: `tests/service_tests.rs`
- Modify: `tests/metadata_tests.rs`

**Interfaces:**
- Produces: metadata support for orphan chunks and service methods for file delete, repo delete, GC dry-run, and batched GC

- [ ] **Step 1: Add orphan metadata to chunk persistence**

Add an `orphaned_at` nullable timestamp column to `chunks` in a new migration and mirror it in `metadata.rs` helpers. New helper responsibilities:

```rust
pub fn mark_chunk_orphaned(&self, sha256: &str) -> anyhow::Result<()>;
pub fn clear_chunk_orphaned(&self, sha256: &str) -> anyhow::Result<()>;
pub fn list_orphan_chunks_batch(&self, limit: usize) -> anyhow::Result<Vec<Chunk>>;
pub fn list_orphan_chunks_stats(&self) -> anyhow::Result<(i64, i64)>;
```

Rule: `ref_count == 0` implies `orphaned_at` is set; `ref_count > 0` implies `orphaned_at` is NULL.

- [ ] **Step 2: Make file deletion update only references and orphan state**

Change file deletion flow so it removes the file row and file-chunk links, decrements `ref_count`, and marks newly zero-ref chunks orphaned without deleting backend data:

```rust
pub async fn delete_file_all_sources(
    &self,
    repo: &str,
    file: &str,
    source: Option<&str>,
) -> anyhow::Result<DeleteResult>;

pub async fn delete_repo_all_sources(
    &self,
    repo: &str,
    source: Option<&str>,
) -> anyhow::Result<DeleteResult>;
```

`DeleteResult` should include counts such as deleted files and affected sources, not reclaimed bytes.

- [ ] **Step 3: Add GC dry-run and batched execute flows**

Implement service methods that inspect orphan chunks without mutating and reclaim them in batches:

```rust
pub async fn gc_dry_run(&self) -> anyhow::Result<GcPreview>;

pub async fn gc_execute(
    &self,
    batch_size: usize,
) -> anyhow::Result<GcResult>;
```

`GcPreview` should include orphan chunk count and candidate bytes. `GcResult` should include deleted chunk count, reclaimed bytes, and skipped chunk count.

- [ ] **Step 4: Add focused tests for delete semantics**

Extend `tests/service_tests.rs` to assert:

```rust
#[tokio::test]
async fn test_delete_marks_zero_ref_chunks_orphaned() { /* ... */ }

#[tokio::test]
async fn test_delete_does_not_remove_backend_data_immediately() { /* ... */ }

#[tokio::test]
async fn test_delete_without_source_removes_all_sources() { /* ... */ }
```

Use existing `MetadataStore` + `LocalBackend` setup, and assert both metadata and backend object presence after delete.

- [ ] **Step 5: Add focused tests for GC**

Extend metadata/service tests to assert:

```rust
#[tokio::test]
async fn test_gc_dry_run_reports_orphan_candidates() { /* ... */ }

#[tokio::test]
async fn test_gc_execute_reclaims_orphan_backend_objects() { /* ... */ }
```

Run:

```bash
cargo test service_tests metadata_tests -- --nocapture
```

Expected: PASS

---

### Task 2: Add `/_hugrs` control-plane API and admin token auth

**Files:**
- Create: `src/control.rs`
- Modify: `src/server.rs`
- Modify: `src/config.rs`
- Modify: `src/lib.rs`
- Modify: `tests/e2e_tests.rs`

**Interfaces:**
- Produces: authenticated `/_hugrs/service`, `/_hugrs/repos`, and `/_hugrs/files` APIs with shared JSON structs

- [ ] **Step 1: Define shared control-plane JSON types**

Create `src/control.rs` with serializable request/response structs such as:

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceStatusResponse { /* version, cache, sources, auth */ }

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceStatsResponse { /* logical/stored/fetched/served */ }

#[derive(Debug, Serialize, Deserialize)]
pub struct RepoListItem { /* repo, sources, files, logical_bytes, last_accessed */ }

#[derive(Debug, Serialize, Deserialize)]
pub struct FileListItem { /* repo, file, sources, size, content_type, last_accessed */ }

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteResponse { /* deleted, deleted_files, sources */ }

#[derive(Debug, Serialize, Deserialize)]
pub struct GcRequest { pub dry_run: bool, pub batch_size: Option<usize> }
```

- [ ] **Step 2: Add admin token config and discovery helpers**

In `src/config.rs`, add config fields/helpers for:

```rust
pub struct AdminConfig {
    pub token: Option<String>,
    pub token_file: PathBuf,
}
```

Define defaults under `~/.cache/hugrs/admin.token`, and add a helper that generates and persists a token when none is configured.

- [ ] **Step 3: Add auth middleware for `/_hugrs`**

In `src/server.rs`, add a middleware or request guard that checks:

```rust
Authorization: Bearer <token>
```

Only `/_hugrs/...` routes require this token. Proxy routes remain unchanged.

- [ ] **Step 4: Add control-plane routes**

In `src/server.rs`, add:

```rust
GET    /_hugrs/service
GET    /_hugrs/service/stats
POST   /_hugrs/service/gc
GET    /_hugrs/repos
GET    /_hugrs/repos/{repo}
DELETE /_hugrs/repos/{repo}
GET    /_hugrs/files
GET    /_hugrs/files/show
DELETE /_hugrs/files
```

`source` is an optional filter. `repo + file` is required for file show/delete.

- [ ] **Step 5: Add e2e route/auth coverage**

Add e2e tests covering:

```rust
#[tokio::test]
async fn test_control_api_rejects_missing_token() { /* 401 */ }

#[tokio::test]
async fn test_control_api_returns_service_status() { /* 200 */ }

#[tokio::test]
async fn test_file_delete_without_source_applies_to_all_sources() { /* ... */ }
```

Run:

```bash
cargo test e2e_tests -- --nocapture
```

Expected: PASS

---

### Task 3: Simplify the `hugrs` binary and default startup behavior

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs`
- Modify: `src/config.rs`

**Interfaces:**
- Produces: `hugrs` defaulting to serve mode with local control-plane discovery artifacts

- [ ] **Step 1: Reduce the server binary CLI surface**

Change `src/cli.rs` so the server binary no longer exposes management commands like `List`, `Info`, `Stats`, and `Gc` locally. Retain only serve-related configuration and an optional `serve` alias:

```rust
#[derive(Subcommand)]
pub enum Command {
    Serve,
}
```

and make `command` optional if bare `hugrs` should serve automatically.

- [ ] **Step 2: Make bare `hugrs` start the server**

Update `src/main.rs` so:

```rust
match cli.command.unwrap_or(Command::Serve) {
    Command::Serve => { /* existing server startup */ }
}
```

Remove local management command dispatch branches from `main`.

- [ ] **Step 3: Persist control-plane discovery info**

At server startup, write or refresh a local discovery artifact under `~/.cache/hugrs/` containing at least the control endpoint and token file path:

```rust
#[derive(Serialize, Deserialize)]
struct ControlDiscovery {
    endpoint: String,
    admin_token_file: String,
}
```

This file is for `hugrsctl` auto-discovery only.

- [ ] **Step 4: Verify the new startup UX**

Run:

```bash
cargo build
cargo run --
cargo run -- serve
```

Expected:

- build succeeds
- `cargo run --` starts the server
- `cargo run -- serve` behaves equivalently

---

### Task 4: Add the `hugrsctl` client binary and resource-grouped commands

**Files:**
- Create: `src/bin/hugrsctl.rs`
- Create: `src/admin_client.rs`
- Modify: `src/lib.rs`
- Modify: `Cargo.toml`
- Create or modify: `tests/control_tests.rs`

**Interfaces:**
- Produces: `hugrsctl service|repos|files` command tree with human output and `--json`

- [ ] **Step 1: Add the admin client transport**

Create `src/admin_client.rs` with a thin reqwest-backed client:

```rust
pub struct AdminClient {
    base_url: String,
    admin_token: String,
    http: reqwest::Client,
}
```

Methods should map directly to the control API:

```rust
pub async fn service_status(&self) -> anyhow::Result<ServiceStatusResponse>;
pub async fn service_stats(&self) -> anyhow::Result<ServiceStatsResponse>;
pub async fn service_gc(&self, dry_run: bool) -> anyhow::Result<GcResult>;
pub async fn repos_list(&self, source: Option<&str>) -> anyhow::Result<RepoListResponse>;
pub async fn files_delete(&self, repo: &str, file: &str, source: Option<&str>) -> anyhow::Result<DeleteResponse>;
```

- [ ] **Step 2: Add `hugrsctl` command parsing**

Create `src/bin/hugrsctl.rs` with grouped commands:

```rust
enum TopLevel {
    Service(ServiceCommand),
    Repos(ReposCommand),
    Files(FilesCommand),
}
```

Default actions:

```rust
hugrsctl service => status
hugrsctl repos   => list
hugrsctl files   => list
```

Support:

- `--source <hf|ms>`
- `--json`
- `--admin-token`
- discovered token fallback

- [ ] **Step 3: Add human-readable output formatting**

Implement output rendering in `hugrsctl` so the API stays raw but CLI output is readable:

```rust
fn human_bytes(bytes: u64) -> String { /* existing format style */ }
```

Keep `--json` as raw serialized API output.

- [ ] **Step 4: Add discovery loading**

Read endpoint and token defaults from the discovery artifact and token file under `~/.cache/hugrs/`, with the following priority:

```text
--admin-token
HUGRS_ADMIN_TOKEN
HUGRS_ADMIN_TOKEN_FILE
~/.cache/hugrs/admin.token
```

and equivalent endpoint override before discovery fallback.

- [ ] **Step 5: Add CLI behavior tests**

Add focused tests to cover:

```rust
fn test_service_defaults_to_status() { /* ... */ }
fn test_repos_defaults_to_list() { /* ... */ }
fn test_files_show_requires_repo_and_file() { /* ... */ }
```

Run:

```bash
cargo test control_tests -- --nocapture
```

Expected: PASS

---

### Task 5: Sync English and Chinese docs for the new control plane

**Files:**
- Modify: `README.md`
- Modify: `README_zh.md`
- Modify: `docs/CONFIG.md`
- Modify: `docs/CONFIG_zh.md`

**Interfaces:**
- Produces: bilingual docs matching the new binary split, control-plane auth, and management workflow

- [ ] **Step 1: Update README command examples**

In both `README.md` and `README_zh.md`, replace local management examples with the new split:

```text
hugrs          # start server
hugrsctl service
hugrsctl repos
hugrsctl files
```

Document that `hugrsctl` talks to `hugrs` over the control API and that `chunk` is not a user-managed concept.

- [ ] **Step 2: Document admin token and discovery paths**

In both `docs/CONFIG.md` and `docs/CONFIG_zh.md`, add:

- default token path `~/.cache/hugrs/admin.token`
- admin token env/config overrides
- control endpoint discovery behavior
- `Authorization: Bearer <token>` requirement for `/_hugrs` APIs

- [ ] **Step 3: Document delete and GC semantics**

In both README pairs or CONFIG pairs where appropriate, explicitly explain:

- delete removes file-cache references
- delete does not immediately free all space
- `hugrsctl service gc --dry-run` previews reclaimable orphan data
- `hugrsctl service gc` performs batched reclamation

- [ ] **Step 4: Verify bilingual sync manually**

Before finishing, compare:

```text
README.md      <-> README_zh.md
docs/CONFIG.md <-> docs/CONFIG_zh.md
```

Check that the same features, flags, paths, and examples exist in both languages.

- [ ] **Step 5: Run final verification**

Run:

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt -- --check
```

Expected: all commands pass.

---

## Self-Review

### Spec coverage

- CLI split into `hugrs` and `hugrsctl`: covered by Tasks 3 and 4.
- `service/repos/files` command style and default actions: covered by Task 4.
- `/_hugrs` control API and admin token auth: covered by Task 2.
- delete as reference removal, GC as physical reclamation: covered by Task 1.
- `service gc --dry-run` and batched GC with conflict skip semantics: covered by Task 1 and Task 2.
- bilingual doc sync: covered explicitly by Task 5.

### Placeholder scan

- No `TODO` / `TBD` placeholders remain.
- Every task names exact files and concrete commands.

### Type consistency

- Control-plane types are centralized in `src/control.rs`.
- `hugrsctl` client uses those shared types through `src/admin_client.rs`.
- Delete and GC semantics use the same `DeleteResult`, `GcPreview`, and `GcResult` concepts across service, server, and CLI.
