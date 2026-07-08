# Refcount Repair and Batched GC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make file deletion metadata updates transactional, add a `reconcile` admin repair path for chunk reference-count integrity, and convert GC execution into client-driven repeated server-side batches.

**Architecture:** Keep chunk ref-count correctness in `MetadataStore`, expose service/admin wrappers through the existing control-plane stack, and keep GC batch execution server-side while moving the loop into `hugrsctl`. Preserve `dry_run` as the shared maintenance mode term across repair and GC behavior.

**Tech Stack:** Rust, tokio, rusqlite, axum, clap, reqwest, serde, existing integration test harnesses

---

## File Structure

- Modify: `src/metadata.rs`
  - transaction-wrap file deletion lifecycle
  - add chunk ref-count reconciliation helpers
- Modify: `src/service.rs`
  - add service-level `reconcile` entry point
  - change GC execution to one batch per call with configurable batch size and `has_more`
- Modify: `src/control.rs`
  - extend GC request/response types
  - add request/response types for `reconcile`
- Modify: `src/server.rs`
  - add `POST /_hugrs/service/reconcile`
  - update `POST /_hugrs/service/gc`
- Modify: `src/admin_client.rs`
  - add client methods for `reconcile`
  - update GC execute request shape
- Modify: `src/hugrsctl_cli.rs`
  - add `service reconcile`
  - loop `service gc` execute calls with a one-second sleep and aggregate output
- Modify: `tests/metadata_tests.rs`
  - add direct metadata-store integrity tests
- Modify: `tests/service_tests.rs`
  - add GC batch behavior tests
- Modify: `tests/control_tests.rs`
  - add CLI command parsing coverage for `service reconcile`
- Modify: `tests/e2e_tests.rs`
  - add control-plane endpoint coverage for `reconcile` and batched GC

### Task 1: Define Transactional Delete and Repair Semantics in Metadata Tests

**Files:**
- Modify: `tests/metadata_tests.rs`
- Test: `tests/metadata_tests.rs`

- [ ] **Step 1: Write the failing test for ref-count repair in dry-run mode**

Add a metadata test that creates a chunk/file linkage mismatch, runs a new metadata-store repair function in `dry_run`, and proves the database is unchanged.

```rust
#[test]
fn test_reconcile_chunk_refs_dry_run_reports_without_mutating() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let file = store.add_file("f.bin", "repo", 100, "hf").unwrap();
    store.ensure_chunk("sha-a", "local", "sh/a", 100, 100).unwrap();
    store.link_file_chunk(file.id, "sha-a", 0, 100).unwrap();

    {
        let conn = store.raw_conn().unwrap();
        conn.execute(
            "UPDATE chunks SET ref_count = 4 WHERE sha256 = 'sha-a'",
            [],
        )
        .unwrap();
    }

    let result = store.reconcile_chunk_refs(true).unwrap();
    assert_eq!(result.scanned_chunks, 1);
    assert_eq!(result.mismatched_chunks, 1);
    assert_eq!(result.refcount_fixed, 1);

    let chunk = store.get_chunk("sha-a").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 4);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test test_reconcile_chunk_refs_dry_run_reports_without_mutating --test metadata_tests`

Expected: FAIL because `reconcile_chunk_refs(...)` does not exist yet.

- [ ] **Step 3: Write the failing test for repair apply mode**

Add a second test proving apply mode resets `ref_count` to actual file-chunk count and clears orphan state when the chunk is truly referenced.

```rust
#[test]
fn test_reconcile_chunk_refs_apply_repairs_refcount_and_orphan_state() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let file = store.add_file("f.bin", "repo", 100, "hf").unwrap();
    store.ensure_chunk("sha-a", "local", "sh/a", 100, 100).unwrap();
    store.link_file_chunk(file.id, "sha-a", 0, 100).unwrap();
    store.mark_chunk_orphaned("sha-a").unwrap();

    {
        let conn = store.raw_conn().unwrap();
        conn.execute(
            "UPDATE chunks SET ref_count = 4 WHERE sha256 = 'sha-a'",
            [],
        )
        .unwrap();
    }

    let result = store.reconcile_chunk_refs(false).unwrap();
    assert_eq!(result.mismatched_chunks, 1);
    assert_eq!(result.refcount_fixed, 1);
    assert_eq!(result.orphaned_cleared, 1);

    let chunk = store.get_chunk("sha-a").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 1);
    assert_eq!(chunk.orphaned_at, None);
}
```

- [ ] **Step 4: Write the failing test for transactional delete rollback**

Add a test that uses a transaction-aware delete helper and injects a failure through a test-only path or a controlled invalid update so partial delete state is observable if no rollback happens.

```rust
#[test]
fn test_delete_file_transaction_rolls_back_on_failure() {
    // Build this around a dedicated test hook introduced only if needed for metadata tests.
    // The assertion target is:
    // - file row still exists
    // - file_chunks still exist
    // - ref_count is unchanged
    // when the delete transaction aborts.
}
```

If a test hook is needed, keep it minimal and local to metadata tests.

- [ ] **Step 5: Run the metadata test file to verify failures**

Run: `cargo test --test metadata_tests`

Expected: FAIL because the repair function and transactional delete behavior are not implemented yet.

- [ ] **Step 6: Commit the failing metadata tests**

```bash
git add tests/metadata_tests.rs
git commit -m "test: define refcount repair semantics"
```

### Task 2: Implement Transactional Delete and `reconcile` in MetadataStore

**Files:**
- Modify: `src/metadata.rs`
- Test: `tests/metadata_tests.rs`

- [ ] **Step 1: Add response type for metadata repair summary**

Define a local metadata-layer summary struct for reconciliation work.

```rust
#[derive(Debug, Clone, Default)]
pub struct ReconcileChunkRefsResult {
    pub scanned_chunks: usize,
    pub mismatched_chunks: usize,
    pub refcount_fixed: usize,
    pub orphaned_marked: usize,
    pub orphaned_cleared: usize,
}
```

- [ ] **Step 2: Make file deletion transactional**

Refactor `delete_file(...)` and `delete_files_by_repo(...)` so they open a transaction and then call a transaction-based helper.

Target shape:

```rust
fn delete_file_by_id_tx(tx: &rusqlite::Transaction<'_>, file_id: i64) -> anyhow::Result<()> {
    let mut stmt = tx.prepare("SELECT sha256 FROM file_chunks WHERE file_id = ?1")?;
    let chunks: Vec<String> = stmt
        .query_map(params![file_id], |row| row.get::<_, String>(0))?
        .filter_map(Result::ok)
        .collect();

    for sha256 in &chunks {
        tx.execute(
            "UPDATE chunks SET ref_count = ref_count - 1 WHERE sha256 = ?1",
            params![sha256],
        )?;
        tx.execute(
            "UPDATE chunks SET orphaned_at = datetime('now') WHERE sha256 = ?1 AND ref_count = 0",
            params![sha256],
        )?;
    }

    tx.execute("DELETE FROM file_chunks WHERE file_id = ?1", params![file_id])?;
    tx.execute("DELETE FROM files WHERE id = ?1", params![file_id])?;
    Ok(())
}
```

The caller should own `tx.commit()?`.

- [ ] **Step 3: Add the metadata repair function**

Implement a method like:

```rust
pub fn reconcile_chunk_refs(&self, dry_run: bool) -> anyhow::Result<ReconcileChunkRefsResult> {
    let mut conn = self.conn()?;
    let tx = conn.transaction()?;

    let mut result = ReconcileChunkRefsResult::default();
    let mut stmt = tx.prepare(
        "WITH actual AS (
             SELECT sha256, COUNT(*) AS actual_refs
             FROM file_chunks
             GROUP BY sha256
         )
         SELECT c.sha256,
                c.ref_count,
                COALESCE(a.actual_refs, 0) AS actual_refs,
                c.orphaned_at
         FROM chunks c
         LEFT JOIN actual a ON a.sha256 = c.sha256"
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<String>>(3)?,
        ))
    })?;

    for row in rows {
        let (sha256, ref_count, actual_refs, orphaned_at) = row?;
        result.scanned_chunks += 1;
        if ref_count == actual_refs && !((actual_refs > 0 && orphaned_at.is_some()) || (actual_refs == 0 && orphaned_at.is_none())) {
            continue;
        }

        if ref_count != actual_refs {
            result.mismatched_chunks += 1;
            result.refcount_fixed += 1;
        }

        if !dry_run {
            tx.execute(
                "UPDATE chunks SET ref_count = ?2 WHERE sha256 = ?1",
                params![sha256, actual_refs],
            )?;
            if actual_refs > 0 {
                if orphaned_at.is_some() {
                    result.orphaned_cleared += 1;
                }
                tx.execute("UPDATE chunks SET orphaned_at = NULL WHERE sha256 = ?1", params![sha256])?;
            } else if orphaned_at.is_none() {
                result.orphaned_marked += 1;
                tx.execute(
                    "UPDATE chunks SET orphaned_at = datetime('now') WHERE sha256 = ?1",
                    params![sha256],
                )?;
            }
        } else {
            if actual_refs > 0 && orphaned_at.is_some() {
                result.orphaned_cleared += 1;
            }
            if actual_refs == 0 && orphaned_at.is_none() {
                result.orphaned_marked += 1;
            }
        }
    }

    if dry_run {
        tx.rollback()?;
    } else {
        tx.commit()?;
    }
    Ok(result)
}
```

Keep the public method name exactly `reconcile_chunk_refs` to match the requested external naming.

- [ ] **Step 4: Run metadata tests to verify they pass**

Run: `cargo test --test metadata_tests`

Expected: PASS

- [ ] **Step 5: Commit the metadata implementation**

```bash
git add src/metadata.rs tests/metadata_tests.rs
git commit -m "fix: make delete transactional and add refcount reconcile"
```

### Task 3: Expose `reconcile` Through Service, Control Types, and Server

**Files:**
- Modify: `src/service.rs`
- Modify: `src/control.rs`
- Modify: `src/server.rs`
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Write the failing e2e test for the new control endpoint dry-run**

Add a new control-plane test that calls `POST /_hugrs/service/reconcile` with `{"dry_run": true}` and expects the new summary response shape.

```rust
#[tokio::test]
async fn test_control_api_reconcile_dry_run_reports_summary() {
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router("http://127.0.0.1:9", &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/_hugrs/service/reconcile")
        .header("Authorization", "Bearer test-admin-token")
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(r#"{"dry_run":true}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test test_control_api_reconcile_dry_run_reports_summary --test e2e_tests`

Expected: FAIL because the route and types do not exist yet.

- [ ] **Step 3: Add control-plane request and response types**

In `src/control.rs`, add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileRequest {
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileResponse {
    pub scanned_chunks: usize,
    pub mismatched_chunks: usize,
    pub refcount_fixed: usize,
    pub orphaned_marked: usize,
    pub orphaned_cleared: usize,
}
```

Also extend GC types:

```rust
pub struct GcRequest {
    pub dry_run: bool,
    pub batch_size: Option<usize>,
}

pub struct GcResultResponse {
    pub deleted_chunks: usize,
    pub reclaimed_bytes: u64,
    pub skipped_chunks: usize,
    pub has_more: bool,
}
```

- [ ] **Step 4: Add service and server entry points**

In `src/service.rs`, add a wrapper like:

```rust
pub fn reconcile_chunk_refs(
    &self,
    dry_run: bool,
) -> anyhow::Result<crate::metadata::ReconcileChunkRefsResult> {
    self.metadata.reconcile_chunk_refs(dry_run)
}
```

In `src/server.rs`, add:

- route: `POST /_hugrs/service/reconcile`
- handler that checks admin auth and returns `ReconcileResponse`

- [ ] **Step 5: Run the control-plane e2e tests to verify they pass**

Run: `cargo test --test e2e_tests test_control_api_reconcile_dry_run_reports_summary test_control_api_rejects_missing_token`

Expected: PASS

- [ ] **Step 6: Commit the control-plane `reconcile` API**

```bash
git add src/service.rs src/control.rs src/server.rs tests/e2e_tests.rs
git commit -m "feat: add chunk ref reconcile admin api"
```

### Task 4: Convert GC to One Batch Per Request

**Files:**
- Modify: `src/service.rs`
- Modify: `src/server.rs`
- Modify: `src/control.rs`
- Test: `tests/service_tests.rs`
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Write the failing service test for single-batch GC**

Add a service test with more than 32 orphan chunks and assert that one `gc_execute_batch(...)` call deletes only one batch and reports `has_more = true`.

```rust
#[tokio::test]
async fn test_gc_execute_batch_limits_one_server_side_batch() {
    // Seed > 32 orphan chunks using metadata helpers.
    // Assert one call deletes exactly 32 and reports has_more.
}
```

Keep the test concrete in the file, not as prose.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test test_gc_execute_batch_limits_one_server_side_batch --test service_tests`

Expected: FAIL because batch-aware GC response does not exist yet.

- [ ] **Step 3: Change service GC execution API**

In `src/service.rs`, replace or extend:

```rust
pub async fn gc_execute_batch(&self, batch_size: usize) -> anyhow::Result<GcResult> {
    let orphans = self.metadata.list_orphan_chunks_batch(batch_size)?;
    let mut result = GcResult::default();

    for chunk in orphans {
        if chunk.ref_count != 0 {
            result.skipped_chunks += 1;
            continue;
        }
        self.backend.delete(&chunk.sha256).await?;
        let reclaimed = chunk.compressed_size.unwrap_or(chunk.size) as u64;
        if self.metadata.delete_chunk(&chunk.sha256)? {
            result.deleted_chunks += 1;
            result.reclaimed_bytes += reclaimed;
        }
    }

    result.has_more = !self.metadata.list_orphan_chunks_batch(1)?.is_empty();
    Ok(result)
}
```

Update the `GcResult` struct to include `has_more: bool`.

- [ ] **Step 4: Update the server handler to use one batch per request**

In `control_service_gc(...)`:

- `dry_run = true` stays as-is
- `dry_run = false` calls `gc_execute_batch(req.batch_size.unwrap_or(32))`

- [ ] **Step 5: Run targeted GC tests to verify the new semantics pass**

Run: `cargo test --test service_tests test_gc_execute_batch_limits_one_server_side_batch test_gc_dry_run_reports_orphan_candidates test_gc_execute_reclaims_orphan_backend_objects`

Expected: PASS

- [ ] **Step 6: Commit the batched server-side GC implementation**

```bash
git add src/service.rs src/server.rs src/control.rs tests/service_tests.rs
git commit -m "feat: batch gc execution per request"
```

### Task 5: Add `hugrsctl service reconcile` and Looping GC UX

**Files:**
- Modify: `src/admin_client.rs`
- Modify: `src/hugrsctl_cli.rs`
- Modify: `tests/control_tests.rs`
- Test: `tests/control_tests.rs`

- [ ] **Step 1: Write the failing CLI parse test for `service reconcile`**

Add a parser test that proves the new subcommand shape is accepted.

```rust
#[test]
fn test_service_reconcile_command_parses() {
    let cli = Cli::try_parse_from([
        "hugrsctl",
        "service",
        "reconcile",
        "--dry-run",
    ])
    .expect("service reconcile should parse");

    match cli.resource {
        Resource::Service(ServiceArgs {
            command: Some(ServiceCommand::Reconcile { dry_run: true }),
        }) => {}
        other => panic!("unexpected parse result: {:?}", other),
    }
}
```

- [ ] **Step 2: Run the parser test to verify it fails**

Run: `cargo test test_service_reconcile_command_parses --test control_tests`

Expected: FAIL because the subcommand does not exist yet.

- [ ] **Step 3: Extend `AdminClient` and CLI enums**

In `src/admin_client.rs`, add methods:

```rust
pub async fn service_reconcile_dry_run(&self) -> anyhow::Result<ReconcileResponse> { ... }
pub async fn service_reconcile_apply(&self) -> anyhow::Result<ReconcileResponse> { ... }
pub async fn service_gc_execute_batch(&self, batch_size: Option<usize>) -> anyhow::Result<GcResultResponse> { ... }
```

In `src/hugrsctl_cli.rs`, add:

```rust
pub enum ServiceCommand {
    Status,
    Stats,
    Gc { dry_run: bool },
    Reconcile { dry_run: bool },
}
```

- [ ] **Step 4: Implement looping GC execute mode with one-second pauses**

In `src/hugrsctl_cli.rs`, change execute mode from one request to a loop:

```rust
let mut total = GcResultResponse {
    deleted_chunks: 0,
    reclaimed_bytes: 0,
    skipped_chunks: 0,
    has_more: false,
};
let mut batch = 1usize;

loop {
    let value = client.service_gc_execute_batch(None).await?;
    if !cli.json {
        println!(
            "batch {}: deleted {} chunks, reclaimed {}, skipped {}",
            batch,
            value.deleted_chunks,
            format_bytes(value.reclaimed_bytes),
            value.skipped_chunks,
        );
    }
    total.deleted_chunks += value.deleted_chunks;
    total.reclaimed_bytes += value.reclaimed_bytes;
    total.skipped_chunks += value.skipped_chunks;
    total.has_more = value.has_more;
    if !value.has_more {
        break;
    }
    batch += 1;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
```

Then print the final aggregate summary.

- [ ] **Step 5: Implement `service reconcile` execution path**

Map:

- `--dry-run` -> dry-run admin call
- default execute mode -> apply admin call

Use the same human-readable vs JSON output conventions as other service commands.

- [ ] **Step 6: Run CLI tests to verify parsing and output wiring**

Run: `cargo test --test control_tests`

Expected: PASS

- [ ] **Step 7: Commit the CLI and admin-client behavior**

```bash
git add src/admin_client.rs src/hugrsctl_cli.rs tests/control_tests.rs
git commit -m "feat: add service reconcile and looping gc client"
```

### Task 6: Full Verification

**Files:**
- Modify: `src/metadata.rs`
- Modify: `src/service.rs`
- Modify: `src/control.rs`
- Modify: `src/server.rs`
- Modify: `src/admin_client.rs`
- Modify: `src/hugrsctl_cli.rs`
- Modify: `tests/metadata_tests.rs`
- Modify: `tests/service_tests.rs`
- Modify: `tests/control_tests.rs`
- Modify: `tests/e2e_tests.rs`

- [ ] **Step 1: Run formatting check**

Run: `cargo fmt -- --check`

Expected: PASS

- [ ] **Step 2: Run clippy across all features**

Run: `cargo clippy --all-features`

Expected: PASS

- [ ] **Step 3: Run full test suite**

Run: `cargo test`

Expected: PASS

- [ ] **Step 4: Inspect final diff for scope control**

Run: `git diff -- src/metadata.rs src/service.rs src/control.rs src/server.rs src/admin_client.rs src/hugrsctl_cli.rs tests/metadata_tests.rs tests/service_tests.rs tests/control_tests.rs tests/e2e_tests.rs`

Expected: diff is limited to transactional delete, `reconcile`, and batched GC changes

- [ ] **Step 5: Commit the verified implementation**

```bash
git add src/metadata.rs src/service.rs src/control.rs src/server.rs src/admin_client.rs src/hugrsctl_cli.rs tests/metadata_tests.rs tests/service_tests.rs tests/control_tests.rs tests/e2e_tests.rs
git commit -m "feat: add reconcile repair and batched gc"
```
