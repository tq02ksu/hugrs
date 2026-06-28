# Stats API Field Renaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `/api/stats` payload and CLI stats output with storage-efficiency fields: `original_bytes`, `stored_bytes`, `bytes_saved`, and `saved_percent`, while keeping traffic counters `fetched_bytes` and `served_bytes`.

**Architecture:** Keep the stats computation in `MetadataStore::get_stats()` and derive the new fields there so every caller sees the same schema. Update the HTTP handler, CLI output, and OpenAPI schema to match the renamed fields, and adjust existing tests to assert the new contract.

**Tech Stack:** Rust (stable), rusqlite, axum, serde, clap, OpenAPI YAML

## Global Constraints

- Transparent cache: upstream responses are forwarded as-is. Do NOT modify content-type, headers, or response body.
- Redirect transparency: 302 responses are followed internally. Headers from the 302 and final 200 are merged. The client always receives 200 with the combined metadata.
- Metadata first: HEAD requests cache file metadata in SQLite without downloading content.
- No guessing: never invent content-type, filenames, or other response metadata.
- Partial downloads resume: interrupted GET downloads restart from the last completed trunk.
- Immutable trunks: trunk data is keyed by SHA256 and never modified.

---

### Task 1: Rename the stats model and computation

**Files:**
- Modify: `src/metadata.rs`
- Modify: `src/service.rs`
- Modify: `tests/metadata_tests.rs`
- Modify: `tests/service_tests.rs`

**Interfaces:**
- Consumes: `MetadataStore::get_stats()`, `CacheService::stats()`
- Produces: `Stats { repo_count, file_count, chunk_count, original_bytes, stored_bytes, bytes_saved, saved_percent, fetched_bytes, served_bytes }`

- [ ] **Step 1: Replace the `Stats` fields**

In `src/metadata.rs`, update `Stats` to:

```rust
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Stats {
    pub repo_count: i64,
    pub file_count: i64,
    pub chunk_count: i64,
    pub original_bytes: i64,
    pub stored_bytes: i64,
    pub bytes_saved: i64,
    pub saved_percent: f64,
    pub fetched_bytes: u64,
    pub served_bytes: u64,
}
```

- [ ] **Step 2: Update `get_stats()` to compute the new fields**

Replace the current `total_size`, `unique_size`, and `compression_ratio` calculation with:

```rust
        let original_bytes: i64 = conn.query_row(
            "SELECT COALESCE(SUM(total_size), 0) FROM files",
            [],
            |row| row.get(0),
        )?;
        let stored_bytes: i64 = conn.query_row(
            "SELECT COALESCE(SUM(COALESCE(compressed_size, size)), 0) FROM chunks",
            [],
            |row| row.get(0),
        )?;
        let bytes_saved = original_bytes.saturating_sub(stored_bytes);
        let saved_percent = if original_bytes > 0 {
            bytes_saved as f64 * 100.0 / original_bytes as f64
        } else {
            0.0
        };
        Ok(Stats {
            repo_count,
            file_count,
            chunk_count,
            original_bytes,
            stored_bytes,
            bytes_saved,
            saved_percent,
            fetched_bytes: 0,
            served_bytes: 0,
        })
```

- [ ] **Step 3: Update `CacheService::stats()` consumers**

In `src/service.rs`, replace the remaining `stats.total_size` check inside `evict_if_needed()` with `stats.original_bytes`:

```rust
            if stats.original_bytes as u64 <= max_size {
```

and:

```rust
                stats.original_bytes
```

- [ ] **Step 4: Update metadata stats tests**

In `tests/metadata_tests.rs`, change assertions to use the new field names and values. For the existing stats test, assert:

```rust
    assert_eq!(stats.original_bytes, 1024);
    assert_eq!(stats.stored_bytes, 512);
    assert_eq!(stats.bytes_saved, 512);
    assert_eq!(stats.saved_percent, 50.0);
```

Use the same repository/file/chunk setup already in the test so the new values stay deterministic.

- [ ] **Step 5: Update service stats tests**

In `tests/service_tests.rs`, change any stats assertions to the renamed fields and expected saved values. If the test only checks totals, update it to assert `original_bytes` instead of `total_size`.

---

### Task 2: Update the CLI and HTTP API output

**Files:**
- Modify: `src/main.rs`
- Modify: `src/server.rs`

**Interfaces:**
- Consumes: `Stats` from Task 1
- Produces: CLI text output and `/api/stats` JSON with the new field names

- [ ] **Step 1: Update CLI stats output**

Replace the `Command::Stats` output block in `src/main.rs` with:

```rust
            Command::Stats => {
                let stats = service.stats().await?;
                println!("Repos:             {}", stats.repo_count);
                println!("Files:             {}", stats.file_count);
                println!("Chunks:            {}", stats.chunk_count);
                println!("Original bytes:    {} bytes", stats.original_bytes);
                println!("Stored bytes:      {} bytes", stats.stored_bytes);
                println!("Bytes saved:       {} bytes", stats.bytes_saved);
                println!("Saved percent:     {:.2}%", stats.saved_percent);
                println!("Fetched (upstream): {}", format_bytes(stats.fetched_bytes));
                println!("Served (client):    {}", format_bytes(stats.served_bytes));
                if let Some(limit) = config.storage.max_size {
                    let pct = (stats.original_bytes as u64 * 100)
                        .checked_div(limit)
                        .unwrap_or(0);
                    println!("Max size:          {} bytes ({}% used)", limit, pct);
                }
            }
```

- [ ] **Step 2: Keep `/api/stats` returning the renamed `Stats` struct**

In `src/server.rs`, the handler can stay structurally the same, but its JSON payload should now serialize the new `Stats` fields automatically:

```rust
async fn stats(State(state): State<AppState>) -> Result<Json<crate::metadata::Stats>, AppError> {
    let service = state.service.lock().await;
    let stats = service.stats().await.map_err(AppError::Anyhow)?;
    Ok(Json(stats))
}
```

No additional headers or body changes are needed.

---

### Task 3: Sync OpenAPI and docs with the new contract

**Files:**
- Modify: `openapi.yaml`
- Modify: `README.md`
- Modify: `README_zh.md`

**Interfaces:**
- Consumes: renamed stats schema from Task 1
- Produces: updated API docs and CLI docs

- [ ] **Step 1: Update the OpenAPI `Stats` schema**

Replace the `Stats` properties block in `openapi.yaml` with:

```yaml
    Stats:
      type: object
      properties:
        repo_count:
          type: integer
          format: int64
        file_count:
          type: integer
          format: int64
        chunk_count:
          type: integer
          format: int64
        original_bytes:
          type: integer
          format: int64
        stored_bytes:
          type: integer
          format: int64
        bytes_saved:
          type: integer
          format: int64
        saved_percent:
          type: number
          format: double
        fetched_bytes:
          type: integer
          format: int64
        served_bytes:
          type: integer
          format: int64
```

- [ ] **Step 2: Update the README stat example if present**

If the README mentions stats field names or sample output, switch it to the new names so it matches the API and CLI.

- [ ] **Step 3: Update the Chinese README the same way**

Mirror the same stat-field wording in `README_zh.md`.

- [ ] **Step 4: Verify the contract with tests and build**

Run:

```bash
cargo test
cargo build
```

Expected: both commands succeed.

- [ ] **Step 5: Commit**

```bash
git add src/metadata.rs src/service.rs src/main.rs src/server.rs tests/metadata_tests.rs tests/service_tests.rs openapi.yaml README.md README_zh.md docs/superpowers/plans/2026-06-28-stats-api-field-renaming.md
git commit -m "feat: rename stats fields for storage efficiency"
```
