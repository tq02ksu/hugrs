# ETag Validation and If-None-Match Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ETag-based cache validation on every request, support If-None-Match (304), fix small-file header storage, delete `upload()`, and add transitional startup backfill for historical NULL metadata.

**Architecture:** Remove `upload()` and inline chunk+store logic into callers with explicit `set_file_headers()` after storage. Insert ETag validation step in `file_proxy_inner()` before serving cached files. Add If-None-Match conditional logic in GET path.

**Tech Stack:** Rust/tokio/axum/rusqlite

---

### Task 1: Inline chunk-store logic in `stream_small_file()`, remove `upload()` call

**Files:**
- Modify: `src/service.rs:1037-1086` (`stream_small_file()`)
- Modify: `src/service.rs:134-194` (delete `upload()`)

- [ ] **Step 1: Replace `upload()` call in `stream_small_file()` with inline chunk+store**

Replace the `svc.upload(...)` call at line 1080 with inline logic that chunks the data, stores chunks, and calls `set_file_headers()`:

```rust
// In stream_small_file(), replace lines 1080-1083:
// OLD:
// if let Err(e) = svc.upload(&fname, &frepo, &source, data.to_vec()).await {
//     let _ = tx.send(Err(e)).await;
//     return;
// }

// NEW:
let chunker::Chunked {
    chunks,
    sha256: _,
} = chunker::chunk_with_hashes(&data, CHUNK_SIZE);
for chunk in &chunks {
    if !svc.backend.exists(&chunk.sha256).await.unwrap_or(false) {
        if let Err(e) = svc.backend.put(&chunk.sha256, &chunk.data).await {
            let _ = tx.send(Err(anyhow::anyhow!("chunk store error: {}", e))).await;
            return;
        }
    }
    let path = svc.chunk_path(&chunk.sha256);
    if let Err(e) = svc.metadata.ensure_chunk(
        &chunk.sha256, "local", &path,
        chunk.chunk_size as i64, chunk.chunk_size as i64,
    ) {
        let _ = tx.send(Err(e)).await;
        return;
    }
    if let Err(e) = svc.metadata.link_file_chunk(
        file.id,
        &chunk.sha256,
        chunk.chunk_index as i64,
        chunk.chunk_size as i64,
    ) {
        let _ = tx.send(Err(e)).await;
        return;
    }
}
// Note: headers already stored by fetch_file_metadata() before stream_small_file() is called,
// so no set_file_headers() needed here unless the upload() delete+recreate cycle lost them.
// But upload() preserved existing_headers, so the inline path keeps the original file row
// from fetch_file_metadata() — no delete+recreate, headers intact.
```

- [ ] **Step 2: Run tests to verify**

```bash
cargo test --lib service
```

Expected: tests pass (no compile errors from removed `upload()` usage in service)

- [ ] **Step 3: Commit**

```bash
git add src/service.rs
git commit -m "refactor: inline chunk storage in stream_small_file(), remove upload() call"
```

---

### Task 2: Inline chunk-store logic in `download_from_url()` small-file path, remove `upload()` call

**Files:**
- Modify: `src/service.rs:267-293` (`download_from_url()` small-file path)

- [ ] **Step 1: Replace `upload()` call with inline logic in `download_from_url()` small-file path**

Replace lines 282-292:

```rust
// OLD (lines 282-292):
// self.metadata.delete_file(name, source)?;
// self.upload(name, repo, source, data.to_vec()).await?;
// self.metadata.set_file_headers(
//     name, source,
//     etag.as_deref(), x_repo_commit.as_deref(),
//     x_linked_size, x_linked_etag.as_deref(),
//     content_type.as_deref(),
// )?;

// NEW:
let file_id = match self.metadata.get_file_by_name(name, source)? {
    Some(existing) => {
        if existing.total_size as u64 != total_size {
            self.metadata.delete_file(name, source)?;
            self.metadata.add_file(name, repo, total_size as i64, source)?.id
        } else {
            existing.id
        }
    }
    None => self.metadata.add_file(name, repo, total_size as i64, source)?.id,
};

let chunker::Chunked { chunks, .. } = chunker::chunk_with_hashes(&data, CHUNK_SIZE);
for chunk in &chunks {
    if !self.backend.exists(&chunk.sha256).await? {
        self.backend.put(&chunk.sha256, &chunk.data).await?;
    }
    let path = self.chunk_path(&chunk.sha256);
    self.metadata.ensure_chunk(
        &chunk.sha256, "local", &path,
        chunk.chunk_size as i64, chunk.chunk_size as i64,
    )?;
    self.metadata.link_file_chunk(
        file_id, &chunk.sha256,
        chunk.chunk_index as i64, chunk.chunk_size as i64,
    )?;
}

self.metadata.set_file_headers(
    name, source,
    etag.as_deref(), x_repo_commit.as_deref(),
    x_linked_size, x_linked_etag.as_deref(),
    content_type.as_deref(),
)?;
```

- [ ] **Step 2: Run tests**

```bash
cargo test --lib service
```

- [ ] **Step 3: Commit**

```bash
git add src/service.rs
git commit -m "refactor: inline chunk storage in download_from_url() small-file path, remove upload() call"
```

---

### Task 3: Delete `upload()` function

**Files:**
- Modify: `src/service.rs:134-194` (remove `upload()` method)
- Modify: `src/service.rs:1075-1085` (remove `self_test()` call to `upload()`)

- [ ] **Step 1: Remove `upload()` method**

Delete lines 134-194 (the entire `upload()` method including its doc comments if any).

- [ ] **Step 2: Remove `self_test()` call to `upload()`**

The `self_test()` method at ~line 1075 calls `svc.upload(...)`. If `self_test()` exists, replace or remove the call:

```rust
// Check if self_test() still compiles — if it called upload(), rewrite to use
// the same inline chunk+store pattern or remove the self_test() entirely.
```

Search for `self_test`:

```bash
rg "fn self_test" src/
```

If `self_test` is dead code, remove it. If used, rewrite to use inline logic.

- [ ] **Step 3: Check for any remaining references to `upload`**

```bash
rg "\.upload\(" src/
```

Should show zero results.

- [ ] **Step 4: Build check**

```bash
cargo build 2>&1
```

- [ ] **Step 5: Commit**

```bash
git add src/service.rs
git commit -m "refactor: delete upload() function"
```

---

### Task 4: Update test files to not use `upload()`

**Files:**
- Modify: `tests/service_tests.rs` — all `.upload()` calls
- Modify: `tests/e2e_tests.rs` — all `.upload()` calls
- Modify: `tests/streaming_tests.rs` — if any `.upload()` calls

- [ ] **Step 1: Replace test `.upload()` calls with direct download or inline chunk+store**

For tests that used `upload()` to seed cache data before testing other behavior, replace with a test helper that:
1. Calls `metadata.add_file()`
2. Chunks data via `chunker::chunk_with_hashes()`
3. Stores chunks via `backend.put()`, `metadata.ensure_chunk()`, `metadata.link_file_chunk()`
4. Calls `metadata.set_file_headers()` with test values like `Some("\"test-etag\"")`, `Some("application/octet-stream")`

Add a helper to `tests/service_tests.rs`:

```rust
async fn seed_file(
    svc: &CacheService,
    name: &str,
    repo: &str,
    source: &str,
    data: &[u8],
    etag: Option<&str>,
    content_type: Option<&str>,
) {
    svc.metadata.add_file(name, repo, data.len() as i64, source).unwrap();
    let file = svc.metadata.get_file_by_name(name, source).unwrap().unwrap();
    let chunks = chunker::chunk_with_hashes(data, CHUNK_SIZE);
    for chunk in &chunks.chunks {
        svc.backend.put(&chunk.sha256, &chunk.data).await.unwrap();
        let path = svc.chunk_path(&chunk.sha256);
        svc.metadata.ensure_chunk(&chunk.sha256, "local", &path, chunk.chunk_size as i64, chunk.chunk_size as i64).unwrap();
        svc.metadata.link_file_chunk(file.id, &chunk.sha256, chunk.chunk_index as i64, chunk.chunk_size as i64).unwrap();
    }
    svc.metadata.set_file_headers(name, source, etag, None, None, None, content_type).unwrap();
}
```

Update all call sites. Each `.upload()` call in tests should be replaced with `seed_file(&svc, ...)`.

- [ ] **Step 2: Run tests**

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/
git commit -m "test: replace upload() calls with seed_file() helper"
```

---

### Task 5: Add ETag validation in GET cache-hit path

**Files:**
- Modify: `src/service.rs` — add `validate_file_etag()` method
- Modify: `src/server.rs:596-657` — insert validation in `file_proxy_inner()` GET path
- Modify: `src/config.rs` — add `etag_validation_timeout_secs`

- [ ] **Step 1: Add config field**

In `src/config.rs`, find `StorageConfig` struct and add after `verify_sha256`:

```rust
/// ETag validation HEAD request timeout in seconds. 0 disables validation.
#[serde(default = "default_etag_validation_timeout")]
pub etag_validation_timeout_secs: u64,
```

Add the default function below the struct:
```rust
fn default_etag_validation_timeout() -> u64 {
    5
}
```

Also add to `StoragePatch` struct (~line 295), after `verify_sha256`:
```rust
#[serde(skip_serializing_if = "Option::is_none")]
pub etag_validation_timeout_secs: Option<u64>,
```

Also add to `DaemonCli` struct in `src/daemon_cli.rs` (after `enable_sha256_verify`):
```rust
#[arg(long)]
pub etag_validation_timeout_secs: Option<u64>,
```

Also add the mapping in `DaemonCli::to_patch()`:
```rust
etag_validation_timeout_secs: self.etag_validation_timeout_secs,
```

In the `Default` impl for the config struct, add:
```rust
etag_validation_timeout_secs: 5,
```

- [ ] **Step 2: Add `validate_file_etag()` to `CacheService`**

In `src/service.rs`, find the `CacheService` struct. Add field:
```rust
etag_validation_timeout: u64,
```

Update `CacheService::new()` signature — add `etag_validation_timeout: u64` as the last parameter:

```rust
// In CacheService::new():
pub fn new(
    metadata: Arc<MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    max_size: Option<u64>,
    http_client: reqwest::Client,
    head_client: reqwest::Client,
    prefetch_depth: usize,
    prefetch_budget_base: usize,
    verify_sha256: bool,
    stream_client: reqwest::Client,
    etag_validation_timeout: u64,  // NEW
) -> Self {
    Self {
        // ... existing fields ...
        etag_validation_timeout,  // NEW
    }
}
```

In `src/main.rs:80-90`, pass the config value:
```rust
let service = CacheService::new(
    metadata, backend, config.storage.max_size,
    http_client, head_client,
    config.storage.prefetch_depth, config.storage.prefetch_budget_base,
    config.storage.verify_sha256, stream_client,
    config.storage.etag_validation_timeout_secs,  // NEW
);
```

Add method below `fetch_file_metadata()`:

```rust
/// Fetch upstream headers and compare ETag with cached value.
/// Returns: Ok(true) if etag matches or upstream has no etag,
///          Ok(false) if etag changed,
///          Err if network unreachable or timed out.
pub async fn validate_file_etag(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    source: &str,
    user_agent: Option<&str>,
    cached_etag: &str,
) -> anyhow::Result<bool> {
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(self.etag_validation_timeout),
        self.fetch_file_metadata(url, name, repo, source, user_agent),
    )
    .await
    .map_err(|_| anyhow::anyhow!("etag validation timed out"))?;

    let (_size, upstream_etag, _ct, _commit, _xl_size, _xl_etag) = result?;
    match upstream_etag {
        Some(ref ue) => Ok(ue == cached_etag),
        None => Ok(true), // upstream has no etag (MS small file), assume valid
    }
}
```

- [ ] **Step 3: Insert ETag validation in `file_proxy_inner()` GET cache-hit path**

In `src/server.rs`, find the GET cache-hit block (starts at line ~596). Replace the block from `let service = state.service.lock().await;` through the `stream_cached_file` call with:

```rust
// ── GET: ETag validation + cache serve ──
let get_start = std::time::Instant::now();

// Determine if etag check is needed
let (needs_etag_check, cached_etag) = {
    let service = state.service.lock().await;
    if service.is_file_complete(&cache_name, source).await.unwrap_or(false) {
        if let Ok(Some(file)) = service.info(&cache_name, source).await {
            let stale = range.map(|r| r.0).unwrap_or(0) >= file.total_size as u64;
            if !stale {
                (file.etag.clone(), file.etag.clone())
            } else {
                (None, None)
            }
        } else {
            (None, None)
        }
    } else {
        (None, None)
    }
};

if let Some(ref etag) = cached_etag {
    // Release lock before network I/O
    let etag = etag.clone();
    let service = state.service.lock().await;
    let result = service.validate_file_etag(
        &url, &cache_name, &repo_id, source,
        user_agent.as_deref(), &etag,
    ).await;
    drop(service);

    match result {
        Ok(true) => {
            tracing::debug!("GET etag validated: {}", cache_name);
        }
        Ok(false) => {
            tracing::info!("GET etag changed, invalidating: {}", cache_name);
            let service = state.service.lock().await;
            service.metadata.delete_file(&cache_name, source)?;
            drop(service);
            // Fall through to stream_from_upstream
        }
        Err(e) => {
            tracing::warn!("GET etag validation failed ({}), serving degraded: {}", e, cache_name);
        }
    }
}

// Serve from cache (or stream from upstream if invalidated)
{
    let service = state.service.lock().await;
    if service.is_file_complete(&cache_name, source).await.unwrap_or(false) {
        if let Ok(Some(file)) = service.info(&cache_name, source).await {
            let stale = range.map(|r| r.0).unwrap_or(0) >= file.total_size as u64;
            if !stale {
                let (file, content_length, stream) = service
                    .stream_cached_file(&cache_name, source, range.map(|r| r.0), range.and_then(|r| r.1))
                    .await?;
                tracing::info!("{}: cache hit, stream ready in {}ms", cache_name, get_start.elapsed().as_millis());
                return build_stream_response(file, content_length, stream, &path, range);
            }
        }
    }
}

// Cache miss or invalidated — stream from upstream
tracing::info!("cache miss, streaming via upstream: {}", cache_name);
let service = state.service.lock().await;
let (file, content_length, stream) = service
    .stream_from_upstream(&url, &cache_name, &repo_id, source, range.map(|r| r.0), range.and_then(|r| r.1), user_agent.as_deref())
    .await?;
drop(service);

tracing::info!("{}: cache miss session ready, {}GB, stream in {}ms", cache_name, file.total_size as f64 / 1_073_741_824.0, get_start.elapsed().as_millis());
build_stream_response(file, content_length, stream, &path, range)
```

- [ ] **Step 4: Run tests**

```bash
cargo test
cargo clippy -- -D warnings
```

- [ ] **Step 5: Commit**

```bash
git add src/service.rs src/server.rs src/config.rs src/main.rs
git commit -m "feat: add ETag validation on GET cache hits"
```

---

### Task 6: Add ETag validation in HEAD cache-hit path

**Files:**
- Modify: `src/server.rs:455-466` (HEAD cache-hit path)

- [ ] **Step 1: Insert ETag validation before returning cached HEAD response**

In `src/server.rs`, find the HEAD cache-hit path at lines 455-466. Replace:

```rust
// OLD (lines 456-466):
// if let Ok(Some(file)) = service.info(&cache_name, source).await {
//     if file.x_repo_commit.is_some() {
//         tracing::debug!("HEAD cache hit (metadata): {}", cache_name);
//         return build_head_response(&file, &path);
//     }
//     ...
// }
```

With:

```rust
if let Ok(Some(file)) = service.info(&cache_name, source).await {
    if file.x_repo_commit.is_some() {
        // ETag validation before serving cached HEAD
        let should_serve_cached = if let Some(ref cached_etag) = file.etag {
            let cached_etag = cached_etag.clone();
            drop(service);
            let service = state.service.lock().await;
            match service.validate_file_etag(
                &url, &cache_name, &repo_id, source,
                user_agent.as_deref(), &cached_etag,
            ).await {
                Ok(true) => true,   // etag match
                Ok(false) => false, // etag changed
                Err(e) => {
                    tracing::warn!("HEAD etag validation failed ({}), serving degraded: {}", e, cache_name);
                    true // degraded, serve cached
                }
            }
        } else {
            true // no etag to validate
        };

        if should_serve_cached {
            let service = state.service.lock().await;
            if let Ok(Some(file)) = service.info(&cache_name, source).await {
                tracing::debug!("HEAD cache hit (metadata): {}", cache_name);
                return build_head_response(&file, &path);
            }
        } else {
            // ETag changed: invalidate and re-fetch from upstream
            let service = state.service.lock().await;
            service.metadata.delete_file(&cache_name, source)?;
            drop(service);
            // Fall through to upstream HEAD fetch below
        }
    }
    // ... existing missing x_repo_commit handling ...
}
drop(service);
// ... existing upstream HEAD fetch ...
```

- [ ] **Step 2: Run tests**

```bash
cargo test
```

- [ ] **Step 3: Commit**

```bash
git add src/server.rs
git commit -m "feat: add ETag validation on HEAD cache hits"
```

---

### Task 7: Add If-None-Match support (304 Not Modified)

**Files:**
- Modify: `src/server.rs` — in `file_proxy_inner()` GET path, add `build_304_response()` and `etag_matches_any()`

- [ ] **Step 1: Add `build_304_response()` and `etag_matches_any()` helpers**

Add to `src/server.rs`, before `file_proxy_inner()`:

```rust
fn etag_matches_any(cached_etag: &str, if_none_match: &str) -> bool {
    let cached_stripped = cached_etag
        .trim_start_matches("W/")
        .trim_matches('"');
    if_none_match
        .split(',')
        .map(|s| s.trim().trim_start_matches("W/").trim_matches('"'))
        .filter(|s| !s.is_empty())
        .any(|e| e == cached_stripped)
}

fn build_304_response(file: &crate::metadata::File) -> Result<Response, AppError> {
    let mut resp = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header("Content-Length", file.total_size)
        .header("Accept-Ranges", "bytes");
    if let Some(ref etag) = file.etag {
        resp = resp.header("ETag", etag);
    }
    if let Some(ref ct) = file.content_type {
        resp = resp.header("Content-Type", ct);
    }
    resp.body(axum::body::Body::empty())
        .map_err(|e| AppError::Anyhow(e.into()))
}
```

- [ ] **Step 2: Parse `If-None-Match` and add 304 check after ETag validation**

At the top of `file_proxy_inner()`, after extracting `range`, add:

```rust
let if_none_match = headers
    .get("if-none-match")
    .and_then(|v| v.to_str().ok())
    .map(|s| s.to_string());
```

In the GET cache-hit path, after successful ETag validation (Task 5's code), before `stream_cached_file()`, insert:

```rust
// After etag validation succeeded, check If-None-Match
if let Some(ref inm) = if_none_match {
    if let Some(ref etag) = cached_etag {
        if etag_matches_any(etag, inm) {
            let service = state.service.lock().await;
            if let Ok(Some(file)) = service.info(&cache_name, source).await {
                tracing::debug!("If-None-Match hit, returning 304: {}", cache_name);
                return build_304_response(&file);
            }
        }
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test
cargo clippy -- -D warnings
```

- [ ] **Step 4: Commit**

```bash
git add src/server.rs
git commit -m "feat: add If-None-Match support (304 Not Modified)"
```

---

### Task 8: Add startup backfill for historical NULL etag/content_type (TRANSITIONAL)

**Files:**
- Modify: `src/metadata.rs` — add `list_files_with_missing_headers()`
- Modify: `src/service.rs` — add `backfill_missing_headers()`
- Modify: `src/main.rs` — call backfill at startup

- [ ] **Step 1: Add `list_files_with_missing_headers()` to metadata**

```rust
// TRANSITIONAL: remove in v0.X.0 ──────────────────────────
/// List files where etag or content_type is NULL.
pub fn list_files_with_missing_headers(&self) -> anyhow::Result<Vec<File>> {
    let conn = self.conn.lock().unwrap();
    let mut stmt = conn.prepare(
        "SELECT id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type
         FROM files
         WHERE etag IS NULL OR content_type IS NULL",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(File {
            id: row.get(0)?,
            name: row.get(1)?,
            repo: row.get(2)?,
            total_size: row.get(3)?,
            created_at: row.get(4)?,
            last_accessed: row.get(5)?,
            source: row.get(6)?,
            etag: row.get(7)?,
            x_repo_commit: row.get(8)?,
            x_linked_size: row.get(9)?,
            x_linked_etag: row.get(10)?,
            content_type: row.get(11)?,
        })
    })?;
    let mut files = Vec::new();
    for row in rows {
        files.push(row?);
    }
    Ok(files)
}
// TRANSITIONAL: end ───────────────────────────────────────
```

- [ ] **Step 2: Add `backfill_missing_headers()` to `CacheService`**

```rust
// TRANSITIONAL: remove in v0.X.0 ──────────────────────────
/// One-time fix: fill in NULL etag/content_type from upstream.
/// Runs at startup. Network failures are logged and skipped.
pub async fn backfill_missing_headers(
    &self,
    hf_endpoint: &str,
    ms_endpoint: &str,
) -> anyhow::Result<usize> {
    let files = self.metadata.list_files_with_missing_headers()?;
    if files.is_empty() {
        return Ok(0);
    }
    tracing::info!("Backfilling {} files with missing headers", files.len());
    
    let mut fixed = 0usize;
    for file in &files {
        let commit = match &file.x_repo_commit {
            Some(c) => c,
            None => {
                tracing::warn!("Skipping {} (no x_repo_commit to construct URL)", file.name);
                continue;
            }
        };
        
        // Reconstruct upstream URL
        let endpoint = match file.source.as_str() {
            "ms" => ms_endpoint,
            _ => hf_endpoint,
        };
        let url = reconstruct_url(&file, commit, endpoint);
        
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.fetch_file_metadata(&url, &file.name, &file.repo, &file.source, None),
        ).await;
        
        match result {
            Ok(Ok(_)) => {
                tracing::info!("Backfilled headers for {}", file.name);
                fixed += 1;
            }
            Ok(Err(e)) => {
                tracing::warn!("Backfill failed for {}: {}", file.name, e);
            }
            Err(_) => {
                tracing::warn!("Backfill timed out for {}", file.name);
            }
        }
    }
    tracing::info!("Backfill complete: {}/{} files fixed", fixed, files.len());
    Ok(fixed)
}

fn reconstruct_url(file: &File, commit: &str, endpoint: &str) -> String {
    // file.name = "org/repo/path/to/file"
    let (repo, filepath) = match file.name.find('/') {
        Some(pos) => {
            let rest = &file.name[pos + 1..];
            match rest.find('/') {
                Some(pos2) => (&file.name[..pos + 1 + pos2], &rest[pos2 + 1..]),
                None => (file.name.as_str(), ""),
            }
        }
        None => (file.name.as_str(), ""),
    };
    
    match file.source.as_str() {
        "ms" => format!(
            "{}/api/v1/models/{}/repo?Revision={}&FilePath={}",
            endpoint, repo, commit, filepath
        ),
        _ => format!(
            "{}/{}/resolve/{}/{}",
            endpoint, repo, commit, filepath
        ),
    }
}
// TRANSITIONAL: end ───────────────────────────────────────
```

- [ ] **Step 3: Call backfill at startup in `main.rs`**

```rust
// After service init, before axum::serve()
// TRANSITIONAL: remove in v0.X.0 ──────────────────────────
let config = config.clone();
let backfill_svc = service.clone();
tokio::spawn(async move {
    if let Err(e) = backfill_svc.lock().await.backfill_missing_headers(
        &config.huggingface.endpoint,
        &config.modelscope.endpoint,
    ).await {
        tracing::warn!("Header backfill failed: {}", e);
    }
});
// TRANSITIONAL: end ───────────────────────────────────────
```

- [ ] **Step 4: Run tests**

```bash
cargo test
cargo clippy -- -D warnings
```

- [ ] **Step 5: Commit**

```bash
git add src/metadata.rs src/service.rs src/main.rs
git commit -m "feat: add transitional startup backfill for NULL etag/content_type"
```

---

### Task 9: Add tests for ETag validation and If-None-Match

**Files:**
- Create: `tests/etag_tests.rs`

Follow the existing test pattern from `tests/streaming_tests.rs` — use `axum` for mock upstream servers and `tempfile` for temp directories.

- [ ] **Step 1: Write `tests/etag_tests.rs`**

```rust
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, head},
    Router,
};
use hugrs::metadata::MetadataStore;
use hugrs::service::{CacheService, CHUNK_SIZE};
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Clone)]
struct EtagMockState {
    head_count: Arc<AtomicU32>,
    etag: Arc<std::sync::Mutex<String>>,
    test_data: Arc<Vec<u8>>,
}

async fn mock_head(state: State<EtagMockState>) -> Response {
    state.head_count.fetch_add(1, Ordering::SeqCst);
    let etag = state.etag.lock().unwrap().clone();
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", state.test_data.len())
        .header("ETag", &etag)
        .header("Content-Type", "application/octet-stream")
        .header("X-Repo-Commit", "abc123")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn mock_get(
    State(state): State<EtagMockState>,
) -> Vec<u8> {
    state.test_data.to_vec()
}

async fn seed_file(
    svc: &CacheService,
    name: &str,
    repo: &str,
    source: &str,
    data: &[u8],
    etag: &str,
) {
    svc.metadata.delete_file(name, source).ok();
    svc.metadata.add_file(name, repo, data.len() as i64, source).unwrap();
    let file = svc.metadata.get_file_by_name(name, source).unwrap().unwrap();
    let chunks = hugrs::chunker::chunk_with_hashes(data, CHUNK_SIZE);
    for chunk in &chunks.chunks {
        svc.backend.put(&chunk.sha256, &chunk.data).await.unwrap();
        let path = svc.chunk_path(&chunk.sha256);
        svc.metadata
            .ensure_chunk(&chunk.sha256, "local", &path, chunk.chunk_size as i64, chunk.chunk_size as i64)
            .unwrap();
        svc.metadata
            .link_file_chunk(file.id, &chunk.sha256, chunk.chunk_index as i64, chunk.chunk_size as i64)
            .unwrap();
    }
    svc.metadata
        .set_file_headers(name, source, Some(etag), Some("abc123"), None, None, Some("application/octet-stream"))
        .unwrap();
}

#[tokio::test]
async fn test_etag_validation_match_serves_cached() {
    let data = vec![0u8; 1024];
    let mock_state = EtagMockState {
        head_count: Arc::new(AtomicU32::new(0)),
        etag: Arc::new(std::sync::Mutex::new("\"same-etag\"".to_string())),
        test_data: Arc::new(data.clone()),
    };

    let app = Router::new()
        .route("/resolve/main/test.bin", head(mock_head))
        .route("/resolve/main/test.bin", get(mock_get));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let tmp = TempDir::new().unwrap();
    let backend = Arc::new(LocalBackend::new(tmp.path().to_path_buf(), Compression::None));
    let db_path = tmp.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let svc = CacheService::new(
        metadata.clone(),
        backend.clone(),
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    // Seed cache with matching etag
    let url = format!("http://{}/resolve/main/test.bin", addr);
    seed_file(&svc, "test.bin", "test-repo", "hf", &data, "\"same-etag\"").await;

    // Validate etag — should match (head_count increases by 1)
    let result = svc.validate_file_etag(&url, "test.bin", "test-repo", "hf", None, "\"same-etag\"").await;
    assert!(result.is_ok());
    assert!(result.unwrap()); // true = match
    assert_eq!(mock_state.head_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_etag_validation_changed_returns_false() {
    let data = vec![1u8; 1024];
    let mock_state = EtagMockState {
        head_count: Arc::new(AtomicU32::new(0)),
        etag: Arc::new(std::sync::Mutex::new("\"new-etag\"".to_string())),
        test_data: Arc::new(data.clone()),
    };

    let app = Router::new()
        .route("/resolve/main/test.bin", head(mock_head))
        .route("/resolve/main/test.bin", get(mock_get));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let tmp = TempDir::new().unwrap();
    let backend = Arc::new(LocalBackend::new(tmp.path().to_path_buf(), Compression::None));
    let db_path = tmp.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let svc = CacheService::new(metadata, backend, None, reqwest::Client::new(), reqwest::Client::new(), 0, 8, true, reqwest::Client::new(), 5);

    // Seed cache with OLD etag
    let url = format!("http://{}/resolve/main/test.bin", addr);
    seed_file(&svc, "test.bin", "test-repo", "hf", &data, "\"old-etag\"").await;

    // Validate — upstream has "new-etag", cached has "old-etag" → mismatch
    let result = svc.validate_file_etag(&url, "test.bin", "test-repo", "hf", None, "\"old-etag\"").await;
    assert!(result.is_ok());
    assert!(!result.unwrap()); // false = changed
}

#[tokio::test]
async fn test_etag_validation_unreachable_returns_error() {
    let tmp = TempDir::new().unwrap();
    let backend = Arc::new(LocalBackend::new(tmp.path().to_path_buf(), Compression::None));
    let db_path = tmp.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let svc = CacheService::new(metadata, backend, None, reqwest::Client::new(), reqwest::Client::new(), 0, 8, true, reqwest::Client::new(), 5);

    // Seed cache with etag
    seed_file(&svc, "test.bin", "test-repo", "hf", &vec![0u8; 100], "\"any-etag\"").await;

    // Validate against unreachable upstream → error
    let result = svc.validate_file_etag(
        "http://127.0.0.1:1/resolve/main/test.bin",
        "test.bin", "test-repo", "hf", None, "\"any-etag\"",
    ).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_etag_validation_skipped_with_null_etag() {
    let tmp = TempDir::new().unwrap();
    let backend = Arc::new(LocalBackend::new(tmp.path().to_path_buf(), Compression::None));
    let db_path = tmp.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let svc = CacheService::new(metadata, backend, None, reqwest::Client::new(), reqwest::Client::new(), 0, 8, true, reqwest::Client::new(), 5);

    // Seed cache without etag
    svc.metadata.add_file("nob.bin", "repo", 100, "hf").unwrap();
    // File has no etag — file_proxy_inner should skip validation

    let file = svc.metadata.get_file_by_name("nob.bin", "hf").unwrap().unwrap();
    assert!(file.etag.is_none()); // verification: etag is None
}

#[test]
fn test_etag_matches_any_weak_and_strong() {
    // Test the etag comparison helper (add these tests once helpers are public/accessible)
    // Strong etag match
    assert!(etag_matches_any("\"abc123\"", "\"abc123\""));
    // Weak etag match
    assert!(etag_matches_any("W/\"abc123\"", "\"abc123\""));
    // Weak etag in If-None-Match
    assert!(etag_matches_any("\"abc123\"", "W/\"abc123\""));
    // Multiple etags — second matches
    assert!(etag_matches_any("\"abc123\"", "\"xyz\", \"abc123\""));
    // Mismatch
    assert!(!etag_matches_any("\"abc123\"", "\"xyz789\""));
}
```

> **Note:** The `etag_matches_any` and `build_304_response` functions added in Task 7 should be declared `pub(crate)` or placed where tests can access them. Move them to `src/service.rs` as public free functions, or add `#[cfg(test)]` re-exports.

- [ ] **Step 2: Run tests**

```bash
cargo test --test etag_tests
```

- [ ] **Step 3: Commit**

```bash
git add tests/etag_tests.rs
git commit -m "test: add ETag validation and If-None-Match tests"
```

---

### Task 10: Final QA

- [ ] **Step 1: Format**

```bash
cargo fmt -- --check
```

- [ ] **Step 2: Lint**

```bash
cargo clippy -- -D warnings
```

- [ ] **Step 3: Full test suite**

```bash
cargo test
```

- [ ] **Step 4: Build release**

```bash
cargo build --release
```

- [ ] **Step 5: Manual smoke test with upstream**

```bash
# Start hugrs with hf-mirror.com as upstream
# curl -v -H 'If-None-Match: "etag"' http://localhost:3000/...
# Verify 304 response
```

- [ ] **Step 6: Final commit**

```bash
git add -A && git diff --cached --stat
git commit -m "chore: final QA pass for ETag validation"
```
