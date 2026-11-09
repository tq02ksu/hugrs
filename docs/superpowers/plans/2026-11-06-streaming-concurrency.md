# Streaming Concurrency Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every streaming response carry exactly its requested byte range, in order, so concurrent downloads of one file can never truncate or shift each other's data.

**Architecture:** Delete the per-file "already downloaded" set from the session actor and give each subscriber its own chunk cursor, serving cached chunks and freshly downloaded chunks through the same path. Route fully cached files through `stream_cached_file` so hot downloads never share session state, and make the "file is complete" predicate validate chunk count, index continuity, and per-chunk size.

**Tech Stack:** Rust, tokio, axum, reqwest, rusqlite, dashmap.

**Spec:** `docs/superpowers/specs/2026-11-06-streaming-concurrency-design.md`

## Global Constraints

- Rust stable; `cargo fmt`, `cargo clippy --all-features --tests`, `cargo test` must pass before any commit.
- `clippy::unwrap_used` / `expect_used` are denied in `lib` and `bin`; test files start with `#![allow(clippy::unwrap_used, clippy::expect_used)]`.
- No new comments except where code is genuinely non-obvious.
- Do not modify response headers, status codes, or content-type behavior.
- Keep `fix:` commits separate from any `release:` or `feat:` work.
- `session.rs` unit tests live in the existing `#[cfg(test)] mod tests`; integration tests use the existing mock-upstream harness in `tests/streaming_tests.rs`.

## Review Focus

- A range that ends exactly on a chunk boundary (`end + 1` divisible by `CHUNK_SIZE`) must include that whole chunk and no byte more.
- A range contained inside a single chunk (`start / CHUNK_SIZE == end / CHUNK_SIZE`) must be sliced correctly on both ends and terminate after one chunk.
- A late subscriber whose first needed chunk is already being prefetched must wait for that download instead of starting a second upstream GET (pinned by the `get_count` assertion in `test_concurrent_range_requests_match_their_slices`).
- A client that disconnects mid-stream must not stall or poison the actor for the next request (pinned by the existing `test_retry_after_client_disconnect_restarts_incomplete_session`).
- A subscriber attaching while a chunk download is finishing must not receive a dead channel instead of the chunk (pinned by `subscribe_after_chunk_stored_serves_from_cache`).
- A cached file whose blob is missing or the wrong size must stop being reported as complete instead of returning 500 forever.

---

### Task 1: Strict completeness predicate

**Files:**
- Modify: `src/service.rs:169-188` (`get_complete_file`), add `file_is_complete` near it
- Modify: `src/server.rs:977-1004` (`aggregate_files`), `src/server.rs:1260-1279` (`control_file_show`)
- Test: `tests/service_tests.rs`, `tests/e2e_tests.rs`

**Interfaces:**
- Consumes: `crate::metadata::FileChunk { file_id: i64, sha256: String, chunk_index: i64, chunk_size: i64 }`, `crate::service::CHUNK_SIZE: usize`.
- Produces: `pub fn file_is_complete(total_size: i64, chunks: &[FileChunk]) -> bool` in `src/service.rs`; `CacheService::get_complete_file` keeps its signature but now uses it.

- [ ] **Step 1: Write the failing predicate tests**

In `tests/service_tests.rs`, add:

```rust
fn chunk(file_id: i64, chunk_index: i64, chunk_size: i64) -> hugrs::metadata::FileChunk {
    hugrs::metadata::FileChunk {
        file_id,
        sha256: format!("sha{chunk_index}"),
        chunk_index,
        chunk_size,
    }
}

#[test]
fn file_is_complete_accepts_exact_chunk_layout() {
    let total = CHUNK_SIZE as i64 * 2 + 100;
    let chunks = vec![
        chunk(1, 0, CHUNK_SIZE as i64),
        chunk(1, 1, CHUNK_SIZE as i64),
        chunk(1, 2, 100),
    ];
    assert!(hugrs::service::file_is_complete(total, &chunks));
}

#[test]
fn file_is_complete_rejects_duplicated_full_size_chunks() {
    let total = CHUNK_SIZE as i64 + 100;
    let chunks = vec![chunk(1, 0, total), chunk(1, 1, total)];
    assert!(!hugrs::service::file_is_complete(total, &chunks));
}

#[test]
fn file_is_complete_rejects_index_gap() {
    let total = CHUNK_SIZE as i64 * 2;
    let chunks = vec![chunk(1, 0, CHUNK_SIZE as i64), chunk(1, 2, CHUNK_SIZE as i64)];
    assert!(!hugrs::service::file_is_complete(total, &chunks));
}

#[test]
fn file_is_complete_rejects_missing_tail() {
    let total = CHUNK_SIZE as i64 * 2 + 100;
    let chunks = vec![chunk(1, 0, CHUNK_SIZE as i64), chunk(1, 1, CHUNK_SIZE as i64)];
    assert!(!hugrs::service::file_is_complete(total, &chunks));
}

#[test]
fn file_is_complete_rejects_non_positive_size() {
    assert!(!hugrs::service::file_is_complete(0, &[]));
}
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `cargo test --test service_tests file_is_complete`
Expected: compile error `cannot find function file_is_complete`.

- [ ] **Step 3: Implement `file_is_complete` and use it in `get_complete_file`**

In `src/service.rs`, add after the `ByteStream` type alias:

```rust
pub fn file_is_complete(total_size: i64, chunks: &[crate::metadata::FileChunk]) -> bool {
    let total = total_size as u64;
    if total_size <= 0 {
        return false;
    }
    let chunk_size = CHUNK_SIZE as u64;
    let expected = total.div_ceil(chunk_size) as usize;
    chunks.len() == expected
        && chunks.iter().enumerate().all(|(i, c)| {
            let start = i as u64 * chunk_size;
            c.chunk_index as usize == i
                && c.chunk_size == (total - start).min(chunk_size) as i64
        })
}
```

`get_complete_file` becomes:

```rust
let chunks = self.metadata.get_file_chunks(file.id)?;
if file_is_complete(file.total_size, &chunks) {
    Ok(Some(file))
} else {
    Ok(None)
}
```

Remove the now-unused `get_file_downloaded_size` call from this function only; keep the metadata method (control-plane display uses it).

- [ ] **Step 4: Run the predicate tests**

Run: `cargo test --test service_tests file_is_complete`
Expected: 5 passed.

- [ ] **Step 5: Write the failing integration test for the legacy duplicated-chunk file**

In `tests/service_tests.rs`:

```rust
#[tokio::test]
async fn test_get_complete_file_rejects_legacy_duplicated_chunks() {
    let dir = TempDir::new().unwrap();
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata.clone(),
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
        3,
    );

    let total = CHUNK_SIZE as i64 + 100;
    let file = metadata.add_file("vocab.json", "org/repo", total, "hf").unwrap();
    for idx in 0..2i64 {
        let data = vec![(idx + 1) as u8; total as usize];
        let sha = hugrs::chunker::sha256_hex(&data);
        metadata.ensure_chunk(&sha, "local", &sha, total, total).unwrap();
        metadata
            .link_file_chunk(file.id, &sha, idx, total)
            .unwrap();
    }

    let found = service
        .get_complete_file("vocab.json", "hf")
        .await
        .unwrap();
    assert!(
        found.is_none(),
        "a file with two full-size chunk rows must not count as complete"
    );
}
```

- [ ] **Step 6: Run the integration test**

Run: `cargo test --test service_tests test_get_complete_file_rejects_legacy_duplicated_chunks`
Expected: PASS. This confirms `get_complete_file` is actually wired to the predicate; if it fails, `get_complete_file` is still comparing `downloaded >= total_size`.

- [ ] **Step 7: Use the predicate in the control plane**

In `src/server.rs::aggregate_files`, replace `complete: downloaded_size >= first.total_size` with:

```rust
complete: entries.iter().any(|entry| {
    metadata
        .get_file_chunks(entry.id)
        .map(|chunks| crate::service::file_is_complete(entry.total_size, &chunks))
        .unwrap_or(false)
}),
```

In `control_file_show`, replace `complete: downloaded_size >= first.total_size` with:

```rust
complete: crate::service::file_is_complete(
    first.total_size,
    &state
        .service
        .metadata
        .get_file_chunks(first.id)
        .map_err(AppError::Anyhow)?,
),
```

- [ ] **Step 8: Run the control-plane tests**

Run: `cargo test --test e2e_tests control_api_files`
Expected: `test_control_api_files_report_incomplete_download_status` and `test_control_api_files_report_complete_download_status` pass. If the complete case fails, check that the seeded file chunked into `ceil(size / CHUNK_SIZE)` rows with the last row short.

- [ ] **Step 9: Commit**

```bash
git add src/service.rs src/server.rs tests/service_tests.rs
git commit -m "fix: require exact chunk layout before marking a file complete"
```

---

### Task 2: Size validation independent of sha256 verification

**Files:**
- Modify: `src/session.rs:178-221` (`ChunkReader::read_cached_chunk`)
- Test: `src/session.rs` (`mod tests`, uses existing `chunk_reader` helper)

**Interfaces:**
- Consumes: `ChunkReader { backend, verify_sha256, .. }` as built by the existing `chunk_reader(dir, chunk_retries)` test helper.
- Produces: unchanged signature `async fn read_cached_chunk(&self, sha256: &str, expected_chunk_size: Option<usize>) -> anyhow::Result<Option<Bytes>>`.

- [ ] **Step 1: Write the failing test**

In `src/session.rs mod tests`:

```rust
#[tokio::test]
async fn read_cached_chunk_rejects_wrong_size_without_verification() {
    let dir = TempDir::new().unwrap();
    let reader = chunk_reader(&dir, 3);
    let stored = vec![3u8; 100];
    let sha = crate::chunker::sha256_hex(&stored);
    reader.backend.put(&sha, &stored).await.unwrap();

    let got = reader.read_cached_chunk(&sha, Some(4096)).await.unwrap();

    assert!(got.is_none(), "wrong-sized chunk must not be served");
    assert!(
        !reader.backend.exists(&sha).await.unwrap(),
        "wrong-sized chunk must be evicted from storage"
    );
}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `cargo test --lib read_cached_chunk_rejects_wrong_size_without_verification`
Expected: FAIL — `got` is `Some` because `chunk_reader` builds `verify_sha256: false` and the size check sits after that early return.

- [ ] **Step 3: Move the size check before the early return**

In `read_cached_chunk`, after `backend.get` and before the `if !self.verify_sha256` block:

```rust
if let Some(expected) = expected_chunk_size {
    if raw.len() != expected {
        tracing::error!(
            "cached chunk size mismatch: len={} expected={}",
            raw.len(),
            expected
        );
        let _ = self.backend.delete(sha256).await;
        return Ok(None);
    }
}
```

Then in the verification branch, delete the now-duplicated size block (keep the sha comparison and its eviction).

- [ ] **Step 4: Run the test**

Run: `cargo test --lib read_cached_chunk_rejects_wrong_size_without_verification`
Expected: PASS.

- [ ] **Step 5: Run the whole session test module**

Run: `cargo test --lib session::`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/session.rs
git commit -m "fix: reject wrong-sized cached chunks regardless of sha256 verification"
```

---

### Task 3: Harden `stream_cached_file` and unlink corrupt chunks

**Files:**
- Modify: `src/metadata.rs` (add `unlink_file_chunk` near `link_file_chunk`)
- Modify: `src/service.rs:362-554` (`stream_cached_file`)
- Test: `tests/metadata_tests.rs`, `tests/service_tests.rs`

**Interfaces:**
- Consumes: `FileChunk`, `StorageBackend::get`, `File`.
- Produces:
  - `pub fn unlink_file_chunk(&self, file_id: i64, chunk_index: i64) -> anyhow::Result<()>` on `MetadataStore`
  - `pub async fn stream_cached_file(&self, file: &File, range_start: Option<u64>, range_end: Option<u64>) -> anyhow::Result<(File, u64, ByteStream)>` (signature change: takes `&File`, returns a clone of it for the response builder)

- [ ] **Step 1: Write the failing metadata test**

In `tests/metadata_tests.rs`, following the file's existing setup pattern:

```rust
#[test]
fn unlink_file_chunk_drops_row_and_orphans_chunk() {
    let (_dir, store) = store();
    let data = b"hello".to_vec();
    let sha = hugrs::chunker::sha256_hex(&data);
    let file = store.add_file("a.bin", "org/repo", 5, "hf").unwrap();
    store.ensure_chunk(&sha, "local", &sha, 5, 5).unwrap();
    store.link_file_chunk(file.id, &sha, 0, 5).unwrap();
    assert_eq!(store.get_chunk(&sha).unwrap().unwrap().ref_count, 1);

    store.unlink_file_chunk(file.id, 0).unwrap();

    assert!(store.get_file_chunks(file.id).unwrap().is_empty());
    let chunk = store.get_chunk(&sha).unwrap().unwrap();
    assert_eq!(chunk.ref_count, 0);
    assert!(chunk.orphaned_at.is_some());
}
```

If the module has no `store()` helper, use the existing pattern in that file for constructing a temp `MetadataStore`.

- [ ] **Step 2: Run it and confirm it fails**

Run: `cargo test --test metadata_tests unlink_file_chunk_drops_row_and_orphans_chunk`
Expected: compile error `no method named unlink_file_chunk`.

- [ ] **Step 3: Implement `unlink_file_chunk`**

Use one transaction that (a) reads the row's `sha256`, (b) deletes the `file_chunks` row, (c) decrements `chunks.ref_count` with `MAX(0, ...)` and sets `orphaned_at = datetime('now')` when the new count is 0. Mirror the bookkeeping in `delete_file_by_id_tx`.

- [ ] **Step 4: Run the metadata test**

Run: `cargo test --test metadata_tests unlink_file_chunk_drops_row_and_orphans_chunk`
Expected: PASS.

- [ ] **Step 5: Write the failing `stream_cached_file` repair test**

In `tests/service_tests.rs`, build a service with `prefetch_depth = 0`, seed a two-chunk file, overwrite chunk 1's blob with same-length garbage, then:

```rust
#[tokio::test]
async fn test_stream_cached_file_unlinks_corrupt_chunk() {
    // ... build service with prefetch_depth 0, seed data of CHUNK_SIZE + 100 bytes ...
    let file = service
        .metadata
        .get_file_by_name("corrupt.bin", "hf")
        .unwrap()
        .unwrap();
    let chunks = service.metadata.get_file_chunks(file.id).unwrap();
    service
        .backend
        .put(&chunks[1].sha256, &vec![7u8; chunks[1].chunk_size as usize])
        .await
        .unwrap();

    let (_file, _len, mut stream) = service.stream_cached_file(&file, None, None).await.unwrap();

    use futures_util::StreamExt;
    let mut saw_err = false;
    while let Some(item) = stream.next().await {
        if item.is_err() {
            saw_err = true;
            break;
        }
    }
    assert!(saw_err, "corrupt chunk must abort the stream");

    let remaining = service.metadata.get_file_chunks(file.id).unwrap();
    assert!(
        remaining.iter().all(|c| c.chunk_index != 1),
        "corrupt chunk must be unlinked so the file is no longer complete"
    );
    assert!(service
        .get_complete_file("corrupt.bin", "hf")
        .await
        .unwrap()
        .is_none());
}
```

- [ ] **Step 6: Run it and confirm it fails**

Run: `cargo test --test service_tests test_stream_cached_file_unlinks_corrupt_chunk`
Expected: FAIL — either compile error on the old `(name, source, ...)` signature or the row still present.

- [ ] **Step 7: Change the signature, validate sizes, and unlink on failure**

In `src/service.rs::stream_cached_file`:

- Take `file: &File`; drop the `get_file_by_name` lookup; use `file.id` / `file.total_size`, and return `file.clone()` in the tuple.
- Clone `Arc<MetadataStore>` into the spawned task alongside `backend` so the repair path can unlink rows.
- Guard `file_chunks.get(first_chunk..=last_chunk)`; bail with `file not found: chunk {} out of range` when the slice is empty instead of indexing.
- For every chunk, before slicing, compute `expected = min(CHUNK_SIZE, total_size - chunk_start)` and on `data.len() != expected` run the same repair path as a sha mismatch: call `metadata.unlink_file_chunk(file.id, ft.chunk_index)`, send `Err(anyhow!("chunk {} size mismatch", ft.chunk_index))`, return from the task.
- In both the direct and the prefetch branch, replace `backend.delete(&sha)` with `unlink_file_chunk` on sha mismatch, then error.
- Keep `served_bytes` accounting per emitted slice.

- [ ] **Step 8: Run the test**

Run: `cargo test --test service_tests test_stream_cached_file_unlinks_corrupt_chunk`
Expected: PASS.

- [ ] **Step 9: Add the concurrent fast-path test**

In `tests/service_tests.rs`, seed a complete file, then read it twice concurrently through `stream_cached_file`:

```rust
#[tokio::test]
async fn test_stream_cached_file_supports_concurrent_readers() {
    // ... build service, seed data of CHUNK_SIZE * 3 + 7 bytes as "shared.bin" ...
    let file = service
        .metadata
        .get_file_by_name("shared.bin", "hf")
        .unwrap()
        .unwrap();
    assert!(service.get_complete_file("shared.bin", "hf").await.unwrap().is_some());

    async fn read_all(svc: Arc<CacheService>, file: File) -> Vec<u8> {
        use futures_util::StreamExt;
        let (_f, _len, mut stream) = svc.stream_cached_file(&file, None, None).await.unwrap();
        let mut body = Vec::new();
        while let Some(item) = stream.next().await {
            body.extend_from_slice(&item.unwrap());
        }
        body
    }

    let (a, b) = tokio::join!(
        read_all(service.clone(), file.clone()),
        read_all(service.clone(), file.clone())
    );
    assert_eq!(a, seeded_data);
    assert_eq!(b, seeded_data);
}
```

Run: `cargo test --test service_tests test_stream_cached_file_supports_concurrent_readers`
Expected: PASS. This is the deterministic proof that hot-path readers are independent; the e2e equivalent is timing-dependent.

- [ ] **Step 10: Commit**

```bash
git add src/metadata.rs src/service.rs tests/metadata_tests.rs tests/service_tests.rs
git commit -m "fix: validate and repair cached chunks when streaming complete files"
```

---

### Task 4: Route complete cache hits through `stream_cached_file`

**Files:**
- Modify: `src/server.rs:710-736` (GET cache-hit branch)
- Test: `tests/e2e_tests.rs`

**Interfaces:**
- Consumes: `CacheService::stream_cached_file(&File, Option<u64>, Option<u64>)` from Task 3.
- Produces: no new API; the GET cache-hit branch no longer calls `stream_http_file`.

- [ ] **Step 1: Write the routing regression test**

In `tests/e2e_tests.rs`, using the existing `start_upstream` + `build_hugrs_router` harness and `tower::ServiceExt::oneshot`. The range request matters most: it pins the argument order of the new `stream_cached_file(&File, start, end)` call.

```rust
#[tokio::test]
async fn test_complete_cache_hit_serves_exact_body_and_range() {
    let test_data: Vec<u8> = (0..CHUNK_SIZE * 2 + 123)
        .map(|i| (i as u8).wrapping_mul(29).wrapping_add(7))
        .collect();
    let (upstream, _state) = start_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    let full = || {
        axum::http::Request::builder()
            .uri("/org/repo/resolve/main/big.bin")
            .body(axum::body::Body::empty())
            .unwrap()
    };
    let ranged = || {
        axum::http::Request::builder()
            .uri("/org/repo/resolve/main/big.bin")
            .header("range", format!("bytes={}-{}", CHUNK_SIZE, test_data.len() - 1))
            .body(axum::body::Body::empty())
            .unwrap()
    };

    let warm = app.clone().oneshot(full()).await.unwrap();
    let warm_body = warm.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(warm_body.as_ref(), test_data.as_slice());

    let resp = app.clone().oneshot(ranged()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers()["content-length"],
        (test_data.len() - CHUNK_SIZE).to_string()
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.as_ref(), &test_data[CHUNK_SIZE..]);

    let resp2 = app.oneshot(full()).await.unwrap();
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body2.as_ref(), test_data.as_slice());
}
```

- [ ] **Step 2: Run it**

Run: `cargo test --test e2e_tests test_complete_cache_hit_serves_exact_body_and_range`
Expected: PASS both before and after Step 3 (it is a wiring guard, not a bug reproduction). It must still PASS after Step 3 — a failure there means the file/range arguments were wired in the wrong order.

- [ ] **Step 3: Switch the cache-hit branch**

In `file_proxy_inner`, replace the `service.stream_http_file(...)` call with:

```rust
let (file, content_length, stream) = service
    .stream_cached_file(file, range.map(|r| r.0), range.and_then(|r| r.1))
    .await?;
```

Keep the surrounding logging and `build_stream_response` call unchanged.

- [ ] **Step 4: Run the e2e test**

Run: `cargo test --test e2e_tests test_complete_cache_hit_serves_exact_body_and_range`
Expected: PASS.

- [ ] **Step 5: Run the full e2e suite**

Run: `cargo test --test e2e_tests`
Expected: all pass (cache-hit tests now exercise the new path).

- [ ] **Step 6: Commit**

```bash
git add src/server.rs tests/e2e_tests.rs
git commit -m "fix: serve complete cache hits without the shared download session"
```

---

### Task 5: Per-subscriber cursors in the file session actor

**Files:**
- Modify: `src/session.rs:17` (`SubscriberEntry`), `src/session.rs:456-794` (actor struct, `run`, `download_and_forward`, `schedule_prefetches`, `compute_active_cursors`, `select_next_chunk`, `cleanup_prefetches`, `forward_chunk`, `clean_subscribers`)
- Modify: `src/session.rs:856-1103` (unit tests)
- Test: `tests/streaming_tests.rs`

**Interfaces:**
- Consumes: `SessionTable::subscribe(file_id, chunk_idx, url, start, end, total_size, chunk_count, user_agent, cached_chunks) -> anyhow::Result<broadcast::Receiver<ChunkMessage>>`; `CHUNK_SIZE`.
- Produces: private `struct Subscriber { start: u64, end: u64, next_idx: usize, last_idx: usize, tx: mpsc::Sender<Result<Bytes, anyhow::Error>> }`; private `prefetch_inflight: HashMap<usize, JoinHandle<()>>`; `FileSessionActor::spawn` / `ActorHandle::subscribe` signatures unchanged.

- [ ] **Step 1: Write the failing late-subscriber regression test**

In `tests/streaming_tests.rs`, copy the boilerplate from `test_partial_cache_no_redundant_download` (mock upstream + `CacheService::new`), then:

```rust
#[tokio::test]
async fn test_late_subscriber_receives_full_range() {
    let total = CHUNK_SIZE * 3 + 777;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(23).wrapping_add(11))
        .collect();
    // ... start mock upstream, build service ...
    let upstream_url = format!("http://{addr}/test/repo/resolve/main/late.bin");

    use futures_util::StreamExt;

    let (_, _, mut first) = service
        .stream_from_upstream(&upstream_url, "late.bin", "test/repo", "hf", None, None, None)
        .await
        .unwrap();

    let mut first_body = Vec::new();
    while first_body.len() < CHUNK_SIZE * 2 {
        first_body.extend_from_slice(&first.next().await.unwrap().unwrap());
    }

    let (_, content_length, mut second) = service
        .stream_from_upstream(&upstream_url, "late.bin", "test/repo", "hf", None, None, None)
        .await
        .unwrap();
    assert_eq!(content_length, total as u64);

    let mut second_body = Vec::new();
    while let Some(item) = second.next().await {
        second_body.extend_from_slice(&item.unwrap());
    }
    while let Some(item) = first.next().await {
        first_body.extend_from_slice(&item.unwrap());
    }

    assert_eq!(first_body, test_data);
    assert_eq!(
        second_body.len(),
        total,
        "late subscriber got {} of {total} bytes",
        second_body.len()
    );
    assert_eq!(second_body, test_data);
}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `cargo test --test streaming_tests test_late_subscriber_receives_full_range`
Expected: FAIL with a short `second_body` (`late subscriber got N of M bytes`, N < M).

- [ ] **Step 3: Write the failing concurrent-range regression test**

Append to `tests/streaming_tests.rs`, same boilerplate, a 7-chunk file, warm chunks 0..=2 through one reader, then request 8 unaligned ranges concurrently:

```rust
#[tokio::test]
async fn test_concurrent_range_requests_match_their_slices() {
    let total = CHUNK_SIZE * 7 + 321;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(5))
        .collect();
    // ... start mock upstream, build service ...
    let upstream_url = format!("http://{addr}/test/repo/resolve/main/ranges.bin");

    use futures_util::StreamExt;

    let (_, _, mut warm) = service
        .stream_from_upstream(&upstream_url, "ranges.bin", "test/repo", "hf", None, None, None)
        .await
        .unwrap();
    let mut warmed = Vec::new();
    while warmed.len() < CHUNK_SIZE * 3 {
        warmed.extend_from_slice(&warm.next().await.unwrap().unwrap());
    }

    let boundaries: Vec<(u64, u64)> = vec![
        (0, total as u64 / 8),
        (total as u64 / 8 + 1, total as u64 / 4),
        (total as u64 / 4 + 17, total as u64 * 3 / 8),
        (total as u64 * 3 / 8 + 1, total as u64 / 2),
        (total as u64 / 2 + 3, total as u64 * 5 / 8),
        (total as u64 * 5 / 8 + 1, total as u64 * 3 / 4),
        (total as u64 * 3 / 4 + 9, total as u64 * 7 / 8),
        (total as u64 * 7 / 8 + 1, total as u64 - 1),
    ];

    let mut tasks = Vec::new();
    for (start, end) in boundaries.clone() {
        let service = service.clone();
        let url = upstream_url.clone();
        tasks.push(tokio::spawn(async move {
            let (_, len, stream) = service
                .stream_from_upstream(&url, "ranges.bin", "test/repo", "hf", Some(start), Some(end), None)
                .await
                .unwrap();
            assert_eq!(len, end - start + 1);
            let mut body = Vec::new();
            let mut stream = stream;
            while let Some(item) = stream.next().await {
                body.extend_from_slice(&item.unwrap());
            }
            (start, end, body)
        }));
    }

    for task in tasks {
        let (start, end, body) = task.await.unwrap();
        assert_eq!(
            body,
            test_data[start as usize..=end as usize],
            "range {start}-{end} mismatch"
        );
    }

    let expected_chunks = (total as u64).div_ceil(CHUNK_SIZE as u64) as u32;
    assert_eq!(
        get_count.load(Ordering::SeqCst),
        expected_chunks,
        "every chunk must be fetched from upstream exactly once"
    );
}
```

- [ ] **Step 4: Run it and confirm it fails**

Run: `cargo test --test streaming_tests test_concurrent_range_requests_match_their_slices`
Expected: FAIL — at least one range that overlaps chunks 0-2 comes back short.

- [ ] **Step 5: Replace the subscriber tuple with a cursor struct**

In `src/session.rs`:

```rust
struct Subscriber {
    start: u64,
    end: u64,
    next_idx: usize,
    last_idx: usize,
    tx: mpsc::Sender<Result<Bytes, anyhow::Error>>,
}
```

Add a constructor used by the mailbox drain:

```rust
fn subscriber(start: u64, end: u64, chunk_count: usize, tx: mpsc::Sender<Result<Bytes, anyhow::Error>>) -> Subscriber {
    let chunk_sz = CHUNK_SIZE as u64;
    Subscriber {
        start,
        end,
        next_idx: (start / chunk_sz) as usize,
        last_idx: ((end / chunk_sz) as usize).min(chunk_count.saturating_sub(1)),
        tx,
    }
}
```

Field changes on `FileSessionActor`: `subscribers: Vec<Subscriber>`, delete `completed`, change `prefetch_inflight` to `HashMap<usize, JoinHandle<()>>`.

- [ ] **Step 6: Rewrite the loop, selection, and forwarding**

`run()` becomes:

```rust
self.subscribers.push(subscriber(range.0, range.1, self.chunk_count, reply));
// ...
while !self.subscribers.is_empty() || !self.prefetch_inflight.is_empty() {
    self.clean_subscribers();
    self.cleanup_prefetches();
    if self.subscribers.is_empty() && self.prefetch_inflight.is_empty() {
        break;
    }
    let Some(idx) = self.select_next_chunk() else { break };
    if self.serve_chunk(idx).await.is_err() {
        break;
    }
    self.schedule_prefetches();
}
self.subscribers.clear();
```

`select_next_chunk()` scans `subscribers` and returns the `next_idx` with the smallest `urgency = (next_idx * CHUNK_SIZE - start).max(0) as f64 / (end - start).max(1) as f64`, tie-breaking on the smaller index. It never returns an index that is not some subscriber's `next_idx`.

`serve_chunk(idx)` replaces `download_and_forward`:

```rust
let start = idx as u64 * CHUNK_SIZE as u64;
let end = (start + CHUNK_SIZE as u64 - 1).min(self.total_size - 1);
let expected_len = (end - start + 1) as usize;
let mut rx = self.session_table.subscribe(/* unchanged arguments */).await?;
match rx.recv().await {
    Ok(Ok(data)) => {
        if data.len() != expected_len {
            self.forward_error(anyhow::anyhow!("chunk {idx} size mismatch")).await;
            return Err(anyhow::anyhow!("chunk {idx} size mismatch"));
        }
        self.forward_chunk(idx, start, &data).await;
        Ok(())
    }
    Ok(Err(err)) => { self.forward_error(...).await; Err(...) }
    Err(e) => { self.forward_error(...).await; Err(...) }
}
```

`forward_chunk(&mut self, idx: usize, chunk_start: u64, data: &Arc<Bytes>)` takes `&mut self`, iterates subscribers, and for each with `next_idx == idx`:

```rust
let chunk_end = chunk_start + data.len() as u64 - 1;
let sl_start = if sub.start > chunk_start { (sub.start - chunk_start) as usize } else { 0 };
let sl_end = if sub.end < chunk_end { (sub.end - chunk_start + 1) as usize } else { data.len() };
let slice = Bytes::copy_from_slice(&data[sl_start..sl_end]);
self.served_bytes.fetch_add(slice.len() as u64, Ordering::Relaxed);
let _ = sub.tx.send(Ok(slice)).await;
sub.next_idx += 1;
```

After the loop, `self.subscribers.retain(|s| s.next_idx <= s.last_idx);` so finished responses have their sender dropped.

`compute_active_cursors()` returns each subscriber's `next_idx` (`sort_unstable` + `dedup`). `schedule_prefetches()` iterates `(cursor + 1)..=(cursor + per_cursor)` and skips indexes already in `cached_chunks` or `prefetch_inflight`, inserting the `JoinHandle` returned by `tokio::spawn`. `cleanup_prefetches()` is:

```rust
let cached = self.cached_chunks.lock_or_recover();
self.prefetch_inflight
    .retain(|idx, handle| !handle.is_finished() && !cached.contains_key(idx));
```

`forward_error` takes `&self` and iterates subscribers as before; `clean_subscribers` retains `!s.tx.is_closed()`.

- [ ] **Step 7: Rewrite the actor unit tests for cursors**

Replace the `completed`-based tests with:

```rust
#[test]
fn select_next_chunk_uses_subscriber_cursor() {
    // two subscribers, one starting at chunk 0 and one at chunk 10
    // assert Some(0), then advance the first to 1 and assert Some(1) ... then Some(10)
}

#[tokio::test]
async fn forward_chunk_advances_only_the_matching_subscriber() {
    // subscriber A next_idx = 0, subscriber B next_idx = 1
    // serve idx 0 -> A receives its slice and advances; B receives nothing
}

#[test]
fn compute_active_cursors_returns_each_next_idx() { .. }

#[tokio::test]
async fn schedule_prefetches_starts_after_cursor() {
    // subscriber next_idx = 3 -> inflight contains 4.., never 3
}
```

Add one review-focus test:

```rust
#[tokio::test]
async fn forward_chunk_handles_range_inside_one_chunk() {
    // subscriber range (CHUNK_SIZE + 10, CHUNK_SIZE + 19), next_idx == last_idx == 1
    // serve chunk 1 -> exactly 10 bytes (offsets 10..=19)
}

#[tokio::test]
async fn forward_chunk_range_ending_on_chunk_boundary_includes_whole_chunk() {
    // subscriber range (CHUNK_SIZE * 2, CHUNK_SIZE * 4 - 1) -> next_idx 2, last_idx 3
    // serve chunk 3 -> last_idx reached, subscriber dropped after exactly CHUNK_SIZE bytes
}
```

- [ ] **Step 8: Close the subscribe/send race in `SessionTable`**

Today the fetch task calls `tx.send(...)` and only then `map.remove(&key)`. A subscriber that attaches between those two calls gets a receiver that never receives, and later faults with `chunk N download closed unexpectedly`. `cached_chunks` is already populated by `fetch_chunk` before it returns, so any subscriber arriving after the fetch completes takes the cache path and never reaches the broadcast. Move `map.remove(&key)` to run before the send in both the success and the error branch.

Add a session unit test:

```rust
#[tokio::test]
async fn subscribe_after_chunk_stored_serves_from_cache() {
    // populate cached_chunks for (file_id, 0), then call subscribe twice
    // both receivers must yield the same bytes without a network request
}
```

- [ ] **Step 9: Run the session unit tests**

Run: `cargo test --lib session::`
Expected: all pass.

- [ ] **Step 10: Run both regression tests**

Run: `cargo test --test streaming_tests`
Expected: both new tests pass, all pre-existing streaming tests still pass (including `test_retry_after_client_disconnect_restarts_incomplete_session` and `test_partial_cache_no_redundant_download`), and the `get_count` assertion in the range test holds.

- [ ] **Step 11: Commit**

```bash
git add src/session.rs tests/streaming_tests.rs
git commit -m "fix: serve each session subscriber from its own chunk cursor"
```

---

### Task 6: Full quality gate

**Files:** none (verification only).

- [ ] **Step 1: Format**

Run: `cargo fmt`
Expected: no diff remains after `git diff --stat` (commit if `cargo fmt` changed files).

- [ ] **Step 2: Clippy**

Run: `cargo clippy --all-features --tests`
Expected: no warnings or errors.

- [ ] **Step 3: Full test suite**

Run: `cargo test`
Expected: all tests pass.

- [ ] **Step 4: Confirm the cache-hit path no longer drives a session**

Run: `grep -n "stream_http_file" src/server.rs`
Expected: no match in the GET cache-hit branch (only `stream_from_upstream_with_metadata` may still use it for cache misses).

- [ ] **Step 5: Commit any formatting-only changes**

```bash
git status --short
git add -A && git commit -m "chore: apply cargo fmt"
```

Only if Step 1 produced changes.
