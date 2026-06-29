# ETag Validation and If-None-Match Support Design

## Overview

Add ETag-based cache validation on every request to detect upstream file changes and re-download stale cache entries. Fix the `download_from_url()` small-file path that drops ETag/Content-Type. Support `If-None-Match` conditional requests (304 Not Modified). Delete the `upload()` function and unify small/large file download logic.

## Root Cause of NULL ETag/Content-Type

In `download_from_url()` (service.rs:267-283), small files (≤4MB) are downloaded and then passed to `self.upload()`. The upstream ETag/Content-Type headers are captured at lines 214-265 but **never passed into `upload()`**, which creates a new file row with NULL headers. The `upload()` function only restores headers from a pre-existing DB row (`existing_headers`), not from the upstream response.

Fix: replace `upload()` call with the same logic used by `stream_small_file()` — download body, chunk it, store in chunk table, and call `set_file_headers()` with the captured ETag/Content-Type.

## Upstream ETag Behavior (verified 2026-06-29)

### HuggingFace / hf-mirror.com

```
Client → GET resolve URL → 307 (x-linked-etag: "7345...") → 302 → 200 (etag: W/"7345...")
```

- First hop 307 has `x-linked-etag` (strong, quoted)
- Final 200 has `etag` (weak, `W/` prefix)
- ETag comparison: string `==`, RFC says ETags are opaque — compare as-is

### ModelScope

**Small files** (e.g. config.json, tokenizer_config.json):
- `GET repo API → 200` (no redirect, no ETag, no x-linked-etag)
- Content-Type always `application/octet-stream`

**Large files** (e.g. model.safetensors):
- `GET repo API → 302 (no x-linked-etag) → CDN (etag: "71294B..."; x-linked-etag: 88c1...)`
- CDN URL has `auth_key` that expires — must re-fetch via repo API each time

## Design

### 1. Fix download_from_url() and Unify Small/Large File Paths

Replace the small-file branch in `download_from_url()` that calls `upload()` with direct chunk storage + `set_file_headers()`:

```rust
// service.rs download_from_url() — small file path (replace upload() call)
let data = self.http_client.get(&downstream_url).send().await?.bytes().await?;

// Ensure file row exists (may have been created by fetch_file_metadata or a prior call)
if self.metadata.get_file_by_name(name, source)?.is_none() {
    self.metadata.add_file(name, repo, data.len() as i64, source)?;
}

// Chunk and store
let chunks = chunker::chunk_with_hashes(&data, CHUNK_SIZE);
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

// Store headers captured from upstream
self.metadata.set_file_headers(
    name, source,
    etag.as_deref(), x_repo_commit.as_deref(),
    x_linked_size, x_linked_etag.as_deref(),
    content_type.as_deref(),
)?;
```

Alternative: refactor `download_from_url()` to reuse `stream_from_upstream()` internally (same logic, different return type). Evaluated but adds complexity — the admin download path doesn't need streaming.

### 2. Startup Backfill for Existing NULL Records (TRANSITIONAL)

Even after fixing the root cause, existing DB rows may still have NULL `etag`/`content_type`. Add a one-time startup backfill that fills in these values from upstream. Marked with `// TRANSITIONAL: remove in v0.X.0` comments — delete entire block in future version.

```
// TRANSITIONAL: remove in v0.X.0 ──────────────────────────
for each row in files WHERE etag IS NULL OR content_type IS NULL:
    if x_repo_commit IS NULL → skip (can't construct URL)
    reconstruct upstream URL from (name, repo, x_repo_commit, source)
    send HEAD (or GET for MS repo API) to upstream
    if response has etag or content_type → UPDATE DB
    if network error → skip, warn log
// TRANSITIONAL: end ───────────────────────────────────────
```

- `metadata.rs`: `list_files_with_missing_headers()`
- `service.rs`: `backfill_missing_headers()` — entire method body wrapped in TRANSITIONAL markers
- `main.rs`: `backfill_missing_headers().await` call — wrapped in TRANSITIONAL markers

### 3. Delete `upload()`

- Remove `service.rs::upload()` (lines 134-194)
- Remove `src/service.rs:283` call site (replaced by fix above)
- Remove `src/service.rs:1080` call in `self_test()` (rewrite to use a dedicated method)
- Update all test files that call `.upload()` to use a test helper that goes through `download_from_url()` or directly calls metadata + backend
- No admin API endpoint uses `upload()` — no API changes needed

### 4. ETag Validation on Every Request

Modify `file_proxy_inner()` GET cache-hit path:

```
Cache hit (is_file_complete = true):
  ├── file.etag IS NULL?
  │   └── Yes → serve_from_cache (can't validate, skip)
  ├── fetch_file_metadata() to get current upstream etag
  │   ├── OK (upstream_reachable):
  │   │   ├── upstream_etag == cached_etag → serve_from_cache
  │   │   │   └── If x_repo_commit or x_linked_etag changed, update DB headers
  │   │   └── upstream_etag != cached_etag → invalidate_and_redownload
  │   └── Err (network timeout/error):
  │       └── serve_from_cache (degraded mode, warn log)
  └── invalidate_and_redownload:
        metadata.delete_file() + stream_from_upstream()
```

Also apply to HEAD cache-hit path: validate ETag before returning cached HEAD response.

**ETag comparison:** Pure string `==`. No parsing of `W/` prefix (RFC says ETags are opaque).

**MS small-file handling:** `fetch_file_metadata()` returns `etag: None` for MS small files (200 direct, no ETag). Skip validation.

**Network failure:** Shorter timeout (5s default). On failure, serve cached content with degraded-mode warning.

**Concurrency:** Use a per-file validation lock in `CacheService` to coalesce concurrent validations for the same file.

### 5. If-None-Match Support

When client sends `If-None-Match` header on GET:

```
GET cache hit:
  ├── If-None-Match header matches cached_etag?
  │   ├── Yes → validate upstream ETag (as in section 3)
  │   │   ├── upstream_etag == cached_etag → 304 Not Modified
  │   │   │   └── Response: 304, ETag header, no body
  │   │   └── upstream_etag != cached_etag → 200 with new content
  │   └── No → normal flow (serve cached or re-download)
  └── If-None-Match without cached_etag → ignore header, normal flow
```

Standard RFC 7232 semantics:
- 304 response includes: `ETag`, `Content-Length` (original total_size), `Accept-Ranges`, `X-Repo-Commit`
- Multiple etags in `If-None-Match` (comma-separated): match if any one matches

### 6. Affected Files

| File | Changes |
|------|---------|
| `src/service.rs` | Fix `download_from_url()` small-file path; delete `upload()`; add etag validation & If-None-Match helpers |
| `src/server.rs` | Insert ETag validation + If-None-Match in `file_proxy_inner()` GET/HEAD cache-hit paths; add `build_304_response()` |
| `src/metadata.rs` | No schema changes needed (etag/content_type columns already exist) |
| `src/config.rs` | Add optional `etag_validation_timeout_secs` config |
| `src/main.rs` | No startup backfill needed (root cause fixed) |
| `src/session.rs` | Remove `dummy_file()` etag=None in tests |
| `tests/service_tests.rs` | Replace `.upload()` calls with download or direct DB operations |
| `tests/e2e_tests.rs` | Replace `.upload()` calls; add 304 tests |
| `tests/streaming_tests.rs` | No changes needed (uses mock upstream with ETag) |

### 7. Configuration

```toml
[cache]
etag_validation_timeout_secs = 5  # default 5, 0 = disable etag validation entirely
```

## Error Handling

- Upstream unreachable during ETag validation: serve cached content (degraded mode), log warn
- Upstream returns no ETag: skip validation, serve cached
- MS small file (200 direct, no ETag): skip validation
- ETag mismatch: delete cached chunks, re-download from upstream
- Concurrent request race: coalesce via per-file lock

## Non-Goals

- No conditional HTTP (If-None-Match) sent to upstream — HEAD is sufficient
- No ETag storage format normalization — store and compare as-is
- No ETag-based invalidation for Git/LFS proxy
- No admin API for manual ETag refresh (can add later)
