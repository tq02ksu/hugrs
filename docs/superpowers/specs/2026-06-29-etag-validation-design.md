# ETag Validation and Header Backfill Design

## Overview

Add ETag-based cache validation to detect upstream file changes and re-download stale cache entries. Also add a transitional startup backfill to fill in NULL `etag` and `content_type` values in the `files` table.

## Upstream ETag Behavior (verified 2026-06-29)

### HuggingFace / hf-mirror.com

```
Client → GET resolve URL → 307 (x-linked-etag: "7345...") → 302 → 200 (etag: W/"7345...")
```

- First hop 307 has `x-linked-etag` (strong, quoted)
- Final 200 has `etag` (weak, `W/` prefix)
- ETag comparison: string `==`, RFC says ETags are opaque — `W/` is a client hint, not part of comparison logic. Compare as-is.

### ModelScope

**Small files** (e.g. config.json, tokenizer_config.json):
```
Client → GET repo API → 200 (content-type: application/octet-stream, NO etag)
```
- No redirect, no ETag, no x-linked-etag
- Content-Type always `application/octet-stream` (generic)

**Large files** (e.g. model.safetensors):
```
Client → GET repo API → 302 (no x-linked-etag) → CDN (etag: "71294B...", x-linked-etag: 88c1...)
```
- First hop 302 has no `x-linked-etag` (unlike HF)
- CDN has both `etag` and `x-linked-etag`
- CDN URL has `auth_key` that expires — must re-fetch via repo API each time

## Design

### 1. Startup Backfill (transitional, remove in future version)

At startup, after SQLite init and before HTTP server start:

```
for each row in files WHERE etag IS NULL OR content_type IS NULL:
    reconstruct upstream URL from (name, repo, x_repo_commit, source)
    send HEAD (or GET for MS repo API) to upstream
    if response has etag or content_type → UPDATE DB
    if network error → skip, warn log
```

**URL reconstruction:**

| source | URL pattern |
|--------|------------|
| hf | `{endpoint}/{repo}/resolve/{x_repo_commit}/{filepath}` |
| ms | `{endpoint}/api/v1/models/{org}/{repo}/repo?Revision={x_repo_commit}&FilePath={filepath}` |

Where `filepath` = `name` minus the `{org}/{repo}/` prefix.

**Edge case:** If `x_repo_commit` is also NULL, skip that file — no revision available to construct URL.

Reuse existing `head_client` and `ms_head_client` with `redirect::Policy::none()`.
Reuse `use_get_for_first_hop_probe()` for MS repo API URLs.

**Affected code:**
- `metadata.rs`: new `list_files_with_missing_headers() -> Result<Vec<File>>`
- `service.rs`: new `backfill_missing_headers() -> Result<usize>`
- `main.rs`: call `backfill_missing_headers()` after service init, before `axum::serve()`

### 2. ETag Validation on Every Request

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

Also apply to HEAD cache-hit path: if HEAD would return cached metadata, validate ETag first and trigger re-download if changed.

**MS small-file handling:** `fetch_file_metadata()` already handles MS first-hop as GET. For files that return 200 directly (no redirect, no etag), the returned `etag` will be `None`. Skip validation when upstream returns no etag.

**ETag comparison:** Pure string `==`. RFC 9110 says ETags are opaque. Store and compare exactly as received. No parsing of `W/` prefix.

**Network failure/timeout:** Use a shorter timeout for the ETag validation HEAD (5s default, configurable). On failure, serve cached content with a warning log.

**Concurrency:** Multiple concurrent requests for the same file may trigger duplicate ETag validation HEADs. Use a per-file validation lock (e.g. `tokio::sync::Mutex<HashMap<String, ()>>` in `CacheService`) to coalesce concurrent validations.

### 3. Reuse Existing Logic

- `fetch_file_metadata()` (service.rs:908-1035) already handles:
  - MS first-hop GET fallback
  - Redirect following
  - ETag/x-linked-etag/content-type extraction
  - Size-based invalidation (keep this)
  
- Need to change `fetch_file_metadata()` (or add a variant) that **does not** delete files on size mismatch — the caller (ETag validation path) handles invalidation.

- Add a `validate_headers_only` variant that returns headers without modifying DB.

### 4. Affected Files

| File | Changes |
|------|---------|
| `src/metadata.rs` | Add `list_files_with_missing_headers()` method |
| `src/service.rs` | Add `backfill_missing_headers()`, `validate_file_etag()`, modify GET path to call etag validation |
| `src/server.rs` | Insert ETag validation in `file_proxy_inner()` GET cache-hit path and HEAD cache-hit path |
| `src/config.rs` | Add optional `etag_validation_timeout_secs` config |
| `src/main.rs` | Call `backfill_missing_headers()` at startup |

### 5. Configuration

```toml
[cache]
etag_validation_timeout_secs = 5  # default 5, 0 = disable
```

## Error Handling

- Upstream unreachable: serve cached content (degraded mode), log warn
- Upstream returns no ETag: skip validation, serve cached
- MS small file (200 direct, no ETag): skip validation
- ETag mismatch: delete cached chunks, re-download from upstream
- Concurrent request race: first request handles validation; subsequent requests wait for result

## Non-Goals

- No conditional HTTP (If-None-Match) to upstream — full HEAD is acceptable
- No ETag storage format normalization — store and compare as-is
- No ETag-based invalidation for Git/LFS proxy
- No admin API for manual ETag refresh (can add later)
