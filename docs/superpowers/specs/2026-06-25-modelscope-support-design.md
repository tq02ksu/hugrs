# ModelScope Support Design

## Overview

Add ModelScope as a second upstream source alongside HuggingFace. Files are cached independently per source — the same file path `org/repo/file.bin` may have different content from HF vs MS.

Request URL pattern: `http://localhost:3000/ms/api/v1/models/{org}/{repo}/...`

## Core Principle: Source-Aware File Identity

`files` table unique constraint changes from `UNIQUE(name)` → `UNIQUE(name, source)`. All lookups, caching, and metadata operations become source-aware. The `source` column (existing, previously unused for identity) now distinguishes:

| source | Meaning |
|--------|---------|
| `"hf"` | HuggingFace upstream |
| `"ms"` | ModelScope upstream |

## Component Changes

### 1. Schema Migration (`src/metadata.rs`)

- Detect if old `UNIQUE(name)` schema exists (check for `UNIQUE(name, source)` constraint)
- If old: create `files_new` with `UNIQUE(name, source)`, copy data (set source="hf" for existing rows), swap tables
- If fresh install: create table directly with `UNIQUE(name, source)`

Methods gaining `source: &str` parameter:
- `get_file_by_name(name)` → `get_file_by_name(name, source)` — `WHERE name = ?1 AND source = ?2`
- `set_file_headers(name, ...)` → `set_file_headers(name, source, ...)` — `WHERE name = ?1 AND source = ?2`
- `delete_file(name)` → `delete_file(name, source)` — `WHERE name = ?1 AND source = ?2`

`add_file(name, repo, total_size, source)` — already has source, no signature change.

### 2. Config (`src/config.rs`)

```rust
pub struct MsConfig {
    pub endpoint: String,              // default "https://modelscope.cn"
    pub token: Option<String>,
    pub proxy: Option<String>,
    pub timeout_secs: u64,             // default 60
    pub connect_timeout_secs: u64,     // default 15
}
```

Env vars: `HUGRS_MS_ENDPOINT`, `HUGRS_MS_TOKEN`, `HUGRS_MS_PROXY`, `HUGRS_MS_TIMEOUT`, `HUGRS_MS_CONNECT_TIMEOUT`.

Top-level `Config` gains `pub modelscope: MsConfig` field.

### 3. CLI (`src/cli.rs`)

`CliOverrides` gains: `ms_endpoint`, `ms_token`, `ms_timeout_secs`, `ms_connect_timeout_secs` (all `Option<T>`).

### 4. Service (`src/service.rs`)

Methods gaining `source: &str` parameter:
- `info(name)` → `info(name, source)`
- `delete(name)` → `delete(name, source)`
- `ensure_file_headers(name, repo, ...)` → `ensure_file_headers(name, repo, source, ...)`
- `is_file_complete(name)` → `is_file_complete(name, source)`
- `stream_cached_file(name, ...)` → `stream_cached_file(name, source, ...)`
- `stream_from_upstream(url, name, repo, ...)` → `stream_from_upstream(url, name, repo, source, ...)`
- `download_from_url(url, name, repo, ...)` → `download_from_url(url, name, repo, source, ...)`

Internal method `fetch_file_metadata` gains `source: &str`. All metadata calls pass the source through.

`upload()` — `source` passed from caller instead of hardcoded.

### 5. Server (`src/server.rs`)

**AppState** gains MS clients:
```rust
pub struct AppState {
    pub service: Arc<Mutex<CacheService>>,
    pub config: Arc<Config>,
    pub http_client: Arc<reqwest::Client>,      // HF
    pub head_client: Arc<reqwest::Client>,      // HF (no redirect)
    pub ms_http_client: Arc<reqwest::Client>,   // MS
    pub ms_head_client: Arc<reqwest::Client>,   // MS (no redirect)
}
```

**Source-based dispatching**: each handler selects endpoint and HTTP client by source string:

| source | endpoint | http_client | head_client |
|--------|----------|-------------|-------------|
| `"hf"` | `config.huggingface.endpoint` | `state.http_client` | `state.head_client` |
| `"ms"` | `config.modelscope.endpoint` | `state.ms_http_client` | `state.ms_head_client` |

**Routes** (MS, under `/ms/` prefix):

```
GET/HEAD /ms/api/v1/models/{org}/{repo}
GET/HEAD /ms/api/v1/models/{org}/{repo}/revision/{rev}
GET       /ms/api/v1/models/{org}/{repo}/{*suffix}
GET/HEAD /ms/{org}/{repo}/resolve/{rev}/{*path}
```

Each MS route calls the same handler as the HF equivalent, passing `source="ms"`. Existing HF routes pass `source="hf"`.

### 6. Main (`src/main.rs`)

Build MS `reqwest::Client` instances using `MsConfig` (paralleling how HF clients are built from `HfConfig`). Pass to `AppState`.

## Files Changed

| File | Changes |
|------|---------|
| `src/config.rs` | +`MsConfig` struct, +`modelscope` field on `Config` |
| `src/cli.rs` | +`ms_*` fields on `CliOverrides` |
| `src/metadata.rs` | Schema migration, +`source` param on name-based queries |
| `src/service.rs` | +`source` param on all file operations |
| `src/server.rs` | +MS clients in AppState, +`/ms/` routes, handler parameterization |
| `src/main.rs` | MS client initialization, AppState wiring |

## Documentation

| File | Changes |
|------|---------|
| `docs/CONFIG.md` | + `[modelscope]` section with env vars, CLI flags, templates |
| `docs/CONFIG_zh.md` | + `[modelscope]` section (Chinese) |
| `README.md` | + ModelScope feature in highlights, quick start |
| `README_zh.md` | + ModelScope feature in highlights, quick start (Chinese) |

No new source files.
