# ModelScope Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ModelScope as a second upstream hub alongside HuggingFace, with source-aware file identity allowing same-path files from different hubs to coexist.

**Architecture:** Add `MsConfig` struct mirroring `HfConfig`, schema-migrate `files` table to `UNIQUE(name, source)`, propagate a `source: &str` parameter through all metadata/service/server layers, reuse existing handlers parametrized by source.

**Tech Stack:** Rust (stable), rusqlite, axum, tokio, reqwest, clap

---

### Task 1: Add MsConfig struct and wire into Config/CLI

**Files:**
- Modify: `src/config.rs`
- Modify: `src/cli.rs`

- [ ] **Step 1: Add MsConfig struct to config.rs**

In `src/config.rs`, after `HfConfig` (after line 89), add:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsConfig {
    #[serde(default = "default_ms_endpoint")]
    pub endpoint: String,

    pub token: Option<String>,

    pub proxy: Option<String>,

    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}
```

Add default function after `default_hf_endpoint`:

```rust
fn default_ms_endpoint() -> String {
    "https://modelscope.cn".into()
}
```

- [ ] **Step 2: Add modelscope field to Config**

In the `Config` struct, after `pub huggingface: HfConfig` (line 17), add:

```rust
    #[serde(default)]
    pub modelscope: MsConfig,
```

- [ ] **Step 3: Implement Default for MsConfig**

After `impl Default for HfConfig`, add:

```rust
impl Default for MsConfig {
    fn default() -> Self {
        Self {
            endpoint: default_ms_endpoint(),
            token: None,
            proxy: None,
            timeout_secs: default_timeout_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
        }
    }
}
```

- [ ] **Step 4: Add MS env var handling to Config::load**

In `Config::load()`, after the HF env var block (after line 282), add:

```rust
        if let Ok(val) = std::env::var("HUGRS_MS_ENDPOINT") {
            config.modelscope.endpoint = val;
        }
        if let Ok(val) = std::env::var("HUGRS_MS_TOKEN") {
            config.modelscope.token = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_MS_PROXY") {
            config.modelscope.proxy = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_MS_TIMEOUT") {
            config.modelscope.timeout_secs = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_MS_CONNECT_TIMEOUT") {
            config.modelscope.connect_timeout_secs = val.parse()?;
        }
```

- [ ] **Step 5: Add CLI override fields to CliOverrides**

In `src/cli.rs`, in `CliOverrides` struct (after `hf_connect_timeout` at line 190), add:

```rust
    pub ms_endpoint: Option<String>,
    pub ms_token: Option<String>,
    pub ms_proxy: Option<String>,
    pub ms_timeout: Option<u64>,
    pub ms_connect_timeout: Option<u64>,
```

- [ ] **Step 6: Add CLI overrides to Config::load**

In `Config::load()`, after the HF CLI override block (after line 339), add:

```rust
        if let Some(v) = overrides.ms_endpoint {
            config.modelscope.endpoint = v;
        }
        if let Some(v) = overrides.ms_token {
            config.modelscope.token = Some(v);
        }
        if let Some(v) = overrides.ms_proxy {
            config.modelscope.proxy = Some(v);
        }
        if let Some(v) = overrides.ms_timeout {
            config.modelscope.timeout_secs = v;
        }
        if let Some(v) = overrides.ms_connect_timeout {
            config.modelscope.connect_timeout_secs = v;
        }
```

- [ ] **Step 7: Add global CLI args for MS**

In `src/cli.rs`, in the `Cli` struct, after the HF-related args, add:

```rust
    /// ModelScope Hub endpoint URL
    #[arg(long = "ms-endpoint", env = "HUGRS_MS_ENDPOINT")]
    pub ms_endpoint: Option<String>,

    /// ModelScope API token
    #[arg(long = "ms-token", env = "HUGRS_MS_TOKEN")]
    pub ms_token: Option<String>,

    /// HTTP proxy for ModelScope requests
    #[arg(long = "ms-proxy", env = "HUGRS_MS_PROXY")]
    pub ms_proxy: Option<String>,

    /// ModelScope request timeout in seconds (default 60)
    #[arg(long = "ms-timeout", env = "HUGRS_MS_TIMEOUT")]
    pub ms_timeout: Option<u64>,

    /// ModelScope connect timeout in seconds (default 15)
    #[arg(long = "ms-connect-timeout", env = "HUGRS_MS_CONNECT_TIMEOUT")]
    pub ms_connect_timeout: Option<u64>,
```

- [ ] **Step 8: Populate CliOverrides from Cli**

In the `Cli::overrides()` method, add:

```rust
            ms_endpoint: self.ms_endpoint.clone(),
            ms_token: self.ms_token.clone(),
            ms_proxy: self.ms_proxy.clone(),
            ms_timeout: self.ms_timeout,
            ms_connect_timeout: self.ms_connect_timeout,
```

- [ ] **Step 9: Build and check compilation**

Run: `cargo build`
Expected: Compiles successfully.

- [ ] **Step 10: Commit**

```bash
git add src/config.rs src/cli.rs
git commit -m "feat: add MsConfig with CLI and env var support"
```

---

### Task 2: Schema migration — UNIQUE(name, source)

**Files:**
- Modify: `src/metadata.rs`

- [ ] **Step 1: Add migration helper to detect and fix schema**

In `init_schema()`, after the existing `CREATE TABLE IF NOT EXISTS files` block (around line 84), replace the initial schema creation with the new `UNIQUE(name, source)` constraint. The new table DDL becomes:

```rust
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS files (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                name          TEXT NOT NULL,
                repo          TEXT NOT NULL DEFAULT '',
                total_size    INTEGER NOT NULL,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
                source        TEXT NOT NULL,
                UNIQUE(name, source)
            );",
        )?;
```

- [ ] **Step 2: Add migration code to convert old single-unique schema**

After the existing migrations block (after `has_compressed_size` check at line 148), add a migration to check for the old `UNIQUE(name)` constraint:

```rust
        let has_old_unique: bool = {
            conn.prepare(
                "SELECT 1 FROM pragma_index_list('files') WHERE name LIKE '%name' AND \"unique\" = 1 AND origin = 'u'"
            ).map(|mut s| s.exists([]).unwrap_or(false)).unwrap_or(false)
        };
        if has_old_unique {
            conn.execute_batch(
                "CREATE TABLE files_new (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    name          TEXT NOT NULL,
                    repo          TEXT NOT NULL DEFAULT '',
                    total_size    INTEGER NOT NULL,
                    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                    last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
                    source        TEXT NOT NULL DEFAULT 'hf',
                    etag          TEXT,
                    x_repo_commit TEXT,
                    x_linked_size INTEGER,
                    x_linked_etag TEXT,
                    content_type  TEXT,
                    UNIQUE(name, source)
                );
                INSERT INTO files_new (id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type)
                    SELECT id, name, repo, total_size, created_at, last_accessed, COALESCE(NULLIF(source, 'upload'), 'hf'), etag, x_repo_commit, x_linked_size, x_linked_etag, content_type FROM files;
                DROP TABLE files;
                ALTER TABLE files_new RENAME TO files;"
            )?;
        }
```

The migration preserves existing data, converting old source values (`pull`/`upload`) to `"hf"`.

- [ ] **Step 3: Build and check compilation**

Run: `cargo build`
Expected: Compiles successfully.

- [ ] **Step 4: Commit**

```bash
git add src/metadata.rs
git commit -m "refactor: migrate files schema to UNIQUE(name, source)"
```

---

### Task 3: Add source parameter to MetadataStore name-based queries

**Files:**
- Modify: `src/metadata.rs`

- [ ] **Step 1: Update get_file_by_name signature and query**

Change the method signature (line 181) to:

```rust
    pub fn get_file_by_name(&self, name: &str, source: &str) -> anyhow::Result<Option<File>> {
```

Update the SQL query (line 184) to:

```rust
            "SELECT id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type FROM files WHERE name = ?1 AND source = ?2",
```

Update the params (line 186) to:

```rust
        let mut rows = stmt.query_map(params![name, source], |row| {
```

- [ ] **Step 2: Update set_file_headers signature and query**

Change the method signature (line 205) to:

```rust
    pub fn set_file_headers(
        &self,
        name: &str,
        source: &str,
        etag: Option<&str>,
        ...
    ) -> anyhow::Result<()> {
```

Update the SQL (line 216) to:

```rust
            "UPDATE files SET etag = ?1, x_repo_commit = ?2, x_linked_size = ?3, x_linked_etag = ?4, content_type = ?5 WHERE name = ?6 AND source = ?7",
            params![etag, x_repo_commit, x_linked_size, x_linked_etag, content_type, name, source],
```

- [ ] **Step 3: Update delete_file signature and query**

Change the method signature (line 231) to:

```rust
    pub fn delete_file(&self, name: &str, source: &str) -> anyhow::Result<bool> {
```

Update the SQL (line 235) to:

```rust
                "SELECT id FROM files WHERE name = ?1 AND source = ?2",
                params![name, source],
```

- [ ] **Step 4: Commit**

```bash
git add src/metadata.rs
git commit -m "refactor: add source param to MetadataStore name-based queries"
```

---

### Task 4: Update metadata tests for source parameter

**Files:**
- Modify: `tests/metadata_tests.rs`

- [ ] **Step 1: Update get_file_by_name calls**

Change all `store.get_file_by_name("...")` to `store.get_file_by_name("...", "hf")`:

Line 36: `let got = store.get_file_by_name("model.bin", "hf").unwrap();`
Line 124: `let f1 = store.get_file_by_name("f1.bin", "hf").unwrap().unwrap();`
Line 125: `let f2 = store.get_file_by_name("f2.bin", "hf").unwrap().unwrap();`

- [ ] **Step 2: Update delete_file call**

Line 72: `store.delete_file("x.bin", "hf").unwrap();`

- [ ] **Step 3: Add test for same-name-different-source**

Add a new test at the end of the file:

```rust
#[test]
fn test_same_name_different_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store.add_file("model.bin", "repo", 100, "hf").unwrap();
    store.add_file("model.bin", "repo", 200, "ms").unwrap();

    let hf = store.get_file_by_name("model.bin", "hf").unwrap().unwrap();
    let ms = store.get_file_by_name("model.bin", "ms").unwrap().unwrap();

    assert_eq!(hf.total_size, 100);
    assert_eq!(ms.total_size, 200);
    assert_ne!(hf.id, ms.id);
}
```

- [ ] **Step 4: Run metadata tests**

Run: `cargo test metadata`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/metadata_tests.rs
git commit -m "test: update metadata tests for source parameter"
```

---

### Task 5: Add source parameter to CacheService methods

**Files:**
- Modify: `src/service.rs`

- [ ] **Step 1: Update info method**

Change line 447 from:

```rust
    pub async fn info(&self, name: &str) -> anyhow::Result<Option<File>> {
        self.metadata.get_file_by_name(name)
    }
```

To:

```rust
    pub async fn info(&self, name: &str, source: &str) -> anyhow::Result<Option<File>> {
        self.metadata.get_file_by_name(name, source)
    }
```

- [ ] **Step 2: Update delete method**

Change `delete(name)` to `delete(name, source)`, pass source to `self.metadata.delete_file(name, source)`:

```rust
    pub async fn delete(&self, name: &str, source: &str) -> anyhow::Result<bool> {
        self.metadata.delete_file(name, source)
    }
```

- [ ] **Step 3: Update is_file_complete to accept source**

Change line 395-396, pass source to `get_file_by_name`:

```rust
    pub async fn is_file_complete(&self, name: &str, source: &str) -> anyhow::Result<bool> {
        let file = match self.metadata.get_file_by_name(name, source)? {
```

- [ ] **Step 4: Update stream_cached_file to accept source**

Change signature (line 534) to add `source: &str`:

```rust
    pub async fn stream_cached_file(
        &self,
        name: &str,
        source: &str,
        range_start: Option<u64>,
        range_end: Option<u64>,
    ) -> anyhow::Result<(File, u64, ByteStream)> {
```

Update the `get_file_by_name` call (line 543) to use source:

```rust
            .get_file_by_name(name, source)?
```

- [ ] **Step 5: Update stream_from_upstream to accept source**

Change signature (line 721) to add `source: &str`:

```rust
    pub async fn stream_from_upstream(
        &self,
        url: &str,
        name: &str,
        repo: &str,
        source: &str,
        range_start: Option<u64>,
        range_end: Option<u64>,
    ) -> anyhow::Result<(File, u64, ByteStream)> {
```

Update `fetch_file_metadata` call (line 729) and `get_file_by_name` call (line 733) to pass source. Change `fetch_file_metadata` signature to accept `source: &str` and pass through to all internal metadata calls.

- [ ] **Step 6: Update ensure_file_headers to accept source**

Change signature (line 420) to add `source: &str`:

```rust
    pub fn ensure_file_headers(
        &self,
        name: &str,
        repo: &str,
        source: &str,
        total_size: u64,
        ...
    ) -> anyhow::Result<()> {
        if self.metadata.get_file_by_name(name, source)?.is_none() {
            self.metadata.delete_file(name, source)?;
            self.metadata.add_file(name, repo, total_size as i64, source)?;
        }
        self.metadata.set_file_headers(name, source, ...)?;
```

- [ ] **Step 7: Update upload to accept source**

Change `upload(name, repo, data)` to `upload(name, repo, source, data)`, replace `"upload"` with `source`:

```rust
    pub async fn upload(&self, name: &str, repo: &str, source: &str, data: Vec<u8>) -> anyhow::Result<()> {
        ...
        self.metadata.delete_file(name, source)?;
        let file = self.metadata.add_file(name, repo, total_size as i64, source)?;
```

- [ ] **Step 8: Update download_from_url to accept source**

Change signature (line 125), pass source to all internal `get_file_by_name`, `delete_file`, `add_file`, `set_file_headers`, `upload` calls.

- [ ] **Step 9: Update download to accept source**

Change `download(name)` to `download(name, source)`, pass to `get_file_by_name`:

```rust
    pub async fn download(&self, name: &str, source: &str) -> anyhow::Result<Vec<u8>> {
        let file = self.metadata.get_file_by_name(name, source)?
```

- [ ] **Step 10: Update fetch_file_metadata to accept source**

Add `source: &str` param, pass to all `get_file_by_name`, `delete_file`, `add_file`, `set_file_headers` calls within.

- [ ] **Step 11: Update stream_small_file internal call**

In `stream_small_file`, update the `is_file_complete` and `stream_cached_file` and `upload` calls to pass source. Since the source is available from `file.source`, use that:

```rust
        if self.is_file_complete(name, &file.source).await? {
            return self.stream_cached_file(name, &file.source, None, None).await;
        }
```

And in the spawned task:
```rust
            if let Err(e) = svc.upload(&fname, &frepo, &file.source, data.to_vec()).await {
```

- [ ] **Step 12: Commit**

```bash
git add src/service.rs
git commit -m "refactor: add source param to CacheService methods"
```

---

### Task 6: Update service tests for source parameter

**Files:**
- Modify: `tests/service_tests.rs`

- [ ] **Step 1: Update all service test method calls**

Add `"hf"` as source argument to all calls:

- `service.upload("test.bin", "test-repo", "hf", data.to_vec())` (line 31)
- `service.info("test.bin", "hf")` (lines 35, 72, 75)
- `service.download("test.bin", "hf")` (line 40)
- `service.upload("x.bin", "repo-a", "hf", ...)` (line 69)
- `service.delete("x.bin", "hf")` (line 74)
- `service.upload("f.bin", "test-repo", "hf", ...)` (line 106)
- `service.upload("dup.bin", "repo-a", "hf", ...)` (lines 137, 141)
- `service.download("dup.bin", "hf")` (line 145)
- `service.upload("big.bin", "repo-big", "hf", ...)` (line 171)
- `service.upload("small.bin", "repo-small", "hf", ...)` (line 175)
- `service.upload("a.txt", "repo-a", "hf", ...)` (line 207)
- `service.upload("b.txt", "repo-a", "hf", ...)` (line 211)
- `service.upload("c.txt", "repo-b", "hf", ...)` (line 215)

- [ ] **Step 2: Run service tests**

Run: `cargo test service`
Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/service_tests.rs
git commit -m "test: update service tests for source parameter"
```

---

### Task 7: Add source field to FileDownloadSession

**Files:**
- Modify: `src/session.rs`

- [ ] **Step 1: Add source field to FileDownloadSession struct**

After `total_size: u64` (line 247), add:

```rust
    source: String,
```

- [ ] **Step 2: Update new() constructor to accept source**

Add `source: String` parameter (after `total_size`, line 274), store in Self:

```rust
        source,
```

- [ ] **Step 3: Update ensure_file_metadata to use self.source**

In `ensure_file_metadata()` (line 525):

Line 526: `self.metadata.get_file_by_name(&self.name, &self.source)?`
Line 599: `self.metadata.delete_file(&self.name, &self.source)?;`
Line 601: `.add_file(&self.name, &self.repo, size as i64, &self.source)?;`
Line 602: `self.metadata.set_file_headers(&self.name, &self.source, ...)?;`
Line 614: `.get_file_by_name(&self.name, &self.source)?`

- [ ] **Step 4: Update get_or_create to accept and pass source**

In `FileSessionManager::get_or_create` (line 707), add `source: &str` parameter, pass to `FileDownloadSession::new`:

```rust
    pub fn get_or_create(
        &self,
        file_id: i64,
        name: &str,
        repo: &str,
        url: &str,
        source: &str,
        total_size: u64,
        prefetch_budget_base: usize,
    ) -> Arc<FileDownloadSession> {
```

And in the closure:
```rust
                    source: source.to_string(),
```

- [ ] **Step 5: Update service.rs call to get_or_create**

In `src/service.rs`, `stream_from_upstream` method, update the call:

```rust
        let session = self.fs_manager.get_or_create(
            file.id,
            name,
            repo,
            url,
            source,
            total_size,
            self.prefetch_budget_base,
        );
```

- [ ] **Step 6: Update session tests for new constructor param**

In the tests at the bottom of `session.rs` (lines 823, 863), add `"hf".to_string()` as the source argument to `FileDownloadSession::new`.

Line 823:
```rust
        let session = Arc::new(FileDownloadSession::new(
            1,
            "test.bin".to_string(),
            "test/repo".to_string(),
            "http://localhost/test.bin".to_string(),
            crate::service::CHUNK_SIZE as u64,
            "hf".to_string(),
            1,
```

Line 863:
```rust
        let session = FileDownloadSession::new(
            1,
            "test.bin".to_string(),
            "test/repo".to_string(),
            "http://localhost/test.bin".to_string(),
            crate::service::CHUNK_SIZE as u64,
            "hf".to_string(),
            8,
```

- [ ] **Step 7: Build and run session tests**

Run: `cargo test session`
Expected: Compiles and tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/session.rs src/service.rs
git commit -m "refactor: add source field to FileDownloadSession"
```

---

### Task 8: Add MS routes and parametrize handlers in server.rs

**Files:**
- Modify: `src/server.rs`

- [ ] **Step 1: Add MS clients to AppState**

After the existing fields (line 70), add:

```rust
    pub ms_http_client: Arc<reqwest::Client>,
    pub ms_head_client: Arc<reqwest::Client>,
```

- [ ] **Step 2: Add a helper to select endpoint and client by source**

Add a private function near the top of the file (before handlers):

```rust
fn hub_config<'a>(state: &'a AppState, source: &str) -> (&'a str, &'a reqwest::Client, &'a reqwest::Client) {
    match source {
        "ms" => (
            &state.config.modelscope.endpoint,
            &state.ms_http_client,
            &state.ms_head_client,
        ),
        _ => (
            &state.config.huggingface.endpoint,
            &state.http_client,
            &state.head_client,
        ),
    }
}
```

- [ ] **Step 3: Update handle_file_proxy to accept source**

Add `source: &str` as an extra argument alongside State. The handler currently uses `Path<(String, String, String, String)>`. We need to inject the source parameter. The simplest approach: create wrapper functions for HF and MS that call the shared handler.

Add parameterized function (rename existing to accept source via state somehow). Since axum doesn't easily pass extra params, the cleanest approach is: extract `source` from the URL path prefix in a wrapper, or create a closure-based approach using `axum::routing::get(move |...| ...)`.

Actually the most ergonomic approach for axum: write "inner handler" functions that take `state`, `source`, and path params, then create thin wrapper async functions for each route.

The existing `handle_file_proxy` becomes `file_proxy_inner`:

```rust
async fn file_proxy_inner(
    state: AppState,
    source: &str,
    method: Method,
    headers: HeaderMap,
    org: String,
    repo: String,
    revision: String,
    path: String,
) -> Result<Response, AppError> {
```

Move the body from `handle_file_proxy` to `file_proxy_inner`, replacing direct config access with `hub_config(&state, source)` and adding source to all service calls.

- [ ] **Step 4: create source-aware service calls in file_proxy_inner**

In the handler body:
- Replace `state.config.huggingface.endpoint` with `hub_config(&state, source).0`
- Replace `state.head_client` with `hub_config(&state, source).2`
- `service.info(&cache_name, source)`
- `service.ensure_file_headers(&cache_name, &repo_id, source, size, ...)`
- `service.is_file_complete(&cache_name, source)`
- `service.stream_cached_file(&cache_name, source, ...)`
- `service.stream_from_upstream(&url, &cache_name, &repo_id, source, ...)`

- [ ] **Step 5: Create HF and MS wrapper handlers for all routes**

File proxy wrapper (existing `handle_file_proxy` → `file_proxy_inner`):

```rust
async fn file_proxy_inner(
    state: AppState,
    source: &str,
    method: Method,
    headers: HeaderMap,
    org: String,
    repo: String,
    revision: String,
    path: String,
) -> Result<Response, AppError> {
    // body of handle_file_proxy, using hub_config(&state, source)
}

async fn hf_file_proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    file_proxy_inner(state, "hf", method, headers, org, repo, revision, path).await
}

async fn ms_file_proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    file_proxy_inner(state, "ms", method, headers, org, repo, revision, path).await
}
```

Model info wrapper (existing `proxy_model_info` → `model_info_inner`). Note MS uses `/api/v1/models/` while HF uses `/api/models/`, so URL is built from endpoint + source-specific path:

```rust
async fn model_info_inner(
    state: AppState,
    source: &str,
    org: String,
    repo: String,
    revision: String,
) -> Result<Response, AppError> {
    let (endpoint, client, _head) = hub_config(&state, source);
    let repo_id = format!("{}/{}", org, repo);
    let api_prefix = if source == "ms" { "api/v1/models" } else { "api/models" };
    let url = format!("{}/{}/{}/revision/{}", endpoint, api_prefix, repo_id, revision);
    // ... rest of proxy_model_info body using client from hub_config
}

async fn hf_model_info(
    State(state): State<AppState>,
    Path((org, repo, revision)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "hf", org, repo, revision).await
}

async fn hf_model_info_simple(
    State(state): State<AppState>,
    Path((org, repo)): Path<(String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "hf", org, repo, "main".to_string()).await
}

async fn ms_model_info(
    State(state): State<AppState>,
    Path((org, repo, revision)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "ms", org, repo, revision).await
}

async fn ms_model_info_simple(
    State(state): State<AppState>,
    Path((org, repo)): Path<(String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "ms", org, repo, "main".to_string()).await
}
```

Model API path wrappers (`handle_api_proxy_suffix` → `model_api_path_inner`):

```rust
async fn model_api_path_inner(
    state: AppState,
    source: &str,
    org: String,
    repo: String,
    suffix: String,
    query: Option<String>,
) -> Result<Response, AppError> {
    let (endpoint, client, _head) = hub_config(&state, source);
    let repo_id = format!("{}/{}", org, repo);
    let mut url = format!("{}/api/models/{}/{}", endpoint, repo_id, suffix);
    // MS uses /api/v1/models/ not /api/models/
    if source == "ms" {
        url = format!("{}/api/v1/models/{}/{}", endpoint, repo_id, suffix);
    }
    if let Some(query) = query {
        url.push('?');
        url.push_str(&query);
    }
    proxy_json(&state, source, &url).await
}

async fn hf_model_api_suffix(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Path((org, repo, suffix)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_api_path_inner(state, "hf", org, repo, suffix, uri.query().map(|s| s.to_string())).await
}

async fn ms_model_api_suffix(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Path((org, repo, suffix)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_api_path_inner(state, "ms", org, repo, suffix, uri.query().map(|s| s.to_string())).await
}
```

- [ ] **Step 6: Update proxy_json helper**

Make `proxy_json` source-aware, use `hub_config` for token/endpoint:

```rust
async fn proxy_json(state: &AppState, source: &str, url: &str) -> Result<Response, AppError> {
    let mut req = state.http_client.get(url); // Use appropriate client from hub_config
    let token = match source {
        "ms" => &state.config.modelscope.token,
        _ => &state.config.huggingface.token,
    };
    if let Some(ref token) = token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    ...
}
```

- [ ] **Step 7: Add /hf/ and /ms/ all routes to Router**

```rust
    // Legacy unprefixed (backward compat, source="hf")
    .route("/api/models/{org}/{repo}", get(hf_model_info_simple).head(hf_model_info_simple))
    .route("/api/models/{org}/{repo}/revision/{revision}", get(hf_model_info).head(hf_model_info))
    .route("/api/models/{org}/{repo}/{*suffix}", get(hf_model_api_suffix))
    .route("/{org}/{repo}/resolve/{revision}/{*path}", get(hf_file_proxy).head(hf_file_proxy))
    .route("/api/resolve-cache/{repo_type}/{org}/{repo}/{revision}/{*path}", get(hf_file_proxy).head(hf_file_proxy))
    // New /hf/ prefix (source="hf")
    .route("/hf/api/models/{org}/{repo}", get(hf_model_info_simple).head(hf_model_info_simple))
    .route("/hf/api/models/{org}/{repo}/revision/{revision}", get(hf_model_info).head(hf_model_info))
    .route("/hf/api/models/{org}/{repo}/{*suffix}", get(hf_model_api_suffix))
    .route("/hf/{org}/{repo}/resolve/{revision}/{*path}", get(hf_file_proxy).head(hf_file_proxy))
    // New /ms/ prefix (source="ms")
    .route("/ms/api/v1/models/{org}/{repo}", get(ms_model_info_simple).head(ms_model_info_simple))
    .route("/ms/api/v1/models/{org}/{repo}/revision/{revision}", get(ms_model_info).head(ms_model_info))
    .route("/ms/api/v1/models/{org}/{repo}/{*suffix}", get(ms_model_api_suffix))
    .route("/ms/{org}/{repo}/resolve/{revision}/{*path}", get(ms_file_proxy).head(ms_file_proxy))
```

- [ ] **Step 8: Update root handler to include MS endpoint**

```rust
async fn root(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    Ok(Json(serde_json::json!({
        "service": "hugrs",
        "version": env!("CARGO_PKG_VERSION"),
        "hf_endpoint": state.config.huggingface.endpoint,
        "ms_endpoint": state.config.modelscope.endpoint,
    })))
}
```

- [ ] **Step 9: Build and check compilation**

```bash
git add src/server.rs
git commit -m "feat: add ModelScope routes and paramerize handlers by source"
```

---

### Task 9: Wire MS clients in main.rs

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Build MS clients**

After the existing HF client build calls (around line 52-54), add:

```rust
    let ms_http_client = {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(config.modelscope.connect_timeout_secs))
            .timeout(std::time::Duration::from_secs(config.modelscope.timeout_secs));
        if let Some(ref proxy) = config.modelscope.proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy)?);
        }
        builder.build()?
    };
    let ms_head_client = {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(config.modelscope.connect_timeout_secs))
            .timeout(std::time::Duration::from_secs(config.modelscope.timeout_secs))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(ref proxy) = config.modelscope.proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy)?);
        }
        builder.build()?
    };
```

- [ ] **Step 2: Pass MS clients to server::run**

Add `ms_http_client` and `ms_head_client` to the `server::run()` call parameters. Update `server::run` signature to accept them, and set in AppState:

```rust
        ms_http_client: Arc::new(ms_http_client),
        ms_head_client: Arc::new(ms_head_client),
```

- [ ] **Step 3: Update server::run signature**

In `src/server.rs`, change `pub async fn run(config: Config, service: CacheService)` to:

```rust
pub async fn run(config: Config, service: CacheService, ms_http_client: reqwest::Client, ms_head_client: reqwest::Client) -> anyhow::Result<()> {
```

And set them:
```rust
        ms_http_client: Arc::new(ms_http_client),
        ms_head_client: Arc::new(ms_head_client),
```

- [ ] **Step 4: Build**

Run: `cargo build`
Expected: Compiles successfully.

- [ ] **Step 5: Commit**

```bash
git add src/main.rs src/server.rs
git commit -m "feat: wire MS clients into server startup"
```

---

### Task 10: Update e2e tests for source parameter

**Files:**
- Modify: `tests/e2e_tests.rs`

- [ ] **Step 1: Update stream_from_upstream calls**

Lines 206, 261, 286, 296: Add `"hf"` as the 4th argument:

```rust
    .stream_from_upstream(&url, "cfg.json", "org/repo", "hf", None, None)
    .stream_from_upstream(&url, "big.bin", "org/repo", "hf", None, None)
    .stream_from_upstream(&url, n, "org/repo", "hf", None, None)
```

- [ ] **Step 2: Run e2e tests**

Run: `cargo test e2e`
Expected: All tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e_tests.rs
git commit -m "test: update e2e tests for source parameter"
```

---

### Task 11: Update CLI and HF client calls for source parameter

**Files:**
- Modify: `src/main.rs`
- Modify: `src/hf.rs`

- [ ] **Step 1: Update service.info call in main.rs**

Line 86: `service.info(&name)` → `service.info(&name, "hf")`

- [ ] **Step 2: Update download_from_url call in hf.rs**

Line 57: `service.download_from_url(&download_url, &sibling.rfilename, repo_id, 8)` → add `"hf"` as 4th arg before concurrency:

```rust
    service.download_from_url(&download_url, &sibling.rfilename, repo_id, "hf", 8)
```

- [ ] **Step 3: Build**

Run: `cargo build`
Expected: Compiles successfully.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs src/hf.rs
git commit -m "fix: pass source param to CLI and hf client calls"
```

---

### Task 12: Run full test suite and Clippy

**Files:** None (verification only)

- [ ] **Step 1: Run all tests**

Run: `cargo test`
Expected: All tests pass.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings.

- [ ] **Step 3: Run fmt check**

Run: `cargo fmt -- --check`
Expected: All files properly formatted.

- [ ] **Step 4: Commit any formatting fixes**

```bash
git add . && git commit -m "chore: clippy and fmt" || true
```
