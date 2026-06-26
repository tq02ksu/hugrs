# User-Agent Pass-Through Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Forward inbound `User-Agent` headers on HuggingFace and ModelScope file proxy requests, including redirect follow-ups and chunk downloads.

**Architecture:** Extract `Option<String>` for `User-Agent` in the file-facing server handlers, thread it explicitly through `server.rs`, `service.rs`, and `session.rs`, and conditionally attach it to every reqwest file request spawned on behalf of that incoming request. Keep metadata probing, caching, and non-file API behavior unchanged.

**Tech Stack:** Rust (stable), axum, tokio, reqwest, tower, tempfile

---

### Task 1: Add regression tests for User-Agent forwarding

**Files:**
- Modify: `tests/e2e_tests.rs`
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Extend the mock upstream state to record User-Agent values**

In `tests/e2e_tests.rs`, update `MockState` so the test server can record which `User-Agent` values it receives on `HEAD`, `GET`, and range requests:

```rust
#[derive(Clone)]
struct MockState {
    data: Arc<Vec<u8>>,
    get_count: Arc<AtomicU32>,
    user_agents: Arc<std::sync::Mutex<Vec<String>>>,
}
```

Update `start_upstream()` to initialize the new field:

```rust
let state = MockState {
    data: Arc::new(data),
    get_count: Arc::new(AtomicU32::new(0)),
    user_agents: Arc::new(std::sync::Mutex::new(Vec::new())),
};
```

- [ ] **Step 2: Record User-Agent inside the mock HEAD and GET handlers**

In `tests/e2e_tests.rs`, change the mock handlers to inspect request headers and push the observed `User-Agent` into `MockState.user_agents`:

```rust
async fn mock_head(State(s): State<MockState>, headers: HeaderMap) -> Response {
    if let Some(ua) = headers.get("user-agent").and_then(|v| v.to_str().ok()) {
        s.user_agents.lock().unwrap().push(ua.to_string());
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", s.data.len())
        .header("ETag", "\"mock-etag\"")
        .header("X-Repo-Commit", "abc123mock")
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::empty())
        .unwrap()
}
```

At the top of `mock_get`, add:

```rust
if let Some(ua) = headers.get("user-agent").and_then(|v| v.to_str().ok()) {
    s.user_agents.lock().unwrap().push(ua.to_string());
}
```

- [ ] **Step 3: Add a failing test for forwarded User-Agent on HEAD and GET**

Append this test to `tests/e2e_tests.rs`:

```rust
#[tokio::test]
async fn test_file_proxy_forwards_inbound_user_agent() {
    let test_data: Vec<u8> = b"hello from upstream".to_vec();
    let (upstream, state) = start_upstream(test_data).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let head_req = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/cfg.json")
        .header("User-Agent", "ua-forward-test/1.0")
        .body(axum::body::Body::empty())
        .unwrap();
    let head_resp = app.clone().oneshot(head_req).await.unwrap();
    assert!(head_resp.status().is_success());

    let get_req = axum::http::Request::builder()
        .method("GET")
        .uri("/org/repo/resolve/main/cfg.json")
        .header("User-Agent", "ua-forward-test/1.0")
        .body(axum::body::Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_req).await.unwrap();
    assert!(get_resp.status().is_success());

    let seen = state.user_agents.lock().unwrap().clone();
    assert!(seen.iter().any(|ua| ua == "ua-forward-test/1.0"));
}
```

- [ ] **Step 4: Add a failing test for range/chunk downloads**

Append this test to `tests/e2e_tests.rs`:

```rust
#[tokio::test]
async fn test_chunk_downloads_forward_inbound_user_agent() {
    let test_data = vec![7u8; CHUNK_SIZE + 128];
    let (upstream, state) = start_upstream(test_data).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let get_req = axum::http::Request::builder()
        .method("GET")
        .uri("/org/repo/resolve/main/big.bin")
        .header("User-Agent", "ua-range-test/1.0")
        .body(axum::body::Body::empty())
        .unwrap();
    let get_resp = app.oneshot(get_req).await.unwrap();
    assert!(get_resp.status().is_success());

    let _body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();

    let seen = state.user_agents.lock().unwrap().clone();
    assert!(seen.iter().any(|ua| ua == "ua-range-test/1.0"));
}
```

- [ ] **Step 5: Add a failing test proving no User-Agent is synthesized**

Append this test to `tests/e2e_tests.rs`:

```rust
#[tokio::test]
async fn test_file_proxy_does_not_invent_user_agent() {
    let test_data: Vec<u8> = b"no ua request".to_vec();
    let (upstream, state) = start_upstream(test_data).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let get_req = axum::http::Request::builder()
        .method("GET")
        .uri("/org/repo/resolve/main/no-ua.bin")
        .body(axum::body::Body::empty())
        .unwrap();
    let get_resp = app.oneshot(get_req).await.unwrap();
    assert!(get_resp.status().is_success());

    let seen = state.user_agents.lock().unwrap().clone();
    assert!(seen.is_empty());
}
```

- [ ] **Step 6: Run the new tests to verify they fail for the right reason**

Run: `cargo test --test e2e_tests -- --nocapture`

Expected: The new `test_file_proxy_forwards_inbound_user_agent` and `test_chunk_downloads_forward_inbound_user_agent` assertions fail because upstream requests currently do not preserve the inbound `User-Agent`.

- [ ] **Step 7: Commit the red test state**

```bash
git add tests/e2e_tests.rs
git commit -m "test: cover user-agent pass-through for file proxy"
```

---

### Task 2: Thread User-Agent through server and service file paths

**Files:**
- Modify: `src/server.rs`
- Modify: `src/service.rs`
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Add a helper to extract inbound User-Agent from request headers**

In `src/server.rs`, near `parse_range`, add:

```rust
fn forwarded_user_agent(headers: &HeaderMap) -> Option<String> {
    headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
```

- [ ] **Step 2: Update file route handlers to pass the optional User-Agent**

In `src/server.rs`, capture the `User-Agent` before passing `headers` deeper:

```rust
let user_agent = forwarded_user_agent(&headers);
file_proxy_inner(state, "hf", url, cache_name, method, headers, path, user_agent).await
```

Apply the same pattern to:

- `hf_file_proxy`
- `ms_file_proxy`
- `ms_repo_file_proxy`

For the `HEAD` branch inside `ms_repo_file_proxy`, update the call to:

```rust
return file_proxy_inner(state, "ms", url, cache_name, method, headers, file_path, user_agent).await;
```

- [ ] **Step 3: Extend `file_proxy_inner` to accept the optional User-Agent**

Change the signature in `src/server.rs` to:

```rust
async fn file_proxy_inner(
    state: AppState,
    source: &str,
    url: String,
    cache_name: String,
    method: Method,
    headers: HeaderMap,
    path: String,
    user_agent: Option<String>,
) -> Result<Response, AppError> {
```

Use `user_agent.as_deref()` when calling service methods.

- [ ] **Step 4: Attach the forwarded User-Agent to direct upstream requests in `server.rs`**

In `ms_repo_file_proxy`, update the metadata HEAD request and passthrough GET request builders:

```rust
let mut req = head_client.head(&url);
if let Some(ref ua) = user_agent {
    req = req.header("User-Agent", ua);
}
match req.send().await {
```

For the redirect follow-up HEAD:

```rust
let mut redirect_req = http_client.head(&redirect_url);
if let Some(ref ua) = user_agent {
    redirect_req = redirect_req.header("User-Agent", ua);
}
match redirect_req.send().await {
```

For the passthrough GET:

```rust
let mut req = http_client.get(&url);
if let Some(ref ua) = user_agent {
    req = req.header("User-Agent", ua);
}
```

- [ ] **Step 5: Attach the forwarded User-Agent to HEAD metadata requests in `file_proxy_inner`**

In the `method == Method::HEAD` branch of `file_proxy_inner`, replace the current request creation with:

```rust
let mut req = head_client.head(&url);
if let Some(ref ua) = user_agent {
    req = req.header("User-Agent", ua);
}
let resp = req.send().await.map_err(|e| AppError::Anyhow(e.into()))?;
```

For the redirect follow-up inside that same branch:

```rust
let mut req2 = http_client.head(location);
if let Some(ref ua) = user_agent {
    req2 = req2.header("User-Agent", ua);
}
match req2.send().await {
```

- [ ] **Step 6: Extend `CacheService::stream_from_upstream` and helpers to accept the optional User-Agent**

In `src/service.rs`, update these signatures:

```rust
pub async fn stream_from_upstream(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    source: &str,
    range_start: Option<u64>,
    range_end: Option<u64>,
    user_agent: Option<&str>,
) -> anyhow::Result<(File, u64, ByteStream)>
```

```rust
async fn fetch_file_metadata(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    source: &str,
    user_agent: Option<&str>,
) -> anyhow::Result<(
    u64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
)>
```

```rust
async fn stream_small_file(
    &self,
    url: &str,
    name: &str,
    file: &File,
    user_agent: Option<&str>,
) -> anyhow::Result<(File, u64, ByteStream)>
```

Update the internal calls accordingly.

- [ ] **Step 7: Conditionally add User-Agent in service-level request builders**

In `fetch_file_metadata`, change both request sites to build a mutable `RequestBuilder` first:

```rust
let mut req = self.head_client.head(url);
if let Some(ua) = user_agent {
    req = req.header("User-Agent", ua);
}
let head_resp = req.send().await?;
```

And for the redirect follow-up:

```rust
let mut req = self.http_client.head(&location);
if let Some(ua) = user_agent {
    req = req.header("User-Agent", ua);
}
match req.send().await {
```

In `stream_small_file`, update the spawned GET request to:

```rust
let user_agent = user_agent.map(str::to_string);
tokio::spawn(async move {
    let mut req = client.get(&url);
    if let Some(ref ua) = user_agent {
        req = req.header("User-Agent", ua);
    }
    let resp = match req.send().await {
```

- [ ] **Step 8: Pass the optional User-Agent from `server.rs` into `stream_from_upstream`**

In `src/server.rs`, update the call site to:

```rust
let (file, content_length, stream) = service
    .stream_from_upstream(
        &url,
        &cache_name,
        &repo_id,
        source,
        range.map(|r| r.0),
        range.and_then(|r| r.1),
        user_agent.as_deref(),
    )
    .await?;
```

- [ ] **Step 9: Run the focused e2e tests to verify the direct file requests now pass**

Run: `cargo test --test e2e_tests test_file_proxy_forwards_inbound_user_agent test_file_proxy_does_not_invent_user_agent -- --nocapture`

Expected: The top-level forwarding tests pass. The chunk/range test may still fail until session-based downloads are updated.

- [ ] **Step 10: Commit the server/service pass-through work**

```bash
git add src/server.rs src/service.rs tests/e2e_tests.rs
git commit -m "feat: pass through user-agent for file proxy requests"
```

---

### Task 3: Thread User-Agent through session-based downloads

**Files:**
- Modify: `src/session.rs`
- Modify: `src/service.rs`
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Extend `SessionTable::subscribe` and `download_and_store` to accept User-Agent**

In `src/session.rs`, change the signatures to:

```rust
pub async fn subscribe(
    &self,
    file_id: i64,
    chunk_idx: i64,
    url: &str,
    start: u64,
    end: u64,
    total_size: u64,
    chunk_count: usize,
    user_agent: Option<&str>,
) -> broadcast::Receiver<Arc<Bytes>>
```

```rust
async fn download_and_store(
    client: reqwest::Client,
    backend: Arc<dyn StorageBackend>,
    metadata: Arc<MetadataStore>,
    url: String,
    fetched_bytes: Arc<AtomicU64>,
    file_id: i64,
    chunk_idx: i64,
    start: u64,
    end: u64,
    _total_size: u64,
    chunk_count: usize,
    user_agent: Option<String>,
) -> anyhow::Result<Option<Bytes>>
```

Pass `user_agent.map(str::to_string)` from `subscribe` into `download_and_store`.

- [ ] **Step 2: Add conditional User-Agent header to chunk download requests**

In `download_and_store`, replace the current request creation with:

```rust
let range_header = format!("bytes={}-{}", start, end);
let mut req = client.get(&url).header("Range", &range_header);
if let Some(ref ua) = user_agent {
    req = req.header("User-Agent", ua);
}
let data = req
    .send()
    .await
    .map_err(|e| anyhow::anyhow!("chunk {} request error: {}", chunk_idx, e))?
    .bytes()
    .await
    .map_err(|e| anyhow::anyhow!("chunk {} download error: {}", chunk_idx, e))?;
```

- [ ] **Step 3: Store the optional User-Agent on `FileDownloadSession`**

In `src/session.rs`, add a field to `FileDownloadSession`:

```rust
    user_agent: Option<String>,
```

Update `FileDownloadSession::new` to accept and store it:

```rust
fn new(
    file_id: i64,
    name: String,
    repo: String,
    url: String,
    total_size: u64,
    source: String,
    chunk_count: usize,
    user_agent: Option<String>,
    session_table: Arc<SessionTable>,
    metadata: Arc<MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    head_client: reqwest::Client,
    served_bytes: Arc<AtomicU64>,
    prefetch_budget_base: usize,
) -> Self
```

- [ ] **Step 4: Store the optional User-Agent in `FileSessionManager::get_or_create`**

In `src/session.rs`, update the manager signature to:

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
    user_agent: Option<&str>,
) -> Arc<FileDownloadSession>
```

Pass `user_agent.map(str::to_string)` into `FileDownloadSession::new`.

In `src/service.rs`, update the call site inside `stream_from_upstream`:

```rust
let session = self.fs_manager.get_or_create(
    file.id,
    name,
    repo,
    url,
    source,
    total_size,
    self.prefetch_budget_base,
    user_agent,
);
```

- [ ] **Step 5: Forward User-Agent in session metadata HEAD requests**

In `FileDownloadSession::ensure_file_metadata`, update both request builders:

```rust
let mut req = self.head_client.head(&self.url);
if let Some(ref ua) = self.user_agent {
    req = req.header("User-Agent", ua);
}
let head_resp = req.send().await?;
```

And for the redirect follow-up:

```rust
let mut req2 = self.head_client.head(&location);
if let Some(ref ua) = self.user_agent {
    req2 = req2.header("User-Agent", ua);
}
let resp2 = req2.send().await?;
```

- [ ] **Step 6: Forward User-Agent in active and prefetch chunk subscriptions**

In both `session_table.subscribe(...)` call sites inside `run_download_loop`, append `self.user_agent.as_deref()`:

```rust
let mut rx = self
    .session_table
    .subscribe(
        self.file_id,
        i as i64,
        &self.url,
        start,
        end,
        self.total_size,
        self.chunk_count,
        self.user_agent.as_deref(),
    )
    .await;
```

Apply the same change to the prefetch path.

- [ ] **Step 7: Run the focused chunk/range test to verify it now passes**

Run: `cargo test --test e2e_tests test_chunk_downloads_forward_inbound_user_agent -- --nocapture`

Expected: PASS.

- [ ] **Step 8: Commit the session-layer pass-through work**

```bash
git add src/session.rs src/service.rs tests/e2e_tests.rs
git commit -m "feat: forward user-agent through chunk downloads"
```

---

### Task 4: Verify the full file proxy behavior

**Files:**
- Modify: `tests/e2e_tests.rs` (only if verification reveals missing coverage)
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Run the complete e2e test file**

Run: `cargo test --test e2e_tests -- --nocapture`

Expected: All existing and new e2e tests pass.

- [ ] **Step 2: Run the focused service tests as a regression check**

Run: `cargo test --test service_tests -- --nocapture`

Expected: All service tests pass.

- [ ] **Step 3: Run a full build**

Run: `cargo build`

Expected: Build succeeds with no compile errors.

- [ ] **Step 4: Commit the verification state**

```bash
git add tests/e2e_tests.rs src/server.rs src/service.rs src/session.rs
git commit -m "test: verify user-agent pass-through behavior"
```
