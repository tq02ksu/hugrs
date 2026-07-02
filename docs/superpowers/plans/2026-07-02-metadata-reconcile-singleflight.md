# Metadata Reconcile Singleflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace boolean `ETag`-only metadata reuse with provider-neutral metadata reconcile logic and add per-file singleflight so concurrent `HEAD` metadata probes share one upstream lookup and one metadata update lifecycle.

**Architecture:** Keep provider-specific probe transport in the existing metadata fetch layer, but move `HEAD` freshness decisions into a single reconcile entry point in `CacheService`. Guard the entire reconcile lifecycle with a per-file inflight map keyed by `source + cache_name`, and have both leader and waiters read final metadata from SQLite after reconcile completes.

**Tech Stack:** Rust, tokio, axum, reqwest, rusqlite, tempfile test fixtures

---

## File Structure

- Modify: `src/service.rs`
  - Replace `validate_file_etag(...)`-centric metadata reuse with `reconcile_file_metadata(...)`
  - Add per-file inflight singleflight coordination
  - Keep provider-specific probe method selection in the existing fetch path
- Modify: `src/server.rs`
  - Update `HEAD` handling to use reconcile instead of boolean `ETag` validation
  - Ensure leader and waiters both build responses from DB-backed final metadata
- Modify: `tests/etag_tests.rs`
  - Replace old boolean validation tests with reconcile-focused unit tests
  - Add same-`ETag`/different-commit coverage
- Modify: `tests/e2e_tests.rs`
  - Add request-level concurrency and one-upstream-probe coverage
  - Add ModelScope first-hop `GET` reconcile coverage if not better placed elsewhere
- Optional Modify: `tests/service_tests.rs`
  - Add small helper coverage if direct service-level reconcile tests are cleaner there than in `etag_tests.rs`

### Task 1: Define Reconcile Semantics in Tests

**Files:**
- Modify: `tests/etag_tests.rs`
- Test: `tests/etag_tests.rs`

- [ ] **Step 1: Write the failing test for same `ETag`, different commit**

Add a new mock state field for commit and return it from `mock_head`.

```rust
#[derive(Clone)]
struct MockState {
    head_count: Arc<AtomicU32>,
    etag: Arc<Mutex<String>>,
    commit: Arc<Mutex<String>>,
    test_data: Arc<Vec<u8>>,
}

async fn mock_head(State(s): State<MockState>) -> Response {
    s.head_count.fetch_add(1, Ordering::SeqCst);
    let etag = s.etag.lock().unwrap().clone();
    let commit = s.commit.lock().unwrap().clone();
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", s.test_data.len())
        .header("ETag", &etag)
        .header("Content-Type", "application/octet-stream")
        .header("X-Repo-Commit", &commit)
        .body(axum::body::Body::empty())
        .unwrap()
}

#[tokio::test]
async fn test_reconcile_same_etag_updates_commit() {
    let data = vec![0u8; 1024];
    let s = MockState {
        head_count: Arc::new(AtomicU32::new(0)),
        etag: Arc::new(Mutex::new("\"same-etag\"".into())),
        commit: Arc::new(Mutex::new("newcommit123".into())),
        test_data: Arc::new(data.clone()),
    };
    let app = Router::new()
        .route("/resolve/main/t.bin", head(mock_head))
        .route("/resolve/main/t.bin", get(mock_get))
        .with_state(s);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let tmp = TempDir::new().unwrap();
    let svc = make_svc(&tmp);
    let url = format!("http://{}/resolve/main/t.bin", addr);
    seed_file(&svc, "t.bin", "test-repo", "hf", &data, "\"same-etag\"").await;
    svc.metadata
        .set_file_headers(
            "t.bin",
            "hf",
            Some("\"same-etag\""),
            Some("oldcommit456"),
            None,
            None,
            Some("application/octet-stream"),
        )
        .unwrap();

    svc.reconcile_file_metadata(&url, "t.bin", "test-repo", "hf", None)
        .await
        .unwrap();

    let file = svc.metadata.get_file_by_name("t.bin", "hf").unwrap().unwrap();
    assert_eq!(file.etag.as_deref(), Some("\"same-etag\""));
    assert_eq!(file.x_repo_commit.as_deref(), Some("newcommit123"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test test_reconcile_same_etag_updates_commit --test etag_tests`

Expected: FAIL because `reconcile_file_metadata(...)` does not exist yet and current validation logic cannot express this behavior.

- [ ] **Step 3: Write the failing test for changed `ETag` invalidation**

Add a second test that proves changed `ETag` deletes stale file metadata and writes the latest upstream values.

```rust
#[tokio::test]
async fn test_reconcile_changed_etag_rebuilds_metadata() {
    let data = vec![1u8; 1024];
    let s = MockState {
        head_count: Arc::new(AtomicU32::new(0)),
        etag: Arc::new(Mutex::new("\"new-etag\"".into())),
        commit: Arc::new(Mutex::new("commit789".into())),
        test_data: Arc::new(data.clone()),
    };
    let app = Router::new()
        .route("/resolve/main/t.bin", head(mock_head))
        .route("/resolve/main/t.bin", get(mock_get))
        .with_state(s);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let tmp = TempDir::new().unwrap();
    let svc = make_svc(&tmp);
    let url = format!("http://{}/resolve/main/t.bin", addr);
    seed_file(&svc, "t.bin", "test-repo", "hf", &data, "\"old-etag\"").await;

    svc.reconcile_file_metadata(&url, "t.bin", "test-repo", "hf", None)
        .await
        .unwrap();

    let file = svc.metadata.get_file_by_name("t.bin", "hf").unwrap().unwrap();
    assert_eq!(file.etag.as_deref(), Some("\"new-etag\""));
    assert_eq!(file.x_repo_commit.as_deref(), Some("commit789"));
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test --test etag_tests`

Expected: FAIL because the reconcile API and reconcile semantics are not implemented yet.

- [ ] **Step 5: Commit the failing tests**

```bash
git add tests/etag_tests.rs
git commit -m "test: define metadata reconcile semantics"
```

### Task 2: Implement Provider-Neutral Reconcile in CacheService

**Files:**
- Modify: `src/service.rs`
- Test: `tests/etag_tests.rs`

- [ ] **Step 1: Add reconcile result and inflight coordination types**

Near the `CacheService` definition, add a small inflight map and a per-key waiter mechanism. Keep it minimal and local to metadata reconcile.

```rust
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::{mpsc, oneshot, Mutex};

type ReconcileWaiter = oneshot::Sender<anyhow::Result<()>>;

#[derive(Default)]
struct MetadataInflight {
    leaders: HashMap<String, Vec<ReconcileWaiter>>,
}
```

Extend `CacheService` with:

```rust
metadata_inflight: Arc<Mutex<MetadataInflight>>,
```

and initialize it in `CacheService::new(...)`.

- [ ] **Step 2: Add a low-level reconcile worker that performs one complete lifecycle**

Implement a helper that does not know about waiters, only the reconcile rules.

```rust
async fn reconcile_file_metadata_inner(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    source: &str,
    user_agent: Option<&str>,
) -> anyhow::Result<()> {
    let (size, etag, content_type, x_repo_commit, x_linked_size, x_linked_etag) =
        self.fetch_file_metadata(url, name, repo, source, user_agent).await?;

    let existing = self.metadata.get_file_by_name(name, source)?;
    let should_delete = match existing.as_ref() {
        Some(file) if file.etag.is_none() => true,
        Some(_) if etag.is_none() => true,
        Some(file) if file.etag.as_deref() != etag.as_deref() => true,
        Some(file) if file.total_size as u64 != size => true,
        _ => false,
    };

    if should_delete {
        self.metadata.delete_file(name, source)?;
    }

    if self.metadata.get_file_by_name(name, source)?.is_none() {
        self.metadata.add_file(name, repo, size as i64, source)?;
    }

    self.metadata.set_file_headers(
        name,
        source,
        etag.as_deref(),
        x_repo_commit.as_deref(),
        x_linked_size,
        x_linked_etag.as_deref(),
        content_type.as_deref(),
    )?;
    self.metadata.touch_repo(repo)?;
    Ok(())
}
```

Do not remove `fetch_file_metadata(...)`; reuse it.

- [ ] **Step 3: Wrap the full reconcile lifecycle in per-file singleflight**

Add the public entry point used by `HEAD`.

```rust
pub async fn reconcile_file_metadata(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    source: &str,
    user_agent: Option<&str>,
) -> anyhow::Result<()> {
    let key = format!("{}:{}", source, name);

    let rx = {
        let mut inflight = self.metadata_inflight.lock().await;
        if let Some(waiters) = inflight.leaders.get_mut(&key) {
            let (tx, rx) = oneshot::channel();
            waiters.push(tx);
            Some(rx)
        } else {
            inflight.leaders.insert(key.clone(), Vec::new());
            None
        }
    };

    if let Some(rx) = rx {
        return rx.await.map_err(|e| anyhow::anyhow!("reconcile waiter dropped: {e}"))?;
    }

    let result = self
        .reconcile_file_metadata_inner(url, name, repo, source, user_agent)
        .await;

    let waiters = {
        let mut inflight = self.metadata_inflight.lock().await;
        inflight.leaders.remove(&key).unwrap_or_default()
    };

    let shared = match &result {
        Ok(()) => Ok(()),
        Err(e) => Err(anyhow::anyhow!(e.to_string())),
    };
    for waiter in waiters {
        let _ = waiter.send(match &shared {
            Ok(()) => Ok(()),
            Err(e) => Err(anyhow::anyhow!(e.to_string())),
        });
    }

    result
}
```

Use the persistent DB state as the source of truth after this function returns.

- [ ] **Step 4: Remove or narrow the old boolean validation path**

Replace `validate_file_etag(...)` use in new call sites. Either delete the function entirely or keep it only if still needed by untouched code.

If kept temporarily, it should no longer be the primary `HEAD` freshness entry point.

- [ ] **Step 5: Run tests to verify the implementation passes**

Run: `cargo test --test etag_tests`

Expected: PASS, including the new commit-refresh and changed-`ETag` reconcile tests.

- [ ] **Step 6: Commit the service implementation**

```bash
git add src/service.rs tests/etag_tests.rs
git commit -m "feat: reconcile metadata before reusing cache"
```

### Task 3: Switch HEAD Handling to Reconcile and Return DB State

**Files:**
- Modify: `src/server.rs`
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Write the failing request-level test for one-probe concurrency**

Add a new mock route and test in `tests/e2e_tests.rs` that fires two concurrent `HEAD` requests for the same file and asserts only one upstream metadata probe occurs.

Use an atomic counter for the upstream `HEAD` handler and a small sleep to make overlap deterministic.

```rust
#[tokio::test]
async fn test_concurrent_head_requests_share_one_upstream_probe() {
    let state = MockState {
        data: Arc::new(vec![7u8; 128]),
        get_count: Arc::new(AtomicU32::new(0)),
        ms_repo_get_count: Arc::new(AtomicU32::new(0)),
        ms_cdn_get_count: Arc::new(AtomicU32::new(0)),
        user_agents: Arc::new(std::sync::Mutex::new(Vec::new())),
    };

    let app = Router::new()
        .route("/{org}/{repo}/resolve/{revision}/{*path}", head(mock_head).get(mock_get))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let app_state = make_test_app_state(format!("http://{}", addr)).await;
    let app = hugrs::server::app_router(app_state);

    let req1 = axum::http::Request::builder()
        .method("HEAD")
        .uri("/test/repo/resolve/main/t.bin")
        .body(axum::body::Body::empty())
        .unwrap();
    let req2 = axum::http::Request::builder()
        .method("HEAD")
        .uri("/test/repo/resolve/main/t.bin")
        .body(axum::body::Body::empty())
        .unwrap();

    let (resp1, resp2) = tokio::join!(app.clone().oneshot(req1), app.oneshot(req2));
    assert_eq!(resp1.unwrap().status(), StatusCode::OK);
    assert_eq!(resp2.unwrap().status(), StatusCode::OK);
    assert_eq!(state_head_count_value(&state), 1);
}
```

Reuse the existing e2e test patterns instead of creating a new harness.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test test_concurrent_head_requests_share_one_upstream_probe --test e2e_tests`

Expected: FAIL because the current `HEAD` path can probe upstream more than once for the same file.

- [ ] **Step 3: Update the HEAD path to call reconcile instead of boolean validation**

In `src/server.rs`, replace the existing `validate_file_etag(...)` branch with:

```rust
let service = state.service.lock().await;
service
    .reconcile_file_metadata(
        &url,
        &cache_name,
        &repo_id,
        source,
        user_agent.as_deref(),
    )
    .await?;
drop(service);

let service = state.service.lock().await;
if let Ok(Some(file)) = service.info(&cache_name, source).await {
    return build_head_response(&file, &path);
}
```

Apply the same final-state rule to both leader and waiter paths: success means DB is now the authority.

- [ ] **Step 4: Keep the direct upstream HEAD branch only for true cache misses or missing final metadata**

After introducing reconcile, simplify the branch structure so `HEAD` does not maintain two competing freshness strategies.

The target behavior is:

- cache present or cache absent both flow through the same metadata reconcile entry point
- response construction reads final metadata from DB
- direct response-building from raw upstream headers is only retained if reconcile cannot persist usable metadata and the behavior is explicitly required

- [ ] **Step 5: Run the request-level tests to verify they pass**

Run: `cargo test --test e2e_tests`

Expected: PASS, including the new one-probe concurrency case and existing HF/MS proxy behavior.

- [ ] **Step 6: Commit the HEAD integration**

```bash
git add src/server.rs tests/e2e_tests.rs
git commit -m "feat: deduplicate metadata probes per file"
```

### Task 4: Cover ModelScope Probe Method and Failure Paths

**Files:**
- Modify: `tests/e2e_tests.rs`
- Modify: `src/service.rs`
- Test: `tests/e2e_tests.rs`

- [ ] **Step 1: Write the failing ModelScope reconcile test for first-hop GET**

Add a test that exercises `/ms/api/v1/models/{org}/{repo}/repo?...` and proves the reconcile path still uses one upstream first-hop `GET` and one downstream CDN metadata follow-up.

Use the existing mock handlers:

- `mock_ms_repo_get`
- `mock_ms_cdn_head`

The test should assert:

- the response is `200`
- `ms_repo_get_count == 1`
- `X-Linked-ETag` and final `ETag` are returned correctly

- [ ] **Step 2: Write the failing waiter error propagation test**

Add a test where the leader probe fails and a second concurrent request waits on the same key.

Expected:

- both requests fail consistently
- a subsequent retry is not blocked forever, proving inflight cleanup happened

- [ ] **Step 3: Implement any missing cleanup and ModelScope probe adjustments**

If the new tests expose gaps, fix only the missing cleanup and provider-specific probe details without changing the overall architecture.

Typical fixes here are:

- cleaning inflight keys on all error branches
- ensuring `/repo?...` still routes through `use_get_for_first_hop_probe(...)`
- avoiding stale waiter channels after leader failure

- [ ] **Step 4: Run targeted tests to verify the provider-specific behavior passes**

Run: `cargo test --test e2e_tests test_concurrent_head_requests_share_one_upstream_probe test_modelscope_reconcile_uses_first_hop_get test_metadata_reconcile_failure_releases_waiters`

Expected: PASS

- [ ] **Step 5: Commit the provider-specific and error-path coverage**

```bash
git add src/service.rs tests/e2e_tests.rs
git commit -m "test: cover metadata reconcile concurrency paths"
```

### Task 5: Run Full Verification

**Files:**
- Modify: `src/service.rs`
- Modify: `src/server.rs`
- Modify: `tests/etag_tests.rs`
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

- [ ] **Step 4: Inspect the final diff before any release or PR work**

Run: `git diff -- src/service.rs src/server.rs tests/etag_tests.rs tests/e2e_tests.rs`

Expected: the diff shows only reconcile, singleflight, and directly related test changes

- [ ] **Step 5: Commit the verification-complete implementation**

```bash
git add src/service.rs src/server.rs tests/etag_tests.rs tests/e2e_tests.rs
git commit -m "fix: refresh metadata with singleflight reconcile"
```
