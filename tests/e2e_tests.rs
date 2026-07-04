use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, head},
    Router,
};
use hugrs::metadata::MetadataStore;
use hugrs::service::CacheService;
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

use hugrs::service::CHUNK_SIZE;

async fn seed_file(svc: &CacheService, name: &str, repo: &str, source: &str, data: &[u8]) {
    let existing = svc.metadata.get_file_by_name(name, source).unwrap();
    svc.metadata.delete_file(name, source).ok();
    let file = svc
        .metadata
        .add_file(name, repo, data.len() as i64, source)
        .unwrap();
    if let Some(ref h) = existing {
        svc.metadata
            .set_file_headers(
                name,
                source,
                h.etag.as_deref(),
                h.x_repo_commit.as_deref(),
                h.x_linked_size,
                h.x_linked_etag.as_deref(),
                h.content_type.as_deref(),
            )
            .unwrap();
    }
    let chunks = hugrs::chunker::chunk_with_hashes(data, CHUNK_SIZE);
    for chunk in &chunks {
        svc.backend.put(&chunk.sha256, &chunk.data).await.unwrap();
        let path = svc.chunk_path(&chunk.sha256);
        svc.metadata
            .ensure_chunk(
                &chunk.sha256,
                "local",
                &path,
                chunk.chunk_size as i64,
                chunk.chunk_size as i64,
            )
            .unwrap();
        svc.metadata
            .link_file_chunk(
                file.id,
                &chunk.sha256,
                chunk.chunk_index as i64,
                chunk.chunk_size as i64,
            )
            .unwrap();
    }
    svc.metadata.touch_repo(repo).unwrap();
    if let Some(limit) = svc.max_size {
        let _ = svc.evict_if_needed(limit).await;
    }
}

async fn seed_incomplete_file(
    svc: &CacheService,
    name: &str,
    repo: &str,
    source: &str,
    total_size: i64,
    downloaded_data: &[u8],
) {
    svc.metadata.delete_file(name, source).ok();
    let file = svc
        .metadata
        .add_file(name, repo, total_size, source)
        .unwrap();
    let chunks = hugrs::chunker::chunk_with_hashes(downloaded_data, CHUNK_SIZE);
    for chunk in &chunks {
        svc.backend.put(&chunk.sha256, &chunk.data).await.unwrap();
        let path = svc.chunk_path(&chunk.sha256);
        svc.metadata
            .ensure_chunk(
                &chunk.sha256,
                "local",
                &path,
                chunk.chunk_size as i64,
                chunk.chunk_size as i64,
            )
            .unwrap();
        svc.metadata
            .link_file_chunk(
                file.id,
                &chunk.sha256,
                chunk.chunk_index as i64,
                chunk.chunk_size as i64,
            )
            .unwrap();
    }
    svc.metadata.touch_repo(repo).unwrap();
}

// ---------- mock upstream ----------

#[derive(Clone)]
struct MockState {
    data: Arc<Vec<u8>>,
    head_count: Arc<AtomicU32>,
    get_count: Arc<AtomicU32>,
    ms_repo_get_count: Arc<AtomicU32>,
    ms_cdn_get_count: Arc<AtomicU32>,
    user_agents: Arc<std::sync::Mutex<Vec<String>>>,
}

fn record_user_agent(state: &MockState, headers: &HeaderMap) {
    if let Some(ua) = headers.get("user-agent").and_then(|v| v.to_str().ok()) {
        state.user_agents.lock().unwrap().push(ua.to_string());
    }
}

async fn mock_head(State(s): State<MockState>, headers: HeaderMap) -> Response {
    s.head_count.fetch_add(1, Ordering::SeqCst);
    record_user_agent(&s, &headers);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", s.data.len())
        .header("ETag", "\"mock-etag\"")
        .header("X-Repo-Commit", "abc123mock")
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn mock_get(
    State(s): State<MockState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    s.get_count.fetch_add(1, Ordering::SeqCst);
    record_user_agent(&s, &headers);
    let total = s.data.len() as u64;
    let (start, end) = if let Some(range) = headers.get("range") {
        let r = range.to_str().unwrap().strip_prefix("bytes=").unwrap();
        let (a, b) = r.split_once('-').unwrap();
        let a: u64 = a.parse().unwrap();
        let b: u64 = if b.is_empty() {
            total - 1
        } else {
            b.parse().unwrap()
        };
        (a, b)
    } else {
        (0u64, total - 1)
    };
    let slice = &s.data[start as usize..=end as usize];
    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("Content-Length", slice.len())
        .header(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end, total),
        )
        .body(axum::body::Body::from(slice.to_vec()))
        .unwrap())
}

async fn mock_ms_repo_head(State(s): State<MockState>, headers: HeaderMap) -> Response {
    record_user_agent(&s, &headers);

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Length", 311)
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn mock_ms_repo_get(State(s): State<MockState>, headers: HeaderMap) -> Response {
    s.ms_repo_get_count.fetch_add(1, Ordering::SeqCst);
    record_user_agent(&s, &headers);

    Response::builder()
        .status(StatusCode::FOUND)
        .header("Location", "/cdn/model.safetensors")
        .header("X-Linked-ETag", "mock-linked-etag")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn mock_ms_cdn_head(State(s): State<MockState>, headers: HeaderMap) -> Response {
    record_user_agent(&s, &headers);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", s.data.len())
        .header("ETag", "\"mock-ms-etag\"")
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn mock_ms_cdn_get(
    State(s): State<MockState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    s.ms_cdn_get_count.fetch_add(1, Ordering::SeqCst);
    record_user_agent(&s, &headers);

    let total = s.data.len() as u64;
    let (start, end, status) = if let Some(range) = headers.get("range") {
        let r = range.to_str().unwrap().strip_prefix("bytes=").unwrap();
        let (a, b) = r.split_once('-').unwrap();
        let a: u64 = a.parse().unwrap();
        let b: u64 = if b.is_empty() {
            total - 1
        } else {
            b.parse().unwrap()
        };
        (a, b, StatusCode::PARTIAL_CONTENT)
    } else {
        (0u64, total - 1, StatusCode::OK)
    };

    let slice = &s.data[start as usize..=end as usize];
    let mut builder = Response::builder()
        .status(status)
        .header("Content-Length", slice.len())
        .header("ETag", "\"mock-ms-etag\"")
        .header("Content-Type", "application/octet-stream");

    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end, total),
        );
    }

    Ok(builder
        .body(axum::body::Body::from(slice.to_vec()))
        .unwrap())
}

async fn mock_model_api_proxy(req: axum::extract::Request) -> Response {
    let uri = req.uri();
    let body = serde_json::to_vec(&json!({
        "path": uri.path(),
        "query": uri.query().unwrap_or(""),
    }))
    .unwrap();

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(body))
        .unwrap()
}

async fn mock_notfound_head() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/plain; charset=utf-8")
        .header("Content-Length", "15")
        .header("X-Repo-Commit", "deadbeef404")
        .header("X-Error-Code", "EntryNotFound")
        .header("X-Error-Message", "Entry not found")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn mock_notfound_get() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/plain; charset=utf-8")
        .header("Content-Length", "15")
        .header("X-Repo-Commit", "deadbeef404")
        .header("X-Error-Code", "EntryNotFound")
        .header("X-Error-Message", "Entry not found")
        .body(axum::body::Body::from("Entry not found"))
        .unwrap()
}

async fn start_notfound_upstream() -> String {
    let app = Router::new().route(
        "/{org}/{repo}/resolve/{revision}/{*path}",
        head(mock_notfound_head).get(mock_notfound_get),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async { axum::serve(l, app).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

async fn start_slow_notfound_upstream() -> String {
    let app = Router::new().route(
        "/{org}/{repo}/resolve/{revision}/{*path}",
        head(|| async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            mock_notfound_head().await
        })
        .get(|| async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            mock_notfound_get().await
        }),
    );
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async { axum::serve(l, app).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

async fn start_upstream(data: Vec<u8>) -> (String, MockState) {
    let state = MockState {
        data: Arc::new(data),
        head_count: Arc::new(AtomicU32::new(0)),
        get_count: Arc::new(AtomicU32::new(0)),
        ms_repo_get_count: Arc::new(AtomicU32::new(0)),
        ms_cdn_get_count: Arc::new(AtomicU32::new(0)),
        user_agents: Arc::new(std::sync::Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(mock_head).get(mock_get),
        )
        .route(
            "/api/models/{org}/{repo}/{*path}",
            get(mock_model_api_proxy),
        )
        .with_state(state.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async { axum::serve(l, app).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, state)
}

async fn start_ms_upstream(data: Vec<u8>) -> (String, MockState) {
    let state = MockState {
        data: Arc::new(data),
        head_count: Arc::new(AtomicU32::new(0)),
        get_count: Arc::new(AtomicU32::new(0)),
        ms_repo_get_count: Arc::new(AtomicU32::new(0)),
        ms_cdn_get_count: Arc::new(AtomicU32::new(0)),
        user_agents: Arc::new(std::sync::Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .route(
            "/api/v1/models/{org}/{repo}/repo",
            head(mock_ms_repo_head).get(mock_ms_repo_get),
        )
        .route("/cdn/{*path}", head(mock_ms_cdn_head).get(mock_ms_cdn_get))
        .with_state(state.clone());
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async { axum::serve(l, app).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, state)
}

// ---------- helpers ----------

fn make_service(dir: &TempDir, db_name: &str) -> CacheService {
    let metadata = Arc::new(MetadataStore::new(&dir.path().join(db_name)).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let http = reqwest::Client::new();
    let head = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    CacheService::new(
        metadata,
        backend,
        None,
        http,
        head,
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    )
}

fn build_hugrs_router(upstream: &str, dir: &TempDir) -> Router {
    use tokio::sync::Mutex as TokioMutex;

    let service = make_service(dir, "http_db");

    let mut config = hugrs::config::Config {
        server: hugrs::config::ServerConfig {
            host: "127.0.0.1".into(),
            port: 3000,
        },
        admin: hugrs::config::AdminConfig {
            token: Some("test-admin-token".into()),
            token_file: dir.path().join("admin.token"),
        },
        storage: hugrs::config::StorageConfig {
            backend: "local".into(),
            local_root: dir.path().join("chunks"),
            s3_bucket: None,
            s3_region: None,
            s3_prefix: None,
            s3_endpoint: None,
            compression: Compression::None,
            max_size: None,
            prefetch_depth: 4,
            prefetch_budget_base: 8,
            verify_sha256: true,
            etag_validation_timeout_secs: 5,
        },
        database: hugrs::config::DatabaseConfig {
            path: dir.path().join("http_db"),
        },
        huggingface: hugrs::config::HfConfig {
            endpoint: upstream.to_string(),
            token: None,
            proxy: None,
            timeout_secs: 120,
            connect_timeout_secs: 15,
        },
        modelscope: hugrs::config::MsConfig::default(),
    };
    config.modelscope.endpoint = upstream.to_string();

    let http_client = Arc::new(reqwest::Client::new());
    let head_client = Arc::new(
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
    );

    let state = hugrs::server::AppState {
        service: Arc::new(service),
        config: Arc::new(config),
        admin_token: Arc::new("test-admin-token".into()),
        http_client: http_client.clone(),
        head_client,
        ms_http_client: http_client,
        ms_head_client: Arc::new(
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        ),
        metadata_inflight: Arc::new(TokioMutex::new(Default::default())),
    };

    hugrs::server::app_router(state)
}

// ---------- tests ----------

#[tokio::test]
async fn test_small_file_bytes_match_upstream() {
    let test_data: Vec<u8> = b"{\"model_type\":\"qwen2\",\"vocab_size\":151936}\n".to_vec();
    assert!(test_data.len() < CHUNK_SIZE);
    let (upstream, _s) = start_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();
    let service = make_service(&dir, "small.db");
    let url = format!("{}/org/repo/resolve/main/cfg.json", upstream);
    let (_, _, stream) = service
        .stream_from_upstream(&url, "cfg.json", "org/repo", "hf", None, None, None)
        .await
        .unwrap();
    use futures_util::StreamExt;
    let mut got = Vec::new();
    let mut stream = stream;
    while let Some(r) = stream.next().await {
        got.extend_from_slice(&r.unwrap());
    }
    assert_eq!(got, test_data);
}

#[tokio::test]
async fn test_small_file_http_layer_matches() {
    let test_data: Vec<u8> = b"{\"model_type\":\"qwen2\",\"vocab_size\":151936}\n".to_vec();
    let (upstream, _s) = start_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();

    let app = build_hugrs_router(&upstream, &dir);

    // First request to HEAD (populate metadata cache)
    use tower::util::ServiceExt;
    let head_req = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/cfg.json")
        .body(axum::body::Body::empty())
        .unwrap();
    let head_resp = app.clone().oneshot(head_req).await.unwrap();
    assert!(head_resp.status().is_success(), "HEAD should succeed");

    // GET the file
    let get_req = axum::http::Request::builder()
        .method("GET")
        .uri("/org/repo/resolve/main/cfg.json")
        .body(axum::body::Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_req).await.unwrap();
    assert!(get_resp.status().is_success(), "GET should succeed");

    let body_bytes = axum::body::to_bytes(get_resp.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body_bytes.as_ref(), test_data.as_slice());
}

#[tokio::test]
async fn test_large_file_bytes_match_upstream() {
    let test_data: Vec<u8> = (0..(CHUNK_SIZE as u64 * 2 + 500) as u8)
        .map(|i| (i.wrapping_mul(17).wrapping_add(31)) as u8)
        .collect();
    let (upstream, s) = start_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();
    let service = make_service(&dir, "large.db");
    let url = format!("{}/org/repo/resolve/main/big.bin", upstream);
    let (_, _, stream) = service
        .stream_from_upstream(&url, "big.bin", "org/repo", "hf", None, None, None)
        .await
        .unwrap();
    use futures_util::StreamExt;
    let mut got = Vec::new();
    let mut stream = stream;
    while let Some(r) = stream.next().await {
        got.extend_from_slice(&r.unwrap());
    }
    assert_eq!(got, test_data);
    let expected = ((test_data.len() as u64).div_ceil(CHUNK_SIZE as u64)) as u32;
    assert_eq!(s.get_count.load(Ordering::SeqCst), expected);
}

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

#[tokio::test]
async fn test_second_get_uses_cache_and_matches() {
    let test_data: Vec<u8> = (0..(CHUNK_SIZE as u64 * 2 + 500) as u8)
        .map(|i| (i.wrapping_mul(17).wrapping_add(31)) as u8)
        .collect();
    let (upstream, s) = start_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();
    let service = make_service(&dir, "cache_hit.db");
    let url = format!("{}/org/repo/resolve/main/big.bin", upstream);
    let n = "big.bin";
    let (_, _, s1) = service
        .stream_from_upstream(&url, n, "org/repo", "hf", None, None, None)
        .await
        .unwrap();
    use futures_util::StreamExt;
    let mut s1 = s1;
    while let Some(r) = s1.next().await {
        let _ = r.unwrap();
    }
    let first = s.get_count.load(Ordering::SeqCst);
    let (_, _, s2) = service
        .stream_from_upstream(&url, n, "org/repo", "hf", None, None, None)
        .await
        .unwrap();
    let mut got = Vec::new();
    let mut s2 = s2;
    while let Some(r) = s2.next().await {
        got.extend_from_slice(&r.unwrap());
    }
    assert_eq!(got, test_data);
    assert_eq!(first, s.get_count.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_generic_model_api_proxy_forwards_suffix_and_query() {
    let (upstream, _s) = start_upstream(b"{}".to_vec()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/api/models/Qwen/Qwen3-Reranker-8B/tree/main?recursive=true&expand=false")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        body["path"],
        json!("/api/models/Qwen/Qwen3-Reranker-8B/tree/main")
    );
    assert_eq!(body["query"], json!("recursive=true&expand=false"));
}

#[tokio::test]
async fn test_ms_repo_second_get_uses_cache() {
    let test_data = b"modelscope cached body".to_vec();
    let (upstream, state) = start_ms_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let uri = "/ms/api/v1/models/Qwen/Qwen3-Embedding-0.6B/repo?Revision=master&FilePath=model.safetensors";

    let req1 = axum::http::Request::builder()
        .method("GET")
        .uri(uri)
        .header("User-Agent", "ua-ms-cache-test/1.0")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert!(resp1.status().is_success());
    let body1 = axum::body::to_bytes(resp1.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body1.as_ref(), test_data.as_slice());

    let first_repo_gets = state.ms_repo_get_count.load(Ordering::SeqCst);
    let first_cdn_gets = state.ms_cdn_get_count.load(Ordering::SeqCst);
    assert!(first_repo_gets > 0);
    assert!(first_cdn_gets > 0);

    let req2 = axum::http::Request::builder()
        .method("GET")
        .uri(uri)
        .header("User-Agent", "ua-ms-cache-test/1.0")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert!(resp2.status().is_success());
    let body2 = axum::body::to_bytes(resp2.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body2.as_ref(), test_data.as_slice());

    assert_eq!(
        first_repo_gets + 1, // +1 for etag validation GET
        state.ms_repo_get_count.load(Ordering::SeqCst)
    );
    assert_eq!(
        first_cdn_gets,
        state.ms_cdn_get_count.load(Ordering::SeqCst) // CDN called with HEAD, count unchanged
    );
}

#[tokio::test]
async fn test_modelscope_reconcile_uses_first_hop_get() {
    let test_data = b"modelscope reconcile body".to_vec();
    let (upstream, state) = start_ms_upstream(test_data).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let uri = "/ms/api/v1/models/Qwen/Qwen3-Embedding-0.6B/repo?Revision=master&FilePath=model.safetensors";
    let req = axum::http::Request::builder()
        .method("HEAD")
        .uri(uri)
        .header("User-Agent", "ua-ms-reconcile-head/1.0")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(state.ms_repo_get_count.load(Ordering::SeqCst), 1);
    assert_eq!(state.ms_cdn_get_count.load(Ordering::SeqCst), 0);
    assert_eq!(
        resp.headers()
            .get("x-linked-etag")
            .and_then(|v| v.to_str().ok()),
        Some("mock-linked-etag")
    );
    assert_eq!(
        resp.headers().get("etag").and_then(|v| v.to_str().ok()),
        Some("\"mock-ms-etag\"")
    );
}

#[tokio::test]
async fn test_ms_repo_stale_small_cache_refreshes_on_valid_range() {
    let test_data = vec![9u8; CHUNK_SIZE + 1024];
    let (upstream, state) = start_ms_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();

    let seed_service = make_service(&dir, "http_db");
    seed_file(
        &seed_service,
        "Qwen/Qwen3-Embedding-0.6B/model.safetensors",
        "Qwen/Qwen3-Embedding-0.6B",
        "ms",
        &[1u8; 10],
    )
    .await;

    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let uri = "/ms/api/v1/models/Qwen/Qwen3-Embedding-0.6B/repo?Revision=master&FilePath=model.safetensors";
    let req = axum::http::Request::builder()
        .method("GET")
        .uri(uri)
        .header("Range", "bytes=100-199")
        .header("User-Agent", "ua-ms-stale-cache-test/1.0")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert!(state.ms_repo_get_count.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn test_small_file_cache_hit_preserves_headers() {
    let test_data: Vec<u8> = b"{\"model_type\":\"qwen2\",\"vocab_size\":151936}\n".to_vec();
    assert!(test_data.len() < CHUNK_SIZE);
    let (upstream, _s) = start_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let get = |uri: &str| {
        let app = app.clone();
        let uri = uri.to_string();
        async move {
            let req = axum::http::Request::builder()
                .method("GET")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap();
            app.oneshot(req).await.unwrap()
        }
    };

    let head = |uri: &str| {
        let app = app.clone();
        let uri = uri.to_string();
        async move {
            let req = axum::http::Request::builder()
                .method("HEAD")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap();
            app.oneshot(req).await.unwrap()
        }
    };

    let head_resp = head("/org/repo/resolve/main/cfg.json").await;
    assert!(head_resp.status().is_success());
    assert!(
        head_resp.headers().get("etag").is_some(),
        "HEAD should return etag"
    );

    let get1 = get("/org/repo/resolve/main/cfg.json").await;
    assert!(get1.status().is_success());
    assert!(
        get1.headers().get("etag").is_some(),
        "first GET should return etag"
    );

    let get2 = get("/org/repo/resolve/main/cfg.json").await;
    assert!(get2.status().is_success());
    assert!(
        get2.headers().get("etag").is_some(),
        "second GET (cache hit) should return etag"
    );
    assert!(
        get2.headers().get("content-type").is_some(),
        "second GET (cache hit) should return content-type"
    );
    assert!(
        get2.headers().get("x-repo-commit").is_some(),
        "second GET (cache hit) should return x-repo-commit"
    );
}

#[tokio::test]
async fn test_concurrent_head_requests_share_one_upstream_probe() {
    let test_data: Vec<u8> = b"head concurrency payload".to_vec();
    let (upstream, state) = start_upstream(test_data).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let req1 = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/cfg.json")
        .body(axum::body::Body::empty())
        .unwrap();
    let req2 = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/cfg.json")
        .body(axum::body::Body::empty())
        .unwrap();

    let (resp1, resp2) = tokio::join!(app.clone().oneshot(req1), app.oneshot(req2));
    let resp1 = resp1.unwrap();
    let resp2 = resp2.unwrap();

    assert!(resp1.status().is_success());
    assert!(resp2.status().is_success());
    assert_eq!(state.head_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_metadata_reconcile_failure_releases_waiters() {
    let upstream = start_slow_notfound_upstream().await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let req1 = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/missing.bin")
        .body(axum::body::Body::empty())
        .unwrap();
    let req2 = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/missing.bin")
        .body(axum::body::Body::empty())
        .unwrap();

    let (resp1, resp2) = tokio::join!(app.clone().oneshot(req1), app.clone().oneshot(req2));
    let resp1 = resp1.unwrap();
    let resp2 = resp2.unwrap();

    assert_eq!(resp1.status(), StatusCode::NOT_FOUND);
    assert_eq!(resp2.status(), StatusCode::NOT_FOUND);

    let good_data: Vec<u8> = b"retry after failure".to_vec();
    let (good_upstream, state) = start_upstream(good_data).await;
    let app = build_hugrs_router(&good_upstream, &dir);

    let req3 = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/missing.bin")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp3 = app.oneshot(req3).await.unwrap();

    assert_eq!(resp3.status(), StatusCode::OK);
    assert_eq!(state.head_count.load(Ordering::SeqCst), 1);
}

async fn start_redirect_upstream(data: Vec<u8>) -> (String,) {
    let data = Arc::new(data);
    let d = data.clone();

    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head({
                let _d = d.clone();
                move || async move {
                    Response::builder()
                        .status(StatusCode::FOUND)
                        .header("X-Repo-Commit", "deadbeef")
                        .header("X-Linked-ETag", "\"linked-etag\"")
                        .header("Location", "/cdn/final.bin")
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            })
            .get({
                let _d = d.clone();
                move || async move {
                    Response::builder()
                        .status(StatusCode::FOUND)
                        .header("X-Repo-Commit", "deadbeef")
                        .header("X-Linked-ETag", "\"linked-etag\"")
                        .header("Location", "/cdn/final.bin")
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            }),
        )
        .route(
            "/cdn/final.bin",
            head({
                let _d = d.clone();
                move || async move {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Length", _d.len())
                        .header("ETag", "\"final-etag\"")
                        .header("Content-Type", "application/json; charset=utf-8")
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            })
            .get({
                move || async move {
                    let body = d.to_vec();
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Length", body.len())
                        .header("Content-Type", "application/json; charset=utf-8")
                        .body(axum::body::Body::from(body))
                        .unwrap()
                }
            }),
        );

    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let upstream = format!("http://127.0.0.1:{}", addr.port());
    tokio::spawn(async move {
        axum::serve(l, app).await.unwrap();
    });
    (upstream,)
}

#[tokio::test]
async fn test_redirect_cache_hit_preserves_headers() {
    let test_data: Vec<u8> = b"{\"model_type\":\"qwen2\",\"vocab_size\":151936}\n".to_vec();
    assert!(test_data.len() < CHUNK_SIZE);
    let (upstream,) = start_redirect_upstream(test_data.clone()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let get = |uri: &str| {
        let app = app.clone();
        let uri = uri.to_string();
        async move {
            let req = axum::http::Request::builder()
                .method("GET")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap();
            app.oneshot(req).await.unwrap()
        }
    };

    let head = |uri: &str| {
        let app = app.clone();
        let uri = uri.to_string();
        async move {
            let req = axum::http::Request::builder()
                .method("HEAD")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap();
            app.oneshot(req).await.unwrap()
        }
    };

    let head_resp = head("/org/repo/resolve/main/cfg.json").await;
    assert!(
        head_resp.status().is_success(),
        "HEAD should succeed: {:?}",
        head_resp.status()
    );
    assert!(
        head_resp.headers().get("etag").is_some(),
        "HEAD should return etag"
    );
    assert!(
        head_resp.headers().get("x-repo-commit").is_some(),
        "HEAD should return x-repo-commit"
    );

    let get1 = get("/org/repo/resolve/main/cfg.json").await;
    assert!(get1.status().is_success(), "first GET should succeed");
    assert!(
        get1.headers().get("etag").is_some(),
        "first GET should return etag"
    );

    let get2 = get("/org/repo/resolve/main/cfg.json").await;
    assert!(
        get2.status().is_success(),
        "second GET (cache hit) should succeed"
    );
    assert!(
        get2.headers().get("etag").is_some(),
        "second GET (cache hit, 302 upstream) should return etag"
    );
    assert!(
        get2.headers().get("content-type").is_some(),
        "second GET (cache hit, 302 upstream) should return content-type"
    );
    assert!(
        get2.headers().get("x-repo-commit").is_some(),
        "second GET (cache hit, 302 upstream) should return x-repo-commit"
    );
    assert!(
        get2.headers().get("x-linked-etag").is_some(),
        "second GET (cache hit, 302 upstream) should return x-linked-etag"
    );
}

#[tokio::test]
async fn test_redirect_small_file_get_populates_db_headers() {
    let test_data: Vec<u8> = b"{\"model_type\":\"qwen2\",\"vocab_size\":151936}\n".to_vec();
    assert!(test_data.len() < CHUNK_SIZE);
    let (upstream,) = start_redirect_upstream(test_data).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    for _ in 0..2 {
        let req = axum::http::Request::builder()
            .method("GET")
            .uri("/org/repo/resolve/main/cfg.json")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert!(resp.status().is_success());
    }

    let store = MetadataStore::new(&dir.path().join("http_db")).unwrap();
    let file = store.get_file_by_name("org/repo/cfg.json", "hf").unwrap();
    let file = file
        .or_else(|| store.get_file_by_name("cfg.json", "hf").unwrap())
        .unwrap();

    assert_eq!(file.etag.as_deref(), Some("\"final-etag\""));
    assert_eq!(file.x_repo_commit.as_deref(), Some("deadbeef"));
    assert_eq!(file.x_linked_etag.as_deref(), Some("\"linked-etag\""));
    assert_eq!(
        file.content_type.as_deref(),
        Some("application/json; charset=utf-8")
    );
}

#[tokio::test]
async fn test_control_api_rejects_missing_token() {
    let (upstream, _s) = start_upstream(b"{}".to_vec()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/_hugrs/service")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_control_api_returns_service_status() {
    let (upstream, _s) = start_upstream(b"{}".to_vec()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/_hugrs/service")
        .header("Authorization", "Bearer test-admin-token")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_control_api_reconsile_dry_run_reports_summary() {
    let (upstream, _s) = start_upstream(b"{}".to_vec()).await;
    let dir = TempDir::new().unwrap();
    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/_hugrs/service/reconsile")
        .header("Authorization", "Bearer test-admin-token")
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(r#"{"dry_run":true}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["scanned_chunks"].is_number());
    assert!(json["mismatched_chunks"].is_number());
    assert!(json["refcount_fixed"].is_number());
    assert!(json["orphaned_marked"].is_number());
    assert!(json["orphaned_cleared"].is_number());
}

#[tokio::test]
async fn test_control_api_file_delete_without_source_applies_to_all_sources() {
    let (upstream, _s) = start_upstream(b"{}".to_vec()).await;
    let dir = TempDir::new().unwrap();

    let seed_service = make_service(&dir, "http_db");
    seed_file(
        &seed_service,
        "shared.bin",
        "repo-a",
        "hf",
        &vec![1, 2, 3, 4],
    )
    .await;
    seed_file(
        &seed_service,
        "shared.bin",
        "repo-a",
        "ms",
        &vec![1, 2, 3, 4],
    )
    .await;

    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let req = axum::http::Request::builder()
        .method("DELETE")
        .uri("/_hugrs/files?repo=repo-a&file=shared.bin")
        .header("Authorization", "Bearer test-admin-token")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let store = MetadataStore::new(&dir.path().join("http_db")).unwrap();
    assert!(store
        .get_file_by_name("shared.bin", "hf")
        .unwrap()
        .is_none());
    assert!(store
        .get_file_by_name("shared.bin", "ms")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_control_api_files_report_incomplete_download_status() {
    let (upstream, _s) = start_upstream(b"{}".to_vec()).await;
    let dir = TempDir::new().unwrap();

    let seed_service = make_service(&dir, "http_db");
    seed_incomplete_file(&seed_service, "partial.bin", "repo-a", "hf", 10, b"1234").await;

    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let list_req = axum::http::Request::builder()
        .method("GET")
        .uri("/_hugrs/files")
        .header("Authorization", "Bearer test-admin-token")
        .body(axum::body::Body::empty())
        .unwrap();
    let list_resp = app.clone().oneshot(list_req).await.unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_body = axum::body::to_bytes(list_resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let list_json: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    let item = &list_json["items"][0];
    assert_eq!(item["size"], json!(10));
    assert_eq!(item["downloaded_size"], json!(4));
    assert_eq!(item["complete"], json!(false));

    let show_req = axum::http::Request::builder()
        .method("GET")
        .uri("/_hugrs/files/show?repo=repo-a&file=partial.bin")
        .header("Authorization", "Bearer test-admin-token")
        .body(axum::body::Body::empty())
        .unwrap();
    let show_resp = app.oneshot(show_req).await.unwrap();
    assert_eq!(show_resp.status(), StatusCode::OK);
    let show_body = axum::body::to_bytes(show_resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let show_json: serde_json::Value = serde_json::from_slice(&show_body).unwrap();
    assert_eq!(show_json["size"], json!(10));
    assert_eq!(show_json["downloaded_size"], json!(4));
    assert_eq!(show_json["complete"], json!(false));
}

#[tokio::test]
async fn test_control_api_files_report_complete_download_status() {
    let (upstream, _s) = start_upstream(b"{}".to_vec()).await;
    let dir = TempDir::new().unwrap();

    let seed_service = make_service(&dir, "http_db");
    seed_file(&seed_service, "complete.bin", "repo-a", "hf", b"12345678").await;

    let app = build_hugrs_router(&upstream, &dir);

    use tower::util::ServiceExt;

    let show_req = axum::http::Request::builder()
        .method("GET")
        .uri("/_hugrs/files/show?repo=repo-a&file=complete.bin")
        .header("Authorization", "Bearer test-admin-token")
        .body(axum::body::Body::empty())
        .unwrap();
    let show_resp = app.oneshot(show_req).await.unwrap();
    assert_eq!(show_resp.status(), StatusCode::OK);
    let show_body = axum::body::to_bytes(show_resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let show_json: serde_json::Value = serde_json::from_slice(&show_body).unwrap();
    assert_eq!(show_json["size"], json!(8));
    assert_eq!(show_json["downloaded_size"], json!(8));
    assert_eq!(show_json["complete"], json!(true));
}

#[tokio::test]
async fn test_resolve_404_head_forwards_upstream_status_and_does_not_cache() {
    let dir = TempDir::new().unwrap();
    let upstream = start_notfound_upstream().await;
    let app = build_hugrs_router(&upstream, &dir);
    let db_path = dir.path().join("http_db");

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("HEAD")
        .uri("/org/repo/resolve/main/missing.json")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "HEAD to nonexistent file should return 404"
    );

    let h = resp.headers();
    assert_eq!(
        h.get("x-repo-commit")
            .and_then(|v| v.to_str().ok())
            .unwrap(),
        "deadbeef404",
        "x-repo-commit should be forwarded from upstream"
    );

    let store = MetadataStore::new(&db_path).unwrap();
    assert!(
        store
            .get_file_by_name("org/repo/missing.json", "hf")
            .unwrap()
            .is_none(),
        "404 responses must not be cached"
    );
}

#[tokio::test]
async fn test_resolve_404_get_forwards_upstream_status_and_does_not_cache() {
    let dir = TempDir::new().unwrap();
    let upstream = start_notfound_upstream().await;
    let app = build_hugrs_router(&upstream, &dir);
    let db_path = dir.path().join("http_db");

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/org/repo/resolve/main/missing.json")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "GET to nonexistent file should return 404"
    );

    let h = resp.headers();
    assert_eq!(
        h.get("x-repo-commit")
            .and_then(|v| v.to_str().ok())
            .unwrap(),
        "deadbeef404",
        "x-repo-commit should be forwarded from upstream"
    );

    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(
        body.as_ref(),
        b"Entry not found",
        "404 body should be forwarded from upstream"
    );

    let store = MetadataStore::new(&db_path).unwrap();
    assert!(
        store
            .get_file_by_name("org/repo/missing.json", "hf")
            .unwrap()
            .is_none(),
        "404 responses must not be cached"
    );
}
