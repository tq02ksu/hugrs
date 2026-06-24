use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::head,
    Router,
};
use hugrs::metadata::MetadataStore;
use hugrs::service::CacheService;
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

use hugrs::service::CHUNK_SIZE;

// ---------- mock upstream ----------

#[derive(Clone)]
struct MockState {
    data: Arc<Vec<u8>>,
    get_count: Arc<AtomicU32>,
}

async fn mock_head(State(s): State<MockState>) -> Response {
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

async fn start_upstream(data: Vec<u8>) -> (String, MockState) {
    let state = MockState {
        data: Arc::new(data),
        get_count: Arc::new(AtomicU32::new(0)),
    };
    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(mock_head).get(mock_get),
        )
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
        dir.path().join("trunks"),
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
        true,
        reqwest::Client::new(),
    )
}

fn build_hugrs_router(upstream: &str, dir: &TempDir) -> Router {
    use axum::routing::get;
    use tokio::sync::Mutex as TokioMutex;

    let service = make_service(dir, "http_db");

    let config = hugrs::config::Config {
        server: hugrs::config::ServerConfig {
            host: "127.0.0.1".into(),
            port: 3000,
        },
        storage: hugrs::config::StorageConfig {
            backend: "local".into(),
            local_root: dir.path().join("trunks"),
            s3_bucket: None,
            s3_region: None,
            s3_prefix: None,
            s3_endpoint: None,
            compression: Compression::None,
            max_size: None,
            prefetch_depth: 4,
            verify_sha256: true,
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
    };

    let http_client = Arc::new(reqwest::Client::new());
    let head_client = Arc::new(
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap(),
    );

    let state = hugrs::server::AppState {
        service: Arc::new(TokioMutex::new(service)),
        config: Arc::new(config),
        http_client,
        head_client,
    };

    Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            get(hugrs::server::handle_file_proxy).head(hugrs::server::handle_file_proxy),
        )
        .with_state(state)
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
        .stream_from_upstream(&url, "cfg.json", "org/repo", None, None)
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
        .stream_from_upstream(&url, "big.bin", "org/repo", None, None)
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
        .stream_from_upstream(&url, n, "org/repo", None, None)
        .await
        .unwrap();
    use futures_util::StreamExt;
    let mut s1 = s1;
    while let Some(r) = s1.next().await {
        let _ = r.unwrap();
    }
    let first = s.get_count.load(Ordering::SeqCst);
    let (_, _, s2) = service
        .stream_from_upstream(&url, n, "org/repo", None, None)
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
