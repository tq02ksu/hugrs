use axum::{
    extract::State,
    http::StatusCode,
    response::Response,
    routing::{get, head},
    Router,
};
use hugrs::metadata::MetadataStore;
use hugrs::service::{CacheService, CHUNK_SIZE};
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

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

async fn mock_get(State(s): State<MockState>) -> Vec<u8> {
    s.test_data.to_vec()
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
    svc.metadata
        .add_file(name, repo, data.len() as i64, source)
        .unwrap();
    let file = svc
        .metadata
        .get_file_by_name(name, source)
        .unwrap()
        .unwrap();
    let chunks = hugrs::chunker::chunk_with_hashes(data, CHUNK_SIZE);
    for chunk in &chunks {
        svc.backend.put(&chunk.sha256, &chunk.data).await.unwrap();
        let path = format!(
            "{}\057{}\057{}",
            &chunk.sha256[0..2],
            &chunk.sha256[2..4],
            chunk.sha256
        );
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
    svc.metadata
        .set_file_headers(
            name,
            source,
            Some(etag),
            Some("abc123"),
            None,
            None,
            Some("application/octet-stream"),
        )
        .unwrap();
}

fn make_svc(dir: &TempDir) -> CacheService {
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("t.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    CacheService::new(
        metadata,
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    )
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

    let file = svc
        .metadata
        .get_file_by_name("t.bin", "hf")
        .unwrap()
        .unwrap();
    assert_eq!(file.etag.as_deref(), Some("\"same-etag\""));
    assert_eq!(file.x_repo_commit.as_deref(), Some("newcommit123"));
}

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

    let file = svc
        .metadata
        .get_file_by_name("t.bin", "hf")
        .unwrap()
        .unwrap();
    assert_eq!(file.etag.as_deref(), Some("\"new-etag\""));
    assert_eq!(file.x_repo_commit.as_deref(), Some("commit789"));
}

#[tokio::test]
async fn test_reconcile_unreachable() {
    let tmp = TempDir::new().unwrap();
    let svc = make_svc(&tmp);
    seed_file(
        &svc,
        "t.bin",
        "test-repo",
        "hf",
        &vec![0u8; 100],
        "\"any-etag\"",
    )
    .await;
    let r = svc
        .reconcile_file_metadata(
            "http://127.0.0.1:1/resolve/main/t.bin",
            "t.bin",
            "test-repo",
            "hf",
            None,
        )
        .await;
    assert!(r.is_err());
}

#[tokio::test]
async fn test_reconcile_missing_cached_etag_rebuilds() {
    let data = vec![2u8; 1024];
    let s = MockState {
        head_count: Arc::new(AtomicU32::new(0)),
        etag: Arc::new(Mutex::new("\"fresh-etag\"".into())),
        commit: Arc::new(Mutex::new("freshcommit".into())),
        test_data: Arc::new(data.clone()),
    };
    let app = Router::new()
        .route("/resolve/main/nob.bin", head(mock_head))
        .route("/resolve/main/nob.bin", get(mock_get))
        .with_state(s);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let tmp = TempDir::new().unwrap();
    let svc = make_svc(&tmp);
    svc.metadata.add_file("nob.bin", "repo", 100, "hf").unwrap();

    let url = format!("http://{}/resolve/main/nob.bin", addr);
    svc.reconcile_file_metadata(&url, "nob.bin", "repo", "hf", None)
        .await
        .unwrap();

    let f = svc
        .metadata
        .get_file_by_name("nob.bin", "hf")
        .unwrap()
        .unwrap();
    assert_eq!(f.etag.as_deref(), Some("\"fresh-etag\""));
    assert_eq!(f.x_repo_commit.as_deref(), Some("freshcommit"));
}

#[test]
fn test_etag_matches_any() {
    assert!(hugrs::server::etag_matches_any("\"abc123\"", "\"abc123\""));
    assert!(hugrs::server::etag_matches_any(
        "W/\"abc123\"",
        "\"abc123\""
    ));
    assert!(hugrs::server::etag_matches_any(
        "\"abc123\"",
        "W/\"abc123\""
    ));
    assert!(hugrs::server::etag_matches_any(
        "\"abc123\"",
        "\"xyz\", \"abc123\""
    ));
    assert!(!hugrs::server::etag_matches_any("\"abc123\"", "\"xyz789\""));
}
