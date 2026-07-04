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
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tempfile::TempDir;

use hugrs::service::CHUNK_SIZE;

#[derive(Clone)]
struct MockState {
    get_count: Arc<AtomicU32>,
    head_count: Arc<AtomicU32>,
    test_data: Arc<Vec<u8>>,
}

async fn mock_head(State(state): State<MockState>) -> Response {
    state.head_count.fetch_add(1, Ordering::SeqCst);
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", state.test_data.len())
        .header("ETag", r#""mock-etag""#)
        .header("X-Repo-Commit", "abc123mock")
        .header("X-Linked-Size", state.test_data.len() as i64)
        .header("X-Linked-ETag", r#""mock-linked-etag""#)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn mock_get(
    State(state): State<MockState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    state.get_count.fetch_add(1, Ordering::SeqCst);

    let total = state.test_data.len() as u64;
    let (start, end) = if let Some(range) = headers.get("range") {
        let range_str = range.to_str().unwrap();
        let range = range_str.strip_prefix("bytes=").unwrap();
        let (s, e) = range.split_once('-').unwrap();
        let start: u64 = s.parse().unwrap();
        let end: u64 = if e.is_empty() {
            total - 1
        } else {
            e.parse().unwrap()
        };
        (start, end)
    } else {
        (0u64, total - 1)
    };

    let data = &state.test_data[start as usize..=end as usize];

    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("Content-Length", data.len())
        .header(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end, total),
        )
        .body(axum::body::Body::from(data.to_vec()))
        .unwrap())
}

#[tokio::test]
async fn test_multiple_gets_no_duplicate_downloads() {
    let total = CHUNK_SIZE + 42;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(13).wrapping_add(47))
        .collect();
    let state = MockState {
        get_count: Arc::new(AtomicU32::new(0)),
        head_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
    };

    let get_count = state.get_count.clone();
    let head_count = state.head_count.clone();
    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(mock_head).get(mock_get),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dir = TempDir::new().unwrap();
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));

    let head_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let http_client = reqwest::Client::new();

    let service = Arc::new(CacheService::new(
        metadata,
        backend,
        None,
        http_client,
        head_client,
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    ));

    let upstream_url = format!("http://{}/test/repo/resolve/main/no-dup.bin", addr);

    let (f1, l1, s1) = service
        .stream_from_upstream(
            &upstream_url,
            "no-dup.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    use futures_util::StreamExt;
    let mut s1 = s1;
    let mut c1 = Vec::new();
    while let Some(result) = s1.next().await {
        c1.extend_from_slice(&result.unwrap());
    }
    assert_eq!(c1, test_data);
    assert_eq!(f1.total_size as usize, total);
    assert_eq!(l1, total as u64);

    let first_gets = get_count.load(Ordering::SeqCst);
    let first_heads = head_count.load(Ordering::SeqCst);

    let (_f2, _l2, s2) = service
        .stream_from_upstream(
            &upstream_url,
            "no-dup.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let mut s2 = s2;
    let mut c2 = Vec::new();
    while let Some(result) = s2.next().await {
        c2.extend_from_slice(&result.unwrap());
    }
    assert_eq!(c2, test_data);

    assert_eq!(get_count.load(Ordering::SeqCst), first_gets);
    assert_eq!(
        head_count.load(Ordering::SeqCst) - first_heads,
        1,
        "second request should only do one HEAD probe"
    );
}

#[tokio::test]
async fn test_partial_cache_no_redundant_download() {
    let total = CHUNK_SIZE * 2 + 100;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(7).wrapping_add(13))
        .collect();
    let state = MockState {
        get_count: Arc::new(AtomicU32::new(0)),
        head_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
    };

    let get_count = state.get_count.clone();
    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(mock_head).get(mock_get),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dir = TempDir::new().unwrap();
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let head_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let http_client = reqwest::Client::new();

    let service = Arc::new(CacheService::new(
        metadata,
        backend,
        None,
        http_client,
        head_client,
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    ));

    let upstream_url = format!("http://{}/test/repo/resolve/main/partial.bin", addr);

    use futures_util::StreamExt;

    let (_, _, first_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "partial.bin",
            "test/repo",
            "hf",
            Some(0),
            Some(CHUNK_SIZE as u64 - 1),
            None,
        )
        .await
        .unwrap();

    let mut first_stream = first_stream;
    let mut first_collected = Vec::new();
    while let Some(result) = first_stream.next().await {
        first_collected.extend_from_slice(&result.unwrap());
    }
    assert_eq!(first_collected.len(), CHUNK_SIZE);
    assert_eq!(&first_collected[..], &test_data[0..CHUNK_SIZE]);

    let gets_before = get_count.load(Ordering::SeqCst);
    let (_, _, second_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "partial.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let mut second_stream = second_stream;
    let mut second_collected = Vec::new();
    while let Some(result) = second_stream.next().await {
        second_collected.extend_from_slice(&result.unwrap());
    }
    assert_eq!(second_collected, test_data);

    let new_gets = get_count.load(Ordering::SeqCst) - gets_before;
    let expected_max = ((CHUNK_SIZE + 100) / CHUNK_SIZE + 1) as u32;
    assert!(
        new_gets <= expected_max,
        "unexpected downstream gets: {new_gets} > {expected_max}"
    );
}

#[tokio::test]
async fn test_retry_after_client_disconnect_restarts_incomplete_session() {
    let total = CHUNK_SIZE * 4 + 500;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(11).wrapping_add(53))
        .collect();
    let state = MockState {
        get_count: Arc::new(AtomicU32::new(0)),
        head_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
    };

    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(mock_head).get(mock_get),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dir = TempDir::new().unwrap();
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));

    let head_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let http_client = reqwest::Client::new();

    let service = Arc::new(CacheService::new(
        metadata,
        backend,
        None,
        http_client,
        head_client,
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    ));

    let upstream_url = format!("http://{}/test/repo/resolve/main/retry.bin", addr);

    use futures_util::StreamExt;

    let (_, _, first_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "retry.bin",
            "test/repo",
            "hf",
            Some(0),
            Some(CHUNK_SIZE as u64 * 2 - 1),
            None,
        )
        .await
        .unwrap();

    let mut first_stream = first_stream;
    let mut first_collected = Vec::new();
    while let Some(result) = first_stream.next().await {
        match result {
            Ok(chunk) => first_collected.extend_from_slice(&chunk),
            Err(_) => break,
        }
    }

    drop(first_stream);

    let (_, _, second_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "retry.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let mut second_stream = second_stream;
    let mut second_collected = Vec::new();
    while let Some(result) = second_stream.next().await {
        second_collected.extend_from_slice(&result.unwrap());
    }

    assert_eq!(second_collected.len(), total);
    assert_eq!(
        second_collected, test_data,
        "second full download must assemble all bytes"
    );
}

#[derive(Clone)]
struct RepairState {
    get_count: Arc<AtomicU32>,
    test_data: Arc<Vec<u8>>,
    fail_ranges: Arc<Mutex<HashSet<(u64, u64)>>>,
}

async fn repair_head(State(state): State<RepairState>) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", state.test_data.len())
        .header("ETag", r#""repair-etag""#)
        .header("X-Repo-Commit", "repair-commit")
        .header("X-Linked-Size", state.test_data.len() as i64)
        .header("X-Linked-ETag", r#""repair-linked-etag""#)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::empty())
        .unwrap()
}

async fn repair_get(
    State(state): State<RepairState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    state.get_count.fetch_add(1, Ordering::SeqCst);

    let total = state.test_data.len() as u64;
    let (start, end) = if let Some(range) = headers.get("range") {
        let range_str = range.to_str().unwrap();
        let range = range_str.strip_prefix("bytes=").unwrap();
        let (s, e) = range.split_once('-').unwrap();
        let start: u64 = s.parse().unwrap();
        let end: u64 = if e.is_empty() {
            total - 1
        } else {
            e.parse().unwrap()
        };
        (start, end)
    } else {
        (0u64, total - 1)
    };

    if state.fail_ranges.lock().unwrap().remove(&(start, end)) {
        return Err((StatusCode::BAD_GATEWAY, "upstream unavailable".to_string()));
    }

    let data = &state.test_data[start as usize..=end as usize];
    let status = if start == 0 && end == total - 1 {
        StatusCode::OK
    } else {
        StatusCode::PARTIAL_CONTENT
    };

    Ok(Response::builder()
        .status(status)
        .header("Content-Length", data.len())
        .header(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end, total),
        )
        .body(axum::body::Body::from(data.to_vec()))
        .unwrap())
}

#[tokio::test]
async fn test_corrupt_cached_large_chunk_refetches_from_upstream() {
    let total = CHUNK_SIZE * 2 + 123;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(5).wrapping_add(17))
        .collect();
    let state = RepairState {
        get_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
        fail_ranges: Arc::new(Mutex::new(HashSet::new())),
    };

    let get_count = state.get_count.clone();
    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(repair_head).get(repair_get),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dir = TempDir::new().unwrap();
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));

    let head_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let http_client = reqwest::Client::new();

    let service = Arc::new(CacheService::new(
        metadata.clone(),
        backend.clone(),
        None,
        http_client,
        head_client,
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    ));

    let upstream_url = format!("http://{}/test/repo/resolve/main/repair-large.bin", addr);

    let (_, _, first_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "repair-large.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    use futures_util::StreamExt;
    let mut first_stream = first_stream;
    let mut first_collected = Vec::new();
    while let Some(result) = first_stream.next().await {
        first_collected.extend_from_slice(&result.unwrap());
    }
    assert_eq!(first_collected, test_data);

    let corrupt_start = CHUNK_SIZE as u64;
    let corrupt_end = corrupt_start + CHUNK_SIZE as u64 - 1;
    let corrupt_sha = hugrs::chunker::sha256_hex(&test_data[CHUNK_SIZE..CHUNK_SIZE * 2]);
    backend
        .put(&corrupt_sha, &vec![9u8; CHUNK_SIZE])
        .await
        .unwrap();

    let gets_before = get_count.load(Ordering::SeqCst);
    let (_, _, second_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "repair-large.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let mut second_stream = second_stream;
    let mut second_collected = Vec::new();
    while let Some(result) = second_stream.next().await {
        second_collected.extend_from_slice(&result.unwrap());
    }

    assert_eq!(second_collected, test_data);
    assert_eq!(
        get_count.load(Ordering::SeqCst) - gets_before,
        1,
        "corrupt cached chunk should trigger one upstream refetch"
    );

    let repaired = backend.get(&corrupt_sha).await.unwrap();
    assert_eq!(repaired, test_data[CHUNK_SIZE..CHUNK_SIZE * 2]);
    assert_eq!(corrupt_start, CHUNK_SIZE as u64);
    assert_eq!(corrupt_end, CHUNK_SIZE as u64 * 2 - 1);

    // Verify metadata updated: sha256 reverted to correct value in file_chunks
    let file = metadata
        .get_file_by_name("repair-large.bin", "hf")
        .unwrap()
        .unwrap();
    let chunks = metadata.get_file_chunks(file.id).unwrap();
    let expected_sha = hugrs::chunker::sha256_hex(&test_data[CHUNK_SIZE..CHUNK_SIZE * 2]);
    assert_eq!(
        chunks[1].sha256, expected_sha,
        "file_chunks must reflect repaired sha256 after refetch"
    );
    assert_eq!(
        chunks[1].chunk_size, CHUNK_SIZE as i64,
        "file_chunks must reflect correct chunk_size after refetch"
    );

    // Verify chunks/file_chunks consistency after refetch
    let refs_result = metadata.reconsile_chunk_refs(false).unwrap();
    assert_eq!(
        refs_result.mismatched_chunks, 0,
        "reconsile must find no ref_count mismatches after corrupt+refetch"
    );
    assert_eq!(
        refs_result.orphaned_marked, 0,
        "reconsile must find no orphan chunks after corrupt+refetch"
    );
}

#[tokio::test]
async fn test_corrupt_cached_small_file_refetches_from_upstream() {
    let test_data: Vec<u8> = (0..1024usize)
        .map(|i| (i as u8).wrapping_mul(19).wrapping_add(3))
        .collect();
    let state = RepairState {
        get_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
        fail_ranges: Arc::new(Mutex::new(HashSet::new())),
    };

    let get_count = state.get_count.clone();
    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(repair_head).get(repair_get),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dir = TempDir::new().unwrap();
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));

    let head_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let http_client = reqwest::Client::new();

    let service = Arc::new(CacheService::new(
        metadata.clone(),
        backend.clone(),
        None,
        http_client,
        head_client,
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    ));

    let upstream_url = format!("http://{}/test/repo/resolve/main/repair-small.bin", addr);

    let (_, _, first_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "repair-small.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    use futures_util::StreamExt;
    let mut first_stream = first_stream;
    let mut first_collected = Vec::new();
    while let Some(result) = first_stream.next().await {
        first_collected.extend_from_slice(&result.unwrap());
    }
    assert_eq!(first_collected, test_data);

    let sha = hugrs::chunker::sha256_hex(&test_data);
    backend
        .put(&sha, &vec![99u8; test_data.len()])
        .await
        .unwrap();

    let gets_before = get_count.load(Ordering::SeqCst);
    let (_, _, second_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "repair-small.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let mut second_stream = second_stream;
    let mut second_collected = Vec::new();
    while let Some(result) = second_stream.next().await {
        second_collected.extend_from_slice(&result.unwrap());
    }

    assert_eq!(second_collected, test_data);
    assert_eq!(
        get_count.load(Ordering::SeqCst) - gets_before,
        1,
        "corrupt small file should trigger one upstream refetch"
    );

    let repaired = backend.get(&sha).await.unwrap();
    assert_eq!(repaired, test_data);

    let file = metadata
        .get_file_by_name("repair-small.bin", "hf")
        .unwrap()
        .unwrap();
    let chunks = metadata.get_file_chunks(file.id).unwrap();
    assert_eq!(
        chunks[0].sha256, sha,
        "file_chunks must reflect repaired sha256 after refetch"
    );
    assert_eq!(
        chunks[0].chunk_size,
        test_data.len() as i64,
        "file_chunks must reflect correct chunk_size after refetch"
    );

    let refs_result = metadata.reconsile_chunk_refs(false).unwrap();
    assert_eq!(refs_result.mismatched_chunks, 0);
    assert_eq!(refs_result.orphaned_marked, 0);
}

#[tokio::test]
async fn test_corrupt_cached_chunk_returns_error_when_upstream_unavailable() {
    let total = CHUNK_SIZE * 2 + 123;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(5).wrapping_add(17))
        .collect();

    let fail_range: (u64, u64) = (CHUNK_SIZE as u64, (CHUNK_SIZE * 2 - 1) as u64);
    let fail_ranges = Arc::new(Mutex::new(HashSet::new()));
    let state = RepairState {
        get_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
        fail_ranges: fail_ranges.clone(),
    };

    let app = Router::new()
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            head(repair_head).get(repair_get),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let dir = TempDir::new().unwrap();
    let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));

    let head_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let http_client = reqwest::Client::new();

    let service = Arc::new(CacheService::new(
        metadata,
        backend.clone(),
        None,
        http_client,
        head_client,
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    ));

    let upstream_url = format!("http://{}/test/repo/resolve/main/repair-fail.bin", addr);

    let (_, _, first_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "repair-fail.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    use futures_util::StreamExt;
    let mut first_stream = first_stream;
    while let Some(result) = first_stream.next().await {
        result.unwrap();
    }

    let corrupt_sha = hugrs::chunker::sha256_hex(&test_data[CHUNK_SIZE..CHUNK_SIZE * 2]);
    backend
        .put(&corrupt_sha, &vec![9u8; CHUNK_SIZE])
        .await
        .unwrap();
    fail_ranges.lock().unwrap().insert(fail_range);

    let (_, _, second_stream) = service
        .stream_from_upstream(
            &upstream_url,
            "repair-fail.bin",
            "test/repo",
            "hf",
            Some(CHUNK_SIZE as u64),
            None,
            None,
        )
        .await
        .unwrap();

    let mut second_stream = second_stream;
    let mut saw_err = false;
    while let Some(result) = second_stream.next().await {
        match result {
            Ok(_) => {}
            Err(err) => {
                saw_err = true;
                assert!(
                    err.to_string().contains("upstream unavailable")
                        || err.to_string().contains("502"),
                    "unexpected error: {err}"
                );
                break;
            }
        }
    }

    assert!(
        saw_err,
        "corrupt chunk with failed upstream refetch should error"
    );
}
