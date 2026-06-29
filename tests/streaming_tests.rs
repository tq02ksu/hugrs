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
        .header("ETag", "\"mock-etag\"")
        .header("X-Repo-Commit", "abc123mock")
        .header("X-Linked-Size", state.test_data.len() as i64)
        .header("X-Linked-ETag", "\"mock-linked-etag\"")
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
    let test_data: Vec<u8> = (0..((CHUNK_SIZE as u64 * 3 + 1000) as u8))
        .map(|i| i.wrapping_mul(7).wrapping_add(13))
        .collect();
    let state = MockState {
        get_count: Arc::new(AtomicU32::new(0)),
        head_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
    };

    let get_count = state.get_count.clone();
    let _head_count = state.head_count.clone();
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

    let upstream_url = format!("http://{}/test/repo/resolve/main/test.bin", addr);

    let (file, content_length, stream) = service
        .stream_from_upstream(
            &upstream_url,
            "test.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    assert_eq!(file.total_size as usize, test_data.len());
    assert_eq!(content_length as usize, test_data.len());

    let mut collected = Vec::new();
    use futures_util::StreamExt;
    let mut stream = stream;
    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        collected.extend_from_slice(&chunk);
    }
    assert_eq!(collected, test_data);

    let expected_chunks = (test_data.len() as f64 / CHUNK_SIZE as f64).ceil() as u32;

    let upstream_gets_after_first = get_count.load(Ordering::SeqCst);
    assert_eq!(upstream_gets_after_first, expected_chunks);

    let (_file2, content_length2, stream2) = service
        .stream_from_upstream(
            &upstream_url,
            "test.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();
    // Verify file is now complete after first download
    assert!(
        service.is_file_complete("test.bin", "hf").await.unwrap(),
        "file should be complete after first GET"
    );

    assert_eq!(content_length2 as usize, test_data.len());

    let mut collected2 = Vec::new();
    let mut stream2 = stream2;
    while let Some(result) = stream2.next().await {
        let chunk = result.unwrap();
        collected2.extend_from_slice(&chunk);
    }
    assert_eq!(collected2, test_data);

    let upstream_gets_after_second = get_count.load(Ordering::SeqCst);
    assert_eq!(
        upstream_gets_after_first, upstream_gets_after_second,
        "second GET should not trigger new upstream downloads"
    );
}

#[tokio::test]
async fn test_partial_cache_no_redundant_download() {
    let chunk_sz = CHUNK_SIZE as u64;
    let total = chunk_sz * 3;
    let test_data: Vec<u8> = (0..total as usize)
        .map(|i| (i.wrapping_mul(3).wrapping_add(7)) as u8)
        .collect();
    let state = MockState {
        get_count: Arc::new(AtomicU32::new(0)),
        head_count: Arc::new(AtomicU32::new(0)),
        test_data: Arc::new(test_data.clone()),
    };

    let get_count = state.get_count.clone();
    let _head_count = state.head_count.clone();
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

    let service = CacheService::new(
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
    );

    let upstream_url = format!("http://{}/test/repo/resolve/main/part.bin", addr);

    // Pre-populate file metadata and first chunk only
    service
        .ensure_file_headers(
            "part.bin",
            "test/repo",
            "hf",
            total,
            Some("mock-etag"),
            Some("abc123mock"),
            Some(total as i64),
            Some("mock-linked-etag"),
            Some("application/octet-stream"),
        )
        .unwrap();

    let file = service.info("part.bin", "hf").await.unwrap().unwrap();
    // Manually insert first chunk as cached
    let first_chunk_data = &test_data[0..CHUNK_SIZE];
    let sha = hugrs::chunker::sha256_hex(first_chunk_data);
    let path = format!("{}/{}/{}", &sha[0..2], &sha[2..4], sha);
    backend.put(&sha, first_chunk_data).await.unwrap();
    metadata
        .ensure_chunk(
            &sha,
            "local",
            &path,
            first_chunk_data.len() as i64,
            first_chunk_data.len() as i64,
        )
        .unwrap();
    metadata
        .link_file_chunk(file.id, &sha, 0, first_chunk_data.len() as i64)
        .unwrap();

    // Now call stream_from_upstream - should only download chunks 1 and 2
    let gets_before = get_count.load(Ordering::SeqCst);
    let (_, _, stream) = service
        .stream_from_upstream(
            &upstream_url,
            "part.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let mut collected = Vec::new();
    use futures_util::StreamExt;
    let mut stream = stream;
    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        collected.extend_from_slice(&chunk);
    }
    assert_eq!(collected, test_data);

    let gets_after = get_count.load(Ordering::SeqCst);
    // Only chunks 1 and 2 should be downloaded (not 0 which was precached)
    assert_eq!(
        gets_after - gets_before,
        2,
        "should only download 2 missing chunks (not the precached one)"
    );
}

#[tokio::test]
async fn test_retry_after_client_disconnect_restarts_incomplete_session() {
    let total = CHUNK_SIZE * 2 + 1000;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(11).wrapping_add(5))
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

    let upstream_url = format!("http://{}/test/repo/resolve/main/test.bin", addr);

    use futures_util::StreamExt;

    let (_, _, stream1) = service
        .stream_from_upstream(
            &upstream_url,
            "test.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let mut stream1 = stream1;
    let first_chunk = tokio::time::timeout(std::time::Duration::from_secs(1), stream1.next())
        .await
        .expect("first chunk should arrive")
        .expect("stream should yield first chunk")
        .expect("first chunk should be ok");
    assert_eq!(first_chunk.len(), CHUNK_SIZE);
    drop(stream1);

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let (_, content_length2, stream2) = service
        .stream_from_upstream(
            &upstream_url,
            "test.bin",
            "test/repo",
            "hf",
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(content_length2 as usize, test_data.len());

    let collected = tokio::time::timeout(std::time::Duration::from_secs(2), async move {
        let mut stream2 = stream2;
        let mut collected = Vec::new();
        while let Some(result) = stream2.next().await {
            collected.extend_from_slice(&result.unwrap());
        }
        collected
    })
    .await
    .expect("second subscriber should not hang after retry");

    assert_eq!(collected, test_data);
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
        .header("ETag", "\"repair-etag\"")
        .header("X-Repo-Commit", "repair-commit")
        .header("X-Linked-Size", state.test_data.len() as i64)
        .header("X-Linked-ETag", "\"repair-linked-etag\"")
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
    backend.put(&sha, b"corrupted").await.unwrap();

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
        "corrupt small-file cache should trigger one upstream refetch"
    );
    assert_eq!(backend.get(&sha).await.unwrap(), test_data);
}

#[tokio::test]
async fn test_corrupt_cached_chunk_returns_error_when_upstream_unavailable() {
    let total = CHUNK_SIZE * 2;
    let test_data: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(7).wrapping_add(1))
        .collect();
    let fail_range = (CHUNK_SIZE as u64, CHUNK_SIZE as u64 * 2 - 1);
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

    let corrupt_sha = hugrs::chunker::sha256_hex(&test_data[CHUNK_SIZE..]);
    backend
        .put(&corrupt_sha, &vec![4u8; CHUNK_SIZE])
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
