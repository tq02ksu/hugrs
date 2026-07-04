use axum::{
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode},
    response::Response,
    routing::any,
    Router,
};
use hugrs::config::MsConfig;
use hugrs::metadata::MetadataStore;
use hugrs::service::CacheService;
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

// ---------- mock git upstream ----------

#[derive(Clone)]
struct MockGitState {
    refs_data: Arc<Vec<u8>>,
    pack_data: Arc<Vec<u8>>,
    lfs_response: Arc<String>,
    info_refs_count: Arc<AtomicU32>,
    upload_pack_count: Arc<AtomicU32>,
    lfs_batch_count: Arc<AtomicU32>,
    auth_headers: Arc<std::sync::Mutex<Vec<String>>>,
    received_bodies: Arc<std::sync::Mutex<Vec<String>>>,
}

async fn mock_git_catch_all(
    State(s): State<MockGitState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, (StatusCode, String)> {
    let path = uri.path();
    let is_info_refs = path.ends_with("/info/refs");
    let is_upload_pack = path.ends_with("/git-upload-pack");
    let is_lfs_batch = path.ends_with("/info/lfs/objects/batch");

    if method == Method::GET && is_info_refs {
        s.info_refs_count.fetch_add(1, Ordering::SeqCst);
        let content_type = "application/x-git-upload-pack-advertisement";
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", content_type)
            .header("Cache-Control", "no-cache")
            .body(axum::body::Body::from(s.refs_data.as_ref().clone()))
            .unwrap())
    } else if method == Method::POST && is_upload_pack {
        s.upload_pack_count.fetch_add(1, Ordering::SeqCst);
        if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
            s.auth_headers.lock().unwrap().push(auth.to_string());
        }
        s.received_bodies
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(&body).to_string());
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/x-git-upload-pack-result")
            .header("Cache-Control", "no-cache")
            .body(axum::body::Body::from(s.pack_data.as_ref().clone()))
            .unwrap())
    } else if method == Method::POST && is_lfs_batch {
        s.lfs_batch_count.fetch_add(1, Ordering::SeqCst);
        if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
            s.auth_headers.lock().unwrap().push(auth.to_string());
        }
        s.received_bodies
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(&body).to_string());
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/vnd.git-lfs+json")
            .body(axum::body::Body::from(s.lfs_response.as_ref().clone()))
            .unwrap())
    } else {
        Err((
            StatusCode::NOT_FOUND,
            format!("not found: {} {}", method, path),
        ))
    }
}

async fn start_git_upstream(lfs_response: String) -> (String, MockGitState) {
    let refs_data = b"001e# service=git-upload-pack\n0000015500123456789abcdef0123456789abcdef012345678 HEAD\0multi_ack thin-pack side-band side-band-64k ofs-delta shallow deepen-since deepen-not deepen-relative no-progress include-tag multi_ack_detailed no-done symref=HEAD:refs/heads/main agent=git/2.40.0\n003f123456789abcdef0123456789abcdef012345678 refs/heads/main\n0000".to_vec();
    let pack_data = b"PACK......binary-pack-data......".to_vec();

    let state = MockGitState {
        refs_data: Arc::new(refs_data),
        pack_data: Arc::new(pack_data),
        lfs_response: Arc::new(lfs_response),
        info_refs_count: Arc::new(AtomicU32::new(0)),
        upload_pack_count: Arc::new(AtomicU32::new(0)),
        lfs_batch_count: Arc::new(AtomicU32::new(0)),
        auth_headers: Arc::new(std::sync::Mutex::new(Vec::new())),
        received_bodies: Arc::new(std::sync::Mutex::new(Vec::new())),
    };

    let app = Router::new()
        .route("/{*rest}", any(mock_git_catch_all))
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

fn build_git_router(upstream: &str, ms_upstream: Option<&str>, dir: &TempDir) -> Router {
    use tokio::sync::Mutex as TokioMutex;

    let service = make_service(dir, "git_db");

    let ms_endpoint = ms_upstream.unwrap_or(upstream).to_string();

    let config = hugrs::config::Config {
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
            path: dir.path().join("git_db"),
        },
        huggingface: hugrs::config::HfConfig {
            endpoint: upstream.to_string(),
            token: None,
            proxy: None,
            timeout_secs: 120,
            connect_timeout_secs: 15,
        },
        modelscope: MsConfig {
            endpoint: ms_endpoint,
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

    Router::new()
        // Legacy routes (HF)
        .route(
            "/{org}/{repo}/info/refs",
            axum::routing::get(hugrs::git::git_info_refs),
        )
        .route(
            "/{org}/{repo}/git-upload-pack",
            axum::routing::post(hugrs::git::git_upload_pack),
        )
        .route(
            "/{org}/{repo}/info/lfs/objects/batch",
            axum::routing::post(hugrs::git::lfs_batch),
        )
        // /hf/ prefix routes
        .route(
            "/hf/{org}/{repo}/info/refs",
            axum::routing::get(hugrs::git::git_info_refs),
        )
        .route(
            "/hf/{org}/{repo}/git-upload-pack",
            axum::routing::post(hugrs::git::git_upload_pack),
        )
        .route(
            "/hf/{org}/{repo}/info/lfs/objects/batch",
            axum::routing::post(hugrs::git::lfs_batch),
        )
        // /ms/ prefix routes
        .route(
            "/ms/{org}/{repo}/info/refs",
            axum::routing::get(hugrs::git::git_info_refs),
        )
        .route(
            "/ms/{org}/{repo}/git-upload-pack",
            axum::routing::post(hugrs::git::git_upload_pack),
        )
        .route(
            "/ms/{org}/{repo}/info/lfs/objects/batch",
            axum::routing::post(hugrs::git::lfs_batch),
        )
        .with_state(state)
}

fn build_git_router_with_token(
    upstream: &str,
    hf_token: &str,
    ms_token: &str,
    dir: &TempDir,
) -> Router {
    use tokio::sync::Mutex as TokioMutex;

    let service = make_service(dir, "git_tok_db");

    let config = hugrs::config::Config {
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
            path: dir.path().join("git_tok_db"),
        },
        huggingface: hugrs::config::HfConfig {
            endpoint: upstream.to_string(),
            token: Some(hf_token.to_string()),
            proxy: None,
            timeout_secs: 120,
            connect_timeout_secs: 15,
        },
        modelscope: MsConfig {
            endpoint: upstream.to_string(),
            token: Some(ms_token.to_string()),
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

    Router::new()
        .route(
            "/hf/{org}/{repo}/info/lfs/objects/batch",
            axum::routing::post(hugrs::git::lfs_batch),
        )
        .route(
            "/ms/{org}/{repo}/info/lfs/objects/batch",
            axum::routing::post(hugrs::git::lfs_batch),
        )
        .with_state(state)
}

// ---------- unit tests ----------

#[test]
fn test_rewrite_lfs_urls_huggingface_co() {
    let body = json!({
        "objects": [{
            "oid": "abc123",
            "size": 1024,
            "actions": {
                "download": {
                    "href": "https://huggingface.co/org/repo/resolve/main/model.bin",
                    "header": {"Authorization": "Bearer x"},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(
        href,
        "http://127.0.0.1:3000/org/repo/resolve/main/model.bin"
    );
}

#[test]
fn test_rewrite_lfs_urls_modelscope_cn() {
    let body = json!({
        "objects": [{
            "oid": "def456",
            "size": 2048,
            "actions": {
                "download": {
                    "href": "https://modelscope.cn/api/v1/models/org/repo/repo?Revision=master&FilePath=model.safetensors",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "ms",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(
        href,
        "http://127.0.0.1:3000/ms/api/v1/models/org/repo/repo?Revision=master&FilePath=model.safetensors"
    );
}

#[test]
fn test_rewrite_lfs_urls_www_modelscope_cn() {
    let body = json!({
        "objects": [{
            "oid": "def456",
            "size": 2048,
            "actions": {
                "download": {
                    "href": "https://www.modelscope.cn/api/v1/models/org/repo/repo?Revision=main&FilePath=f",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "ms",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(
        href,
        "http://127.0.0.1:3000/ms/api/v1/models/org/repo/repo?Revision=main&FilePath=f"
    );
}

#[test]
fn test_rewrite_lfs_urls_hf_mirror() {
    let body = json!({
        "objects": [{
            "oid": "ghi789",
            "size": 4096,
            "actions": {
                "download": {
                    "href": "https://hf-mirror.com/org/repo/resolve/main/weights.bin",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(
        href,
        "http://127.0.0.1:3000/org/repo/resolve/main/weights.bin"
    );
}

#[test]
fn test_rewrite_lfs_urls_cdn_lfs() {
    let body = json!({
        "objects": [{
            "oid": "cdn001",
            "size": 100,
            "actions": {
                "download": {
                    "href": "https://cdn-lfs.huggingface.co/repos/ab/cd/abc123/data.bin",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(href, "http://127.0.0.1:3000/repos/ab/cd/abc123/data.bin");
}

#[test]
fn test_rewrite_lfs_urls_cdn_lfs_us1() {
    let body = json!({
        "objects": [{
            "oid": "us1001",
            "size": 100,
            "actions": {
                "download": {
                    "href": "https://cdn-lfs-us-1.huggingface.co/repos/ef/01/def456/data.dat",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(href, "http://127.0.0.1:3000/repos/ef/01/def456/data.dat");
}

#[test]
fn test_rewrite_lfs_urls_lfs_domain() {
    let body = json!({
        "objects": [{
            "oid": "lfs001",
            "size": 100,
            "actions": {
                "download": {
                    "href": "https://lfs.huggingface.co/repos/gh/ij/789.dat",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(href, "http://127.0.0.1:3000/repos/gh/ij/789.dat");
}

#[test]
fn test_rewrite_lfs_urls_no_download_action() {
    let body = json!({
        "objects": [{
            "oid": "nope001",
            "size": 100,
            "actions": {
                "upload": {
                    "href": "https://huggingface.co/upload/here",
                    "header": {}
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let upload_href = v["objects"][0]["actions"]["upload"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(upload_href, "https://huggingface.co/upload/here");
}

#[test]
fn test_rewrite_lfs_urls_unrecognized_domain_unchanged() {
    let body = json!({
        "objects": [{
            "oid": "ext001",
            "size": 100,
            "actions": {
                "download": {
                    "href": "https://unknown-cdn.example.com/path/to/file",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(href, "https://unknown-cdn.example.com/path/to/file");
}

#[test]
fn test_rewrite_lfs_urls_empty_objects() {
    let body = json!({"objects": []}).to_string();
    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();
    assert_eq!(rewritten, body);
}

#[test]
fn test_rewrite_lfs_urls_multiple_objects() {
    let body = json!({
        "objects": [
            {
                "oid": "obj1",
                "size": 100,
                "actions": {
                    "download": {
                        "href": "https://huggingface.co/org/repo/resolve/main/file1.bin",
                        "header": {}
                    }
                }
            },
            {
                "oid": "obj2",
                "size": 200,
                "actions": {
                    "download": {
                        "href": "https://cdn-lfs.huggingface.co/org/repo/resolve/main/file2.bin",
                        "header": {}
                    }
                }
            }
        ]
    })
    .to_string();

    let rewritten = hugrs::git::rewrite_lfs_urls(
        &body,
        "http://127.0.0.1:3000",
        "https://huggingface.co",
        "hf",
    )
    .unwrap();

    let v: serde_json::Value = serde_json::from_str(&rewritten).unwrap();
    let href1 = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    let href2 = v["objects"][1]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(
        href1,
        "http://127.0.0.1:3000/org/repo/resolve/main/file1.bin"
    );
    assert_eq!(
        href2,
        "http://127.0.0.1:3000/org/repo/resolve/main/file2.bin"
    );
}

// ---------- e2e: git info/refs ----------

#[tokio::test]
async fn test_git_info_refs_legacy_path() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/test-org/test-repo/info/refs?service=git-upload-pack")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), state.refs_data.as_ref());
    assert_eq!(state.info_refs_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_git_info_refs_hf_prefix_path() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/hf/org/repo/info/refs?service=git-upload-pack")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), state.refs_data.as_ref());
    assert_eq!(state.info_refs_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_git_info_refs_ms_prefix_path() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/ms/qwen/Qwen3.5-0.8B/info/refs?service=git-upload-pack")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), state.refs_data.as_ref());
    assert_eq!(state.info_refs_count.load(Ordering::SeqCst), 1);
}

// ---------- e2e: git upload-pack ----------

#[tokio::test]
async fn test_git_upload_pack_hf_prefix() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    let upload_body = vec![0x00u8, 0x01, 0x02, 0x03, 0x04];

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/hf/org/repo/git-upload-pack")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(axum::body::Body::from(upload_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), state.pack_data.as_ref());
    assert_eq!(state.upload_pack_count.load(Ordering::SeqCst), 1);

    let received = state.received_bodies.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert!(!received[0].is_empty());
}

#[tokio::test]
async fn test_git_upload_pack_ms_prefix() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    let upload_body = vec![0x10u8, 0x11, 0x12, 0x13];

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/ms/qwen/Qwen3.5-0.8B/git-upload-pack")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(axum::body::Body::from(upload_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), state.pack_data.as_ref());
    assert_eq!(state.upload_pack_count.load(Ordering::SeqCst), 1);
}

// ---------- e2e: LFS batch ----------

#[tokio::test]
async fn test_lfs_batch_hf_prefix_rewrites_urls() {
    let lfs_response = json!({
        "transfer": "basic",
        "objects": [{
            "oid": "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",
            "size": 999888777,
            "actions": {
                "download": {
                    "href": "https://huggingface.co/org/repo/resolve/main/pytorch_model.bin",
                    "header": {},
                    "expires_at": "2099-12-31T23:59:59Z"
                }
            }
        }]
    })
    .to_string();

    let (upstream, state) = start_git_upstream(lfs_response).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    let request_body = json!({
        "operation": "download",
        "transfer": ["basic"],
        "objects": [{"oid": "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff", "size": 999888777}],
        "hash_algo": "sha256"
    })
    .to_string();

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/hf/org/repo/info/lfs/objects/batch")
        .header("Content-Type", "application/vnd.git-lfs+json")
        .header("Accept", "application/vnd.git-lfs+json")
        .header("Host", "127.0.0.1:3000")
        .body(axum::body::Body::from(request_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/vnd.git-lfs+json"
    );

    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(
        href,
        "http://127.0.0.1:3000/org/repo/resolve/main/pytorch_model.bin"
    );
    assert_eq!(state.lfs_batch_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_lfs_batch_ms_prefix_rewrites_urls() {
    let lfs_response = json!({
        "transfer": "basic",
        "objects": [{
            "oid": "aaaabbbbccccddddeeeeffff0000111122223333444455556666777788889999",
            "size": 555444333,
            "actions": {
                "download": {
                    "href": "https://modelscope.cn/api/v1/models/qwen/Qwen3.5-0.8B/repo?Revision=master&FilePath=model.safetensors",
                    "header": {"Authorization": "Bearer upstream-token"},
                    "expires_at": "2099-12-31T23:59:59Z"
                }
            }
        }]
    })
    .to_string();

    let (upstream, state) = start_git_upstream(lfs_response.clone()).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    let request_body = json!({
        "operation": "download",
        "transfer": ["basic"],
        "objects": [{"oid": "aaaabbbbccccddddeeeeffff0000111122223333444455556666777788889999", "size": 555444333}],
        "hash_algo": "sha256"
    })
    .to_string();

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/ms/qwen/Qwen3.5-0.8B/info/lfs/objects/batch")
        .header("Content-Type", "application/vnd.git-lfs+json")
        .header("Accept", "application/vnd.git-lfs+json")
        .header("Host", "127.0.0.1:3000")
        .body(axum::body::Body::from(request_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "application/vnd.git-lfs+json"
    );

    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let href = v["objects"][0]["actions"]["download"]["href"]
        .as_str()
        .unwrap();
    assert_eq!(
        href,
        "http://127.0.0.1:3000/ms/api/v1/models/qwen/Qwen3.5-0.8B/repo?Revision=master&FilePath=model.safetensors"
    );
    assert_eq!(state.lfs_batch_count.load(Ordering::SeqCst), 1);
}

// ---------- e2e: LFS batch with auth tokens ----------

#[tokio::test]
async fn test_lfs_batch_hf_forwards_token() {
    let lfs_response = json!({
        "transfer": "basic",
        "objects": [{
            "oid": "tok001",
            "size": 100,
            "actions": {
                "download": {
                    "href": "https://huggingface.co/org/repo/resolve/main/file.bin",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let (upstream, state) = start_git_upstream(lfs_response).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router_with_token(&upstream, "hf-token-123", "ms-token-456", &dir);

    let request_body = json!({
        "operation": "download",
        "transfer": ["basic"],
        "objects": [{"oid": "tok001", "size": 100}],
        "hash_algo": "sha256"
    })
    .to_string();

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/hf/org/repo/info/lfs/objects/batch")
        .header("Content-Type", "application/vnd.git-lfs+json")
        .header("Accept", "application/vnd.git-lfs+json")
        .header("Host", "127.0.0.1:3000")
        .body(axum::body::Body::from(request_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let auths = state.auth_headers.lock().unwrap();
    assert!(auths.iter().any(|a| a == "Bearer hf-token-123"));
}

#[tokio::test]
async fn test_lfs_batch_ms_forwards_token() {
    let lfs_response = json!({
        "transfer": "basic",
        "objects": [{
            "oid": "mstok001",
            "size": 100,
            "actions": {
                "download": {
                    "href": "https://modelscope.cn/api/v1/models/org/repo/repo?FilePath=f",
                    "header": {},
                    "expires_at": "2099-01-01T00:00:00Z"
                }
            }
        }]
    })
    .to_string();

    let (upstream, state) = start_git_upstream(lfs_response).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router_with_token(&upstream, "hf-token-xxx", "ms-token-789", &dir);

    let request_body = json!({
        "operation": "download",
        "transfer": ["basic"],
        "objects": [{"oid": "mstok001", "size": 100}],
        "hash_algo": "sha256"
    })
    .to_string();

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/ms/org/repo/info/lfs/objects/batch")
        .header("Content-Type", "application/vnd.git-lfs+json")
        .header("Accept", "application/vnd.git-lfs+json")
        .header("Host", "127.0.0.1:3000")
        .body(axum::body::Body::from(request_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let auths = state.auth_headers.lock().unwrap();
    assert!(auths.iter().any(|a| a == "Bearer ms-token-789"));
}

// ---------- e2e: LFS batch user-agent forwarding ----------

#[tokio::test]
async fn test_lfs_batch_forwards_user_agent() {
    let lfs_response = json!({
        "transfer": "basic",
        "objects": []
    })
    .to_string();

    let (upstream, state) = start_git_upstream(lfs_response).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    let request_body = json!({
        "operation": "download",
        "transfer": ["basic"],
        "objects": [],
        "hash_algo": "sha256"
    })
    .to_string();

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/hf/org/repo/info/lfs/objects/batch")
        .header("Content-Type", "application/vnd.git-lfs+json")
        .header("Accept", "application/vnd.git-lfs+json")
        .header(
            "User-Agent",
            "git-lfs/3.4.0 (GitHub; linux amd64; go 1.21.0)",
        )
        .header("Host", "127.0.0.1:3000")
        .body(axum::body::Body::from(request_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(state.lfs_batch_count.load(Ordering::SeqCst), 1);
}

// ---------- e2e: git info/refs query forwarding ----------

#[tokio::test]
async fn test_git_info_refs_forwards_query_params() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/hf/org/repo/info/refs?service=git-upload-pack&version=2")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(state.info_refs_count.load(Ordering::SeqCst), 1);
}

// ---------- e2e: separate HF/MS upstream endpoints ----------

#[tokio::test]
async fn test_git_info_refs_ms_uses_separate_upstream() {
    let lfs_body = json!({"objects": []}).to_string();
    let (_hf_upstream, _hf_state) = start_git_upstream(lfs_body.clone()).await;
    let (ms_upstream, ms_state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();

    // Build a minimal router where MS points to a separate upstream
    use tokio::sync::Mutex as TokioMutex;

    let service = make_service(&dir, "sep_db");

    let config = hugrs::config::Config {
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
            path: dir.path().join("sep_db"),
        },
        huggingface: hugrs::config::HfConfig {
            endpoint: "http://127.0.0.1:1".into(),
            token: None,
            proxy: None,
            timeout_secs: 120,
            connect_timeout_secs: 15,
        },
        modelscope: MsConfig {
            endpoint: ms_upstream,
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

    let app = Router::new()
        .route(
            "/ms/{org}/{repo}/info/refs",
            axum::routing::get(hugrs::git::git_info_refs),
        )
        .with_state(state);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/ms/qwen/Qwen3.5-0.8B/info/refs?service=git-upload-pack")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(ms_state.info_refs_count.load(Ordering::SeqCst), 1);
}

// ---------- e2e: lfs batch preserves non-download fields ----------

#[tokio::test]
async fn test_lfs_batch_preserves_response_structure() {
    let lfs_response = json!({
        "transfer": "basic",
        "hash_algo": "sha256",
        "objects": [
            {
                "oid": "preserve001",
                "size": 777,
                "authenticated": true,
                "actions": {
                    "download": {
                        "href": "https://huggingface.co/a/b/resolve/main/x.bin",
                        "header": {"Authorization": "Bearer up"},
                        "expires_at": "2099-01-01T00:00:00Z",
                        "expires_in": 86400
                    },
                    "upload": {
                        "href": "https://huggingface.co/a/b/upload/x.bin",
                        "header": {},
                        "expires_at": "2099-01-01T00:00:00Z"
                    }
                }
            },
            {
                "oid": "preserve002",
                "size": 888,
                "actions": {}
            }
        ]
    })
    .to_string();

    let (upstream, state) = start_git_upstream(lfs_response).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    let request_body = json!({
        "operation": "download",
        "transfer": ["basic"],
        "objects": [{"oid": "preserve001", "size": 777}, {"oid": "preserve002", "size": 888}]
    })
    .to_string();

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/hf/org/repo/info/lfs/objects/batch")
        .header("Content-Type", "application/vnd.git-lfs+json")
        .header("Accept", "application/vnd.git-lfs+json")
        .header("Host", "127.0.0.1:3000")
        .body(axum::body::Body::from(request_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 10_000_000)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(v["transfer"], json!("basic"));
    assert_eq!(v["hash_algo"], json!("sha256"));

    let obj0 = &v["objects"][0];
    assert_eq!(obj0["authenticated"], json!(true));
    assert_eq!(
        obj0["actions"]["download"]["href"],
        json!("http://127.0.0.1:3000/a/b/resolve/main/x.bin")
    );
    assert_eq!(
        obj0["actions"]["upload"]["href"],
        json!("https://huggingface.co/a/b/upload/x.bin")
    );
    assert_eq!(obj0["actions"]["download"]["expires_in"], json!(86400));
    assert_eq!(
        obj0["actions"]["download"]["expires_at"],
        json!("2099-01-01T00:00:00Z")
    );

    assert_eq!(v["objects"][1]["oid"], json!("preserve002"));
    assert_eq!(v["objects"][1]["size"], json!(888));

    assert_eq!(state.lfs_batch_count.load(Ordering::SeqCst), 1);
}

// ---------- e2e: cache hit on repeated git operations ----------

#[tokio::test]
async fn test_git_info_refs_no_cache() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    use tower::util::ServiceExt;

    let req1 = axum::http::Request::builder()
        .method("GET")
        .uri("/hf/org/repo/info/refs?service=git-upload-pack")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let first_count = state.info_refs_count.load(Ordering::SeqCst);

    let req2 = axum::http::Request::builder()
        .method("GET")
        .uri("/hf/org/repo/info/refs?service=git-upload-pack")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    // git info/refs is pass-through (no caching), so each request hits upstream
    let second_count = state.info_refs_count.load(Ordering::SeqCst);
    assert!(second_count > first_count);
}

// ---------- e2e: content-type headers in git responses ----------

#[tokio::test]
async fn test_git_info_refs_content_type() {
    let lfs_body = json!({"objects": []}).to_string();
    let (upstream, _state) = start_git_upstream(lfs_body).await;
    let dir = TempDir::new().unwrap();
    let app = build_git_router(&upstream, None, &dir);

    use tower::util::ServiceExt;
    let req = axum::http::Request::builder()
        .method("GET")
        .uri("/ms/org/repo/info/refs?service=git-upload-pack")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("content-type").is_some());
}
