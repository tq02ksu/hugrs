use crate::config::Config;
use crate::control::{
    AuthInfo, CacheInfo, DeleteResponse, FileListItem, FileListResponse, FileShowResponse,
    GcPreviewResponse, GcRequest, GcResultResponse, RepoListItem, RepoListResponse,
    RepoShowResponse, ServiceStatsResponse, ServiceStatusResponse, SourceInfo, SourcesInfo,
};
use crate::git;
use crate::hf;
use crate::service::CacheService;
use axum::{
    extract::{OriginalUri, Path, Query, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn run(
    mut config: Config,
    service: CacheService,
    ms_http_client: reqwest::Client,
    ms_head_client: reqwest::Client,
) -> anyhow::Result<()> {
    let admin_token = config.ensure_admin_token()?;
    let http_client = hf::build_client(&config)?;
    let head_client = hf::build_head_client(&config)?;
    let app_state = AppState {
        service: Arc::new(Mutex::new(service)),
        config: Arc::new(config),
        admin_token: Arc::new(admin_token),
        http_client: Arc::new(http_client),
        head_client: Arc::new(head_client),
        ms_http_client: Arc::new(ms_http_client),
        ms_head_client: Arc::new(ms_head_client),
    };

    let addr = format!(
        "{}:{}",
        app_state.config.server.host, app_state.config.server.port
    );

    let app = app_router(app_state);

    tracing::info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn app_router(app_state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/api/whoami-v2", get(whoami))
        // Legacy unprefixed HF routes (backward compat)
        .route(
            "/api/models/{org}/{repo}",
            get(hf_model_info_simple).head(hf_model_info_simple),
        )
        .route(
            "/api/models/{org}/{repo}/revision/{revision}",
            get(hf_model_info).head(hf_model_info),
        )
        .route(
            "/api/models/{org}/{repo}/{*suffix}",
            get(hf_model_api_suffix),
        )
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            get(hf_file_proxy).head(hf_file_proxy),
        )
        .route(
            "/api/resolve-cache/{repo_type}/{org}/{repo}/{revision}/{*path}",
            get(hf_file_proxy).head(hf_file_proxy),
        )
        // Git/LFS proxy (legacy)
        .route("/{org}/{repo}/info/refs", get(git::git_info_refs))
        .route("/{org}/{repo}/git-upload-pack", post(git::git_upload_pack))
        .route("/{org}/{repo}/info/lfs/objects/batch", post(git::lfs_batch))
        // New /hf/ prefix routes
        .route(
            "/hf/api/models/{org}/{repo}",
            get(hf_model_info_simple).head(hf_model_info_simple),
        )
        .route(
            "/hf/api/models/{org}/{repo}/revision/{revision}",
            get(hf_model_info).head(hf_model_info),
        )
        .route(
            "/hf/api/models/{org}/{repo}/{*suffix}",
            get(hf_model_api_suffix),
        )
        .route(
            "/hf/{org}/{repo}/resolve/{revision}/{*path}",
            get(hf_file_proxy).head(hf_file_proxy),
        )
        // Git/LFS proxy (/hf/)
        .route("/hf/{org}/{repo}/info/refs", get(git::git_info_refs))
        .route(
            "/hf/{org}/{repo}/git-upload-pack",
            post(git::git_upload_pack),
        )
        .route(
            "/hf/{org}/{repo}/info/lfs/objects/batch",
            post(git::lfs_batch),
        )
        // New /ms/ prefix routes
        .route(
            "/ms/api/v1/models/{org}/{repo}",
            get(ms_model_info_simple).head(ms_model_info_simple),
        )
        .route(
            "/ms/api/v1/models/{org}/{repo}/revision/{revision}",
            get(ms_model_info).head(ms_model_info),
        )
        .route(
            "/ms/api/v1/models/{org}/{repo}/{*suffix}",
            get(ms_model_api_suffix),
        )
        .route(
            "/ms/api/v1/models/{org}/{repo}/repo",
            get(ms_repo_file_proxy).head(ms_repo_file_proxy),
        )
        .route(
            "/ms/{org}/{repo}/resolve/{revision}/{*path}",
            get(ms_file_proxy).head(ms_file_proxy),
        )
        // Git/LFS proxy (/ms/)
        .route("/ms/{org}/{repo}/info/refs", get(git::git_info_refs))
        .route(
            "/ms/{org}/{repo}/git-upload-pack",
            post(git::git_upload_pack),
        )
        .route(
            "/ms/{org}/{repo}/info/lfs/objects/batch",
            post(git::lfs_batch),
        )
        .route("/api/stats", get(stats))
        .route("/api/agent-harnesses", get(agent_harnesses))
        .route("/_hugrs/service", get(control_service_status))
        .route("/_hugrs/service/stats", get(control_service_stats))
        .route("/_hugrs/service/gc", post(control_service_gc))
        .route("/_hugrs/repos", get(control_repos_list))
        .route(
            "/_hugrs/repos/{*repo}",
            get(control_repo_show).delete(control_repo_delete),
        )
        .route(
            "/_hugrs/files",
            get(control_files_list).delete(control_file_delete),
        )
        .route("/_hugrs/files/show", get(control_file_show))
        .layer(middleware::from_fn(log_request))
        .with_state(app_state)
}

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<Mutex<CacheService>>,
    pub config: Arc<Config>,
    pub admin_token: Arc<String>,
    pub http_client: Arc<reqwest::Client>,
    pub head_client: Arc<reqwest::Client>,
    pub ms_http_client: Arc<reqwest::Client>,
    pub ms_head_client: Arc<reqwest::Client>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

async fn log_request(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().to_string();
    let start = std::time::Instant::now();

    let resp = next.run(req).await;

    let elapsed = start.elapsed();
    tracing::info!(
        "{} {} -> {} ({:.0}ms)",
        method,
        uri,
        resp.status().as_u16(),
        elapsed.as_secs_f64() * 1000.0
    );

    resp
}

pub fn hub_config<'a>(
    state: &'a AppState,
    source: &str,
) -> (&'a str, &'a reqwest::Client, &'a reqwest::Client) {
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

async fn root(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    Ok(Json(serde_json::json!({
        "service": "hugrs",
        "version": env!("CARGO_PKG_VERSION"),
        "hf_endpoint": state.config.huggingface.endpoint,
        "ms_endpoint": state.config.modelscope.endpoint,
    })))
}

async fn whoami() -> Result<Json<serde_json::Value>, AppError> {
    Ok(Json(serde_json::json!({
        "name": "mirror",
        "auth": true,
    })))
}

// ── Model info handlers (source-aware) ──

async fn hf_model_info_simple(
    State(state): State<AppState>,
    Path((org, repo)): Path<(String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "hf", org, repo, "main".to_string()).await
}

async fn hf_model_info(
    State(state): State<AppState>,
    Path((org, repo, revision)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "hf", org, repo, revision).await
}

async fn ms_model_info_simple(
    State(state): State<AppState>,
    Path((org, repo)): Path<(String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "ms", org, repo, "main".to_string()).await
}

async fn ms_model_info(
    State(state): State<AppState>,
    Path((org, repo, revision)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_info_inner(state, "ms", org, repo, revision).await
}

async fn model_info_inner(
    state: AppState,
    source: &str,
    org: String,
    repo: String,
    revision: String,
) -> Result<Response, AppError> {
    let (endpoint, client, _head) = hub_config(&state, source);
    let repo_id = format!("{}/{}", org, repo);
    let api_prefix = if source == "ms" {
        "api/v1/models"
    } else {
        "api/models"
    };
    let url = format!(
        "{}/{}/{}/revision/{}",
        endpoint, api_prefix, repo_id, revision
    );

    tracing::info!("model_info proxy to: {}", url);
    let mut req = client.get(&url);
    let token = match source {
        "ms" => &state.config.modelscope.token,
        _ => &state.config.huggingface.token,
    };
    if let Some(ref token) = token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    let resp = req.send().await.map_err(|e| AppError::Anyhow(e.into()))?;
    let status = resp.status();
    let upstream_headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .filter(|(n, _)| *n != "transfer-encoding")
        .map(|(n, v)| (n.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let body = resp
        .text()
        .await
        .map_err(|e| AppError::Anyhow(e.into()))?
        .into_bytes();

    let mut builder = Response::builder().status(status);
    for (name, value) in &upstream_headers {
        builder = builder.header(name, value);
    }
    builder
        .body(body.into())
        .map_err(|e| AppError::Anyhow(e.into()))
}

// ── Model API path handlers (source-aware) ──

pub async fn hf_model_api_suffix(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Path((org, repo, suffix)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_api_path_inner(
        state,
        "hf",
        org,
        repo,
        suffix,
        uri.query().map(|s| s.to_string()),
    )
    .await
}

async fn ms_model_api_suffix(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Path((org, repo, suffix)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    model_api_path_inner(
        state,
        "ms",
        org,
        repo,
        suffix,
        uri.query().map(|s| s.to_string()),
    )
    .await
}

async fn model_api_path_inner(
    state: AppState,
    source: &str,
    org: String,
    repo: String,
    suffix: String,
    query: Option<String>,
) -> Result<Response, AppError> {
    let (endpoint, _client, _head) = hub_config(&state, source);
    let repo_id = format!("{}/{}", org, repo);
    let api_prefix = if source == "ms" {
        "api/v1/models"
    } else {
        "api/models"
    };
    let mut url = format!("{}/{}/{}/{}", endpoint, api_prefix, repo_id, suffix);
    if let Some(query) = query {
        url.push('?');
        url.push_str(&query);
    }

    proxy_json(&state, source, &url).await
}

// ── File proxy handler (reused by HF and MS) ──

pub async fn hf_file_proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    let (endpoint, _, _) = hub_config(&state, "hf");
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    let url = format!("{}/{}/resolve/{}/{}", endpoint, repo_id, revision, path);
    let user_agent = forwarded_user_agent(&headers);
    file_proxy_inner(
        state, "hf", url, cache_name, method, headers, path, user_agent, false,
    )
    .await
}

pub async fn ms_file_proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    let (endpoint, _, _) = hub_config(&state, "ms");
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    let url = format!("{}/{}/resolve/{}/{}", endpoint, repo_id, revision, path);
    let user_agent = forwarded_user_agent(&headers);
    file_proxy_inner(
        state, "ms", url, cache_name, method, headers, path, user_agent, false,
    )
    .await
}

pub async fn ms_repo_file_proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, AppError> {
    let revision = params
        .get("Revision")
        .cloned()
        .unwrap_or_else(|| "master".to_string());
    let file_path = params.get("FilePath").cloned().unwrap_or_default();
    let (endpoint, _, _) = hub_config(&state, "ms");
    let cache_name = format!("{}/{}/{}", org, repo, file_path);
    let url = format!(
        "{}/api/v1/models/{}/{}/repo?Revision={}&FilePath={}",
        endpoint, org, repo, revision, file_path
    );

    let user_agent = forwarded_user_agent(&headers);
    file_proxy_inner(
        state, "ms", url, cache_name, method, headers, file_path, user_agent, true,
    )
    .await
}

fn etag_matches_any(cached_etag: &str, if_none_match: &str) -> bool {
    let cached_stripped = cached_etag.trim_start_matches("W/").trim_matches('"');
    if_none_match
        .split(',')
        .map(|s| s.trim().trim_start_matches("W/").trim_matches('"'))
        .filter(|s| !s.is_empty())
        .any(|e| e == cached_stripped)
}

fn build_304_response(file: &crate::metadata::File) -> Result<Response, AppError> {
    let mut resp = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header("Content-Length", file.total_size)
        .header("Accept-Ranges", "bytes");
    if let Some(ref etag) = file.etag {
        resp = resp.header("ETag", etag);
    }
    if let Some(ref ct) = file.content_type {
        resp = resp.header("Content-Type", ct);
    }
    resp.body(axum::body::Body::empty())
        .map_err(|e| AppError::Anyhow(e.into()))
}

#[allow(clippy::too_many_arguments)]
async fn file_proxy_inner(
    state: AppState,
    source: &str,
    url: String,
    cache_name: String,
    method: Method,
    headers: HeaderMap,
    path: String,
    user_agent: Option<String>,
    first_hop_get: bool,
) -> Result<Response, AppError> {
    let (_endpoint, http_client, head_client) = hub_config(&state, source);
    let repo_id = {
        let parts: Vec<&str> = cache_name.splitn(3, '/').collect();
        if parts.len() >= 2 {
            format!("{}/{}", parts[0], parts[1])
        } else {
            String::new()
        }
    };
    let range = parse_range(&headers);
    let if_none_match = headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    if method == Method::HEAD {
        let mut proceed_to_upstream = true;
        {
            let service = state.service.lock().await;
            if let Ok(Some(file)) = service.info(&cache_name, source).await {
                if file.x_repo_commit.is_some() {
                    let should_serve_cached = if let Some(ref cached_etag) = file.etag {
                        let cached_etag = cached_etag.clone();
                        drop(service);
                        match state
                            .service
                            .lock()
                            .await
                            .validate_file_etag(
                                &url, &cache_name, &repo_id, source,
                                user_agent.as_deref(), &cached_etag,
                            )
                            .await
                        {
                            Ok(true) => true,
                            Ok(false) => false,
                            Err(e) => {
                                tracing::warn!("HEAD etag validation failed ({}), serving degraded: {}", e, cache_name);
                                true
                            }
                        }
                    } else {
                        true
                    };

                    if should_serve_cached {
                        let service = state.service.lock().await;
                        if let Ok(Some(file)) = service.info(&cache_name, source).await {
                            tracing::debug!("HEAD cache hit (metadata): {}", cache_name);
                            return build_head_response(&file, &path);
                        }
                    } else {
                        let service = state.service.lock().await;
                        let _ = service.metadata.delete_file(&cache_name, source);
                        // Fall through to upstream HEAD fetch
                    }
                    proceed_to_upstream = false;
                } else {
                    tracing::debug!("HEAD cache hit but missing x_repo_commit, refreshing from upstream: {}", cache_name);
                    proceed_to_upstream = true;
                }
            } else {
                proceed_to_upstream = true;
            }
        } // service lock dropped

        if proceed_to_upstream {
            tracing::info!("HEAD proxy to upstream: {}", url);
        let mut req = if first_hop_get {
            head_client.get(&url)
        } else {
            head_client.head(&url)
        };
        if let Some(ref ua) = user_agent {
            req = req.header("User-Agent", ua);
        }
        let resp = req.send().await.map_err(|e| AppError::Anyhow(e.into()))?;
        let status = resp.status();
        let first_headers = resp.headers();

        tracing::info!("HEAD upstream response: status={}", status);

        let x_repo_commit = first_headers
            .get("x-repo-commit")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let xl_size: Option<i64> = first_headers
            .get("x-linked-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let x_linked_etag = first_headers
            .get("x-linked-etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let (total_size, etag, content_type) = if status.is_redirection() {
            let location = first_headers
                .get("location")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let location = resolve_redirect(&url, location);
            tracing::info!("HEAD following redirect: {}", location);
            let mut req2 = http_client.head(location);
            if let Some(ref ua) = user_agent {
                req2 = req2.header("User-Agent", ua);
            }
            match req2.send().await {
                Ok(resp2) => {
                    let h = resp2.headers();
                    let cl: u64 = h
                        .get("content-length")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    let et = h
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    let ct = h
                        .get("content-type")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    (cl, et, ct)
                }
                Err(e) => {
                    tracing::warn!("HEAD redirect failed: {}", e);
                    (0u64, None, None)
                }
            }
        } else {
            let cl: u64 = first_headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let et = first_headers
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let ct = first_headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            (cl, et, ct)
        };

        let size = if total_size > 0 {
            total_size
        } else {
            xl_size.unwrap_or(0) as u64
        };

        if size > 0 {
            let service = state.service.lock().await;
            let _ = service.ensure_file_headers(
                &cache_name,
                &repo_id,
                source,
                size,
                etag.as_deref(),
                x_repo_commit.as_deref(),
                xl_size,
                x_linked_etag.as_deref(),
                content_type.as_deref(),
            );
            tracing::info!("cached HEAD metadata for {} ({} bytes)", cache_name, size);
        }

        let mut builder = Response::builder().status(StatusCode::OK);
        if let Some(ref ct) = content_type {
            builder = builder.header("Content-Type", ct.as_str());
        }
        if size > 0 {
            builder = builder.header("Content-Length", size);
        }
        builder = builder.header("Accept-Ranges", "bytes");
        if let Some(ref et) = etag {
            builder = builder.header("ETag", et.as_str());
        }
        if let Some(ref commit) = x_repo_commit {
            builder = builder.header("X-Repo-Commit", commit.as_str());
        }
        if let Some(sz) = xl_size {
            builder = builder.header("X-Linked-Size", sz);
        }
        if let Some(ref le) = x_linked_etag {
            builder = builder.header("X-Linked-ETag", le.as_str());
        }
        tracing::info!("HEAD returning 200 (size={})", size);
        return builder
            .body(axum::body::Body::empty())
            .map_err(|e| AppError::Anyhow(e.into()));
        } // end if proceed_to_upstream
    }

    // GET
    let get_start = std::time::Instant::now();

    // Check cache and extract cached etag for validation
    let cached_etag = {
        let service = state.service.lock().await;
        if service
            .is_file_complete(&cache_name, source)
            .await
            .unwrap_or(false)
        {
            if let Ok(Some(file)) = service.info(&cache_name, source).await {
                let stale = range.map(|r| r.0).unwrap_or(0) >= file.total_size as u64;
                if !stale {
                    file.etag.clone()
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    };

    // Validate etag with upstream if we have a cached etag
    if let Some(ref etag) = cached_etag {
        let result = {
            let service = state.service.lock().await;
            service
                .validate_file_etag(
                    &url,
                    &cache_name,
                    &repo_id,
                    source,
                    user_agent.as_deref(),
                    etag,
                )
                .await
        };

        match result {
            Ok(true) => {
                tracing::debug!("GET etag validated: {}", cache_name);
            }
            Ok(false) => {
                tracing::info!("GET etag changed, invalidating: {}", cache_name);
                let service = state.service.lock().await;
                let _ = service.metadata.delete_file(&cache_name, source);
                drop(service);
                // Fall through to stream_from_upstream
            }
            Err(e) => {
                tracing::warn!(
                    "GET etag validation failed ({}), serving degraded: {}",
                    e,
                    cache_name
                );
            }
        }
    }

    // If-None-Match: return 304 if client's etag matches our validated cache
    if let (Some(ref inm), Some(ref etag)) = (&if_none_match, &cached_etag) {
        if etag_matches_any(etag, inm) {
            let service = state.service.lock().await;
            if let Ok(Some(file)) = service.info(&cache_name, source).await {
                tracing::debug!("If-None-Match hit, returning 304: {}", cache_name);
                return build_304_response(&file);
            }
        }
    }

    // Serve from cache if still complete
    {
        let service = state.service.lock().await;
        if service
            .is_file_complete(&cache_name, source)
            .await
            .unwrap_or(false)
        {
            if let Ok(Some(file)) = service.info(&cache_name, source).await {
                let stale = range.map(|r| r.0).unwrap_or(0) >= file.total_size as u64;
                if !stale {
                    tracing::debug!("GET cache hit (streaming): {}", cache_name);
                    let (file, content_length, stream) = service
                        .stream_cached_file(
                            &cache_name,
                            source,
                            range.map(|r| r.0),
                            range.and_then(|r| r.1),
                        )
                        .await?;
                    tracing::info!(
                        "{}: cache hit, stream ready in {}ms",
                        cache_name,
                        get_start.elapsed().as_millis()
                    );
                    return build_stream_response(file, content_length, stream, &path, range);
                }
                tracing::debug!(
                    "GET cache stale (range beyond cached size), refreshing from upstream: {}",
                    cache_name
                );
            }
        }
    }

    tracing::info!("cache miss, streaming via upstream: {}", cache_name);
    let service = state.service.lock().await;
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
    drop(service);

    tracing::info!(
        "{}: cache miss session ready, {}GB, stream in {}ms",
        cache_name,
        file.total_size as f64 / 1_073_741_824.0,
        get_start.elapsed().as_millis()
    );

    build_stream_response(file, content_length, stream, &path, range)
}

fn build_head_response(file: &crate::metadata::File, path: &str) -> Result<Response, AppError> {
    let filename = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(path);
    let disposition = format!(
        "inline; filename*=UTF-8''{}; filename=\"{}\"",
        filename, filename
    );

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header("Content-Disposition", &disposition)
        .header("Content-Length", file.total_size)
        .header("Accept-Ranges", "bytes");

    if let Some(ref ct) = file.content_type {
        resp = resp.header("Content-Type", ct);
    }

    if let Some(ref etag) = file.etag {
        resp = resp.header("ETag", etag);
    }
    if let Some(ref commit) = file.x_repo_commit {
        resp = resp.header("X-Repo-Commit", commit);
    }
    if let Some(size) = file.x_linked_size {
        resp = resp.header("X-Linked-Size", size);
    }
    if let Some(ref linked_etag) = file.x_linked_etag {
        resp = resp.header("X-Linked-ETag", linked_etag);
    }

    resp.body(axum::body::Body::empty())
        .map_err(|e| AppError::Anyhow(e.into()))
}

pub fn resolve_redirect(base_url: &str, location: &str) -> String {
    if location.is_empty() {
        return base_url.to_string();
    }
    if location.contains("://") {
        return location.to_string();
    }
    if location.starts_with('/') {
        if let Some(pos) = base_url.find("://") {
            let scheme_end = base_url[pos + 3..].find('/').map(|p| pos + 3 + p);
            if let Some(host_end) = scheme_end {
                return format!("{}{}", &base_url[..host_end], location);
            }
            return format!("{}{}", base_url, location);
        }
        return format!("{}{}", base_url, location);
    }
    let base_dir = match base_url.rfind('/') {
        Some(pos) if pos > base_url.find("://").map(|p| p + 3).unwrap_or(0) => &base_url[..pos],
        _ => base_url,
    };
    format!("{}/{}", base_dir, location)
}

fn parse_range(headers: &HeaderMap) -> Option<(u64, Option<u64>)> {
    let range = headers.get("range")?.to_str().ok()?;
    let range = range.strip_prefix("bytes=")?;
    let (start, end) = range.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: Option<u64> = if end.is_empty() {
        None
    } else {
        Some(end.parse().ok()?)
    };
    if let Some(end_val) = end {
        if start > end_val {
            return None;
        }
    }
    Some((start, end))
}

fn forwarded_user_agent(headers: &HeaderMap) -> Option<String> {
    headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn build_stream_response(
    file: crate::metadata::File,
    content_length: u64,
    stream: crate::service::ByteStream,
    path: &str,
    range: Option<(u64, Option<u64>)>,
) -> Result<Response, AppError> {
    let filename = std::path::Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(path);
    let disposition = format!(
        "inline; filename*=UTF-8''{}; filename=\"{}\"",
        filename, filename
    );

    let body = axum::body::Body::from_stream(stream);

    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let mut resp = Response::builder()
        .status(status)
        .header("Content-Disposition", &disposition)
        .header("Content-Length", content_length)
        .header("Accept-Ranges", "bytes");

    if let Some((start, end)) = range {
        let end_str = end
            .map(|e| e.to_string())
            .unwrap_or_else(|| "*".to_string());
        resp = resp.header(
            "Content-Range",
            format!("bytes {}-{}/{}", start, end_str, file.total_size as u64),
        );
    }

    if let Some(ref ct) = file.content_type {
        resp = resp.header("Content-Type", ct);
    }
    if let Some(ref etag) = file.etag {
        resp = resp.header("ETag", etag);
    }
    if let Some(ref commit) = file.x_repo_commit {
        resp = resp.header("X-Repo-Commit", commit);
    }
    if let Some(size) = file.x_linked_size {
        resp = resp.header("X-Linked-Size", size);
    }
    if let Some(ref linked_etag) = file.x_linked_etag {
        resp = resp.header("X-Linked-ETag", linked_etag);
    }

    resp.body(body).map_err(|e| AppError::Anyhow(e.into()))
}

async fn stats(State(state): State<AppState>) -> Result<Json<crate::metadata::Stats>, AppError> {
    let service = state.service.lock().await;
    let stats = service.stats().await.map_err(AppError::Anyhow)?;
    Ok(Json(stats))
}

async fn agent_harnesses(State(state): State<AppState>) -> Result<Response, AppError> {
    let url = format!("{}/api/agent-harnesses", state.config.huggingface.endpoint);
    proxy_json(&state, "hf", &url).await
}

#[derive(serde::Deserialize)]
struct SourceQuery {
    source: Option<String>,
}

#[derive(serde::Deserialize)]
struct FileQuery {
    repo: String,
    file: String,
    source: Option<String>,
}

fn require_admin(headers: &HeaderMap, state: &AppState) -> Result<(), AppError> {
    let value = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(AppError::Unauthorized)?;
    let expected = format!("Bearer {}", state.admin_token.as_str());
    if value == expected {
        Ok(())
    } else {
        Err(AppError::Unauthorized)
    }
}

fn aggregate_files(files: &[crate::metadata::File]) -> Vec<FileListItem> {
    let mut grouped: BTreeMap<(String, String), Vec<crate::metadata::File>> = BTreeMap::new();
    for file in files {
        grouped
            .entry((file.repo.clone(), file.name.clone()))
            .or_default()
            .push(file.clone());
    }

    grouped
        .into_iter()
        .map(|((repo, file), entries)| {
            let first = &entries[0];
            FileListItem {
                repo,
                file,
                sources: entries.iter().map(|f| f.source.clone()).collect(),
                size: first.total_size,
                content_type: first.content_type.clone(),
                last_accessed: entries
                    .iter()
                    .map(|f| f.last_accessed.clone())
                    .max()
                    .unwrap_or_default(),
            }
        })
        .collect()
}

async fn control_service_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ServiceStatusResponse>, AppError> {
    require_admin(&headers, &state)?;
    Ok(Json(ServiceStatusResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        status: "ok".to_string(),
        endpoint: format!(
            "http://{}:{}",
            state.config.server.host, state.config.server.port
        ),
        cache: CacheInfo {
            db_path: state.config.database.path.display().to_string(),
            root: state.config.storage.local_root.display().to_string(),
            max_size: state.config.storage.max_size,
        },
        sources: SourcesInfo {
            hf: SourceInfo {
                enabled: true,
                endpoint: state.config.huggingface.endpoint.clone(),
            },
            ms: SourceInfo {
                enabled: true,
                endpoint: state.config.modelscope.endpoint.clone(),
            },
        },
        auth: AuthInfo {
            admin_token_configured: !state.admin_token.is_empty(),
            admin_token_file: state.config.admin.token_file.display().to_string(),
        },
    }))
}

async fn control_service_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ServiceStatsResponse>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    let stats = service.stats().await.map_err(AppError::Anyhow)?;
    Ok(Json(ServiceStatsResponse {
        repos: stats.repo_count,
        files: stats.file_count,
        logical_bytes: stats.original_bytes,
        stored_bytes: stats.stored_bytes,
        saved_bytes: stats.bytes_saved,
        saved_percent: stats.saved_percent,
        fetched_bytes: stats.fetched_bytes,
        served_bytes: stats.served_bytes,
    }))
}

async fn control_service_gc(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<GcRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    if req.dry_run {
        let preview = service.gc_dry_run().await.map_err(AppError::Anyhow)?;
        Ok(Json(
            serde_json::to_value(GcPreviewResponse {
                candidate_chunks: preview.candidate_chunks,
                candidate_bytes: preview.candidate_bytes,
            })
            .map_err(|e| AppError::Anyhow(e.into()))?,
        ))
    } else {
        let result = service
            .gc_execute(req.batch_size.unwrap_or(100))
            .await
            .map_err(AppError::Anyhow)?;
        Ok(Json(
            serde_json::to_value(GcResultResponse {
                deleted_chunks: result.deleted_chunks,
                reclaimed_bytes: result.reclaimed_bytes,
                skipped_chunks: result.skipped_chunks,
            })
            .map_err(|e| AppError::Anyhow(e.into()))?,
        ))
    }
}

async fn control_repos_list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SourceQuery>,
) -> Result<Json<RepoListResponse>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    let files = service.list().await.map_err(AppError::Anyhow)?;
    let mut grouped: BTreeMap<String, Vec<crate::metadata::File>> = BTreeMap::new();
    for file in files {
        if query.source.as_deref().is_some()
            && query.source.as_deref() != Some(file.source.as_str())
        {
            continue;
        }
        grouped.entry(file.repo.clone()).or_default().push(file);
    }
    let items: Vec<RepoListItem> = grouped
        .into_iter()
        .map(|(repo, entries)| RepoListItem {
            repo,
            sources: entries
                .iter()
                .map(|f| f.source.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect(),
            files: entries.len(),
            logical_bytes: entries.iter().map(|f| f.total_size).sum(),
            last_accessed: entries
                .iter()
                .map(|f| f.last_accessed.clone())
                .max()
                .unwrap_or_default(),
        })
        .collect();
    Ok(Json(RepoListResponse {
        total: items.len(),
        items,
    }))
}

async fn control_repo_show(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(repo): Path<String>,
    Query(query): Query<SourceQuery>,
) -> Result<Json<RepoShowResponse>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    let files = service.list().await.map_err(AppError::Anyhow)?;
    let matched: Vec<_> = files
        .into_iter()
        .filter(|f| f.repo == repo)
        .filter(|f| {
            query.source.as_deref().is_none() || query.source.as_deref() == Some(f.source.as_str())
        })
        .collect();
    if matched.is_empty() {
        return Err(AppError::NotFound(format!("repo not found: {}", repo)));
    }
    let items = aggregate_files(&matched);
    let sources = matched
        .iter()
        .map(|f| f.source.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(Json(RepoShowResponse {
        repo,
        sources,
        files: items.len(),
        logical_bytes: matched.iter().map(|f| f.total_size).sum(),
        last_accessed: matched
            .iter()
            .map(|f| f.last_accessed.clone())
            .max()
            .unwrap_or_default(),
        items,
    }))
}

async fn control_repo_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(repo): Path<String>,
    Query(query): Query<SourceQuery>,
) -> Result<Json<DeleteResponse>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    let result = service
        .delete_repo_all_sources(&repo, query.source.as_deref())
        .await
        .map_err(AppError::Anyhow)?;
    Ok(Json(DeleteResponse {
        deleted: result.deleted_files > 0,
        deleted_files: result.deleted_files,
        sources: result.sources,
    }))
}

async fn control_files_list(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SourceQuery>,
) -> Result<Json<FileListResponse>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    let files = service.list().await.map_err(AppError::Anyhow)?;
    let filtered: Vec<_> = files
        .into_iter()
        .filter(|f| {
            query.source.as_deref().is_none() || query.source.as_deref() == Some(f.source.as_str())
        })
        .collect();
    let items = aggregate_files(&filtered);
    Ok(Json(FileListResponse {
        total: items.len(),
        items,
    }))
}

async fn control_file_show(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FileQuery>,
) -> Result<Json<FileShowResponse>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    let files = service.list().await.map_err(AppError::Anyhow)?;
    let matched: Vec<_> = files
        .into_iter()
        .filter(|f| f.repo == query.repo && f.name == query.file)
        .filter(|f| {
            query.source.as_deref().is_none() || query.source.as_deref() == Some(f.source.as_str())
        })
        .collect();
    if matched.is_empty() {
        return Err(AppError::NotFound(format!(
            "file not found: {}/{}",
            query.repo, query.file
        )));
    }
    let first = &matched[0];
    let sources = matched.iter().map(|f| f.source.clone()).collect();
    Ok(Json(FileShowResponse {
        repo: query.repo,
        file: query.file,
        sources,
        size: first.total_size,
        content_type: first.content_type.clone(),
        last_accessed: first.last_accessed.clone(),
    }))
}

async fn control_file_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FileQuery>,
) -> Result<Json<DeleteResponse>, AppError> {
    require_admin(&headers, &state)?;
    let service = state.service.lock().await;
    let result = service
        .delete_file_all_sources(&query.repo, &query.file, query.source.as_deref())
        .await
        .map_err(AppError::Anyhow)?;
    Ok(Json(DeleteResponse {
        deleted: result.deleted_files > 0,
        deleted_files: result.deleted_files,
        sources: result.sources,
    }))
}

async fn proxy_json(state: &AppState, source: &str, url: &str) -> Result<Response, AppError> {
    let (_, client, _) = hub_config(state, source);
    let mut req = client.get(url);
    let token = match source {
        "ms" => &state.config.modelscope.token,
        _ => &state.config.huggingface.token,
    };
    if let Some(ref token) = token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    let resp = req.send().await.map_err(|e| AppError::Anyhow(e.into()))?;
    let status = resp.status();
    let body = resp.text().await.map_err(|e| AppError::Anyhow(e.into()))?;
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(body.into())
        .map_err(|e| AppError::Anyhow(e.into()))
}

pub enum AppError {
    Anyhow(anyhow::Error),
    Unauthorized,
    NotFound(String),
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::Anyhow(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Anyhow(e) => {
                tracing::error!("{}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e))
            }
            AppError::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
            AppError::NotFound(message) => (StatusCode::NOT_FOUND, message.clone()),
        };
        let body = Json(ErrorResponse { error: message });
        (status, body).into_response()
    }
}
