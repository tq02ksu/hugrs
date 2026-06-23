use crate::config::Config;
use crate::hf;
use crate::service::CacheService;
use axum::{
    extract::{Path, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn run(config: Config, service: CacheService) -> anyhow::Result<()> {
    let http_client = hf::build_client(&config)?;
    let head_client = hf::build_head_client(&config)?;
    let app_state = AppState {
        service: Arc::new(Mutex::new(service)),
        config: Arc::new(config),
        http_client: Arc::new(http_client),
        head_client: Arc::new(head_client),
    };

    let addr = format!(
        "{}:{}",
        app_state.config.server.host, app_state.config.server.port
    );

    let app = Router::new()
        .route("/", get(root))
        .route("/api/whoami-v2", get(whoami))
        .route(
            "/api/models/{org}/{repo}/revision/{revision}",
            get(model_info_revision).head(model_info_revision),
        )
        .route(
            "/{org}/{repo}/resolve/{revision}/{*path}",
            get(file_resolve).head(file_resolve),
        )
        .route(
            "/api/resolve-cache/{repo_type}/{org}/{repo}/{revision}/{*path}",
            get(resolve_cache).head(resolve_cache),
        )
        .route("/api/stats", get(stats))
        .route("/api/agent-harnesses", get(agent_harnesses))
        .layer(middleware::from_fn(log_request))
        .with_state(app_state);

    tracing::info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<Mutex<CacheService>>,
    pub config: Arc<Config>,
    pub http_client: Arc<reqwest::Client>,
    pub head_client: Arc<reqwest::Client>,
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

async fn root(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    Ok(Json(serde_json::json!({
        "service": "hugrs",
        "version": env!("CARGO_PKG_VERSION"),
        "hf_endpoint": state.config.huggingface.endpoint,
    })))
}

async fn whoami() -> Result<Json<serde_json::Value>, AppError> {
    Ok(Json(serde_json::json!({
        "name": "mirror",
        "auth": true,
    })))
}

async fn model_info_revision(
    State(state): State<AppState>,
    Path((org, repo, revision)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let url = format!(
        "{}/api/models/{}/revision/{}",
        state.config.huggingface.endpoint, repo_id, revision
    );

    {
        let service = state.service.lock().await;
        if let Ok(Some((status, headers, body))) = service.get_http_cache(&url) {
            tracing::info!("model_info cache hit: {}", url);
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
            let mut builder = Response::builder().status(status);
            for line in headers.lines() {
                if let Some(col) = line.find(':') {
                    let name = line[..col].trim();
                    let value = line[col + 1..].trim();
                    builder = builder.header(name, value);
                }
            }
            return builder
                .body(body.into())
                .map_err(|e| AppError::Anyhow(e.into()));
        }
        drop(service);
    }

    tracing::info!("model_info proxy to: {}", url);
    let mut req = state.http_client.get(&url);
    if let Some(ref token) = state.config.huggingface.token {
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

    let headers_text = upstream_headers
        .iter()
        .map(|(n, v)| format!("{}: {}", n, v))
        .collect::<Vec<_>>()
        .join("\n");

    {
        let service = state.service.lock().await;
        let _ = service.set_http_cache(&url, status.as_u16(), &headers_text, &body);
        drop(service);
    }

    let mut builder = Response::builder().status(status);
    for (name, value) in &upstream_headers {
        builder = builder.header(name, value);
    }
    builder
        .body(body.into())
        .map_err(|e| AppError::Anyhow(e.into()))
}

pub async fn file_resolve(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    let range = parse_range(&headers);
    serve_file(
        &state,
        method,
        &repo_id,
        &cache_name,
        &revision,
        &path,
        range,
    )
    .await
}

async fn resolve_cache(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((_repo_type, org, repo, revision, path)): Path<(String, String, String, String, String)>,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    let range = parse_range(&headers);
    serve_file(
        &state,
        method,
        &repo_id,
        &cache_name,
        &revision,
        &path,
        range,
    )
    .await
}

async fn serve_file(
    state: &AppState,
    method: Method,
    repo_id: &str,
    cache_name: &str,
    revision: &str,
    path: &str,
    range: Option<(u64, Option<u64>)>,
) -> Result<Response, AppError> {
    tracing::debug!("{} {} cache={}", method, cache_name, cache_name);

    let url = format!(
        "{}/{}/resolve/{}/{}",
        state.config.huggingface.endpoint, repo_id, revision, path
    );

    if method == Method::HEAD {
        let service = state.service.lock().await;
        if let Ok(Some(file)) = service.info(cache_name).await {
            if file.x_repo_commit.is_some() {
                tracing::debug!("HEAD cache hit (metadata): {}", cache_name);
                return build_head_response(&file, path);
            }
            tracing::debug!(
                "HEAD cache hit but missing x_repo_commit, refreshing from upstream: {}",
                cache_name
            );
        }
        drop(service);

        tracing::info!("HEAD proxy to upstream: {}", url);
        let resp = state
            .head_client
            .head(&url)
            .send()
            .await
            .map_err(|e| AppError::Anyhow(e.into()))?;
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
            match state.http_client.head(location).send().await {
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
            service.ensure_file_headers(
                cache_name,
                repo_id,
                size,
                etag.as_deref(),
                x_repo_commit.as_deref(),
                xl_size,
                x_linked_etag.as_deref(),
                content_type.as_deref(),
            )?;
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
    }

    {
        let service = state.service.lock().await;
        if service.is_file_complete(cache_name).await.unwrap_or(false) {
            tracing::debug!("GET cache hit (streaming): {}", cache_name);
            let (file, content_length, stream) = service
                .stream_cached_file(cache_name, range.map(|r| r.0), range.and_then(|r| r.1))
                .await?;
            return build_stream_response(file, content_length, stream, path, range);
        }
    }

    tracing::info!("cache miss, streaming from upstream: {}", cache_name);

    let svc = {
        let guard = state.service.lock().await;
        guard.clone()
    };

    let (file, content_length, stream) = svc
        .stream_from_upstream(
            &url,
            cache_name,
            repo_id,
            range.map(|r| r.0),
            range.and_then(|r| r.1),
        )
        .await?;

    build_stream_response(file, content_length, stream, path, range)
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
    // Absolute URL
    if location.contains("://") {
        return location.to_string();
    }
    // Absolute path
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
    // Relative path: resolve against the directory of the base URL
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
    proxy_json(&state, &url).await
}

async fn proxy_json(state: &AppState, url: &str) -> Result<Response, AppError> {
    let mut req = state.http_client.get(url);
    if let Some(ref token) = state.config.huggingface.token {
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
        };
        let body = Json(ErrorResponse { error: message });
        (status, body).into_response()
    }
}
