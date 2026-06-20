use crate::config::Config;
use crate::hf;
use crate::service::CacheService;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn run(config: Config, service: CacheService) -> anyhow::Result<()> {
    let http_client = hf::build_client(&config)?;
    let app_state = AppState {
        service: Arc::new(Mutex::new(service)),
        config: Arc::new(config),
        http_client: Arc::new(http_client),
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
        .with_state(app_state);

    tracing::info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Clone)]
struct AppState {
    service: Arc<Mutex<CacheService>>,
    config: Arc<Config>,
    http_client: Arc<reqwest::Client>,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
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
    proxy_json(&state, &url).await
}

async fn file_resolve(
    State(state): State<AppState>,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    serve_file(&state, &repo_id, &cache_name, &revision, &path).await
}

async fn resolve_cache(
    State(state): State<AppState>,
    Path((_repo_type, org, repo, revision, path)): Path<(String, String, String, String, String)>,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    serve_file(&state, &repo_id, &cache_name, &revision, &path).await
}

async fn serve_file(
    state: &AppState,
    repo_id: &str,
    cache_name: &str,
    revision: &str,
    path: &str,
) -> Result<Response, AppError> {
    {
        let service = state.service.lock().await;
        if let Ok(Some(file)) = service.info(cache_name).await {
            let data = service.download(cache_name).await?;
            return build_file_response(data, &file, path);
        }
    }

    let url = format!(
        "{}/{}/resolve/{}/{}",
        state.config.huggingface.endpoint, repo_id, revision, path
    );

    let service = state.service.lock().await;
    service
        .download_from_url(&url, cache_name, repo_id, 8)
        .await?;
    let file = service.info(cache_name).await?.unwrap();
    let data = service.download(cache_name).await?;
    drop(service);

    build_file_response(data, &file, path)
}

fn build_file_response(
    data: Vec<u8>,
    file: &crate::metadata::File,
    path: &str,
) -> Result<Response, AppError> {
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
        .header("content-type", "application/octet-stream")
        .header("content-disposition", &disposition)
        .header("content-length", data.len())
        .header("accept-ranges", "bytes");

    if let Some(ref etag) = file.etag {
        resp = resp.header("etag", etag);
    }
    if let Some(ref commit) = file.x_repo_commit {
        resp = resp.header("x-repo-commit", commit);
    }
    if let Some(size) = file.x_linked_size {
        resp = resp.header("x-linked-size", size);
    }
    if let Some(ref linked_etag) = file.x_linked_etag {
        resp = resp.header("x-linked-etag", linked_etag);
    }

    resp.body(data.into())
        .map_err(|e| AppError::Anyhow(e.into()))
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
        .header("content-type", "application/json")
        .body(body.into())
        .map_err(|e| AppError::Anyhow(e.into()))
}

enum AppError {
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
