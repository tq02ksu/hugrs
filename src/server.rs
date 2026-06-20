use crate::config::Config;
use crate::service::CacheService;
use axum::{
    extract::{Multipart, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn run(config: Config, service: CacheService) -> anyhow::Result<()> {
    let app_state = AppState {
        service: Arc::new(Mutex::new(service)),
        config: Arc::new(config),
    };

    let addr = format!(
        "{}:{}",
        app_state.config.server.host, app_state.config.server.port
    );

    let app = Router::new()
        .route("/files", post(upload_file))
        .route("/files/{name}", get(download_file).delete(delete_file))
        .route("/files/{name}/info", get(file_info))
        .route("/files/pull", post(pull_model))
        .route("/stats", get(stats))
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
}

#[derive(Deserialize)]
struct PullRequest {
    repo: String,
    #[serde(default)]
    file: Option<String>,
}

#[derive(Serialize)]
struct FileInfoResponse {
    name: String,
    repo: String,
    total_size: i64,
    created_at: String,
    last_accessed: String,
    source: String,
}

#[derive(Serialize)]
struct StatsResponse {
    repo_count: i64,
    file_count: i64,
    trunk_count: i64,
    total_size: i64,
    unique_size: i64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

async fn upload_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    while let Some(field) = multipart.next_field().await? {
        let name = field.file_name().unwrap_or("unnamed").to_string();
        let data = field.bytes().await?;
        let service = state.service.lock().await;
        service.upload(&name, "upload", data.to_vec()).await?;
        tracing::info!("HTTP upload: {} ({} bytes)", name, data.len());
    }
    Ok(StatusCode::CREATED)
}

async fn download_file(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, AppError> {
    let service = state.service.lock().await;
    let data = service
        .download(&name)
        .await
        .map_err(|_| AppError::NotFound(format!("file not found: {}", name)))?;
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .body(data.into())
        .map_err(|e| AppError::Anyhow(e.into()))
}

async fn file_info(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<FileInfoResponse>, AppError> {
    let service = state.service.lock().await;
    match service.info(&name).await? {
        Some(f) => Ok(Json(FileInfoResponse {
            name: f.name,
            repo: f.repo,
            total_size: f.total_size,
            created_at: f.created_at,
            last_accessed: f.last_accessed,
            source: f.source,
        })),
        None => Err(AppError::NotFound(format!("file not found: {}", name))),
    }
}

async fn delete_file(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, AppError> {
    let service = state.service.lock().await;
    let deleted = service.delete(&name).await?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AppError::NotFound(format!("file not found: {}", name)))
    }
}

async fn pull_model(
    State(state): State<AppState>,
    Json(req): Json<PullRequest>,
) -> Result<StatusCode, AppError> {
    let service = state.service.lock().await;
    crate::hf::pull_model(&state.config, &service, &req.repo, req.file.as_deref()).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, AppError> {
    let service = state.service.lock().await;
    let s = service.stats().await?;
    Ok(Json(StatsResponse {
        repo_count: s.repo_count,
        file_count: s.file_count,
        trunk_count: s.trunk_count,
        total_size: s.total_size,
        unique_size: s.unique_size,
    }))
}

enum AppError {
    Anyhow(anyhow::Error),
    NotFound(String),
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::Anyhow(e)
    }
}

impl From<axum::extract::multipart::MultipartError> for AppError {
    fn from(e: axum::extract::multipart::MultipartError) -> Self {
        AppError::Anyhow(e.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Anyhow(e) => {
                tracing::error!("Internal error: {}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e))
            }
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
        };
        let body = Json(ErrorResponse { error: message });
        (status, body).into_response()
    }
}
