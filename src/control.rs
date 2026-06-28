use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatusResponse {
    pub version: String,
    pub status: String,
    pub endpoint: String,
    pub cache: CacheInfo,
    pub sources: SourcesInfo,
    pub auth: AuthInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInfo {
    pub db_path: String,
    pub root: String,
    pub max_size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourcesInfo {
    pub hf: SourceInfo,
    pub ms: SourceInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    pub enabled: bool,
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthInfo {
    pub admin_token_configured: bool,
    pub admin_token_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatsResponse {
    pub repos: i64,
    pub files: i64,
    pub logical_bytes: i64,
    pub stored_bytes: i64,
    pub saved_bytes: i64,
    pub saved_percent: f64,
    pub fetched_bytes: u64,
    pub served_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoListResponse {
    pub items: Vec<RepoListItem>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoListItem {
    pub repo: String,
    pub sources: Vec<String>,
    pub files: usize,
    pub logical_bytes: i64,
    pub last_accessed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoShowResponse {
    pub repo: String,
    pub sources: Vec<String>,
    pub files: usize,
    pub logical_bytes: i64,
    pub last_accessed: String,
    pub items: Vec<FileListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileListResponse {
    pub items: Vec<FileListItem>,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileListItem {
    pub repo: String,
    pub file: String,
    pub sources: Vec<String>,
    pub size: i64,
    pub content_type: Option<String>,
    pub last_accessed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileShowResponse {
    pub repo: String,
    pub file: String,
    pub sources: Vec<String>,
    pub size: i64,
    pub content_type: Option<String>,
    pub last_accessed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub deleted: bool,
    pub deleted_files: usize,
    pub sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcRequest {
    pub dry_run: bool,
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcPreviewResponse {
    pub candidate_chunks: usize,
    pub candidate_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcResultResponse {
    pub deleted_chunks: usize,
    pub reclaimed_bytes: u64,
    pub skipped_chunks: usize,
}
