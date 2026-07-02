use crate::config::default_admin_token_file_path;
use crate::control::{
    DeleteResponse, FileListResponse, FileShowResponse, GcPreviewResponse, GcRequest,
    GcResultResponse, ReconsileRequest, ReconsileResponse, RepoListResponse,
    RepoShowResponse, ServiceStatsResponse, ServiceStatusResponse,
};
use std::path::PathBuf;

#[derive(Clone)]
pub struct AdminClient {
    base_url: String,
    admin_token: String,
    http: reqwest::Client,
}

impl AdminClient {
    pub fn discover(
        endpoint_override: Option<String>,
        admin_token_override: Option<String>,
    ) -> anyhow::Result<Self> {
        let base_url = endpoint_override
            .or_else(|| std::env::var("HUGRS_CONTROL_ENDPOINT").ok())
            .unwrap_or_else(default_control_endpoint);
        let token = resolve_admin_token(admin_token_override)?;
        Ok(Self {
            base_url,
            admin_token: token,
            http: reqwest::Client::new(),
        })
    }

    pub async fn service_status(&self) -> anyhow::Result<ServiceStatusResponse> {
        self.get("/_hugrs/service").await
    }

    pub async fn service_stats(&self) -> anyhow::Result<ServiceStatsResponse> {
        self.get("/_hugrs/service/stats").await
    }

    pub async fn service_gc_preview(&self) -> anyhow::Result<GcPreviewResponse> {
        self.post(
            "/_hugrs/service/gc",
            &GcRequest {
                dry_run: true,
                batch_size: None,
            },
        )
            .await
    }

    pub async fn service_gc_execute(&self) -> anyhow::Result<GcResultResponse> {
        self.post(
            "/_hugrs/service/gc",
            &GcRequest {
                dry_run: false,
                batch_size: None,
            },
        )
        .await
    }

    pub async fn service_gc_execute_batch(
        &self,
        batch_size: Option<usize>,
    ) -> anyhow::Result<GcResultResponse> {
        self.post(
            "/_hugrs/service/gc",
            &GcRequest {
                dry_run: false,
                batch_size,
            },
        )
        .await
    }

    pub async fn service_reconsile_dry_run(&self) -> anyhow::Result<ReconsileResponse> {
        self.post(
            "/_hugrs/service/reconsile",
            &ReconsileRequest { dry_run: true },
        )
        .await
    }

    pub async fn service_reconsile_apply(&self) -> anyhow::Result<ReconsileResponse> {
        self.post(
            "/_hugrs/service/reconsile",
            &ReconsileRequest { dry_run: false },
        )
        .await
    }

    pub async fn repos_list(&self, source: Option<&str>) -> anyhow::Result<RepoListResponse> {
        let path = with_source("/_hugrs/repos", source);
        self.get(&path).await
    }

    pub async fn repos_show(
        &self,
        repo: &str,
        source: Option<&str>,
    ) -> anyhow::Result<RepoShowResponse> {
        let path = with_source(&format!("/_hugrs/repos/{repo}"), source);
        self.get(&path).await
    }

    pub async fn repos_delete(
        &self,
        repo: &str,
        source: Option<&str>,
    ) -> anyhow::Result<DeleteResponse> {
        let path = with_source(&format!("/_hugrs/repos/{repo}"), source);
        self.delete(&path).await
    }

    pub async fn files_list(&self, source: Option<&str>) -> anyhow::Result<FileListResponse> {
        let path = with_source("/_hugrs/files", source);
        self.get(&path).await
    }

    pub async fn files_show(
        &self,
        repo: &str,
        file: &str,
        source: Option<&str>,
    ) -> anyhow::Result<FileShowResponse> {
        let mut path = format!("/_hugrs/files/show?repo={repo}&file={file}");
        if let Some(source) = source {
            path.push_str("&source=");
            path.push_str(source);
        }
        self.get(&path).await
    }

    pub async fn files_delete(
        &self,
        repo: &str,
        file: &str,
        source: Option<&str>,
    ) -> anyhow::Result<DeleteResponse> {
        let mut path = format!("/_hugrs/files?repo={repo}&file={file}");
        if let Some(source) = source {
            path.push_str("&source=");
            path.push_str(source);
        }
        self.delete(&path).await
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        Ok(self
            .http
            .get(format!("{}{}", self.base_url, path))
            .bearer_auth(&self.admin_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        Ok(self
            .http
            .post(format!("{}{}", self.base_url, path))
            .bearer_auth(&self.admin_token)
            .json(body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn delete<T: serde::de::DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        Ok(self
            .http
            .delete(format!("{}{}", self.base_url, path))
            .bearer_auth(&self.admin_token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }
}

fn resolve_admin_token(admin_token_override: Option<String>) -> anyhow::Result<String> {
    if let Some(token) = admin_token_override {
        return Ok(token);
    }
    if let Ok(token) = std::env::var("HUGRS_ADMIN_TOKEN") {
        return Ok(token);
    }

    let token_file = std::env::var("HUGRS_ADMIN_TOKEN_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_admin_token_file());
    let token = std::fs::read_to_string(&token_file)?;
    Ok(token.trim().to_string())
}

fn default_admin_token_file() -> PathBuf {
    default_admin_token_file_path()
}

fn default_control_endpoint() -> String {
    "http://127.0.0.1:3000".to_string()
}

fn with_source(path: &str, source: Option<&str>) -> String {
    match source {
        Some(source) => format!("{path}?source={source}"),
        None => path.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::AdminClient;
    use crate::config::default_admin_token_file_path;

    #[test]
    fn discover_uses_explicit_endpoint_and_token() {
        let client = match AdminClient::discover(
            Some("http://127.0.0.1:3000".into()),
            Some("test-admin-token".into()),
        ) {
            Ok(client) => client,
            Err(err) => panic!("discover should succeed: {err}"),
        };
        assert_eq!(client.base_url, "http://127.0.0.1:3000");
        assert_eq!(client.admin_token, "test-admin-token");
    }

    #[test]
    fn discover_requires_endpoint_without_override_or_env() {
        std::env::remove_var("HUGRS_CONTROL_ENDPOINT");
        let client = match AdminClient::discover(None, Some("test-admin-token".into())) {
            Ok(client) => client,
            Err(err) => panic!("discover should fall back to default endpoint: {err}"),
        };
        assert_eq!(client.base_url, "http://127.0.0.1:3000");
    }

    #[test]
    fn default_admin_token_path_matches_daemon_default() {
        assert_eq!(
            super::default_admin_token_file(),
            default_admin_token_file_path()
        );
    }
}
