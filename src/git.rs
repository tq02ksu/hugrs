use axum::{
    extract::{OriginalUri, Path, Request, State},
    http::{HeaderMap, Method},
    response::Response,
};
use serde_json::Value;

use crate::server::{hub_config, AppState};

pub fn rewrite_lfs_urls(
    body: &str,
    proxy_base: &str,
    upstream_endpoint: &str,
) -> anyhow::Result<String> {
    let mut json: Value = serde_json::from_str(body)?;

    if let Some(objects) = json["objects"].as_array_mut() {
        for obj in objects {
            if let Some(actions) = obj["actions"].as_object_mut() {
                if let Some(download) = actions.get_mut("download") {
                    if let Some(href) = download["href"].as_str() {
                        let rewritten = rewrite_href(href, proxy_base, upstream_endpoint);
                        download["href"] = Value::String(rewritten);
                    }
                }
            }
        }
    }

    Ok(serde_json::to_string(&json)?)
}

fn rewrite_href(href: &str, proxy_base: &str, upstream_endpoint: &str) -> String {
    let endpoints = [
        upstream_endpoint.to_string(),
        "https://huggingface.co".to_string(),
        "https://hf-mirror.com".to_string(),
        "https://cdn-lfs.huggingface.co".to_string(),
        "https://cdn-lfs-us-1.huggingface.co".to_string(),
        "https://lfs.huggingface.co".to_string(),
        "https://www.modelscope.cn".to_string(),
    ];

    for ep in &endpoints {
        if let Some(rest) = href.strip_prefix(ep) {
            return format!("{}{}", proxy_base, rest);
        }
    }

    href.to_string()
}

pub async fn git_info_refs(
    State(state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Response, crate::server::AppError> {
    let source = git_source_from_path(uri.path());
    let (endpoint, client, _) = hub_config(&state, source);
    let query = uri.query().unwrap_or("");
    let upstream_url = format!("{}/{}/{}.git/info/refs?{}", endpoint, owner, repo, query);
    git_proxy_pass(client, &upstream_url, Method::GET, headers, None).await
}

pub async fn git_upload_pack(
    State(state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    request: Request,
) -> Result<Response, crate::server::AppError> {
    let source = git_source_from_path(uri.path());
    let (_, client, _) = hub_config(&state, source);
    let endpoint = match source {
        "ms" => &state.config.modelscope.endpoint,
        _ => &state.config.huggingface.endpoint,
    };
    let upstream_url = format!("{}/{}/{}.git/git-upload-pack", endpoint, owner, repo);

    let body = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))?;

    git_proxy_pass(client, &upstream_url, Method::POST, headers, Some(body.to_vec())).await
}

pub async fn lfs_batch(
    State(state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    request: Request,
) -> Result<Response, crate::server::AppError> {
    let source = git_source_from_path(uri.path());
    let (endpoint, client, _) = hub_config(&state, source);
    let upstream_url = format!(
        "{}/{}/{}.git/info/lfs/objects/batch",
        endpoint, owner, repo
    );

    let body = axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024)
        .await
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))?;

    let mut req = client.post(&upstream_url);
    req = req.header("Content-Type", "application/vnd.git-lfs+json");
    req = req.header("Accept", "application/vnd.git-lfs+json");

    let token = match source {
        "ms" => &state.config.modelscope.token,
        _ => &state.config.huggingface.token,
    };
    if let Some(ref token) = token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    if let Some(ua) = headers.get("user-agent").and_then(|v| v.to_str().ok()) {
        req = req.header("User-Agent", ua);
    }

    let resp = req.body(body).send().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("LFS upstream error: {}", e))
    })?;

    let status = resp.status();
    let resp_text = resp.text().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("LFS read error: {}", e))
    })?;

    let scheme = "http";
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1:3000");
    let proxy_base = format!("{}://{}", scheme, host);

    let rewritten = rewrite_lfs_urls(&resp_text, &proxy_base, endpoint)
        .map_err(crate::server::AppError::from)?;

    axum::response::Response::builder()
        .status(status)
        .header("Content-Type", "application/vnd.git-lfs+json")
        .body(axum::body::Body::from(rewritten))
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))
}

async fn git_proxy_pass(
    client: &reqwest::Client,
    url: &str,
    method: Method,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
) -> Result<Response, crate::server::AppError> {
    let mut req = client.request(method.clone(), url);

    for (key, value) in headers.iter() {
        let key_str = key.as_str().to_lowercase();
        if key_str != "host" && key_str != "content-length" && key_str != "transfer-encoding" {
            req = req.header(key, value);
        }
    }

    if let Some(ref b) = body {
        req = req.body(b.clone());
    }

    let resp = req.send().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("git upstream error: {}", e))
    })?;

    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let resp_body = resp.bytes().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("git read error: {}", e))
    })?;

    let mut builder = axum::response::Response::builder().status(status);
    for (key, value) in resp_headers.iter() {
        let key_str = key.as_str().to_lowercase();
        if key_str != "transfer-encoding" && key_str != "content-length" {
            builder = builder.header(key, value);
        }
    }

    builder
        .body(axum::body::Body::from(resp_body))
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))
}

fn git_source_from_path(path: &str) -> &str {
    if path.starts_with("/ms/") {
        "ms"
    } else {
        "hf"
    }
}
