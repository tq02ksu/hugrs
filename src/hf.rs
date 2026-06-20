use crate::config::Config;
use crate::service::CacheService;
use serde::Deserialize;

#[derive(Deserialize)]
struct HfSibling {
    rfilename: String,
}

#[derive(Deserialize)]
struct HfModelInfo {
    siblings: Vec<HfSibling>,
}

pub async fn pull_model(
    config: &Config,
    service: &CacheService,
    repo_id: &str,
    file_filter: Option<&str>,
) -> anyhow::Result<()> {
    let client = build_client(config)?;
    let api_url = format!("{}/api/models/{}", config.huggingface.endpoint, repo_id);
    let mut headers = reqwest::header::HeaderMap::new();

    if let Some(ref token) = config.huggingface.token {
        headers.insert(
            "Authorization",
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))?,
        );
    }

    let info: HfModelInfo = client
        .get(&api_url)
        .headers(headers.clone())
        .send()
        .await?
        .json()
        .await?;

    for sibling in &info.siblings {
        if let Some(filter) = file_filter {
            if sibling.rfilename != filter {
                continue;
            }
        }

        let download_url = format!(
            "{}/{}/resolve/main/{}",
            config.huggingface.endpoint, repo_id, sibling.rfilename
        );

        tracing::info!("Pulling {} from {}", sibling.rfilename, download_url);

        service
            .download_from_url(&download_url, &sibling.rfilename, repo_id, 8)
            .await?;
    }

    Ok(())
}

pub fn build_client(config: &Config) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();

    if let Some(ref proxy_url) = config.huggingface.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)?;
        builder = builder.proxy(proxy);
    }

    Ok(builder.build()?)
}
