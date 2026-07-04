use crate::config::Config;
use std::time::Duration;

pub fn build_client(config: &Config) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.huggingface.connect_timeout_secs))
        .timeout(Duration::from_secs(config.huggingface.timeout_secs));

    if let Some(ref proxy_url) = config.huggingface.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)?;
        builder = builder.proxy(proxy);
    }

    Ok(builder.build()?)
}

pub fn build_head_client(config: &Config) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(config.huggingface.connect_timeout_secs))
        .timeout(Duration::from_secs(config.huggingface.timeout_secs));

    if let Some(ref proxy_url) = config.huggingface.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)?;
        builder = builder.proxy(proxy);
    }

    Ok(builder.build()?)
}

pub fn build_stream_client(config: &Config) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.huggingface.connect_timeout_secs));

    if let Some(ref proxy_url) = config.huggingface.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)?;
        builder = builder.proxy(proxy);
    }

    Ok(builder.build()?)
}
