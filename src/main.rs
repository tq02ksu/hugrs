use clap::Parser;
use hugrs::config::Config;
use hugrs::daemon_cli::DaemonCli;
use hugrs::metadata::MetadataStore;
use hugrs::service::CacheService;
use hugrs::{hf, server, storage};
use std::sync::Arc;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hugrs=info".into()),
        )
        .init();

    let cli = DaemonCli::parse();
    let config = Config::load(cli.overrides())?;
    let metadata = Arc::new(MetadataStore::new(&config.database.path)?);
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async move {
        let backend: Arc<dyn storage::StorageBackend> = match config.storage.backend.as_str() {
            "s3" => {
                let bucket = config
                    .storage
                    .s3_bucket
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("S3 bucket not configured"))?;
                let region = config
                    .storage
                    .s3_region
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("S3 region not configured"))?;
                Arc::new(
                    storage::s3::S3Backend::new(
                        bucket,
                        region,
                        config.storage.s3_prefix.clone(),
                        config.storage.s3_endpoint.clone(),
                    )
                    .await?,
                )
            }
            _ => Arc::new(storage::local::LocalBackend::new(
                config.storage.local_root.clone(),
                config.storage.compression,
            )),
        };
        let http_client = hf::build_client(&config)?;
        let head_client = hf::build_head_client(&config)?;
        let stream_client = hf::build_stream_client(&config)?;
        let ms_http_client = {
            let mut builder = reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(
                    config.modelscope.connect_timeout_secs,
                ))
                .timeout(std::time::Duration::from_secs(
                    config.modelscope.timeout_secs,
                ));
            if let Some(ref proxy) = config.modelscope.proxy {
                builder = builder.proxy(reqwest::Proxy::all(proxy)?);
            }
            builder.build()?
        };
        let ms_head_client = {
            let mut builder = reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(
                    config.modelscope.connect_timeout_secs,
                ))
                .timeout(std::time::Duration::from_secs(
                    config.modelscope.timeout_secs,
                ))
                .redirect(reqwest::redirect::Policy::none());
            if let Some(ref proxy) = config.modelscope.proxy {
                builder = builder.proxy(reqwest::Proxy::all(proxy)?);
            }
            builder.build()?
        };
        let service = CacheService::new(
            metadata,
            backend,
            config.storage.max_size,
            http_client,
            head_client,
            config.storage.prefetch_depth,
            config.storage.prefetch_budget_base,
            config.storage.verify_sha256,
            stream_client,
        );

        server::run(config, service, ms_http_client, ms_head_client).await?;
        anyhow::Ok(())
    })
}
