use hugrs::cli::Command;
use hugrs::metadata::MetadataStore;
use hugrs::service::CacheService;
use hugrs::{cli, config, hf, server, storage};

use clap::Parser;
use std::sync::Arc;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hugrs=info".into()),
        )
        .init();

    let cli = cli::Cli::parse();
    let overrides = cli.overrides();
    let config = config::Config::load(overrides)?;

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
        let service = CacheService::new(
            metadata,
            backend,
            config.storage.max_size,
            http_client,
            head_client,
            config.storage.prefetch_depth,
            config.storage.verify_sha256,
        );

        match cli.command {
            Command::Pull { repo, file } => {
                hf::pull_model(&config, &service, &repo, file.as_deref()).await?;
            }

            Command::List => {
                let files = service.list().await?;
                if files.is_empty() {
                    tracing::info!("No cached files");
                } else {
                    for f in &files {
                        println!(
                            "{}  {}  {}  {}  {}  {}",
                            f.name, f.repo, f.total_size, f.source, f.created_at, f.last_accessed
                        );
                    }
                }
            }

            Command::Info { name } => match service.info(&name).await? {
                Some(f) => {
                    println!("Name:          {}", f.name);
                    println!("Repo:          {}", f.repo);
                    println!("Size:          {} bytes", f.total_size);
                    println!("Source:        {}", f.source);
                    println!("Created:       {}", f.created_at);
                    println!("Last accessed: {}", f.last_accessed);
                }
                None => {
                    tracing::info!("File not found: {}", name);
                }
            },

            Command::Stats => {
                let stats = service.stats().await?;
                println!("Repos:       {}", stats.repo_count);
                println!("Files:       {}", stats.file_count);
                println!("Trunks:      {}", stats.trunk_count);
                println!("Total size:  {} bytes", stats.total_size);
                println!("Unique size: {} bytes", stats.unique_size);
                if let Some(limit) = config.storage.max_size {
                    let pct = (stats.total_size as u64 * 100)
                        .checked_div(limit)
                        .unwrap_or(0);
                    println!("Max size:    {} bytes ({}% used)", limit, pct);
                }
            }

            Command::Gc => {
                let count = service.gc().await?;
                tracing::info!("Garbage collected {} trunks", count);
                if let Some(limit) = config.storage.max_size {
                    let stats = service.stats().await?;
                    if stats.total_size as u64 > limit {
                        tracing::info!("Cache still exceeds max size, manual eviction needed");
                    }
                }
            }

            Command::Serve => {
                server::run(config, service).await?;
            }
        }

        anyhow::Ok(())
    })
}
