use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "hugrs")]
#[command(about = "HuggingFace content-addressed caching service")]
pub struct Cli {
    #[arg(short = 'c', long, global = true)]
    pub config: Option<String>,

    #[arg(long, global = true)]
    pub db_path: Option<String>,

    #[arg(long, global = true)]
    pub storage_backend: Option<String>,

    #[arg(long, global = true)]
    pub local_root: Option<String>,

    #[arg(long, global = true)]
    pub s3_bucket: Option<String>,

    #[arg(long, global = true)]
    pub s3_region: Option<String>,

    #[arg(long, global = true)]
    pub s3_prefix: Option<String>,

    #[arg(long, global = true)]
    pub s3_endpoint: Option<String>,

    #[arg(long, global = true)]
    pub server_host: Option<String>,

    #[arg(long, global = true)]
    pub server_port: Option<u16>,

    #[arg(long, global = true)]
    pub hf_endpoint: Option<String>,

    #[arg(long, global = true)]
    pub hf_token: Option<String>,

    #[arg(long, global = true)]
    pub hf_proxy: Option<String>,

    #[arg(long, global = true)]
    pub compression: Option<String>,

    #[arg(long, global = true)]
    pub max_size: Option<u64>,

    #[arg(long, global = true, default_value_t = 0)]
    pub prefetch_depth: usize,

    #[arg(long, global = true)]
    pub enable_sha256_verify: Option<bool>,

    #[arg(long, global = true)]
    pub hf_timeout: Option<u64>,

    #[arg(long, global = true)]
    pub hf_connect_timeout: Option<u64>,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn overrides(&self) -> crate::config::CliOverrides {
        crate::config::CliOverrides {
            db_path: self.db_path.clone(),
            storage_backend: self.storage_backend.clone(),
            local_root: self.local_root.clone(),
            s3_bucket: self.s3_bucket.clone(),
            s3_region: self.s3_region.clone(),
            s3_prefix: self.s3_prefix.clone(),
            s3_endpoint: self.s3_endpoint.clone(),
            server_host: self.server_host.clone(),
            server_port: self.server_port,
            hf_endpoint: self.hf_endpoint.clone(),
            hf_token: self.hf_token.clone(),
            hf_proxy: self.hf_proxy.clone(),
            config_file: self.config.clone(),
            compression: self.compression.clone(),
            max_size: self.max_size,
            prefetch_depth: Some(self.prefetch_depth),
            enable_sha256_verify: self.enable_sha256_verify,
            hf_timeout: self.hf_timeout,
            hf_connect_timeout: self.hf_connect_timeout,
        }
    }
}

#[derive(Subcommand)]
pub enum Command {
    Pull {
        repo: String,

        #[arg(short, long)]
        file: Option<String>,
    },

    List,

    Info {
        name: String,
    },

    Stats,

    Gc,

    Serve,
}
