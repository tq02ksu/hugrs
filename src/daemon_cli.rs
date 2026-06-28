use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "hugrs")]
#[command(about = "Transparent caching proxy for HuggingFace and ModelScope")]
pub struct DaemonCli {
    #[arg(short = 'c', long = "config")]
    pub config_file: Option<String>,

    #[arg(long)]
    pub db_path: Option<String>,

    #[arg(long)]
    pub storage_backend: Option<String>,

    #[arg(long)]
    pub local_root: Option<String>,

    #[arg(long)]
    pub s3_bucket: Option<String>,

    #[arg(long)]
    pub s3_region: Option<String>,

    #[arg(long)]
    pub s3_prefix: Option<String>,

    #[arg(long)]
    pub s3_endpoint: Option<String>,

    #[arg(long)]
    pub server_host: Option<String>,

    #[arg(long)]
    pub server_port: Option<u16>,

    #[arg(long)]
    pub hf_endpoint: Option<String>,

    #[arg(long)]
    pub hf_token: Option<String>,

    #[arg(long)]
    pub hf_proxy: Option<String>,

    #[arg(long)]
    pub hf_timeout: Option<u64>,

    #[arg(long)]
    pub hf_connect_timeout: Option<u64>,

    #[arg(long)]
    pub ms_endpoint: Option<String>,

    #[arg(long)]
    pub ms_token: Option<String>,

    #[arg(long)]
    pub ms_proxy: Option<String>,

    #[arg(long)]
    pub ms_timeout: Option<u64>,

    #[arg(long)]
    pub ms_connect_timeout: Option<u64>,

    #[arg(long)]
    pub admin_token: Option<String>,

    #[arg(long)]
    pub admin_token_file: Option<String>,

    #[arg(long)]
    pub compression: Option<String>,

    #[arg(long)]
    pub max_size: Option<u64>,

    #[arg(long)]
    pub prefetch_depth: Option<usize>,

    #[arg(long)]
    pub prefetch_budget_base: Option<usize>,

    #[arg(long = "enable-sha256-verify")]
    pub enable_sha256_verify: Option<bool>,
}

impl DaemonCli {
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
            hf_timeout: self.hf_timeout,
            hf_connect_timeout: self.hf_connect_timeout,
            ms_endpoint: self.ms_endpoint.clone(),
            ms_token: self.ms_token.clone(),
            ms_proxy: self.ms_proxy.clone(),
            ms_timeout: self.ms_timeout,
            ms_connect_timeout: self.ms_connect_timeout,
            admin_token: self.admin_token.clone(),
            admin_token_file: self.admin_token_file.clone(),
            config_file: self.config_file.clone(),
            compression: self.compression.clone(),
            max_size: self.max_size,
            prefetch_depth: self.prefetch_depth,
            prefetch_budget_base: self.prefetch_budget_base,
            enable_sha256_verify: self.enable_sha256_verify,
        }
    }
}
