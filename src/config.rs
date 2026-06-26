use crate::storage::Compression;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub database: DatabaseConfig,

    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub huggingface: HfConfig,

    #[serde(default)]
    pub modelscope: MsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_backend")]
    pub backend: String,

    #[serde(default = "default_local_root")]
    pub local_root: PathBuf,

    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_prefix: Option<String>,
    pub s3_endpoint: Option<String>,

    #[serde(default)]
    pub compression: Compression,

    pub max_size: Option<u64>,

    #[serde(default = "default_prefetch_depth")]
    pub prefetch_depth: usize,

    #[serde(default = "default_prefetch_budget_base")]
    pub prefetch_budget_base: usize,

    #[serde(default = "default_verify_sha256")]
    pub verify_sha256: bool,
}

fn default_prefetch_depth() -> usize {
    0 // 0 means auto-detect (num CPUs, max 16)
}

fn default_prefetch_budget_base() -> usize {
    8
}

fn default_verify_sha256() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HfConfig {
    #[serde(default = "default_hf_endpoint")]
    pub endpoint: String,

    pub token: Option<String>,

    pub proxy: Option<String>,

    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MsConfig {
    #[serde(default = "default_ms_endpoint")]
    pub endpoint: String,

    pub token: Option<String>,

    pub proxy: Option<String>,

    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

fn default_backend() -> String {
    "local".into()
}
fn default_cache_base() -> PathBuf {
    let cache = dirs::cache_dir().unwrap_or_else(|| {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".cache")
    });
    if cache.is_relative() {
        std::env::current_dir().unwrap_or_default().join(cache)
    } else {
        cache
    }
}

fn default_local_root() -> PathBuf {
    default_cache_base().join("hugrs").join("chunks")
}
fn default_db_path() -> PathBuf {
    default_cache_base().join("hugrs").join("hugrs.db")
}
fn default_host() -> String {
    "127.0.0.1".into()
}
fn default_port() -> u16 {
    3000
}
fn default_hf_endpoint() -> String {
    "https://huggingface.co".into()
}
fn default_ms_endpoint() -> String {
    "https://modelscope.cn".into()
}
fn default_timeout_secs() -> u64 {
    60
}
fn default_connect_timeout_secs() -> u64 {
    15
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            local_root: default_local_root(),
            s3_bucket: None,
            s3_region: None,
            s3_prefix: None,
            s3_endpoint: None,
            compression: Compression::default(),
            max_size: None,
            prefetch_depth: default_prefetch_depth(),
            prefetch_budget_base: default_prefetch_budget_base(),
            verify_sha256: default_verify_sha256(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: default_db_path(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl Default for HfConfig {
    fn default() -> Self {
        Self {
            endpoint: default_hf_endpoint(),
            token: None,
            proxy: None,
            timeout_secs: default_timeout_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
        }
    }
}

impl Default for MsConfig {
    fn default() -> Self {
        Self {
            endpoint: default_ms_endpoint(),
            token: None,
            proxy: None,
            timeout_secs: default_timeout_secs(),
            connect_timeout_secs: default_connect_timeout_secs(),
        }
    }
}

pub struct CliOverrides {
    pub db_path: Option<String>,
    pub storage_backend: Option<String>,
    pub local_root: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_prefix: Option<String>,
    pub s3_endpoint: Option<String>,
    pub server_host: Option<String>,
    pub server_port: Option<u16>,
    pub hf_endpoint: Option<String>,
    pub hf_token: Option<String>,
    pub hf_proxy: Option<String>,
    pub hf_timeout: Option<u64>,
    pub hf_connect_timeout: Option<u64>,
    pub ms_endpoint: Option<String>,
    pub ms_token: Option<String>,
    pub ms_proxy: Option<String>,
    pub ms_timeout: Option<u64>,
    pub ms_connect_timeout: Option<u64>,
    pub config_file: Option<String>,
    pub compression: Option<String>,
    pub max_size: Option<u64>,
    pub prefetch_depth: Option<usize>,
    pub prefetch_budget_base: Option<usize>,
    pub enable_sha256_verify: Option<bool>,
}

impl Config {
    pub fn load(overrides: CliOverrides) -> anyhow::Result<Self> {
        let mut config = Config::default();

        let config_paths: Vec<String> = if let Some(ref path) = overrides.config_file {
            vec![path.clone()]
        } else {
            let home_config = dirs::home_dir()
                .unwrap_or_default()
                .join(".config")
                .join("hugrs")
                .join("hugrs.toml");
            vec![
                "hugrs.toml".to_string(),
                home_config.to_string_lossy().to_string(),
            ]
        };

        for path in &config_paths {
            if let Ok(content) = std::fs::read_to_string(path) {
                config = toml::from_str(&content)?;
                break;
            }
        }

        dotenvy::dotenv().ok();

        if let Ok(val) = std::env::var("HUGRS_STORAGE_BACKEND") {
            config.storage.backend = val;
        }
        if let Ok(val) = std::env::var("HUGRS_LOCAL_ROOT") {
            config.storage.local_root = val.into();
        }
        if let Ok(val) = std::env::var("HUGRS_S3_BUCKET") {
            config.storage.s3_bucket = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_S3_REGION") {
            config.storage.s3_region = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_S3_PREFIX") {
            config.storage.s3_prefix = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_S3_ENDPOINT") {
            config.storage.s3_endpoint = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_COMPRESSION") {
            config.storage.compression = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_MAX_SIZE") {
            config.storage.max_size = Some(val.parse()?);
        }
        if let Ok(val) = std::env::var("HUGRS_PREFETCH_DEPTH") {
            config.storage.prefetch_depth = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_PREFETCH_BUDGET_BASE") {
            config.storage.prefetch_budget_base = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_VERIFY_SHA256") {
            config.storage.verify_sha256 = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_DB_PATH") {
            config.database.path = val.into();
        }
        if let Ok(val) = std::env::var("HUGRS_SERVER_HOST") {
            config.server.host = val;
        }
        if let Ok(val) = std::env::var("HUGRS_SERVER_PORT") {
            config.server.port = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_HF_ENDPOINT") {
            config.huggingface.endpoint = val;
        }
        if let Ok(val) = std::env::var("HUGRS_HF_TOKEN") {
            config.huggingface.token = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_HF_PROXY") {
            config.huggingface.proxy = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_HF_TIMEOUT") {
            config.huggingface.timeout_secs = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_HF_CONNECT_TIMEOUT") {
            config.huggingface.connect_timeout_secs = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_MS_ENDPOINT") {
            config.modelscope.endpoint = val;
        }
        if let Ok(val) = std::env::var("HUGRS_MS_TOKEN") {
            config.modelscope.token = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_MS_PROXY") {
            config.modelscope.proxy = Some(val);
        }
        if let Ok(val) = std::env::var("HUGRS_MS_TIMEOUT") {
            config.modelscope.timeout_secs = val.parse()?;
        }
        if let Ok(val) = std::env::var("HUGRS_MS_CONNECT_TIMEOUT") {
            config.modelscope.connect_timeout_secs = val.parse()?;
        }

        if let Some(v) = overrides.db_path {
            config.database.path = v.into();
        }
        if let Some(v) = overrides.storage_backend {
            config.storage.backend = v;
        }
        if let Some(v) = overrides.local_root {
            config.storage.local_root = v.into();
        }
        if let Some(v) = overrides.s3_bucket {
            config.storage.s3_bucket = Some(v);
        }
        if let Some(v) = overrides.s3_region {
            config.storage.s3_region = Some(v);
        }
        if let Some(v) = overrides.s3_prefix {
            config.storage.s3_prefix = Some(v);
        }
        if let Some(v) = overrides.s3_endpoint {
            config.storage.s3_endpoint = Some(v);
        }
        if let Some(v) = overrides.compression {
            config.storage.compression = v.parse()?;
        }
        if let Some(v) = overrides.max_size {
            config.storage.max_size = Some(v);
        }
        if let Some(v) = overrides.prefetch_depth {
            config.storage.prefetch_depth = v;
        }
        if let Some(v) = overrides.prefetch_budget_base {
            config.storage.prefetch_budget_base = v;
        }
        if let Some(v) = overrides.enable_sha256_verify {
            config.storage.verify_sha256 = v;
        }
        if let Some(v) = overrides.server_host {
            config.server.host = v;
        }
        if let Some(v) = overrides.server_port {
            config.server.port = v;
        }
        if let Some(v) = overrides.hf_endpoint {
            config.huggingface.endpoint = v;
        }
        if let Some(v) = overrides.hf_token {
            config.huggingface.token = Some(v);
        }
        if let Some(v) = overrides.hf_proxy {
            config.huggingface.proxy = Some(v);
        }
        if let Some(v) = overrides.hf_timeout {
            config.huggingface.timeout_secs = v;
        }
        if let Some(v) = overrides.hf_connect_timeout {
            config.huggingface.connect_timeout_secs = v;
        }
        if let Some(v) = overrides.ms_endpoint {
            config.modelscope.endpoint = v;
        }
        if let Some(v) = overrides.ms_token {
            config.modelscope.token = Some(v);
        }
        if let Some(v) = overrides.ms_proxy {
            config.modelscope.proxy = Some(v);
        }
        if let Some(v) = overrides.ms_timeout {
            config.modelscope.timeout_secs = v;
        }
        if let Some(v) = overrides.ms_connect_timeout {
            config.modelscope.connect_timeout_secs = v;
        }

        if config.storage.backend == "local" {
            let root = &config.storage.local_root;
            if !root.exists() {
                if let Some(parent) = root.parent() {
                    let legacy = parent.join("trunks");
                    if legacy.exists() {
                        tracing::info!(
                            "Migrating legacy trunks dir: {} -> {}",
                            legacy.display(),
                            root.display()
                        );
                        std::fs::rename(&legacy, root)?;
                    }
                }
            }
        }

        Ok(config)
    }
}
