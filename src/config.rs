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
    default_cache_base().join("hugrs").join("trunks")
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
    pub config_file: Option<String>,
    pub compression: Option<String>,
    pub max_size: Option<u64>,
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

        Ok(config)
    }
}
