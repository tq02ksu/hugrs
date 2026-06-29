use crate::storage::Compression;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::io::Write;
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
    pub admin: AdminConfig,

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

    #[serde(default = "default_etag_validation_timeout")]
    pub etag_validation_timeout_secs: u64,
}

fn default_prefetch_depth() -> usize {
    0
}

fn default_prefetch_budget_base() -> usize {
    8
}

fn default_verify_sha256() -> bool {
    true
}

fn default_etag_validation_timeout() -> u64 {
    5
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
pub struct AdminConfig {
    pub token: Option<String>,

    #[serde(default = "default_admin_token_file")]
    pub token_file: PathBuf,
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

fn default_admin_token_file() -> PathBuf {
    default_cache_base().join("hugrs").join("admin.token")
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
            etag_validation_timeout_secs: default_etag_validation_timeout(),
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

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            token: None,
            token_file: default_admin_token_file(),
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

#[derive(Default)]
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
    pub admin_token: Option<String>,
    pub admin_token_file: Option<String>,
    pub config_file: Option<String>,
    pub compression: Option<String>,
    pub max_size: Option<u64>,
    pub prefetch_depth: Option<usize>,
    pub prefetch_budget_base: Option<usize>,
    pub enable_sha256_verify: Option<bool>,
    pub etag_validation_timeout_secs: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct ConfigPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    storage: Option<StoragePatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    database: Option<DatabasePatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server: Option<ServerPatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    admin: Option<AdminPatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    huggingface: Option<HfPatch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    modelscope: Option<MsPatch>,
}

#[derive(Debug, Default, Serialize)]
struct StoragePatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_root: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s3_bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s3_region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s3_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    s3_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<Compression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefetch_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefetch_budget_base: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verify_sha256: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag_validation_timeout_secs: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct DatabasePatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<PathBuf>,
}

#[derive(Debug, Default, Serialize)]
struct ServerPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
}

#[derive(Debug, Default, Serialize)]
struct AdminPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_file: Option<PathBuf>,
}

#[derive(Debug, Default, Serialize)]
struct HfPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    proxy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connect_timeout_secs: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct MsPatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    proxy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connect_timeout_secs: Option<u64>,
}

impl Config {
    pub fn load(overrides: CliOverrides) -> anyhow::Result<Self> {
        dotenvy::dotenv().ok();

        let mut figment = Figment::from(Serialized::defaults(Config::default()));
        for path in config_paths(&overrides) {
            if std::fs::metadata(&path).is_ok() {
                figment = figment.merge(Toml::file(path));
                break;
            }
        }

        figment = figment.merge(Serialized::defaults(env_patch()?));
        figment = figment.merge(Serialized::defaults(cli_patch(overrides)?));

        let config: Config = figment.extract()?;

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

    pub fn ensure_admin_token(&mut self) -> anyhow::Result<String> {
        if let Some(token) = &self.admin.token {
            return Ok(token.clone());
        }

        if let Ok(token) = std::fs::read_to_string(&self.admin.token_file) {
            let token = token.trim().to_string();
            if !token.is_empty() {
                self.admin.token = Some(token.clone());
                return Ok(token);
            }
        }

        if let Some(parent) = self.admin.token_file.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let token = format!(
            "hugrs-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );
        let mut file = std::fs::File::create(&self.admin.token_file)?;
        file.write_all(token.as_bytes())?;
        self.admin.token = Some(token.clone());
        Ok(token)
    }
}

fn config_paths(overrides: &CliOverrides) -> Vec<String> {
    if let Some(path) = &overrides.config_file {
        return vec![path.clone()];
    }

    let home_config = dirs::home_dir()
        .unwrap_or_default()
        .join(".config")
        .join("hugrs")
        .join("hugrs.toml");
    vec![
        "hugrs.toml".to_string(),
        home_config.to_string_lossy().to_string(),
    ]
}

fn cli_patch(overrides: CliOverrides) -> anyhow::Result<ConfigPatch> {
    let mut patch = ConfigPatch::default();

    if has_any(&[
        overrides.storage_backend.is_some(),
        overrides.local_root.is_some(),
        overrides.s3_bucket.is_some(),
        overrides.s3_region.is_some(),
        overrides.s3_prefix.is_some(),
        overrides.s3_endpoint.is_some(),
        overrides.compression.is_some(),
        overrides.max_size.is_some(),
        overrides.prefetch_depth.is_some(),
        overrides.prefetch_budget_base.is_some(),
        overrides.enable_sha256_verify.is_some(),
        overrides.etag_validation_timeout_secs.is_some(),
    ]) {
        patch.storage = Some(StoragePatch {
            backend: overrides.storage_backend,
            local_root: overrides.local_root.map(PathBuf::from),
            s3_bucket: overrides.s3_bucket,
            s3_region: overrides.s3_region,
            s3_prefix: overrides.s3_prefix,
            s3_endpoint: overrides.s3_endpoint,
            compression: match overrides.compression {
                Some(value) => Some(value.parse()?),
                None => None,
            },
            max_size: overrides.max_size,
            prefetch_depth: overrides.prefetch_depth,
            prefetch_budget_base: overrides.prefetch_budget_base,
            verify_sha256: overrides.enable_sha256_verify,
            etag_validation_timeout_secs: overrides.etag_validation_timeout_secs,
        });
    }

    if overrides.db_path.is_some() {
        patch.database = Some(DatabasePatch {
            path: overrides.db_path.map(PathBuf::from),
        });
    }

    if has_any(&[
        overrides.server_host.is_some(),
        overrides.server_port.is_some(),
    ]) {
        patch.server = Some(ServerPatch {
            host: overrides.server_host,
            port: overrides.server_port,
        });
    }

    if has_any(&[
        overrides.admin_token.is_some(),
        overrides.admin_token_file.is_some(),
    ]) {
        patch.admin = Some(AdminPatch {
            token: overrides.admin_token,
            token_file: overrides.admin_token_file.map(PathBuf::from),
        });
    }

    if has_any(&[
        overrides.hf_endpoint.is_some(),
        overrides.hf_token.is_some(),
        overrides.hf_proxy.is_some(),
        overrides.hf_timeout.is_some(),
        overrides.hf_connect_timeout.is_some(),
    ]) {
        patch.huggingface = Some(HfPatch {
            endpoint: overrides.hf_endpoint,
            token: overrides.hf_token,
            proxy: overrides.hf_proxy,
            timeout_secs: overrides.hf_timeout,
            connect_timeout_secs: overrides.hf_connect_timeout,
        });
    }

    if has_any(&[
        overrides.ms_endpoint.is_some(),
        overrides.ms_token.is_some(),
        overrides.ms_proxy.is_some(),
        overrides.ms_timeout.is_some(),
        overrides.ms_connect_timeout.is_some(),
    ]) {
        patch.modelscope = Some(MsPatch {
            endpoint: overrides.ms_endpoint,
            token: overrides.ms_token,
            proxy: overrides.ms_proxy,
            timeout_secs: overrides.ms_timeout,
            connect_timeout_secs: overrides.ms_connect_timeout,
        });
    }

    Ok(patch)
}

fn env_patch() -> anyhow::Result<ConfigPatch> {
    let mut patch = ConfigPatch::default();

    let storage = StoragePatch {
        backend: env_string("HUGRS_STORAGE_BACKEND"),
        local_root: env_path("HUGRS_LOCAL_ROOT"),
        s3_bucket: env_string("HUGRS_S3_BUCKET"),
        s3_region: env_string("HUGRS_S3_REGION"),
        s3_prefix: env_string("HUGRS_S3_PREFIX"),
        s3_endpoint: env_string("HUGRS_S3_ENDPOINT"),
        compression: env_parsed("HUGRS_COMPRESSION")?,
        max_size: env_parsed("HUGRS_MAX_SIZE")?,
        prefetch_depth: env_parsed("HUGRS_PREFETCH_DEPTH")?,
        prefetch_budget_base: env_parsed("HUGRS_PREFETCH_BUDGET_BASE")?,
        verify_sha256: env_parsed("HUGRS_VERIFY_SHA256")?,
        etag_validation_timeout_secs: env_parsed("HUGRS_ETAG_VALIDATION_TIMEOUT")?,
    };
    if !storage_is_empty(&storage) {
        patch.storage = Some(storage);
    }

    let database = DatabasePatch {
        path: env_path("HUGRS_DB_PATH"),
    };
    if database.path.is_some() {
        patch.database = Some(database);
    }

    let server = ServerPatch {
        host: env_string("HUGRS_SERVER_HOST"),
        port: env_parsed("HUGRS_SERVER_PORT")?,
    };
    if !server_is_empty(&server) {
        patch.server = Some(server);
    }

    let admin = AdminPatch {
        token: env_string("HUGRS_ADMIN_TOKEN"),
        token_file: env_path("HUGRS_ADMIN_TOKEN_FILE"),
    };
    if !admin_is_empty(&admin) {
        patch.admin = Some(admin);
    }

    let huggingface = HfPatch {
        endpoint: env_string("HUGRS_HF_ENDPOINT"),
        token: env_string("HUGRS_HF_TOKEN"),
        proxy: env_string("HUGRS_HF_PROXY"),
        timeout_secs: env_parsed("HUGRS_HF_TIMEOUT")?,
        connect_timeout_secs: env_parsed("HUGRS_HF_CONNECT_TIMEOUT")?,
    };
    if !hf_is_empty(&huggingface) {
        patch.huggingface = Some(huggingface);
    }

    let modelscope = MsPatch {
        endpoint: env_string("HUGRS_MS_ENDPOINT"),
        token: env_string("HUGRS_MS_TOKEN"),
        proxy: env_string("HUGRS_MS_PROXY"),
        timeout_secs: env_parsed("HUGRS_MS_TIMEOUT")?,
        connect_timeout_secs: env_parsed("HUGRS_MS_CONNECT_TIMEOUT")?,
    };
    if !ms_is_empty(&modelscope) {
        patch.modelscope = Some(modelscope);
    }

    Ok(patch)
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn env_path(key: &str) -> Option<PathBuf> {
    env_string(key).map(PathBuf::from)
}

fn env_parsed<T>(key: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(value) => Ok(Some(
            value
                .parse()
                .map_err(|err| anyhow::anyhow!("{key}: {err}"))?,
        )),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn has_any(values: &[bool]) -> bool {
    values.iter().any(|value| *value)
}

fn storage_is_empty(value: &StoragePatch) -> bool {
    !has_any(&[
        value.backend.is_some(),
        value.local_root.is_some(),
        value.s3_bucket.is_some(),
        value.s3_region.is_some(),
        value.s3_prefix.is_some(),
        value.s3_endpoint.is_some(),
        value.compression.is_some(),
        value.max_size.is_some(),
        value.prefetch_depth.is_some(),
        value.prefetch_budget_base.is_some(),
        value.verify_sha256.is_some(),
    ])
}

fn server_is_empty(value: &ServerPatch) -> bool {
    !has_any(&[value.host.is_some(), value.port.is_some()])
}

fn admin_is_empty(value: &AdminPatch) -> bool {
    !has_any(&[value.token.is_some(), value.token_file.is_some()])
}

fn hf_is_empty(value: &HfPatch) -> bool {
    !has_any(&[
        value.endpoint.is_some(),
        value.token.is_some(),
        value.proxy.is_some(),
        value.timeout_secs.is_some(),
        value.connect_timeout_secs.is_some(),
    ])
}

fn ms_is_empty(value: &MsPatch) -> bool {
    !has_any(&[
        value.endpoint.is_some(),
        value.token.is_some(),
        value.proxy.is_some(),
        value.timeout_secs.is_some(),
        value.connect_timeout_secs.is_some(),
    ])
}
