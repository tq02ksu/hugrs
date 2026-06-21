pub mod local;
pub mod s3;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    #[default]
    Zstd,
    None,
}

impl std::str::FromStr for Compression {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "zstd" => Ok(Compression::Zstd),
            "none" => Ok(Compression::None),
            other => anyhow::bail!("unknown compression algorithm: {}", other),
        }
    }
}

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, sha256: &str, data: &[u8]) -> anyhow::Result<()>;

    async fn get(&self, sha256: &str) -> anyhow::Result<Vec<u8>>;

    async fn exists(&self, sha256: &str) -> anyhow::Result<bool>;

    async fn delete(&self, sha256: &str) -> anyhow::Result<()>;
}
