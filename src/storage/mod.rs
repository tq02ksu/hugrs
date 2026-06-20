pub mod local;
pub mod s3;

use async_trait::async_trait;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, sha256: &str, data: &[u8]) -> anyhow::Result<()>;

    async fn get(&self, sha256: &str) -> anyhow::Result<Vec<u8>>;

    async fn exists(&self, sha256: &str) -> anyhow::Result<bool>;

    async fn delete(&self, sha256: &str) -> anyhow::Result<()>;
}
