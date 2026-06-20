use super::StorageBackend;
use async_trait::async_trait;
use std::path::PathBuf;

pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    pub fn new(root: PathBuf) -> Self {
        std::fs::create_dir_all(&root).ok();
        Self { root }
    }

    fn trunk_path(&self, sha256: &str) -> PathBuf {
        let dir = self.root.join(&sha256[0..2]).join(&sha256[2..4]);
        dir.join(sha256)
    }
}

#[async_trait]
impl StorageBackend for LocalBackend {
    async fn put(&self, sha256: &str, data: &[u8]) -> anyhow::Result<()> {
        let path = self.trunk_path(sha256);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    async fn get(&self, sha256: &str) -> anyhow::Result<Vec<u8>> {
        let path = self.trunk_path(sha256);
        Ok(tokio::fs::read(&path).await?)
    }

    async fn exists(&self, sha256: &str) -> anyhow::Result<bool> {
        let path = self.trunk_path(sha256);
        Ok(tokio::fs::metadata(&path).await.is_ok())
    }

    async fn delete(&self, sha256: &str) -> anyhow::Result<()> {
        let path = self.trunk_path(sha256);
        if tokio::fs::metadata(&path).await.is_ok() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }
}
