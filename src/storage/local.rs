use super::{Compression, StorageBackend};
use async_trait::async_trait;
use std::path::PathBuf;

const COMPRESS_THRESHOLD: usize = 4096;

pub struct LocalBackend {
    root: PathBuf,
    compression: Compression,
}

impl LocalBackend {
    pub fn new(root: PathBuf, compression: Compression) -> Self {
        std::fs::create_dir_all(&root).ok();
        Self { root, compression }
    }

    fn trunk_path(&self, sha256: &str) -> PathBuf {
        let dir = self.root.join(&sha256[0..2]).join(&sha256[2..4]);
        dir.join(sha256)
    }

    fn compressed_path(&self, sha256: &str) -> PathBuf {
        let mut p = self.trunk_path(sha256);
        p.as_mut_os_string().push(".zst");
        p
    }
}

#[async_trait]
impl StorageBackend for LocalBackend {
    async fn put(&self, sha256: &str, data: &[u8]) -> anyhow::Result<u64> {
        let path = self.trunk_path(sha256);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if self.compression == Compression::Zstd && data.len() > COMPRESS_THRESHOLD {
            let compressed = zstd::encode_all(data, 0)?;
            let len = compressed.len() as u64;
            tokio::fs::write(&self.compressed_path(sha256), &compressed).await?;
            Ok(len)
        } else {
            let len = data.len() as u64;
            tokio::fs::write(&path, data).await?;
            Ok(len)
        }
    }

    async fn get(&self, sha256: &str) -> anyhow::Result<Vec<u8>> {
        let compressed_path = self.compressed_path(sha256);
        if tokio::fs::metadata(&compressed_path).await.is_ok() {
            let compressed = tokio::fs::read(&compressed_path).await?;
            Ok(zstd::decode_all(&compressed[..])?)
        } else {
            let path = self.trunk_path(sha256);
            Ok(tokio::fs::read(&path).await?)
        }
    }

    async fn exists(&self, sha256: &str) -> anyhow::Result<bool> {
        let path = self.trunk_path(sha256);
        let compressed_path = self.compressed_path(sha256);
        Ok(tokio::fs::metadata(&path).await.is_ok()
            || tokio::fs::metadata(&compressed_path).await.is_ok())
    }

    async fn delete(&self, sha256: &str) -> anyhow::Result<()> {
        let path = self.trunk_path(sha256);
        let compressed_path = self.compressed_path(sha256);
        if tokio::fs::metadata(&path).await.is_ok() {
            tokio::fs::remove_file(&path).await?;
        }
        if tokio::fs::metadata(&compressed_path).await.is_ok() {
            tokio::fs::remove_file(&compressed_path).await?;
        }
        Ok(())
    }
}
