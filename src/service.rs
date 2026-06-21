use crate::chunker;
use crate::metadata::{File, MetadataStore, Stats};
use crate::storage::StorageBackend;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub struct CacheService {
    metadata: Arc<MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    max_size: Option<u64>,
    http_client: reqwest::Client,
    download_locks: StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

struct DownloadedChunk {
    index: usize,
    sha256: String,
    size: usize,
    data: Vec<u8>,
}

impl CacheService {
    pub fn new(
        metadata: Arc<MetadataStore>,
        backend: Arc<dyn StorageBackend>,
        max_size: Option<u64>,
        http_client: reqwest::Client,
    ) -> Self {
        Self {
            metadata,
            backend,
            max_size,
            http_client,
            download_locks: StdMutex::new(HashMap::new()),
        }
    }

    pub async fn upload(&self, name: &str, repo: &str, data: Vec<u8>) -> anyhow::Result<()> {
        let total_size = data.len() as i64;

        self.metadata.delete_file(name)?;

        let chunks = chunker::chunk_with_hashes(&data, CHUNK_SIZE);

        let file = self.metadata.add_file(name, repo, total_size, "upload")?;

        for chunk in &chunks {
            if !self.backend.exists(&chunk.sha256).await? {
                self.backend.put(&chunk.sha256, &chunk.data).await?;
            }

            let path = self.trunk_path(&chunk.sha256);
            self.metadata
                .ensure_trunk(&chunk.sha256, "local", &path, chunk.chunk_size as i64)?;

            self.metadata.link_file_trunk(
                file.id,
                &chunk.sha256,
                chunk.chunk_index as i64,
                chunk.chunk_size as i64,
            )?;
        }

        self.metadata.touch_repo(repo)?;

        if let Some(limit) = self.max_size {
            self.evict_if_needed(limit).await?;
        }

        Ok(())
    }

    pub async fn download_from_url(
        &self,
        url: &str,
        name: &str,
        repo: &str,
        concurrency: usize,
    ) -> anyhow::Result<()> {
        if self.is_file_complete(name).await? {
            tracing::info!("{} already cached, skipping", name);
            self.metadata.touch_repo(repo)?;
            return Ok(());
        }

        let file_lock = {
            let mut locks = self.download_locks.lock().unwrap();
            locks
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = file_lock.lock().await;

        if self.is_file_complete(name).await? {
            return Ok(());
        }
        self.metadata.delete_file(name)?;

        let head = self.http_client.head(url).send().await?;
        let headers = head.headers();
        let total_size: u64 = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| anyhow::anyhow!("Cannot determine file size for {}", url))?;

        let etag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let x_repo_commit = headers
            .get("x-repo-commit")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let x_linked_size = headers
            .get("x-linked-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let x_linked_etag = headers
            .get("x-linked-etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if total_size <= CHUNK_SIZE as u64 {
            tracing::info!(
                "Downloading {} ({} bytes, single request)",
                name,
                total_size
            );
            let data = self.http_client.get(url).send().await?.bytes().await?;
            self.upload(name, repo, data.to_vec()).await?;
            self.metadata.set_file_headers(
                name,
                etag.as_deref(),
                x_repo_commit.as_deref(),
                x_linked_size,
                x_linked_etag.as_deref(),
            )?;
            return Ok(());
        }

        tracing::info!(
            "Downloading {} ({} bytes, {} chunks, {} concurrent)",
            name,
            total_size,
            (total_size as usize).div_ceil(CHUNK_SIZE),
            concurrency
        );

        let chunk_count = (total_size as usize).div_ceil(CHUNK_SIZE);
        let mut handles = Vec::with_capacity(chunk_count);

        for i in 0..chunk_count {
            let start = (i * CHUNK_SIZE) as u64;
            let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, total_size - 1);
            let url = url.to_string();
            let client = self.http_client.clone();

            handles.push(tokio::spawn(async move {
                let range_header = format!("bytes={}-{}", start, end);
                let resp = client
                    .get(&url)
                    .header("Range", &range_header)
                    .send()
                    .await?;

                let data = resp.bytes().await?;
                let sha256 = chunker::sha256_hex(&data);

                anyhow::Ok(DownloadedChunk {
                    index: i,
                    sha256,
                    size: data.len(),
                    data: data.to_vec(),
                })
            }));
        }

        let chunk_count = handles.len();
        let mut chunks: Vec<DownloadedChunk> = Vec::with_capacity(chunk_count);
        let mut futs = futures_util::stream::FuturesUnordered::from_iter(handles);

        use futures_util::StreamExt;
        while let Some(result) = futs.next().await {
            let chunk = result??;
            chunks.push(chunk);
        }

        chunks.sort_by_key(|c| c.index);

        self.metadata.delete_file(name)?;
        let file = self
            .metadata
            .add_file(name, repo, total_size as i64, "pull")?;

        for chunk in &chunks {
            if !self.backend.exists(&chunk.sha256).await? {
                self.backend.put(&chunk.sha256, &chunk.data).await?;
            }

            let path = self.trunk_path(&chunk.sha256);
            self.metadata
                .ensure_trunk(&chunk.sha256, "local", &path, chunk.size as i64)?;

            self.metadata.link_file_trunk(
                file.id,
                &chunk.sha256,
                chunk.index as i64,
                chunk.size as i64,
            )?;
        }

        self.metadata.touch_repo(repo)?;

        self.metadata.set_file_headers(
            name,
            etag.as_deref(),
            x_repo_commit.as_deref(),
            x_linked_size,
            x_linked_etag.as_deref(),
        )?;

        if let Some(limit) = self.max_size {
            self.evict_if_needed(limit).await?;
        }

        Ok(())
    }

    pub async fn download(&self, name: &str) -> anyhow::Result<Vec<u8>> {
        let file = self
            .metadata
            .get_file_by_name(name)?
            .ok_or_else(|| anyhow::anyhow!("file not found: {}", name))?;

        let repo = file.repo.clone();
        self.metadata.touch_repo(&repo)?;

        let trunks = self.metadata.get_file_trunks(file.id)?;
        let mut chunks = Vec::new();

        for ft in &trunks {
            let data = self.backend.get(&ft.sha256).await?;
            let actual_hash = chunker::sha256_hex(&data);
            if actual_hash != ft.sha256 {
                anyhow::bail!(
                    "checksum mismatch for {} chunk {}: expected {} got {}",
                    name,
                    ft.chunk_index,
                    ft.sha256,
                    actual_hash
                );
            }
            chunks.push(data);
        }

        Ok(chunker::assemble_chunks(&chunks))
    }

    async fn is_file_complete(&self, name: &str) -> anyhow::Result<bool> {
        let file = match self.metadata.get_file_by_name(name)? {
            Some(f) => f,
            None => return Ok(false),
        };

        let trunks = self.metadata.get_file_trunks(file.id)?;
        let expected = (file.total_size as usize).div_ceil(CHUNK_SIZE);
        if trunks.len() != expected {
            return Ok(false);
        }

        for (i, ft) in trunks.iter().enumerate() {
            if ft.chunk_index != i as i64 {
                return Ok(false);
            }
            if !self.backend.exists(&ft.sha256).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    pub async fn info(&self, name: &str) -> anyhow::Result<Option<File>> {
        self.metadata.get_file_by_name(name)
    }

    pub async fn delete(&self, name: &str) -> anyhow::Result<bool> {
        self.metadata.delete_file(name)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<File>> {
        self.metadata.list_files()
    }

    pub async fn stats(&self) -> anyhow::Result<Stats> {
        self.metadata.get_stats()
    }

    pub async fn gc(&self) -> anyhow::Result<usize> {
        let orphans = self.metadata.get_orphan_trunks()?;
        let count = orphans.len();
        for sha256 in &orphans {
            self.backend.delete(sha256).await?;
        }
        Ok(count)
    }

    async fn evict_if_needed(&self, max_size: u64) -> anyhow::Result<()> {
        loop {
            let stats = self.metadata.get_stats()?;
            if stats.total_size as u64 <= max_size {
                break;
            }

            let candidates = self.metadata.list_repos_by_access(10)?;
            if candidates.is_empty() {
                break;
            }

            let victim_repo = &candidates[0];
            let deleted = self.metadata.delete_files_by_repo(victim_repo)?;
            tracing::warn!(
                "Evicted repo '{}' ({} files, {} bytes total)",
                victim_repo,
                deleted,
                stats.total_size
            );

            let orphans = self.metadata.get_orphan_trunks()?;
            for sha256 in &orphans {
                self.backend.delete(sha256).await?;
            }
        }
        Ok(())
    }

    fn trunk_path(&self, sha256: &str) -> String {
        format!("{}/{}/{}", &sha256[0..2], &sha256[2..4], sha256)
    }
}
