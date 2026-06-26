use crate::chunker;
use crate::metadata::{File, MetadataStore, Stats};
use crate::storage::StorageBackend;
use bytes::Bytes;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub(crate) fn use_get_for_first_hop_probe(source: &str, url: &str) -> bool {
    source == "ms" && url.contains("/api/v1/models/") && url.contains("/repo?")
}

pub type ByteStream = ReceiverStream<Result<Bytes, anyhow::Error>>;

struct DownloadedChunk {
    index: usize,
    sha256: String,
    size: usize,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct CacheService {
    pub metadata: Arc<MetadataStore>,
    pub backend: Arc<dyn StorageBackend>,
    max_size: Option<u64>,
    pub http_client: reqwest::Client,
    pub head_client: reqwest::Client,
    prefetch_depth: usize,
    prefetch_budget_base: usize,
    verify_sha256: bool,
    fetched_bytes: Arc<AtomicU64>,
    served_bytes: Arc<AtomicU64>,
    fs_manager: Arc<crate::session::FileSessionManager>,
}

impl CacheService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata: Arc<MetadataStore>,
        backend: Arc<dyn StorageBackend>,
        max_size: Option<u64>,
        http_client: reqwest::Client,
        head_client: reqwest::Client,
        prefetch_depth: usize,
        prefetch_budget_base: usize,
        verify_sha256: bool,
        stream_client: reqwest::Client,
    ) -> Self {
        let fetched_bytes = Arc::new(AtomicU64::new(0));
        let served_bytes = Arc::new(AtomicU64::new(0));

        let session_table = Arc::new(crate::session::SessionTable::new(
            stream_client,
            backend.clone(),
            metadata.clone(),
            fetched_bytes.clone(),
        ));

        let fs_manager = Arc::new(crate::session::FileSessionManager::new(
            session_table.clone(),
            metadata.clone(),
            backend.clone(),
            head_client.clone(),
            served_bytes.clone(),
        ));

        Self {
            metadata,
            backend,
            max_size,
            http_client,
            head_client,
            prefetch_depth,
            prefetch_budget_base,
            verify_sha256,
            fetched_bytes,
            served_bytes,
            fs_manager,
        }
    }

    pub async fn upload(
        &self,
        name: &str,
        repo: &str,
        source: &str,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        let total_size = data.len() as i64;

        self.metadata.delete_file(name, source)?;

        let chunks = chunker::chunk_with_hashes(&data, CHUNK_SIZE);

        let file = self.metadata.add_file(name, repo, total_size, source)?;

        for chunk in &chunks {
            let stored_size: i64 = if !self.backend.exists(&chunk.sha256).await? {
                self.backend.put(&chunk.sha256, &chunk.data).await? as i64
            } else {
                chunk.chunk_size as i64
            };

            let path = self.chunk_path(&chunk.sha256);
            self.metadata.ensure_chunk(
                &chunk.sha256,
                "local",
                &path,
                chunk.chunk_size as i64,
                stored_size,
            )?;

            self.metadata.link_file_chunk(
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
        source: &str,
        concurrency: usize,
    ) -> anyhow::Result<()> {
        if self.is_file_complete(name, source).await? {
            tracing::info!("{} already cached, skipping", name);
            self.metadata.touch_repo(repo)?;
            return Ok(());
        }

        let head_resp = self.head_client.head(url).send().await?;
        let first_headers = head_resp.headers();
        let status = head_resp.status();

        let x_repo_commit = first_headers
            .get("x-repo-commit")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let x_linked_size: Option<i64> = first_headers
            .get("x-linked-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let x_linked_etag = first_headers
            .get("x-linked-etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let (total_size, etag, content_type, downstream_url) = if status.is_redirection() {
            let location = first_headers
                .get("location")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let location = crate::server::resolve_redirect(url, location);
            tracing::info!("download_from_url following redirect: {}", location);
            let resp2 = self.http_client.head(&location).send().await?;
            let h = resp2.headers();
            let cl: u64 = h
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| anyhow::anyhow!("Cannot determine file size for {}", url))?;
            let et = h
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let ct = h
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            (cl, et, ct, location.to_string())
        } else {
            let cl: u64 = first_headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .ok_or_else(|| anyhow::anyhow!("Cannot determine file size for {}", url))?;
            let et = first_headers
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let ct = first_headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            (cl, et, ct, url.to_string())
        };

        if total_size <= CHUNK_SIZE as u64 {
            tracing::info!(
                "Downloading {} ({} bytes, single request)",
                name,
                total_size
            );
            let data = self
                .http_client
                .get(&downstream_url)
                .send()
                .await?
                .bytes()
                .await?;
            self.fetched_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            self.metadata.delete_file(name, source)?;
            self.upload(name, repo, source, data.to_vec()).await?;
            self.metadata.set_file_headers(
                name,
                source,
                etag.as_deref(),
                x_repo_commit.as_deref(),
                x_linked_size,
                x_linked_etag.as_deref(),
                content_type.as_deref(),
            )?;
            return Ok(());
        }

        let chunk_count = (total_size as usize).div_ceil(CHUNK_SIZE);

        let (file_id, completed) = match self.metadata.get_file_by_name(name, source)? {
            Some(existing) => {
                if existing.total_size as u64 != total_size {
                    self.metadata.delete_file(name, source)?;
                    let file = self
                        .metadata
                        .add_file(name, repo, total_size as i64, source)?;
                    (file.id, HashSet::new())
                } else {
                    let file_chunks = self.metadata.get_file_chunks(existing.id)?;
                    let completed: HashSet<usize> =
                        file_chunks.iter().map(|t| t.chunk_index as usize).collect();
                    (existing.id, completed)
                }
            }
            None => {
                let file = self
                    .metadata
                    .add_file(name, repo, total_size as i64, source)?;
                (file.id, HashSet::new())
            }
        };

        let missing: Vec<usize> = (0..chunk_count)
            .filter(|i| !completed.contains(i))
            .collect();

        if missing.is_empty() {
            self.metadata.touch_repo(repo)?;
            self.metadata.set_file_headers(
                name,
                source,
                etag.as_deref(),
                x_repo_commit.as_deref(),
                x_linked_size,
                x_linked_etag.as_deref(),
                content_type.as_deref(),
            )?;
            return Ok(());
        }

        tracing::info!(
            "Downloading {} ({} bytes, {} chunks total, {} remaining, {} concurrent)",
            name,
            total_size,
            chunk_count,
            missing.len(),
            concurrency
        );

        let mut handles = Vec::with_capacity(missing.len());
        for &i in &missing {
            let start = (i * CHUNK_SIZE) as u64;
            let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, total_size - 1);
            let downstream_url = downstream_url.clone();
            let client = self.http_client.clone();
            let concurrency_sem = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
            let fetched_bytes = self.fetched_bytes.clone();

            handles.push(tokio::spawn(async move {
                let _permit = concurrency_sem.acquire().await;
                let range_header = format!("bytes={}-{}", start, end);
                let resp = client
                    .get(&downstream_url)
                    .header("Range", &range_header)
                    .send()
                    .await?;

                let data = resp.bytes().await?;
                fetched_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                let sha256 = chunker::sha256_hex(&data);

                anyhow::Ok(DownloadedChunk {
                    index: i,
                    sha256,
                    size: data.len(),
                    data: data.to_vec(),
                })
            }));
        }

        use futures_util::StreamExt;
        let mut futs = futures_util::stream::FuturesUnordered::from_iter(handles);
        let mut completed_count = 0;
        let total = missing.len();
        while let Some(result) = futs.next().await {
            let chunk = result??;

            let stored_size: i64 = if !self.backend.exists(&chunk.sha256).await? {
                self.backend.put(&chunk.sha256, &chunk.data).await? as i64
            } else {
                chunk.size as i64
            };

            let path = self.chunk_path(&chunk.sha256);
            self.metadata.ensure_chunk(
                &chunk.sha256,
                "local",
                &path,
                chunk.size as i64,
                stored_size,
            )?;

            self.metadata.link_file_chunk(
                file_id,
                &chunk.sha256,
                chunk.index as i64,
                chunk.size as i64,
            )?;

            completed_count += 1;
            tracing::info!(
                "{} chunk {}/{} done ({}/{} total)",
                name,
                chunk.index + 1,
                chunk_count,
                completed_count,
                total
            );
        }

        self.metadata.touch_repo(repo)?;

        self.metadata.set_file_headers(
            name,
            source,
            etag.as_deref(),
            x_repo_commit.as_deref(),
            x_linked_size,
            x_linked_etag.as_deref(),
            content_type.as_deref(),
        )?;

        if let Some(limit) = self.max_size {
            self.evict_if_needed(limit).await?;
        }

        Ok(())
    }

    pub async fn download(&self, name: &str, source: &str) -> anyhow::Result<Vec<u8>> {
        let file = self
            .metadata
            .get_file_by_name(name, source)?
            .ok_or_else(|| anyhow::anyhow!("file not found: {}", name))?;

        let repo = file.repo.clone();
        self.metadata.touch_repo(&repo)?;

        let file_chunks = self.metadata.get_file_chunks(file.id)?;
        let mut chunks = Vec::new();

        for ft in &file_chunks {
            let data = self.backend.get(&ft.sha256).await?;
            let actual_hash = chunker::sha256_hex(&data);
            if actual_hash != ft.sha256 {
                tracing::error!(
                    "checksum mismatch for {} chunk {}: expected {} got {}",
                    name,
                    ft.chunk_index,
                    ft.sha256,
                    actual_hash
                );
                let _ = self.backend.delete(&ft.sha256).await;
                anyhow::bail!("checksum mismatch for {} chunk {}", name, ft.chunk_index);
            }
            chunks.push(data);
        }

        Ok(chunker::assemble_chunks(&chunks))
    }

    pub async fn is_file_complete(&self, name: &str, source: &str) -> anyhow::Result<bool> {
        let file = match self.metadata.get_file_by_name(name, source)? {
            Some(f) => f,
            None => return Ok(false),
        };

        let chunks = self.metadata.get_file_chunks(file.id)?;
        let expected = (file.total_size as usize).div_ceil(CHUNK_SIZE);
        if chunks.len() != expected {
            return Ok(false);
        }

        for (i, ft) in chunks.iter().enumerate() {
            if ft.chunk_index != i as i64 {
                return Ok(false);
            }
            if !self.backend.exists(&ft.sha256).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ensure_file_headers(
        &self,
        name: &str,
        repo: &str,
        source: &str,
        total_size: u64,
        etag: Option<&str>,
        x_repo_commit: Option<&str>,
        x_linked_size: Option<i64>,
        x_linked_etag: Option<&str>,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        if self.metadata.get_file_by_name(name, source)?.is_none() {
            self.metadata.delete_file(name, source)?;
            self.metadata
                .add_file(name, repo, total_size as i64, source)?;
        }
        self.metadata.set_file_headers(
            name,
            source,
            etag,
            x_repo_commit,
            x_linked_size,
            x_linked_etag,
            content_type,
        )?;
        Ok(())
    }

    pub async fn info(&self, name: &str, source: &str) -> anyhow::Result<Option<File>> {
        self.metadata.get_file_by_name(name, source)
    }

    pub async fn delete(&self, name: &str, source: &str) -> anyhow::Result<bool> {
        self.metadata.delete_file(name, source)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<File>> {
        self.metadata.list_files()
    }

    pub async fn stats(&self) -> anyhow::Result<Stats> {
        let mut stats = self.metadata.get_stats()?;
        stats.fetched_bytes = self.fetched_bytes.load(Ordering::Relaxed);
        stats.served_bytes = self.served_bytes.load(Ordering::Relaxed);
        Ok(stats)
    }

    pub async fn gc(&self) -> anyhow::Result<usize> {
        let orphans = self.metadata.get_orphan_chunks()?;
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

            let orphans = self.metadata.get_orphan_chunks()?;
            for sha256 in &orphans {
                self.backend.delete(sha256).await?;
            }
        }
        Ok(())
    }

    fn chunk_path(&self, sha256: &str) -> String {
        format!("{}/{}/{}", &sha256[0..2], &sha256[2..4], sha256)
    }

    pub async fn backend_exists(&self, key: &str) -> anyhow::Result<bool> {
        self.backend.exists(key).await
    }

    pub async fn backend_get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        self.backend.get(key).await
    }

    pub async fn backend_put(&self, key: &str, data: &[u8]) -> anyhow::Result<u64> {
        self.backend.put(key, data).await
    }

    pub fn get_http_cache(&self, url: &str) -> anyhow::Result<Option<(u16, String, Vec<u8>)>> {
        self.metadata.get_http_cache(url)
    }

    pub fn set_http_cache(
        &self,
        url: &str,
        status: u16,
        headers: &str,
        body: &[u8],
    ) -> anyhow::Result<()> {
        self.metadata.set_http_cache(url, status, headers, body)
    }

    pub async fn stream_cached_file(
        &self,
        name: &str,
        source: &str,
        range_start: Option<u64>,
        range_end: Option<u64>,
    ) -> anyhow::Result<(File, u64, ByteStream)> {
        let file = self
            .metadata
            .get_file_by_name(name, source)?
            .ok_or_else(|| anyhow::anyhow!("file not found: {}", name))?;

        let total_size = file.total_size as u64;
        let start = range_start.unwrap_or(0);
        let end = range_end
            .unwrap_or(total_size.saturating_sub(1))
            .min(total_size.saturating_sub(1));

        if start > end || start >= total_size {
            anyhow::bail!("invalid range: bytes={}-{}/{}", start, end, total_size);
        }

        let content_length = end - start + 1;
        let file_chunks = self.metadata.get_file_chunks(file.id)?;
        let chunk_size_u64 = CHUNK_SIZE as u64;
        let first_chunk = (start / chunk_size_u64) as usize;
        let last_chunk = ((end / chunk_size_u64) as usize).min(file_chunks.len().saturating_sub(1));

        let backend = self.backend.clone();
        let relevant: Vec<_> = file_chunks[first_chunk..=last_chunk].to_vec();

        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        let prefetch_depth = self.prefetch_depth;
        let verify_sha256 = self.verify_sha256;
        let fname = name.to_string();
        let served_bytes = self.served_bytes.clone();

        tokio::spawn(async move {
            let mut byte_offset = first_chunk as u64 * chunk_size_u64;
            let mut prefetches: VecDeque<(
                usize,
                tokio::task::JoinHandle<anyhow::Result<Vec<u8>>>,
            )> = VecDeque::new();

            for (i, ft) in relevant.iter().enumerate() {
                let data = if let Some((_idx, h)) = prefetches.pop_front() {
                    match h.await {
                        Ok(Ok(d)) => d,
                        Ok(Err(e)) => {
                            tracing::warn!(
                                "{}: prefetch stream aborted, chunk {} failed: {}",
                                fname,
                                ft.chunk_index,
                                e
                            );
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                        Err(_) => {
                            tracing::warn!(
                                "{}: prefetch stream aborted, chunk {} task panicked",
                                fname,
                                ft.chunk_index
                            );
                            let _ = tx
                                .send(Err(anyhow::anyhow!(
                                    "chunk {} pre-fetch panicked",
                                    ft.chunk_index
                                )))
                                .await;
                            return;
                        }
                    }
                } else {
                    let raw = match backend.get(&ft.sha256).await {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!(
                                "{}: cached stream aborted, chunk {} read error: {}",
                                fname,
                                ft.chunk_index,
                                e
                            );
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    };
                    if verify_sha256 {
                        let (raw2, actual) = tokio::task::spawn_blocking(move || {
                            let h = chunker::sha256_hex(&raw);
                            (raw, h)
                        })
                        .await
                        .unwrap();
                        if actual != ft.sha256 {
                            tracing::error!(
                                "checksum mismatch for chunk {}: expected {} got {}",
                                ft.chunk_index,
                                ft.sha256,
                                actual
                            );
                            let _ = backend.delete(&ft.sha256).await;
                            let _ = tx
                                .send(Err(anyhow::anyhow!(
                                    "checksum mismatch for chunk {}",
                                    ft.chunk_index
                                )))
                                .await;
                            return;
                        }
                        raw2
                    } else {
                        raw
                    }
                };

                while prefetches.len() < prefetch_depth {
                    let next_i = i + prefetches.len() + 1;
                    if next_i >= relevant.len() {
                        break;
                    }
                    let next_ft = relevant[next_i].clone();
                    let be = backend.clone();
                    prefetches.push_back((
                        next_i,
                        tokio::spawn(async move {
                            let raw = be.get(&next_ft.sha256).await?;
                            if verify_sha256 {
                                let (raw2, actual) = tokio::task::spawn_blocking(move || {
                                    let h = chunker::sha256_hex(&raw);
                                    (raw, h)
                                })
                                .await
                                .map_err(|e| anyhow::anyhow!("sha256 panicked: {}", e))?;
                                if actual != next_ft.sha256 {
                                    tracing::error!(
                                        "checksum mismatch for chunk {}: expected {} got {}",
                                        next_ft.chunk_index,
                                        next_ft.sha256,
                                        actual
                                    );
                                    let _ = be.delete(&next_ft.sha256).await;
                                    anyhow::bail!(
                                        "checksum mismatch for chunk {}",
                                        next_ft.chunk_index
                                    );
                                }
                                Ok(raw2)
                            } else {
                                Ok(raw)
                            }
                        }),
                    ));
                }

                let chunk_start = byte_offset;
                let chunk_end = byte_offset + data.len() as u64 - 1;

                let sl_start = if start > chunk_start {
                    (start - chunk_start) as usize
                } else {
                    0
                };
                let sl_end = if end < chunk_end {
                    (end - chunk_start + 1) as usize
                } else {
                    data.len()
                };

                let data_len = data.len();
                let bytes = if sl_start == 0 && sl_end == data_len {
                    Bytes::from(data)
                } else {
                    Bytes::copy_from_slice(&data[sl_start..sl_end])
                };
                served_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                if tx.send(Ok(bytes)).await.is_err() {
                    return;
                }

                byte_offset += data_len as u64;
            }
        });

        Ok((file, content_length, ReceiverStream::new(rx)))
    }

    pub async fn stream_from_upstream(
        &self,
        url: &str,
        name: &str,
        repo: &str,
        source: &str,
        range_start: Option<u64>,
        range_end: Option<u64>,
        user_agent: Option<&str>,
    ) -> anyhow::Result<(File, u64, ByteStream)> {
        self.fetch_file_metadata(url, name, repo, source, user_agent)
            .await?;

        let file = self
            .metadata
            .get_file_by_name(name, source)?
            .ok_or_else(|| anyhow::anyhow!("file disappeared after creation"))?;
        let total_size = file.total_size as u64;

        if total_size <= CHUNK_SIZE as u64 {
            return self.stream_small_file(url, name, &file, user_agent).await;
        }

        let session = self.fs_manager.get_or_create(
            file.id,
            name,
            repo,
            url,
            source,
            total_size,
            self.prefetch_budget_base,
            user_agent,
        );
        session
            .subscribe(Some((range_start.unwrap_or(0), range_end)))
            .await
    }

    async fn fetch_file_metadata(
        &self,
        url: &str,
        name: &str,
        repo: &str,
        source: &str,
        user_agent: Option<&str>,
    ) -> anyhow::Result<(
        u64,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
    )> {
        let mut req = if use_get_for_first_hop_probe(source, url) {
            self.head_client.get(url)
        } else {
            self.head_client.head(url)
        };
        if let Some(ua) = user_agent {
            req = req.header("User-Agent", ua);
        }
        let head_resp = req.send().await?;
        let first_headers = head_resp.headers();
        let status = head_resp.status();

        let x_repo_commit = first_headers
            .get("x-repo-commit")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let x_linked_size: Option<i64> = first_headers
            .get("x-linked-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let x_linked_etag = first_headers
            .get("x-linked-etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let (upstream_size, etag, content_type) = if status.is_redirection() {
            let location = first_headers
                .get("location")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let location = crate::server::resolve_redirect(url, location);
            tracing::info!("fetch_file_metadata following redirect: {}", location);
            let mut req = self.http_client.head(&location);
            if let Some(ua) = user_agent {
                req = req.header("User-Agent", ua);
            }
            match req.send().await {
                Ok(resp2) => {
                    let h = resp2.headers();
                    let cl: u64 = h
                        .get("content-length")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0);
                    let et = h
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    let ct = h
                        .get("content-type")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    (cl, et, ct)
                }
                Err(e) => anyhow::bail!("redirect follow failed for {}: {}", url, e),
            }
        } else {
            let cl: u64 = first_headers
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let et = first_headers
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let ct = first_headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            (cl, et, ct)
        };

        let size = if upstream_size > 0 {
            upstream_size
        } else {
            x_linked_size.unwrap_or(0) as u64
        };
        if size == 0 {
            anyhow::bail!("cannot determine file size for {}", url);
        }

        let existing = self.metadata.get_file_by_name(name, source)?;
        if existing
            .as_ref()
            .map(|f| f.total_size as u64 != size)
            .unwrap_or(true)
        {
            self.metadata.delete_file(name, source)?;
        }
        if self.metadata.get_file_by_name(name, source)?.is_none() {
            self.metadata.add_file(name, repo, size as i64, source)?;
        }
        self.metadata.set_file_headers(
            name,
            source,
            etag.as_deref(),
            x_repo_commit.as_deref(),
            x_linked_size,
            x_linked_etag.as_deref(),
            content_type.as_deref(),
        )?;
        self.metadata.touch_repo(repo)?;

        Ok((
            size,
            etag,
            content_type,
            x_repo_commit,
            x_linked_size,
            x_linked_etag,
        ))
    }

    async fn stream_small_file(
        &self,
        url: &str,
        name: &str,
        file: &File,
        user_agent: Option<&str>,
    ) -> anyhow::Result<(File, u64, ByteStream)> {
        let source = file.source.clone();
        if self.is_file_complete(name, &source).await? {
            return self.stream_cached_file(name, &source, None, None).await;
        }

        let total_size = file.total_size as u64;
        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(1);
        let client = self.http_client.clone();
        let url = url.to_string();
        let svc = self.clone();
        let fname = name.to_string();
        let frepo = file.repo.clone();
        let user_agent = user_agent.map(str::to_string);
        let fetched_bytes = self.fetched_bytes.clone();
        let served_bytes = self.served_bytes.clone();

        tokio::spawn(async move {
            let mut req = client.get(&url);
            if let Some(ref ua) = user_agent {
                req = req.header("User-Agent", ua);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(anyhow::anyhow!("request error: {}", e))).await;
                    return;
                }
            };
            let data = match resp.bytes().await {
                Ok(d) => d,
                Err(e) => {
                    let _ = tx.send(Err(anyhow::anyhow!("download error: {}", e))).await;
                    return;
                }
            };
            fetched_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
            if let Err(e) = svc.upload(&fname, &frepo, &source, data.to_vec()).await {
                let _ = tx.send(Err(e)).await;
                return;
            }
            served_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
            let _ = tx.send(Ok(data)).await;
        });

        Ok((file.clone(), total_size, ReceiverStream::new(rx)))
    }
}
