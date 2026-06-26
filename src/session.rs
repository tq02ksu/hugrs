use crate::chunker;
use crate::metadata::{File, MetadataStore};
use crate::storage::StorageBackend;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

pub const CHUNK_SIZE: usize = crate::service::CHUNK_SIZE;

type ClientRange = (u64, u64);
type ClientSender = mpsc::Sender<Result<Bytes, anyhow::Error>>;
type Subscribers = StdMutex<Vec<(ClientRange, ClientSender)>>;

fn prefetch_budget(base: usize, active_cursors: usize) -> usize {
    match active_cursors {
        0 | 1 => base,
        2 => (base / 2).max(1),
        _ => (base / 4).max(1),
    }
}

fn compute_active_cursors(
    client_ranges: &[ClientRange],
    completed: &std::collections::HashSet<usize>,
    chunk_sz: u64,
    chunk_count: usize,
) -> Vec<usize> {
    let mut cursors = Vec::new();
    let max_idx = chunk_count.saturating_sub(1);

    for (start, end) in client_ranges {
        let first = (*start / chunk_sz) as usize;
        let last = ((*end / chunk_sz) as usize).min(max_idx);
        if let Some(next) = (first..=last).find(|idx| !completed.contains(idx)) {
            cursors.push(next);
        }
    }

    cursors.sort_unstable();
    cursors.dedup();
    cursors
}

fn select_next_chunk(
    client_ranges: &[ClientRange],
    completed: &std::collections::HashSet<usize>,
    chunk_sz: u64,
    chunk_count: usize,
) -> Option<usize> {
    compute_active_cursors(client_ranges, completed, chunk_sz, chunk_count)
        .into_iter()
        .next()
}

fn retain_active_prefetches(
    inflight_prefetches: &mut HashSet<usize>,
    completed: &HashSet<usize>,
    cached: &HashSet<usize>,
) -> usize {
    inflight_prefetches.retain(|idx| !completed.contains(idx) && !cached.contains(idx));
    inflight_prefetches.len()
}

// ── ChunkSession ──────────────────────────────────────────────

pub struct ChunkSession {
    pub tx: broadcast::Sender<Arc<Bytes>>,
    _task: JoinHandle<()>,
}

pub struct SessionTable {
    map: Arc<DashMap<(i64, i64), Arc<ChunkSession>>>,
    http_client: reqwest::Client,
    backend: Arc<dyn StorageBackend>,
    metadata: Arc<MetadataStore>,
    fetched_bytes: Arc<AtomicU64>,
}

impl SessionTable {
    pub fn new(
        http_client: reqwest::Client,
        backend: Arc<dyn StorageBackend>,
        metadata: Arc<MetadataStore>,
        fetched_bytes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            http_client,
            backend,
            metadata,
            fetched_bytes,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &self,
        file_id: i64,
        chunk_idx: i64,
        url: &str,
        start: u64,
        end: u64,
        total_size: u64,
        chunk_count: usize,
        user_agent: Option<&str>,
    ) -> broadcast::Receiver<Arc<Bytes>> {
        let key = (file_id, chunk_idx);

        if let Ok(Some(sha)) = self.metadata.is_chunk_linked(file_id, chunk_idx as usize) {
            if let Ok(data) = self.backend.get(&sha).await {
                let (tx, _) = broadcast::channel::<Arc<Bytes>>(1);
                let rx = tx.subscribe();
                let _ = tx.send(Arc::new(Bytes::from(data)));
                return rx;
            }
        }

        if let Some(session) = self.map.get(&key) {
            return session.tx.subscribe();
        }

        let (tx, _) = broadcast::channel::<Arc<Bytes>>(4);
        let rx = tx.subscribe();

        let backend = self.backend.clone();
        let metadata = self.metadata.clone();
        let client = self.http_client.clone();
        let url = url.to_string();
        let tx2 = tx.clone();
        let fetched_bytes = self.fetched_bytes.clone();
        let map = self.map.clone();
        let user_agent = user_agent.map(str::to_string);
        let task = tokio::spawn(async move {
            match Self::download_and_store(
                client,
                backend,
                metadata,
                url,
                fetched_bytes,
                file_id,
                chunk_idx,
                start,
                end,
                total_size,
                chunk_count,
                user_agent,
            )
            .await
            {
                Ok(Some(data)) => {
                    let _ = tx.send(Arc::new(data));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!("chunk {} download failed: {:?}", chunk_idx, e);
                }
            }
            map.remove(&key);
        });

        self.map.insert(
            key,
            Arc::new(ChunkSession {
                tx: tx2,
                _task: task,
            }),
        );
        rx
    }

    #[allow(clippy::too_many_arguments)]
    async fn download_and_store(
        client: reqwest::Client,
        backend: Arc<dyn StorageBackend>,
        metadata: Arc<MetadataStore>,
        url: String,
        fetched_bytes: Arc<AtomicU64>,
        file_id: i64,
        chunk_idx: i64,
        start: u64,
        end: u64,
        _total_size: u64,
        chunk_count: usize,
        user_agent: Option<String>,
    ) -> anyhow::Result<Option<Bytes>> {
        if let Ok(Some(sha)) = metadata.is_chunk_linked(file_id, chunk_idx as usize) {
            if let Ok(data) = backend.get(&sha).await {
                tracing::info!(
                    "[f{}] chunk {}/{} already cached, skipping",
                    file_id,
                    chunk_idx + 1,
                    chunk_count
                );
                return Ok(Some(Bytes::from(data)));
            }
        }

        let range_header = format!("bytes={}-{}", start, end);
        let mut req = client.get(&url).header("Range", &range_header);
        if let Some(ref ua) = user_agent {
            req = req.header("User-Agent", ua);
        }
        let data = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("chunk {} request error: {}", chunk_idx, e))?
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("chunk {} download error: {}", chunk_idx, e))?;

        fetched_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);

        let (sha256, data) = tokio::task::spawn_blocking(move || {
            let h = chunker::sha256_hex(&data);
            (h, data)
        })
        .await
        .map_err(|e| anyhow::anyhow!("chunk {} sha256 panicked: {}", chunk_idx, e))?;

        let stored_size: i64 = if !backend.exists(&sha256).await.unwrap_or(false) {
            backend.put(&sha256, &data).await? as i64
        } else {
            data.len() as i64
        };

        let path = format!("{}/{}/{}", &sha256[0..2], &sha256[2..4], sha256);
        metadata.ensure_chunk(&sha256, "local", &path, data.len() as i64, stored_size)?;
        metadata.link_file_chunk(file_id, &sha256, chunk_idx, data.len() as i64)?;

        tracing::info!(
            "[f{}] chunk {}/{} done ({} bytes)",
            file_id,
            chunk_idx + 1,
            chunk_count,
            data.len()
        );

        Ok(Some(data))
    }
}

// ── FileDownloadSession ───────────────────────────────────────

pub struct FileDownloadSessionConfig {
    pub file_id: i64,
    pub name: String,
    pub repo: String,
    pub url: String,
    pub total_size: u64,
    pub source: String,
    pub chunk_count: usize,
    pub user_agent: Option<String>,
    pub prefetch_budget_base: usize,
}

pub struct FileDownloadSessionDeps {
    pub session_table: Arc<SessionTable>,
    pub metadata: Arc<MetadataStore>,
    pub backend: Arc<dyn StorageBackend>,
    pub head_client: reqwest::Client,
    pub served_bytes: Arc<AtomicU64>,
}

pub struct FileDownloadSession {
    file_id: i64,
    name: String,
    repo: String,
    url: String,
    total_size: u64,
    chunk_count: usize,
    source: String,
    user_agent: Option<String>,

    subscriber_count: AtomicUsize,
    subscribers: Subscribers,
    inflight_prefetches: StdMutex<HashSet<usize>>,

    session_table: Arc<SessionTable>,
    metadata: Arc<MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    head_client: reqwest::Client,
    served_bytes: Arc<AtomicU64>,
    prefetch_budget_base: usize,

    task: StdMutex<Option<JoinHandle<()>>>,
    state: AtomicU8,
    file_ready: AtomicBool,
    file_data: StdMutex<Option<(File, u64)>>,
}

impl FileDownloadSession {
    fn new(cfg: FileDownloadSessionConfig, deps: FileDownloadSessionDeps) -> Self {
        Self {
            file_id: cfg.file_id,
            name: cfg.name,
            repo: cfg.repo,
            url: cfg.url,
            total_size: cfg.total_size,
            chunk_count: cfg.chunk_count.max(1),
            source: cfg.source,
            user_agent: cfg.user_agent,
            subscriber_count: AtomicUsize::new(0),
            subscribers: StdMutex::new(Vec::new()),
            inflight_prefetches: StdMutex::new(HashSet::new()),
            session_table: deps.session_table,
            metadata: deps.metadata,
            backend: deps.backend,
            head_client: deps.head_client,
            served_bytes: deps.served_bytes,
            prefetch_budget_base: cfg.prefetch_budget_base,
            task: StdMutex::new(None),
            state: AtomicU8::new(0),
            file_ready: AtomicBool::new(false),
            file_data: StdMutex::new(None),
        }
    }

    fn signal_file_ready(&self, file: File, total_size: u64) {
        self.file_data.lock().unwrap().replace((file, total_size));
        self.file_ready.store(true, Ordering::SeqCst);
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        range: Option<(u64, Option<u64>)>,
    ) -> anyhow::Result<(File, u64, crate::service::ByteStream)> {
        let total_size = self.total_size;
        let req_start = range.map(|r| r.0).unwrap_or(0);
        let req_end = range
            .and_then(|r| r.1)
            .unwrap_or(total_size.saturating_sub(1))
            .min(total_size.saturating_sub(1));

        if req_start > req_end || req_start >= total_size {
            anyhow::bail!(
                "invalid range: bytes={}-{}/{}",
                req_start,
                req_end,
                total_size
            );
        }
        let content_length = req_end - req_start + 1;

        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        {
            let mut subs = self.subscribers.lock().unwrap();
            subs.push(((req_start, req_end), tx));
        }
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);

        let file = self.ensure_file_metadata().await?;
        self.signal_file_ready(file.clone(), total_size);

        self.ensure_running();

        Ok((
            file,
            content_length,
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    fn ensure_running(self: &Arc<Self>) {
        let mut task_guard = self.task.lock().unwrap();
        if task_guard.is_some() {
            return;
        }
        let self_clone = self.clone();
        *task_guard = Some(tokio::spawn(async move {
            self_clone.run_download_loop().await;
        }));
    }

    async fn run_download_loop(self: Arc<Self>) {
        let session_start = std::time::Instant::now();

        tracing::info!(
            "[f{}] {}: session started, {} chunks total",
            self.file_id,
            self.name,
            self.chunk_count,
        );

        let chunk_sz = CHUNK_SIZE as u64;

        let mut completed: HashSet<usize> = HashSet::new();
        loop {
            let client_ranges: Vec<(u64, u64)> = {
                let subs = self.subscribers.lock().unwrap();
                subs.iter().map(|(r, _)| *r).collect()
            };

            tracing::debug!(
                "[f{}] {}: loop start, subscribers={}, completed={}/{}",
                self.file_id,
                self.name,
                client_ranges.len(),
                completed.len(),
                self.chunk_count,
            );

            if client_ranges.is_empty() {
                self.state.store(2, Ordering::Relaxed);
                self.subscribers.lock().unwrap().clear();
                break;
            }

            let active_cursors =
                compute_active_cursors(&client_ranges, &completed, chunk_sz, self.chunk_count);

            if let Some(i) =
                select_next_chunk(&client_ranges, &completed, chunk_sz, self.chunk_count)
            {
                self.untrack_prefetch(i);
                let start = (i * CHUNK_SIZE) as u64;
                let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, self.total_size - 1);

                let chunk_start = std::time::Instant::now();
                let mut rx = self
                    .session_table
                    .subscribe(
                        self.file_id,
                        i as i64,
                        &self.url,
                        start,
                        end,
                        self.total_size,
                        self.chunk_count,
                        self.user_agent.as_deref(),
                    )
                    .await;

                match rx.recv().await {
                    Ok(data) => {
                        let elapsed_ms = chunk_start.elapsed().as_millis();
                        let chunk_start = i as u64 * chunk_sz;
                        self.forward_chunk(chunk_start, &data).await;
                        self.served_bytes
                            .fetch_add(data.len() as u64, Ordering::Relaxed);

                        completed.insert(i);
                        let active_prefetches = self.finish_prefetches(&completed, &HashSet::new());

                        tracing::info!(
                            "[f{}] {} chunk {}/{}: {} bytes in {}ms, prefetch_active={}",
                            self.file_id,
                            self.name,
                            i + 1,
                            self.chunk_count,
                            data.len(),
                            elapsed_ms,
                            active_prefetches,
                        );
                        if elapsed_ms > 5_000 {
                            tracing::warn!(
                                "[f{}] {} chunk {}/{}: SLOW — {} bytes in {}ms, prefetch_active={}",
                                self.file_id,
                                self.name,
                                i + 1,
                                self.chunk_count,
                                data.len(),
                                elapsed_ms,
                                active_prefetches,
                            );
                        }

                        let budget =
                            prefetch_budget(self.prefetch_budget_base, active_cursors.len());
                        let per_cursor = (budget / active_cursors.len().max(1)).max(1);
                        let mut cached_prefetches = HashSet::new();
                        for cursor in active_cursors {
                            for j in (cursor + 1)..(cursor + 1 + per_cursor).min(self.chunk_count) {
                                if completed.contains(&j) {
                                    continue;
                                }
                                if self.is_chunk_cached(j).await {
                                    cached_prefetches.insert(j);
                                    continue;
                                }
                                if !self.track_prefetch(j) {
                                    continue;
                                }
                                let pstart = (j * CHUNK_SIZE) as u64;
                                let pend = std::cmp::min(
                                    pstart + CHUNK_SIZE as u64 - 1,
                                    self.total_size - 1,
                                );
                                let _ = self
                                    .session_table
                                    .subscribe(
                                        self.file_id,
                                        j as i64,
                                        &self.url,
                                        pstart,
                                        pend,
                                        self.total_size,
                                        self.chunk_count,
                                        self.user_agent.as_deref(),
                                    )
                                    .await;
                            }
                        }
                        self.finish_prefetches(&completed, &cached_prefetches);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::warn!("chunk {} download closed unexpectedly", i);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("chunk {} receiver lagged by {} messages", i, n);
                    }
                }
            } else {
                self.state.store(2, Ordering::Relaxed);
                self.subscribers.lock().unwrap().clear();
                break;
            }

            self.clean_subscribers();
        }

        if let Ok(Some(f)) = self.metadata.get_file_by_name(&self.name, &self.source) {
            self.signal_file_ready(f.clone(), f.total_size as u64);
        }

        self.subscribers.lock().unwrap().clear();

        tracing::info!(
            "[f{}] {}: session finished in {}ms, prefetch_active={}, all senders dropped",
            self.file_id,
            self.name,
            session_start.elapsed().as_millis(),
            self.prefetch_active(),
        );

        self.task.lock().unwrap().take();
    }

    async fn ensure_file_metadata(&self) -> anyhow::Result<File> {
        if let Some(f) = self.metadata.get_file_by_name(&self.name, &self.source)? {
            if f.x_repo_commit.is_some() && f.total_size > 0 {
                self.signal_file_ready(f.clone(), f.total_size as u64);
                return Ok(f);
            }
        }

        let mut req = if crate::service::use_get_for_first_hop_probe(&self.source, &self.url) {
            self.head_client.get(&self.url)
        } else {
            self.head_client.head(&self.url)
        };
        if let Some(ref ua) = self.user_agent {
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
            let location = crate::server::resolve_redirect(&self.url, location);
            let mut req2 = self.head_client.head(&location);
            if let Some(ref ua) = self.user_agent {
                req2 = req2.header("User-Agent", ua);
            }
            let resp2 = req2.send().await?;
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
            anyhow::bail!("cannot determine file size for {}", self.url);
        }

        self.metadata.delete_file(&self.name, &self.source)?;
        self.metadata
            .add_file(&self.name, &self.repo, size as i64, &self.source)?;
        self.metadata.set_file_headers(
            &self.name,
            &self.source,
            etag.as_deref(),
            x_repo_commit.as_deref(),
            x_linked_size,
            x_linked_etag.as_deref(),
            content_type.as_deref(),
        )?;
        self.metadata.touch_repo(&self.repo)?;

        let file = self
            .metadata
            .get_file_by_name(&self.name, &self.source)?
            .ok_or_else(|| anyhow::anyhow!("file disappeared after creation"))?;
        self.signal_file_ready(file.clone(), size);
        Ok(file)
    }

    async fn is_chunk_cached(&self, i: usize) -> bool {
        match self.metadata.is_chunk_linked(self.file_id, i) {
            Ok(Some(sha)) => self.backend.exists(&sha).await.unwrap_or(false),
            _ => false,
        }
    }

    fn track_prefetch(&self, idx: usize) -> bool {
        self.inflight_prefetches.lock().unwrap().insert(idx)
    }

    fn untrack_prefetch(&self, idx: usize) {
        self.inflight_prefetches.lock().unwrap().remove(&idx);
    }

    fn finish_prefetches(&self, completed: &HashSet<usize>, cached: &HashSet<usize>) -> usize {
        let mut inflight = self.inflight_prefetches.lock().unwrap();
        retain_active_prefetches(&mut inflight, completed, cached)
    }

    fn prefetch_active(&self) -> usize {
        self.inflight_prefetches.lock().unwrap().len()
    }

    async fn forward_chunk(&self, chunk_start: u64, data: &[u8]) {
        let targets: Vec<(ClientRange, ClientSender)> = {
            let subs = self.subscribers.lock().unwrap();
            subs.iter()
                .map(|((s, e), tx)| ((*s, *e), tx.clone()))
                .collect()
        };
        for ((s, e), tx) in &targets {
            let chunk_end = chunk_start + data.len() as u64 - 1;
            if chunk_end < *s || chunk_start > *e {
                continue;
            }
            let sl_start = if *s > chunk_start {
                (*s - chunk_start) as usize
            } else {
                0
            };
            let sl_end = if *e < chunk_end {
                (*e - chunk_start + 1) as usize
            } else {
                data.len()
            };
            let slice = Bytes::copy_from_slice(&data[sl_start..sl_end]);
            if tx.send(Ok(slice)).await.is_err() {}
        }
    }

    fn clean_subscribers(&self) {
        let mut subs = self.subscribers.lock().unwrap();
        subs.retain(|(_, tx)| !tx.is_closed());
        self.subscriber_count.store(subs.len(), Ordering::Relaxed);
    }
}

// ── FileSessionManager ────────────────────────────────────────

pub struct FileSessionManager {
    map: DashMap<i64, Arc<FileDownloadSession>>,
    session_table: Arc<SessionTable>,
    metadata: Arc<MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    head_client: reqwest::Client,
    served_bytes: Arc<AtomicU64>,
}

impl FileSessionManager {
    pub fn new(
        session_table: Arc<SessionTable>,
        metadata: Arc<MetadataStore>,
        backend: Arc<dyn StorageBackend>,
        head_client: reqwest::Client,
        served_bytes: Arc<AtomicU64>,
    ) -> Self {
        Self {
            map: DashMap::new(),
            session_table,
            metadata,
            backend,
            head_client,
            served_bytes,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_or_create(
        &self,
        file_id: i64,
        name: &str,
        repo: &str,
        url: &str,
        source: &str,
        total_size: u64,
        prefetch_budget_base: usize,
        user_agent: Option<&str>,
    ) -> Arc<FileDownloadSession> {
        self.map
            .entry(file_id)
            .or_insert_with(|| {
                let chunk_count = (total_size as usize).div_ceil(CHUNK_SIZE);
                Arc::new(FileDownloadSession::new(
                    FileDownloadSessionConfig {
                        file_id,
                        name: name.to_string(),
                        repo: repo.to_string(),
                        url: url.to_string(),
                        total_size,
                        source: source.to_string(),
                        chunk_count,
                        user_agent: user_agent.map(str::to_string),
                        prefetch_budget_base,
                    },
                    FileDownloadSessionDeps {
                        session_table: self.session_table.clone(),
                        metadata: self.metadata.clone(),
                        backend: self.backend.clone(),
                        head_client: self.head_client.clone(),
                        served_bytes: self.served_bytes.clone(),
                    },
                ))
            })
            .clone()
    }

    pub fn remove(&self, file_id: i64) {
        self.map.remove(&file_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compute_active_cursors, prefetch_budget, retain_active_prefetches, select_next_chunk,
        FileDownloadSession, FileDownloadSessionConfig, FileDownloadSessionDeps,
    };
    use crate::config::Config;
    use crate::metadata::MetadataStore;
    use crate::storage::local::LocalBackend;
    use crate::storage::Compression;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::task::JoinHandle;

    #[test]
    fn single_subscriber_picks_smallest_incomplete_chunk() {
        let completed = HashSet::from([0usize, 1, 2]);
        let client_ranges = vec![(0u64, 9 * 4 * 1024 * 1024u64)];

        let next = select_next_chunk(&client_ranges, &completed, 4 * 1024 * 1024, 10);

        assert_eq!(next, Some(3));
    }

    #[test]
    fn active_cursors_deduplicate_and_budget_drops_with_more_cursors() {
        let completed = HashSet::new();
        let client_ranges = vec![
            (0u64, 9 * 4 * 1024 * 1024u64),
            (0u64, 9 * 4 * 1024 * 1024u64),
            (20 * 4 * 1024 * 1024u64, 29 * 4 * 1024 * 1024u64),
        ];

        let cursors = compute_active_cursors(&client_ranges, &completed, 4 * 1024 * 1024, 30);

        assert_eq!(cursors, vec![0, 20]);
        assert_eq!(prefetch_budget(8, 1), 8);
        assert_eq!(prefetch_budget(8, 2), 4);
        assert_eq!(prefetch_budget(8, 3), 2);
    }

    #[test]
    fn config_defaults_prefetch_budget_base_to_eight() {
        let config = Config::default();

        assert_eq!(config.storage.prefetch_budget_base, 8);
    }

    #[test]
    fn retain_active_prefetches_drops_completed_and_cached_chunks() {
        let mut inflight = HashSet::from([18usize, 19, 20, 21]);
        let completed = HashSet::from([18usize]);
        let cached = HashSet::from([20usize]);

        let active = retain_active_prefetches(&mut inflight, &completed, &cached);

        assert_eq!(active, 2);
        assert_eq!(inflight, HashSet::from([19usize, 21]));
    }

    #[tokio::test]
    async fn finished_session_clears_task_slot() {
        let dir = TempDir::new().unwrap();
        let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
        let backend: Arc<dyn crate::storage::StorageBackend> = Arc::new(LocalBackend::new(
            dir.path().join("chunks"),
            Compression::None,
        ));
        let fetched_bytes = Arc::new(AtomicU64::new(0));
        let served_bytes = Arc::new(AtomicU64::new(0));
        let client = reqwest::Client::new();
        let session_table = Arc::new(super::SessionTable::new(
            client.clone(),
            backend.clone(),
            metadata.clone(),
            fetched_bytes,
        ));

        let session = Arc::new(FileDownloadSession::new(
            FileDownloadSessionConfig {
                file_id: 1,
                name: "test.bin".to_string(),
                repo: "test/repo".to_string(),
                url: "http://localhost/test.bin".to_string(),
                total_size: crate::service::CHUNK_SIZE as u64,
                source: "hf".to_string(),
                chunk_count: 1,
                user_agent: None,
                prefetch_budget_base: 8,
            },
            FileDownloadSessionDeps {
                session_table,
                metadata,
                backend,
                head_client: client,
                served_bytes,
            },
        ));

        *session.task.lock().unwrap() = Some(tokio::spawn(async {}) as JoinHandle<()>);

        session.clone().run_download_loop().await;

        assert!(session.task.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn file_session_tracks_prefetch_state() {
        let dir = TempDir::new().unwrap();
        let metadata = Arc::new(MetadataStore::new(&dir.path().join("test.db")).unwrap());
        let backend: Arc<dyn crate::storage::StorageBackend> = Arc::new(LocalBackend::new(
            dir.path().join("chunks"),
            Compression::None,
        ));
        let fetched_bytes = Arc::new(AtomicU64::new(0));
        let served_bytes = Arc::new(AtomicU64::new(0));
        let client = reqwest::Client::new();
        let session_table = Arc::new(super::SessionTable::new(
            client.clone(),
            backend.clone(),
            metadata.clone(),
            fetched_bytes,
        ));

        let session = FileDownloadSession::new(
            FileDownloadSessionConfig {
                file_id: 1,
                name: "test.bin".to_string(),
                repo: "test/repo".to_string(),
                url: "http://localhost/test.bin".to_string(),
                total_size: crate::service::CHUNK_SIZE as u64,
                source: "hf".to_string(),
                chunk_count: 8,
                user_agent: None,
                prefetch_budget_base: 8,
            },
            FileDownloadSessionDeps {
                session_table,
                metadata,
                backend,
                head_client: client,
                served_bytes,
            },
        );

        session.track_prefetch(18);
        session.track_prefetch(19);
        session.track_prefetch(20);

        let active =
            session.finish_prefetches(&HashSet::from([18usize]), &HashSet::from([20usize]));

        assert_eq!(active, 1);
        assert_eq!(session.prefetch_active(), 1);
    }
}
