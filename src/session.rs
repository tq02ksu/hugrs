use crate::chunker;
use crate::metadata::File;
use crate::storage::StorageBackend;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

pub const CHUNK_SIZE: usize = crate::service::CHUNK_SIZE;

type ClientRange = (u64, u64);
type ClientSender = mpsc::Sender<Result<Bytes, anyhow::Error>>;
type Subscribers = StdMutex<Vec<(ClientRange, ClientSender)>>;
type ChunkMessage = Result<Arc<Bytes>, Arc<String>>;

trait LockExt<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockExt<T> for StdMutex<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T> {
        match self.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("recovering from poisoned session mutex");
                poisoned.into_inner()
            }
        }
    }
}

fn prefetch_budget(base: usize, active_cursors: usize) -> usize {
    match active_cursors {
        0 | 1 => base,
        2 => (base / 2).max(1),
        _ => (base / 4).max(1),
    }
}

fn compute_active_cursors(
    client_ranges: &[ClientRange],
    completed: &HashSet<usize>,
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
    completed: &HashSet<usize>,
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

pub struct ChunkStoredEvent {
    pub sha256: String,
    pub path: String,
    pub data_len: i64,
    pub stored_size: i64,
    pub file_id: i64,
    pub chunk_idx: i64,
}

// ── ChunkSession ──────────────────────────────────────────────

pub struct ChunkSession {
    pub tx: broadcast::Sender<ChunkMessage>,
    _task: JoinHandle<()>,
}

struct ChunkReader {
    http_client: reqwest::Client,
    backend: Arc<dyn StorageBackend>,
    event_tx: mpsc::UnboundedSender<ChunkStoredEvent>,
    fetched_bytes: Arc<AtomicU64>,
    verify_sha256: bool,
    write_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
}

#[cfg(test)]
impl ChunkReader {
    pub(crate) fn new_for_test(backend: Arc<dyn StorageBackend>, verify_sha256: bool) -> Self {
        let (event_tx, _) = tokio::sync::mpsc::unbounded_channel::<ChunkStoredEvent>();
        Self {
            http_client: reqwest::Client::new(),
            backend,
            event_tx,
            fetched_bytes: Arc::new(AtomicU64::new(0)),
            verify_sha256,
            write_locks: DashMap::new(),
        }
    }

    pub(crate) async fn read_cached_chunk_test(
        &self,
        sha256: &str,
        expected_chunk_size: Option<usize>,
    ) -> anyhow::Result<Option<Bytes>> {
        self.read_cached_chunk(sha256, expected_chunk_size).await
    }
}

pub struct SessionTable {
    map: Arc<DashMap<(i64, i64), Arc<ChunkSession>>>,
    reader: Arc<ChunkReader>,
}

impl SessionTable {
    pub fn new(
        http_client: reqwest::Client,
        backend: Arc<dyn StorageBackend>,
        event_tx: mpsc::UnboundedSender<ChunkStoredEvent>,
        fetched_bytes: Arc<AtomicU64>,
        verify_sha256: bool,
    ) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            reader: Arc::new(ChunkReader {
                http_client,
                backend,
                event_tx,
                fetched_bytes,
                verify_sha256,
                write_locks: DashMap::new(),
            }),
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
        cached_chunks: &Arc<StdMutex<HashMap<usize, String>>>,
    ) -> anyhow::Result<broadcast::Receiver<ChunkMessage>> {
        let key = (file_id, chunk_idx);

        let cached_sha = cached_chunks
            .lock_or_recover()
            .get(&(chunk_idx as usize))
            .cloned();
        if let Some(ref sha) = cached_sha {
            let expected_size = (end - start + 1) as usize;
            if let Some(data) = self
                .reader
                .read_cached_chunk(sha, Some(expected_size))
                .await?
            {
                let (tx, _) = broadcast::channel::<ChunkMessage>(1);
                let rx = tx.subscribe();
                let _ = tx.send(Ok(Arc::new(data)));
                return Ok(rx);
            }
            cached_chunks
                .lock_or_recover()
                .remove(&(chunk_idx as usize));
        }

        let reader = self.reader.clone();
        let url = url.to_string();
        let user_agent = user_agent.map(str::to_string);
        let cached_chunks = cached_chunks.clone();
        let map = self.map.clone();

        let entry = self.map.entry(key).or_insert_with(|| {
            let (tx, _) = broadcast::channel::<ChunkMessage>(4);
            let tx2 = tx.clone();
            let map = map.clone();
            let task = tokio::spawn(async move {
                let result = reader
                    .fetch_chunk(
                        url,
                        file_id,
                        chunk_idx,
                        start,
                        end,
                        total_size,
                        chunk_count,
                        user_agent,
                        cached_chunks,
                    )
                    .await;
                match result {
                    Ok(data) => {
                        let _ = tx.send(Ok(Arc::new(data)));
                    }
                    Err(e) => {
                        tracing::warn!("chunk {} download failed: {:?}", chunk_idx, e);
                        let _ = tx.send(Err(Arc::new(e.to_string())));
                    }
                };
                map.remove(&key);
            });
            Arc::new(ChunkSession {
                tx: tx2,
                _task: task,
            })
        });
        Ok(entry.tx.subscribe())
    }
}

impl ChunkReader {
    async fn read_cached_chunk(
        &self,
        sha256: &str,
        expected_chunk_size: Option<usize>,
    ) -> anyhow::Result<Option<Bytes>> {
        let raw = match self.backend.get(sha256).await {
            Ok(raw) => raw,
            Err(_) => return Ok(None),
        };

        if !self.verify_sha256 {
            return Ok(Some(Bytes::from(raw)));
        }

        let (raw, actual) = tokio::task::spawn_blocking(move || {
            let actual = chunker::sha256_hex(&raw);
            (raw, actual)
        })
        .await
        .map_err(|e| anyhow::anyhow!("sha256 panicked: {e}"))?;

        if actual == sha256 {
            if let Some(expected) = expected_chunk_size {
                if raw.len() != expected {
                    tracing::error!(
                        "chunk size mismatch: sha256 matches but len={} != expected={}",
                        raw.len(),
                        expected
                    );
                    let _ = self.backend.delete(sha256).await;
                    return Ok(None);
                }
            }
            return Ok(Some(Bytes::from(raw)));
        }

        tracing::error!(
            "checksum mismatch for cached chunk: expected {} got {}",
            sha256,
            actual
        );
        let _ = self.backend.delete(sha256).await;
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    async fn fetch_chunk(
        &self,
        url: String,
        file_id: i64,
        chunk_idx: i64,
        start: u64,
        end: u64,
        _total_size: u64,
        chunk_count: usize,
        user_agent: Option<String>,
        cached_chunks: Arc<StdMutex<HashMap<usize, String>>>,
    ) -> anyhow::Result<Bytes> {
        let range_header = format!("bytes={start}-{end}");
        let mut req = self.http_client.get(&url).header("Range", &range_header);
        if let Some(ref ua) = user_agent {
            req = req.header("User-Agent", ua);
        }
        let response = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("chunk {chunk_idx} request error: {e}"))?;
        let status = response.status();
        let full_data = response
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("chunk {chunk_idx} request error: {e}"))?
            .bytes()
            .await
            .map_err(|e| anyhow::anyhow!("chunk {chunk_idx} download error: {e}"))?;

        let expected_len = (end - start + 1) as usize;
        let data = if status == reqwest::StatusCode::OK && full_data.len() > expected_len {
            let offset = start as usize;
            let slice_end = (end + 1) as usize;
            let slice_end = slice_end.min(full_data.len());
            full_data.slice(offset..slice_end)
        } else if full_data.len() != expected_len {
            anyhow::bail!(
                "chunk {chunk_idx} download incomplete: expected {} bytes, got {}",
                expected_len,
                full_data.len()
            );
        } else {
            full_data
        };

        self.fetched_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        let (sha256, data) = tokio::task::spawn_blocking(move || {
            let h = chunker::sha256_hex(&data);
            (h, data)
        })
        .await
        .map_err(|e| anyhow::anyhow!("chunk {chunk_idx} sha256 panicked: {e}"))?;

        let expected_sha = cached_chunks
            .lock_or_recover()
            .get(&(chunk_idx as usize))
            .cloned();
        if self.verify_sha256
            && expected_sha
                .as_ref()
                .map(|expected| expected != &sha256)
                .unwrap_or(false)
        {
            anyhow::bail!(
                "checksum mismatch for chunk {}: expected {} got {}",
                chunk_idx,
                expected_sha.unwrap_or_default(),
                sha256
            );
        }

        let lock = self
            .write_locks
            .entry(sha256.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        let stored_size: i64 = if !self.backend.exists(&sha256).await.unwrap_or(false) {
            self.backend.put(&sha256, &data).await? as i64
        } else {
            data.len() as i64
        };

        let path = format!("{}/{}/{}", &sha256[0..2], &sha256[2..4], sha256);
        cached_chunks
            .lock_or_recover()
            .insert(chunk_idx as usize, sha256.clone());
        let _ = self.event_tx.send(ChunkStoredEvent {
            sha256: sha256.clone(),
            path,
            data_len: data.len() as i64,
            stored_size,
            file_id,
            chunk_idx,
        });

        tracing::info!(
            "[f{}] chunk {}/{} done ({} bytes)",
            file_id,
            chunk_idx + 1,
            chunk_count,
            data.len()
        );

        Ok(data)
    }
}

// ── FileDownloadSession ───────────────────────────────────────

pub struct FileDownloadSessionConfig {
    pub file_id: i64,
    pub name: String,
    pub url: String,
    pub total_size: u64,
    pub chunk_count: usize,
    pub user_agent: Option<String>,
    pub prefetch_budget_base: usize,
    pub cached_chunks: StdMutex<HashMap<usize, String>>,
    pub file: File,
}

pub struct FileDownloadSessionDeps {
    pub session_table: Arc<SessionTable>,
    pub served_bytes: Arc<AtomicU64>,
}

pub struct FileDownloadSession {
    file_id: i64,
    name: String,
    url: String,
    total_size: u64,
    chunk_count: usize,
    user_agent: Option<String>,

    subscriber_count: AtomicUsize,
    subscribers: Subscribers,
    inflight_prefetches: StdMutex<HashSet<usize>>,

    session_table: Arc<SessionTable>,
    served_bytes: Arc<AtomicU64>,
    prefetch_budget_base: usize,
    cached_chunks: Arc<StdMutex<HashMap<usize, String>>>,
    file: File,

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
            url: cfg.url,
            total_size: cfg.total_size,
            chunk_count: cfg.chunk_count.max(1),
            user_agent: cfg.user_agent,
            subscriber_count: AtomicUsize::new(0),
            subscribers: StdMutex::new(Vec::new()),
            inflight_prefetches: StdMutex::new(HashSet::new()),
            session_table: deps.session_table,
            served_bytes: deps.served_bytes,
            prefetch_budget_base: cfg.prefetch_budget_base,
            cached_chunks: Arc::new(cfg.cached_chunks),
            file: cfg.file,
            task: StdMutex::new(None),
            state: AtomicU8::new(0),
            file_ready: AtomicBool::new(false),
            file_data: StdMutex::new(None),
        }
    }

    fn signal_file_ready(&self, file: File, total_size: u64) {
        self.file_data.lock_or_recover().replace((file, total_size));
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
            anyhow::bail!("invalid range: bytes={req_start}-{req_end}/{total_size}");
        }
        let content_length = req_end - req_start + 1;

        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        {
            let mut subs = self.subscribers.lock_or_recover();
            subs.push(((req_start, req_end), tx));
        }
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);

        self.signal_file_ready(self.file.clone(), total_size);

        self.ensure_running();

        Ok((
            self.file.clone(),
            content_length,
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    fn ensure_running(self: &Arc<Self>) {
        let mut task_guard = self.task.lock_or_recover();
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
                let subs = self.subscribers.lock_or_recover();
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
                self.subscribers.lock_or_recover().clear();
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
                let mut rx = match self
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
                        &self.cached_chunks,
                    )
                    .await
                {
                    Ok(rx) => rx,
                    Err(e) => {
                        self.forward_error(anyhow::anyhow!("chunk {i} subscribe failed: {e}"))
                            .await;
                        break;
                    }
                };

                match rx.recv().await {
                    Ok(Ok(data)) => {
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
                        for cursor in active_cursors {
                            for j in (cursor + 1)..(cursor + 1 + per_cursor).min(self.chunk_count) {
                                if completed.contains(&j) {
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
                                        &self.cached_chunks,
                                    )
                                    .await;
                            }
                        }
                        self.finish_prefetches(&completed, &HashSet::new());
                    }
                    Ok(Err(err)) => {
                        self.forward_error(anyhow::anyhow!(err.as_ref().clone()))
                            .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        self.forward_error(anyhow::anyhow!(
                            "chunk {i} download closed unexpectedly"
                        ))
                        .await;
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        self.forward_error(anyhow::anyhow!(
                            "chunk {i} receiver lagged by {n} messages"
                        ))
                        .await;
                        break;
                    }
                }
            } else {
                self.state.store(2, Ordering::Relaxed);
                self.subscribers.lock_or_recover().clear();
                break;
            }

            self.clean_subscribers();
        }

        self.subscribers.lock_or_recover().clear();

        tracing::info!(
            "[f{}] {}: session finished in {}ms, prefetch_active={}, all senders dropped",
            self.file_id,
            self.name,
            session_start.elapsed().as_millis(),
            self.prefetch_active(),
        );

        self.task.lock_or_recover().take();
    }

    fn track_prefetch(&self, idx: usize) -> bool {
        self.inflight_prefetches.lock_or_recover().insert(idx)
    }

    fn untrack_prefetch(&self, idx: usize) {
        self.inflight_prefetches.lock_or_recover().remove(&idx);
    }

    fn finish_prefetches(&self, completed: &HashSet<usize>, cached: &HashSet<usize>) -> usize {
        let mut inflight = self.inflight_prefetches.lock_or_recover();
        retain_active_prefetches(&mut inflight, completed, cached)
    }

    fn prefetch_active(&self) -> usize {
        self.inflight_prefetches.lock_or_recover().len()
    }

    async fn forward_chunk(&self, chunk_start: u64, data: &[u8]) {
        let targets: Vec<(ClientRange, ClientSender)> = {
            let subs = self.subscribers.lock_or_recover();
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

    async fn forward_error(&self, err: anyhow::Error) {
        let message = err.to_string();
        let targets: Vec<ClientSender> = {
            let subs = self.subscribers.lock_or_recover();
            subs.iter().map(|(_, tx)| tx.clone()).collect()
        };
        for tx in &targets {
            let _ = tx.send(Err(anyhow::anyhow!(message.clone()))).await;
        }
    }

    fn clean_subscribers(&self) {
        let mut subs = self.subscribers.lock_or_recover();
        subs.retain(|(_, tx)| !tx.is_closed());
        self.subscriber_count.store(subs.len(), Ordering::Relaxed);
    }
}

// ── FileSessionManager ────────────────────────────────────────

pub struct FileSessionManager {
    map: DashMap<i64, Arc<FileDownloadSession>>,
    session_table: Arc<SessionTable>,
    served_bytes: Arc<AtomicU64>,
}

impl FileSessionManager {
    pub fn new(session_table: Arc<SessionTable>, served_bytes: Arc<AtomicU64>) -> Self {
        Self {
            map: DashMap::new(),
            session_table,
            served_bytes,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn get_or_create(
        &self,
        file_id: i64,
        name: &str,
        url: &str,
        total_size: u64,
        prefetch_budget_base: usize,
        user_agent: Option<&str>,
        cached_chunks: HashMap<usize, String>,
        file: File,
    ) -> Arc<FileDownloadSession> {
        self.map
            .entry(file_id)
            .or_insert_with(|| {
                let chunk_count = (total_size as usize).div_ceil(CHUNK_SIZE);
                Arc::new(FileDownloadSession::new(
                    FileDownloadSessionConfig {
                        file_id,
                        name: name.to_string(),
                        url: url.to_string(),
                        total_size,
                        chunk_count,
                        user_agent: user_agent.map(str::to_string),
                        prefetch_budget_base,
                        cached_chunks: StdMutex::new(cached_chunks),
                        file,
                    },
                    FileDownloadSessionDeps {
                        session_table: self.session_table.clone(),
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
        ChunkStoredEvent, FileDownloadSession, FileDownloadSessionConfig, FileDownloadSessionDeps,
        LockExt,
    };
    use crate::config::Config;
    use crate::metadata::File;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex as StdMutex};
    use tempfile::TempDir;
    use tokio::task::JoinHandle;

    use axum::{extract::State as AxumState, http::StatusCode, routing::get, Router};

    fn dummy_file() -> File {
        File {
            id: 1,
            name: "test.bin".to_string(),
            repo: "test/repo".to_string(),
            total_size: crate::service::CHUNK_SIZE as i64,
            created_at: String::new(),
            last_accessed: String::new(),
            source: "hf".to_string(),
            etag: None,
            x_repo_commit: None,
            x_linked_size: None,
            x_linked_etag: None,
            content_type: None,
        }
    }

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
        let _dir = TempDir::new().unwrap();
        let fetched_bytes = Arc::new(AtomicU64::new(0));
        let served_bytes = Arc::new(AtomicU64::new(0));
        let client = reqwest::Client::new();
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkStoredEvent>();
        let backend: Arc<dyn crate::storage::StorageBackend> =
            Arc::new(crate::storage::local::LocalBackend::new(
                _dir.path().join("chunks"),
                crate::storage::Compression::None,
            ));
        let session_table = Arc::new(super::SessionTable::new(
            client,
            backend,
            event_tx,
            fetched_bytes,
            true,
        ));

        let session = Arc::new(FileDownloadSession::new(
            FileDownloadSessionConfig {
                file_id: 1,
                name: "test.bin".to_string(),
                url: "http://localhost/test.bin".to_string(),
                total_size: crate::service::CHUNK_SIZE as u64,
                chunk_count: 1,
                user_agent: None,
                prefetch_budget_base: 8,
                cached_chunks: StdMutex::new(HashMap::new()),
                file: dummy_file(),
            },
            FileDownloadSessionDeps {
                session_table,
                served_bytes,
            },
        ));

        *session.task.lock_or_recover() = Some(tokio::spawn(async {}) as JoinHandle<()>);

        session.clone().run_download_loop().await;

        assert!(session.task.lock_or_recover().is_none());
    }

    #[tokio::test]
    async fn file_session_tracks_prefetch_state() {
        let _dir = TempDir::new().unwrap();
        let fetched_bytes = Arc::new(AtomicU64::new(0));
        let served_bytes = Arc::new(AtomicU64::new(0));
        let client = reqwest::Client::new();
        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkStoredEvent>();
        let backend: Arc<dyn crate::storage::StorageBackend> =
            Arc::new(crate::storage::local::LocalBackend::new(
                _dir.path().join("chunks"),
                crate::storage::Compression::None,
            ));
        let session_table = Arc::new(super::SessionTable::new(
            client,
            backend,
            event_tx,
            fetched_bytes,
            true,
        ));

        let session = FileDownloadSession::new(
            FileDownloadSessionConfig {
                file_id: 1,
                name: "test.bin".to_string(),
                url: "http://localhost/test.bin".to_string(),
                total_size: crate::service::CHUNK_SIZE as u64,
                chunk_count: 8,
                user_agent: None,
                prefetch_budget_base: 8,
                cached_chunks: StdMutex::new(HashMap::new()),
                file: dummy_file(),
            },
            FileDownloadSessionDeps {
                session_table,
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

    #[tokio::test]
    async fn read_cached_chunk_rejects_incomplete_data_even_when_sha256_matches() {
        let _dir = TempDir::new().unwrap();
        let backend: Arc<dyn crate::storage::StorageBackend> =
            Arc::new(crate::storage::local::LocalBackend::new(
                _dir.path().join("chunks"),
                crate::storage::Compression::None,
            ));

        let incomplete = b"";
        let incomplete_sha = crate::chunker::sha256_hex(incomplete);

        backend.put(&incomplete_sha, incomplete).await.unwrap();

        let reader = super::ChunkReader::new_for_test(backend, true);

        let result = reader
            .read_cached_chunk_test(&incomplete_sha, Some(super::CHUNK_SIZE))
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "sha256 matches but data.len()=0 != expected_chunk_size={}. \
             Must return None to trigger re-fetch.",
            super::CHUNK_SIZE
        );
    }

    #[derive(Clone)]
    struct SubscribeTestState {
        chunk_data: Arc<Vec<u8>>,
    }

    async fn serve_full_file(
        AxumState(state): AxumState<SubscribeTestState>,
    ) -> axum::response::Response {
        axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Length", state.chunk_data.len())
            .header("Content-Type", "application/octet-stream")
            .body(axum::body::Body::from(state.chunk_data.to_vec()))
            .unwrap()
    }

    #[tokio::test]
    async fn subscribe_clears_cached_chunks_when_corruption_detected() {
        let _dir = TempDir::new().unwrap();
        let backend: Arc<dyn crate::storage::StorageBackend> =
            Arc::new(crate::storage::local::LocalBackend::new(
                _dir.path().join("chunks"),
                crate::storage::Compression::None,
            ));

        let corrupted = b"";
        let corrupted_sha = crate::chunker::sha256_hex(corrupted);
        backend.put(&corrupted_sha, corrupted).await.unwrap();

        let cached_chunks = Arc::new(StdMutex::new(HashMap::from([(
            0usize,
            corrupted_sha.clone(),
        )])));

        let test_data: Vec<u8> = (0..super::CHUNK_SIZE)
            .map(|i| (i as u8).wrapping_mul(13).wrapping_add(47))
            .collect();
        let state = SubscribeTestState {
            chunk_data: Arc::new(test_data.clone()),
        };

        let app = Router::new()
            .route("/test.bin", get(serve_full_file))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/test.bin");

        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkStoredEvent>();
        let session_table = Arc::new(super::SessionTable::new(
            reqwest::Client::new(),
            backend,
            event_tx,
            Arc::new(AtomicU64::new(0)),
            true,
        ));

        let mut rx = session_table
            .subscribe(
                1,
                0,
                &url,
                0,
                (super::CHUNK_SIZE - 1) as u64,
                super::CHUNK_SIZE as u64,
                1,
                None,
                &cached_chunks,
            )
            .await
            .unwrap();

        {
            let ck = cached_chunks.lock_or_recover();
            assert!(
                !ck.contains_key(&0),
                "subscribe must clear stale cached_chunks entry after corruption detection, \
                 so that fetch_chunk does not compare fresh data against the stale sha256; \
                 found: {:?}",
                ck.get(&0)
            );
        }

        match rx.recv().await {
            Ok(Ok(data)) => {
                assert_eq!(
                    data.len(),
                    super::CHUNK_SIZE,
                    "expected full chunk from upstream after corruption was cleared"
                );
            }
            Ok(Err(e)) => {
                panic!(
                    "chunk download should succeed after clearing stale cached_chunks, but got error: {}",
                    e
                );
            }
            Err(e) => {
                panic!("receiver closed unexpectedly: {e}");
            }
        }
    }

    #[tokio::test]
    async fn fetch_chunk_truncates_full_file_on_200() {
        let _dir = TempDir::new().unwrap();
        let backend: Arc<dyn crate::storage::StorageBackend> =
            Arc::new(crate::storage::local::LocalBackend::new(
                _dir.path().join("chunks"),
                crate::storage::Compression::None,
            ));

        let two_chunks: Vec<u8> = (0..2 * super::CHUNK_SIZE)
            .map(|i| (i as u8).wrapping_mul(13).wrapping_add(47))
            .collect();
        let state = SubscribeTestState {
            chunk_data: Arc::new(two_chunks.clone()),
        };

        let app = Router::new()
            .route("/test.bin", get(serve_full_file))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/test.bin");
        let cached_chunks = Arc::new(StdMutex::new(HashMap::new()));
        let total_size = (2 * super::CHUNK_SIZE) as u64;

        let (event_tx, _event_rx) = tokio::sync::mpsc::unbounded_channel::<ChunkStoredEvent>();
        let session_table = Arc::new(super::SessionTable::new(
            reqwest::Client::new(),
            backend,
            event_tx,
            Arc::new(AtomicU64::new(0)),
            true,
        ));

        let mut rx = session_table
            .subscribe(
                1,
                1,
                &url,
                super::CHUNK_SIZE as u64,
                (2 * super::CHUNK_SIZE - 1) as u64,
                total_size,
                2,
                None,
                &cached_chunks,
            )
            .await
            .unwrap();

        match rx.recv().await {
            Ok(Ok(data)) => {
                assert_eq!(
                    data.len(),
                    super::CHUNK_SIZE,
                    "should return exactly one chunk, not the full {} bytes",
                    two_chunks.len()
                );
                let expected: Vec<u8> =
                    two_chunks[super::CHUNK_SIZE..2 * super::CHUNK_SIZE].to_vec();
                assert_eq!(
                    data.as_ref(),
                    expected.as_slice(),
                    "sliced data must match the second chunk of the full file"
                );
            }
            Ok(Err(e)) => {
                panic!("chunk download should succeed, but got error: {}", e);
            }
            Err(e) => {
                panic!("receiver closed unexpectedly: {e}");
            }
        }
    }
}
