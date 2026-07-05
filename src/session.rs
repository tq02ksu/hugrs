use crate::chunker;
use crate::metadata::File;
use crate::storage::StorageBackend;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

pub const CHUNK_SIZE: usize = crate::service::CHUNK_SIZE;

type ChunkMessage = Result<Arc<Bytes>, Arc<String>>;

type SubscriberEntry = (u64, u64, mpsc::Sender<Result<Bytes, anyhow::Error>>);

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

// ── Actor Messages ────────────────────────────────────────────

/// Messages for the actor-based file session.
pub enum SessionMsg {
    Subscribe {
        reply: mpsc::Sender<Result<Bytes, anyhow::Error>>,
        file: Box<File>,
        range: (u64, u64),
    },
    ChunkReady {
        idx: usize,
        data: Arc<Bytes>,
        sha256: String,
        elapsed_ms: u64,
    },
    ChunkFailed {
        idx: usize,
        error: Arc<String>,
    },
    TickBackpressure,
}

// ── ActorHandle ───────────────────────────────────────────────

/// Handle to communicate with a FileSessionActor.
#[derive(Clone)]
pub struct ActorHandle {
    pub(crate) mailbox: mpsc::UnboundedSender<SessionMsg>,
    pub(crate) file: File,
    pub(crate) total_size: u64,
}

impl ActorHandle {
    pub async fn subscribe(
        &self,
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

        if self
            .mailbox
            .send(SessionMsg::Subscribe {
                reply: tx,
                file: Box::new(self.file.clone()),
                range: (req_start, req_end),
            })
            .is_err()
        {
            anyhow::bail!("session actor has stopped");
        }

        Ok((
            self.file.clone(),
            content_length,
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }
}

// ── FileSessionActor ──────────────────────────────────────────

struct FileSessionActor {
    // Mailbox
    mailbox_rx: mpsc::UnboundedReceiver<SessionMsg>,
    mailbox_tx: mpsc::UnboundedSender<SessionMsg>,
    // Fixed config
    file_id: i64,
    name: String,
    url: String,
    total_size: u64,
    chunk_count: usize,
    user_agent: Option<String>,
    prefetch_budget_base: usize,
    session_table: Arc<SessionTable>,
    served_bytes: Arc<AtomicU64>,
    // Mutable state (owned by actor — no locks)
    subscribers: Vec<SubscriberEntry>,
    completed: HashSet<usize>,
    inflight: HashSet<usize>,
    cached_chunks: Arc<StdMutex<HashMap<usize, String>>>,
    avg_latency_ms: f64,
    backpressure_ratio: f64,
}

impl FileSessionActor {
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        file_id: i64,
        name: String,
        url: String,
        total_size: u64,
        chunk_count: usize,
        user_agent: Option<String>,
        prefetch_budget_base: usize,
        session_table: Arc<SessionTable>,
        served_bytes: Arc<AtomicU64>,
        cached_chunks: HashMap<usize, String>,
        file: File,
    ) -> (ActorHandle, JoinHandle<()>) {
        let (mailbox_tx, mailbox_rx) = mpsc::unbounded_channel::<SessionMsg>();

        let mailbox_tx_clone = mailbox_tx.clone();

        let handle = ActorHandle {
            mailbox: mailbox_tx,
            file,
            total_size,
        };

        let actor = FileSessionActor {
            mailbox_rx,
            mailbox_tx: mailbox_tx_clone,
            file_id,
            name,
            url,
            total_size,
            chunk_count: chunk_count.max(1),
            user_agent,
            prefetch_budget_base,
            session_table,
            served_bytes,
            subscribers: Vec::new(),
            completed: HashSet::new(),
            inflight: HashSet::new(),
            cached_chunks: Arc::new(StdMutex::new(cached_chunks)),
            avg_latency_ms: 1000.0, // initial estimate
            backpressure_ratio: 0.0,
        };

        let join_handle = tokio::spawn(actor.run());

        (handle, join_handle)
    }

    async fn run(mut self) {
        tracing::info!(
            "[f{}] {}: actor started, {} chunks total",
            self.file_id,
            self.name,
            self.chunk_count,
        );

        let session_start = std::time::Instant::now();

        while let Some(msg) = self.mailbox_rx.recv().await {
            match msg {
                SessionMsg::Subscribe { reply, file, range } => {
                    self.on_subscribe(reply, *file, range).await;
                }
                SessionMsg::ChunkReady {
                    idx,
                    data,
                    sha256,
                    elapsed_ms,
                } => {
                    self.on_chunk_ready(idx, data, sha256, elapsed_ms).await;
                }
                SessionMsg::ChunkFailed { idx, error } => {
                    self.on_chunk_failed(idx, error).await;
                    break;
                }
                SessionMsg::TickBackpressure => {
                    self.on_tick_backpressure();
                }
            }

            self.clean_subscribers();
            if self.subscribers.is_empty() && self.inflight.is_empty() {
                break;
            }
        }

        // Drop remaining subscriber channels cleanly — receiver sees EOF
        self.subscribers.clear();

        tracing::info!(
            "[f{}] {}: actor finished in {}ms",
            self.file_id,
            self.name,
            session_start.elapsed().as_millis(),
        );
    }

    async fn on_subscribe(
        &mut self,
        reply: mpsc::Sender<Result<Bytes, anyhow::Error>>,
        _file: File,
        range: (u64, u64),
    ) {
        self.subscribers.push((range.0, range.1, reply));
        self.schedule_next_and_prefetch();
    }

    async fn on_chunk_ready(
        &mut self,
        idx: usize,
        data: Arc<Bytes>,
        sha256: String,
        elapsed_ms: u64,
    ) {
        // Update moving average latency
        self.avg_latency_ms = self.avg_latency_ms * 0.7 + elapsed_ms as f64 * 0.3;

        self.completed.insert(idx);
        self.inflight.remove(&idx);

        // Record the sha256 in cached_chunks
        self.cached_chunks.lock_or_recover().insert(idx, sha256);

        // Forward to all relevant subscribers
        let chunk_start = idx as u64 * CHUNK_SIZE as u64;
        self.forward_chunk(chunk_start, &data).await;

        self.served_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        if elapsed_ms > 5_000 {
            tracing::warn!(
                "[f{}] {} chunk {}/{}: SLOW — {} bytes in {}ms",
                self.file_id,
                self.name,
                idx + 1,
                self.chunk_count,
                data.len(),
                elapsed_ms,
            );
        }

        tracing::info!(
            "[f{}] {} chunk {}/{}: {} bytes in {}ms",
            self.file_id,
            self.name,
            idx + 1,
            self.chunk_count,
            data.len(),
            elapsed_ms,
        );

        self.schedule_next_and_prefetch();
    }

    async fn on_chunk_failed(&mut self, idx: usize, error: Arc<String>) {
        tracing::warn!("[f{}] chunk {} failed: {}", self.file_id, idx, error);
        self.inflight.remove(&idx);
        self.forward_error(anyhow::anyhow!("{error}")).await;
    }

    fn on_tick_backpressure(&mut self) {
        let total = self.subscribers.len();
        if total == 0 {
            self.backpressure_ratio = 0.0;
            return;
        }
        let mut blocked = 0usize;
        for (_, _, tx) in &self.subscribers {
            if tx.capacity() == 0 {
                blocked += 1;
            }
        }
        self.backpressure_ratio = blocked as f64 / total as f64;
    }

    fn schedule_next_and_prefetch(&mut self) {
        if let Some(idx) = self.select_next_chunk_priority() {
            self.inflight.insert(idx);
            let start = (idx * CHUNK_SIZE) as u64;
            let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, self.total_size - 1);

            // Spawn a chunk watcher that bridges SessionTable broadcast → mailbox
            let mailbox = self.mailbox_tx.clone();
            let session_table = self.session_table.clone();
            let url = self.url.clone();
            let user_agent = self.user_agent.clone();
            let cached_chunks = self.cached_chunks.clone();
            let fid = self.file_id;
            let cidx = idx as i64;

            tokio::spawn(async move {
                let chunk_start = std::time::Instant::now();
                let mut rx = match session_table
                    .subscribe(
                        fid,
                        cidx,
                        &url,
                        start,
                        end,
                        0,
                        0,
                        user_agent.as_deref(),
                        &cached_chunks,
                    )
                    .await
                {
                    Ok(rx) => rx,
                    Err(e) => {
                        let _ = mailbox.send(SessionMsg::ChunkFailed {
                            idx,
                            error: Arc::new(e.to_string()),
                        });
                        return;
                    }
                };

                match rx.recv().await {
                    Ok(Ok(data)) => {
                        // Calculate sha256 from data for cached_chunks update
                        let data_clone = data.clone();
                        let sha256 =
                            tokio::task::spawn_blocking(move || chunker::sha256_hex(&data_clone))
                                .await
                                .unwrap_or_else(|_| "unknown".to_string());

                        let elapsed_ms: u64 = chunk_start.elapsed().as_millis() as u64;
                        let _ = mailbox.send(SessionMsg::ChunkReady {
                            idx,
                            data,
                            sha256,
                            elapsed_ms,
                        });
                    }
                    Ok(Err(err)) => {
                        let _ = mailbox.send(SessionMsg::ChunkFailed { idx, error: err });
                    }
                    Err(e) => {
                        let _ = mailbox.send(SessionMsg::ChunkFailed {
                            idx,
                            error: Arc::new(format!("chunk receiver error: {e}")),
                        });
                    }
                }
            });
        }

        // Schedule prefetches
        self.schedule_prefetches();

        // No chunks to schedule or in-flight → no more work will ever arrive.
        // Close the mailbox so the run loop can exit rather than hang on recv().
        if self.inflight.is_empty() {
            self.mailbox_rx.close();
        }
    }

    fn schedule_prefetches(&mut self) {
        let active_cursors = self.compute_active_cursors();
        if active_cursors.is_empty() {
            return;
        }

        // Dynamic budget: adjust based on latency and backpressure
        let budget = self.compute_prefetch_budget(active_cursors.len());
        if budget == 0 {
            return;
        }

        let per_cursor = (budget / active_cursors.len().max(1)).max(1);

        for cursor in &active_cursors {
            for j in (*cursor + 1)..(*cursor + 1 + per_cursor).min(self.chunk_count) {
                if self.completed.contains(&j) || self.inflight.contains(&j) {
                    continue;
                }
                self.inflight.insert(j);

                let mailbox = self.mailbox_tx.clone();
                let session_table = self.session_table.clone();
                let url = self.url.clone();
                let user_agent = self.user_agent.clone();
                let cached_chunks = self.cached_chunks.clone();
                let fid = self.file_id;
                let cidx = j as i64;
                let pstart = (j * CHUNK_SIZE) as u64;
                let pend = std::cmp::min(pstart + CHUNK_SIZE as u64 - 1, self.total_size - 1);
                let total_size = self.total_size;
                let chunk_count = self.chunk_count;

                tokio::spawn(async move {
                    let chunk_start = std::time::Instant::now();
                    let mut rx = match session_table
                        .subscribe(
                            fid,
                            cidx,
                            &url,
                            pstart,
                            pend,
                            total_size,
                            chunk_count,
                            user_agent.as_deref(),
                            &cached_chunks,
                        )
                        .await
                    {
                        Ok(rx) => rx,
                        Err(e) => {
                            let _ = mailbox.send(SessionMsg::ChunkFailed {
                                idx: j,
                                error: Arc::new(e.to_string()),
                            });
                            return;
                        }
                    };

                    match rx.recv().await {
                        Ok(Ok(data)) => {
                            let data_clone = data.clone();
                            let sha256 = tokio::task::spawn_blocking(move || {
                                chunker::sha256_hex(&data_clone)
                            })
                            .await
                            .unwrap_or_else(|_| "unknown".to_string());

                            let elapsed_ms: u64 = chunk_start.elapsed().as_millis() as u64;
                            let _ = mailbox.send(SessionMsg::ChunkReady {
                                idx: j,
                                data,
                                sha256,
                                elapsed_ms,
                            });
                        }
                        Ok(Err(err)) => {
                            let _ = mailbox.send(SessionMsg::ChunkFailed { idx: j, error: err });
                        }
                        Err(e) => {
                            let _ = mailbox.send(SessionMsg::ChunkFailed {
                                idx: j,
                                error: Arc::new(format!("chunk receiver error: {e}")),
                            });
                        }
                    }
                });
            }
        }
    }

    fn compute_prefetch_budget(&self, active_cursor_count: usize) -> usize {
        let base = self.prefetch_budget_base;

        // Dynamic scaling based on observed latency
        let latency_factor = if self.avg_latency_ms < 500.0 {
            1.5
        } else if self.avg_latency_ms < 5000.0 {
            1.0
        } else {
            0.5
        };

        // Backpressure scaling: if more than half the subscriber channels are full, reduce
        let bp_factor = if self.backpressure_ratio > 0.5 {
            0.25
        } else if self.backpressure_ratio > 0.25 {
            0.5
        } else {
            1.0
        };

        let adjusted = (base as f64 * latency_factor * bp_factor).round() as usize;
        prefetch_budget(adjusted.max(1), active_cursor_count)
    }

    fn compute_active_cursors(&self) -> Vec<usize> {
        let mut cursors = Vec::new();
        let max_idx = self.chunk_count.saturating_sub(1);
        let chunk_sz = CHUNK_SIZE as u64;

        for (start, end, _) in &self.subscribers {
            let first = (*start / chunk_sz) as usize;
            let last = ((*end / chunk_sz) as usize).min(max_idx);
            if let Some(next) = (first..=last)
                .find(|idx| !self.completed.contains(idx) && !self.inflight.contains(idx))
            {
                cursors.push(next);
            }
        }

        cursors.sort_unstable();
        cursors.dedup();
        cursors
    }

    fn select_next_chunk_priority(&self) -> Option<usize> {
        let chunk_sz = CHUNK_SIZE as u64;
        let mut best: Option<(usize, f64)> = None;

        for (start, end, _) in &self.subscribers {
            let first_chunk = (*start / chunk_sz) as usize;
            let last_chunk = ((*end / chunk_sz) as usize).min(self.chunk_count.saturating_sub(1));
            let range_len = (*end - *start).max(1) as f64;

            for idx in first_chunk..=last_chunk {
                if self.completed.contains(&idx) || self.inflight.contains(&idx) {
                    continue;
                }
                // Urgency: how far is this chunk from the subscriber's start, relative to their range
                let chunk_byte_start = idx as u64 * chunk_sz;
                let distance = if chunk_byte_start >= *start {
                    (chunk_byte_start - *start) as f64
                } else {
                    0.0
                };
                let urgency = distance / range_len;
                let is_better = match best {
                    Some((_, best_urgency)) => urgency < best_urgency,
                    None => true,
                };
                if is_better {
                    best = Some((idx, urgency));
                }
            }
        }

        best.map(|(idx, _)| idx)
    }

    async fn forward_chunk(&self, chunk_start: u64, data: &Arc<Bytes>) {
        let chunk_end = chunk_start + data.len() as u64 - 1;
        for (s, e, tx) in &self.subscribers {
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
        for (_, _, tx) in &self.subscribers {
            let _ = tx.send(Err(anyhow::anyhow!(message.clone()))).await;
        }
    }

    fn clean_subscribers(&mut self) {
        self.subscribers.retain(|(_, _, tx)| !tx.is_closed());
    }
}

// ── FileSessionManager ────────────────────────────────────────

pub struct FileSessionManager {
    map: DashMap<i64, (ActorHandle, JoinHandle<()>)>,
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
    ) -> ActorHandle {
        // Replace stale entry whose actor has exited (mailbox closed)
        if let Some(entry) = self.map.get(&file_id) {
            if entry.0.mailbox.is_closed() {
                drop(entry);
                self.map.remove(&file_id);
            }
        }

        self.map
            .entry(file_id)
            .or_insert_with(|| {
                let chunk_count = (total_size as usize).div_ceil(CHUNK_SIZE);
                FileSessionActor::spawn(
                    file_id,
                    name.to_string(),
                    url.to_string(),
                    total_size,
                    chunk_count,
                    user_agent.map(str::to_string),
                    prefetch_budget_base,
                    self.session_table.clone(),
                    self.served_bytes.clone(),
                    cached_chunks,
                    file,
                )
            })
            .0
            .clone()
    }

    pub fn remove(&self, file_id: i64) {
        self.map.remove(&file_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        prefetch_budget, ChunkStoredEvent, FileSessionActor, LockExt, SessionTable, CHUNK_SIZE,
    };
    use crate::storage::StorageBackend;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex as StdMutex};
    use tempfile::TempDir;

    use axum::{extract::State as AxumState, http::StatusCode, routing::get, Router};
    use bytes::Bytes;

    /// Helper: build a minimal FileSessionActor for unit-testing private methods.
    fn test_actor() -> FileSessionActor {
        let _dir = TempDir::new().unwrap();
        let (mailbox_tx, mailbox_rx) = tokio::sync::mpsc::unbounded_channel();
        let (event_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let backend: Arc<dyn StorageBackend> = Arc::new(crate::storage::local::LocalBackend::new(
            _dir.path().join("chunks"),
            crate::storage::Compression::None,
        ));
        let session_table = Arc::new(SessionTable::new(
            reqwest::Client::new(),
            backend,
            event_tx,
            Arc::new(AtomicU64::new(0)),
            true,
        ));
        FileSessionActor {
            mailbox_rx,
            mailbox_tx,
            file_id: 1,
            name: "test".into(),
            url: "http://localhost/test".into(),
            total_size: 4 * CHUNK_SIZE as u64,
            chunk_count: 4,
            user_agent: None,
            prefetch_budget_base: 8,
            session_table,
            served_bytes: Arc::new(AtomicU64::new(0)),
            subscribers: Vec::new(),
            completed: HashSet::new(),
            inflight: HashSet::new(),
            cached_chunks: Arc::new(StdMutex::new(HashMap::new())),
            avg_latency_ms: 1000.0,
            backpressure_ratio: 0.0,
        }
    }

    // ── prefetch_budget (free function) ───────────────────────

    #[test]
    fn prefetch_budget_scales_with_active_cursors() {
        assert_eq!(prefetch_budget(8, 0), 8);
        assert_eq!(prefetch_budget(8, 1), 8);
        assert_eq!(prefetch_budget(8, 2), 4);
        assert_eq!(prefetch_budget(8, 3), 2);
    }

    // ── compute_prefetch_budget (actor method) ────────────────

    #[test]
    fn compute_prefetch_budget_scales_with_latency() {
        let mut actor = test_actor();
        actor.prefetch_budget_base = 8;
        actor.backpressure_ratio = 0.0;

        // Medium latency (500–5000ms) → 1.0× factor, 1 cursor → base
        actor.avg_latency_ms = 1000.0;
        assert_eq!(actor.compute_prefetch_budget(1), 8);

        // Fast latency (<500ms) → 1.5× → 8 * 1.5 = 12
        actor.avg_latency_ms = 200.0;
        assert_eq!(actor.compute_prefetch_budget(1), 12);

        // Slow latency (>5000ms) → 0.5× → 8 * 0.5 = 4
        actor.avg_latency_ms = 10_000.0;
        assert_eq!(actor.compute_prefetch_budget(1), 4);
    }

    #[test]
    fn compute_prefetch_budget_respects_backpressure() {
        let mut actor = test_actor();
        actor.prefetch_budget_base = 8;
        actor.avg_latency_ms = 1000.0; // neutral latency

        // No backpressure → 1.0×
        actor.backpressure_ratio = 0.0;
        assert_eq!(actor.compute_prefetch_budget(1), 8);

        // Medium backpressure (0.25–0.5) → 0.5× → 8 / 2 = 4
        actor.backpressure_ratio = 0.3;
        assert_eq!(actor.compute_prefetch_budget(1), 4);

        // High backpressure (>0.5) → 0.25× → (8 * 0.25) = 2
        actor.backpressure_ratio = 0.6;
        assert_eq!(actor.compute_prefetch_budget(1), 2);
    }

    #[test]
    fn compute_prefetch_budget_divides_among_cursors() {
        let mut actor = test_actor();
        actor.prefetch_budget_base = 8;
        actor.avg_latency_ms = 1000.0;
        actor.backpressure_ratio = 0.0;

        // 2 cursors → budget = base/2 = 4
        assert_eq!(actor.compute_prefetch_budget(2), 4);

        // 3+ cursors → budget = base/4 = 2
        assert_eq!(actor.compute_prefetch_budget(3), 2);
        assert_eq!(actor.compute_prefetch_budget(4), 2);
    }

    // ── select_next_chunk_priority ────────────────────────────

    #[test]
    fn select_next_chunk_priority_picks_closest_to_subscriber_start() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);

        // Subscriber wants bytes 1*CHUNK_SIZE .. 4*CHUNK_SIZE-1
        actor
            .subscribers
            .push((CHUNK_SIZE as u64, 4 * CHUNK_SIZE as u64 - 1, tx));

        // Urgency: chunk 1 (byte offset = CHUNK_SIZE) is closest to subscriber start
        assert_eq!(actor.select_next_chunk_priority(), Some(1));

        // Mark chunk 1 done → chunk 2 becomes most urgent
        actor.completed.insert(1);
        assert_eq!(actor.select_next_chunk_priority(), Some(2));
    }

    #[test]
    fn select_next_chunk_priority_returns_none_when_all_done() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 9 * CHUNK_SIZE as u64 - 1, tx));

        // All chunks completed
        for i in 0..10 {
            actor.completed.insert(i);
        }
        assert_eq!(actor.select_next_chunk_priority(), None);
    }

    #[test]
    fn select_next_chunk_priority_skips_inflight_chunks() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 9 * CHUNK_SIZE as u64 - 1, tx));

        // Chunks 0 and 1 are in-flight, not yet completed
        actor.inflight.insert(0);
        actor.inflight.insert(1);

        // Should pick the first available chunk: 2
        assert_eq!(actor.select_next_chunk_priority(), Some(2));
    }

    #[test]
    fn select_next_chunk_priority_handles_two_subscribers() {
        let mut actor = test_actor();
        actor.chunk_count = 20;
        let (tx1, _rx1) = tokio::sync::mpsc::channel(32);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(32);

        // Subscriber A: bytes 0..4*CHUNK_SIZE-1
        actor.subscribers.push((0, 4 * CHUNK_SIZE as u64 - 1, tx1));
        // Subscriber B: bytes 10*CHUNK_SIZE..14*CHUNK_SIZE-1
        actor
            .subscribers
            .push((10 * CHUNK_SIZE as u64, 14 * CHUNK_SIZE as u64 - 1, tx2));

        // Most urgent: chunk 0 (closest to subscriber A's start, distance=0)
        assert_eq!(actor.select_next_chunk_priority(), Some(0));

        // Complete chunks 0..4 for subscriber A
        for i in 0..5 {
            actor.completed.insert(i);
        }
        // Now most urgent: chunk 10 (closest to subscriber B's start)
        assert_eq!(actor.select_next_chunk_priority(), Some(10));
    }

    // ── compute_active_cursors ────────────────────────────────

    #[test]
    fn compute_active_cursors_finds_next_missing_for_each_subscriber() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx1, _rx1) = tokio::sync::mpsc::channel(32);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(32);

        actor.subscribers.push((0, 4 * CHUNK_SIZE as u64 - 1, tx1));
        actor
            .subscribers
            .push((6 * CHUNK_SIZE as u64, 9 * CHUNK_SIZE as u64 - 1, tx2));

        // No chunks done → cursors at 0 and 6
        let cursors = actor.compute_active_cursors();
        assert_eq!(cursors, vec![0, 6]);

        // Complete chunk 0 for sub A → its cursor advances to 1
        actor.completed.insert(0);
        let cursors = actor.compute_active_cursors();
        assert_eq!(cursors, vec![1, 6]);
    }

    #[test]
    fn compute_active_cursors_excludes_inflight_chunks() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 9 * CHUNK_SIZE as u64 - 1, tx));

        // Chunk 0 is in-flight → cursor should be 1
        actor.inflight.insert(0);
        let cursors = actor.compute_active_cursors();
        assert_eq!(cursors, vec![1]);
    }

    // ── on_tick_backpressure ──────────────────────────────────

    #[tokio::test]
    async fn on_tick_backpressure_detects_blocked_channels() {
        let mut actor = test_actor();
        // Use capacity-1 channels so a single send fills them
        let (tx1, _rx1) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(1);
        let (tx2, _rx2) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(1);

        actor.subscribers.push((0, 100, tx1.clone()));
        actor.subscribers.push((0, 100, tx2.clone()));

        // Both channels start empty → no backpressure
        actor.on_tick_backpressure();
        assert_eq!(actor.backpressure_ratio, 0.0);

        // Fill both channels (capacity-1, one send is enough)
        let _ = tx1.send(Ok(Bytes::from("a"))).await;
        let _ = tx2.send(Ok(Bytes::from("b"))).await;

        actor.on_tick_backpressure();
        assert_eq!(actor.backpressure_ratio, 1.0);

        // Drop one receiver → its sender becomes closed but not "full"
        drop(_rx1);
        // After dropping, the sender's capacity becomes 0 anyway (broken pipe).
        // Check that at least ratio is non-zero.
        actor.on_tick_backpressure();
        assert!(actor.backpressure_ratio > 0.0);
    }

    // ── forward_chunk ─────────────────────────────────────────

    #[tokio::test]
    async fn forward_chunk_sends_relevant_slice_to_each_subscriber() {
        let mut actor = test_actor();
        let (tx1, mut rx1) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(32);
        let (tx2, mut rx2) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        // Sub A: wants bytes 100..299  (overlaps with chunk starting at 0*CHUNK_SIZE)
        // Sub B: wants bytes 500..799  (overlaps with chunk starting at 0*CHUNK_SIZE)
        actor.subscribers.push((100, 299, tx1));
        actor.subscribers.push((500, 799, tx2));

        let chunk_data = Arc::new(Bytes::from(vec![0u8; CHUNK_SIZE])); // 4MB chunk
        let chunk_start = 0u64; // chunk 0

        actor.forward_chunk(chunk_start, &chunk_data).await;

        // Sub A should get bytes 100..299 (200 bytes)
        let received1 = rx1.try_recv().unwrap().unwrap();
        assert_eq!(received1.len(), 200);

        // Sub B should get bytes 500..799 (300 bytes)
        let received2 = rx2.try_recv().unwrap().unwrap();
        assert_eq!(received2.len(), 300);
    }

    #[tokio::test]
    async fn forward_chunk_skips_non_overlapping_subscribers() {
        let mut actor = test_actor();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        // Subscriber wants bytes in chunk 5 (offset 5*CHUNK_SIZE)
        actor
            .subscribers
            .push((5 * CHUNK_SIZE as u64, 6 * CHUNK_SIZE as u64 - 1, tx));

        // Forward chunk 0 (offset 0) — does NOT overlap with subscriber
        let chunk_data = Arc::new(Bytes::from(vec![0u8; CHUNK_SIZE]));
        actor.forward_chunk(0, &chunk_data).await;

        // Subscriber should NOT receive anything for this chunk
        assert!(rx.try_recv().is_err());
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
