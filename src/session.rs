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
    chunk_retries: u32,
    write_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
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
        chunk_retries: u32,
    ) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            reader: Arc::new(ChunkReader {
                http_client,
                backend,
                event_tx,
                fetched_bytes,
                verify_sha256,
                chunk_retries,
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
        let max_retries = self.chunk_retries.max(1);

        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let mut req = self.http_client.get(&url).header("Range", &range_header);
            if let Some(ref ua) = user_agent {
                req = req.header("User-Agent", ua);
            }

            let response = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    if attempt >= max_retries {
                        return Err(anyhow::anyhow!(
                            "chunk {chunk_idx} request error after {attempt} attempts: {e}"
                        ));
                    }
                    tracing::warn!(
                        "chunk {chunk_idx} request attempt {attempt} failed: {e}, retrying..."
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                        .await;
                    continue;
                }
            };

            let status = response.status();

            if status.is_server_error() {
                if attempt >= max_retries {
                    return Err(anyhow::anyhow!(
                        "chunk {chunk_idx} server error {status} after {attempt} attempts"
                    ));
                }
                tracing::warn!(
                    "chunk {chunk_idx} server returned {status} on attempt {attempt}, retrying..."
                );
                tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
                continue;
            }

            let full_data = match response
                .error_for_status()
                .map_err(|e| anyhow::anyhow!("chunk {chunk_idx} request error: {e}"))?
                .bytes()
                .await
            {
                Ok(d) => d,
                Err(e) => {
                    if attempt >= max_retries {
                        return Err(anyhow::anyhow!(
                            "chunk {chunk_idx} download error after {attempt} attempts: {e}"
                        ));
                    }
                    tracing::warn!(
                        "chunk {chunk_idx} download attempt {attempt} failed: {e}, retrying..."
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64))
                        .await;
                    continue;
                }
            };

            let expected_len = (end - start + 1) as usize;
            let data = if status == reqwest::StatusCode::OK && full_data.len() > expected_len {
                let offset = start as usize;
                let slice_end = (end + 1) as usize;
                let slice_end = slice_end.min(full_data.len());
                full_data.slice(offset..slice_end)
            } else if full_data.len() != expected_len {
                if attempt >= max_retries {
                    anyhow::bail!(
                        "chunk {chunk_idx} download incomplete after \
                         {attempt} attempts: expected {expected_len} bytes, \
                         got {}",
                        full_data.len()
                    );
                }
                tracing::warn!(
                    "chunk {chunk_idx} download attempt {attempt} incomplete: \
                     expected {expected_len} bytes, got {}, retrying...",
                    full_data.len()
                );
                tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
                continue;
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

            return Ok(data);
        }
    }
}

// ── Actor Messages ────────────────────────────────────────────

/// Messages for the actor-based file session.
pub enum SessionMessage {
    Subscribe {
        reply: mpsc::Sender<Result<Bytes, anyhow::Error>>,
        file: Box<File>,
        range: (u64, u64),
    },
    TickBackpressure,
}

// ── ActorHandle ───────────────────────────────────────────────

/// Handle to communicate with a FileSessionActor.
#[derive(Clone)]
pub struct ActorHandle {
    pub(crate) mailbox: mpsc::UnboundedSender<SessionMessage>,
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
            .send(SessionMessage::Subscribe {
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
    mailbox_rx: mpsc::UnboundedReceiver<SessionMessage>,
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
    prefetch_inflight: HashSet<usize>,
    cached_chunks: Arc<StdMutex<HashMap<usize, String>>>,
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
        let (mailbox_tx, mailbox_rx) = mpsc::unbounded_channel::<SessionMessage>();

        let handle = ActorHandle {
            mailbox: mailbox_tx,
            file,
            total_size,
        };

        let actor = FileSessionActor {
            mailbox_rx,
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
            prefetch_inflight: HashSet::new(),
            cached_chunks: Arc::new(StdMutex::new(cached_chunks)),
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

        loop {
            // Drain pending subscribe messages so we always have up-to-date
            // subscriber list before deciding what to drive.
            while let Ok(msg) = self.mailbox_rx.try_recv() {
                match msg {
                    SessionMessage::Subscribe { reply, file, range } => {
                        self.subscribers.push((range.0, range.1, reply));
                        let _ = file;
                    }
                    SessionMessage::TickBackpressure => {}
                }
            }

            self.clean_subscribers();
            self.cleanup_prefetches();

            if self.subscribers.is_empty() && self.prefetch_inflight.is_empty() {
                break;
            }

            if let Some(idx) = self.select_next_chunk() {
                // Promote from prefetch-inflight to active (like untrack_prefetch).
                self.prefetch_inflight.remove(&idx);

                if self.download_and_forward(idx).await.is_err() {
                    break;
                }
                self.schedule_prefetches();
            } else {
                // All chunks in every subscriber's range are already completed.
                self.subscribers.clear();
                break;
            }
        }

        self.subscribers.clear();

        tracing::info!(
            "[f{}] {}: actor finished in {}ms, all senders dropped",
            self.file_id,
            self.name,
            session_start.elapsed().as_millis(),
        );
    }

    /// Download one chunk sequentially (await inside the actor).
    /// On success the chunk is forwarded to all interested subscribers.
    async fn download_and_forward(&mut self, idx: usize) -> Result<(), anyhow::Error> {
        let start = (idx * CHUNK_SIZE) as u64;
        let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, self.total_size - 1);

        let chunk_start = std::time::Instant::now();

        let mut rx = self
            .session_table
            .subscribe(
                self.file_id,
                idx as i64,
                &self.url,
                start,
                end,
                self.total_size,
                self.chunk_count,
                self.user_agent.as_deref(),
                &self.cached_chunks,
            )
            .await
            .map_err(|e| anyhow::anyhow!("chunk {idx} subscribe failed: {e}"))?;

        match rx.recv().await {
            Ok(Ok(data)) => {
                let elapsed_ms = chunk_start.elapsed().as_millis();

                self.forward_chunk(start, &data).await;
                self.served_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                self.completed.insert(idx);

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

                Ok(())
            }
            Ok(Err(err)) => {
                let msg = err.as_ref().to_string();
                self.forward_error(anyhow::anyhow!(msg)).await;
                Err(anyhow::anyhow!("chunk {idx} download error: {err}"))
            }
            Err(broadcast::error::RecvError::Closed) => {
                let msg = format!("chunk {idx} download closed unexpectedly");
                self.forward_error(anyhow::anyhow!("{msg}")).await;
                Err(anyhow::anyhow!(msg))
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                let msg = format!("chunk {idx} receiver lagged by {n} messages");
                self.forward_error(anyhow::anyhow!("{msg}")).await;
                Err(anyhow::anyhow!(msg))
            }
        }
    }

    /// Fire-and-forget prefetches: subscribe to upstream downloads
    /// without consuming the result. The Downloaded data reaches storage
    /// via the ChunkStoredEvent pathway.
    fn schedule_prefetches(&mut self) {
        let cursors = self.compute_active_cursors();
        if cursors.is_empty() {
            return;
        }

        let budget = prefetch_budget(self.prefetch_budget_base, cursors.len());
        let per_cursor = (budget / cursors.len().max(1)).max(1);

        for cursor in cursors {
            for j in (cursor + 1)..(cursor + 1 + per_cursor).min(self.chunk_count) {
                if self.completed.contains(&j) || self.prefetch_inflight.contains(&j) || {
                    let cc = self.cached_chunks.lock_or_recover();
                    cc.contains_key(&j)
                } {
                    continue;
                }
                self.prefetch_inflight.insert(j);

                let pstart = (j * CHUNK_SIZE) as u64;
                let pend = std::cmp::min(pstart + CHUNK_SIZE as u64 - 1, self.total_size - 1);

                let (session_table, url, user_agent, cached_chunks) = (
                    self.session_table.clone(),
                    self.url.clone(),
                    self.user_agent.clone(),
                    self.cached_chunks.clone(),
                );
                let (fid, cidx, total_size, chunk_count) =
                    (self.file_id, j as i64, self.total_size, self.chunk_count);

                tokio::spawn(async move {
                    let _ = session_table
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
                        .await;
                });
            }
        }
    }

    fn compute_active_cursors(&self) -> Vec<usize> {
        let mut cursors = Vec::new();
        let max_idx = self.chunk_count.saturating_sub(1);
        let chunk_sz = CHUNK_SIZE as u64;

        for (start, end, _) in &self.subscribers {
            let first = (*start / chunk_sz) as usize;
            let last = ((*end / chunk_sz) as usize).min(max_idx);
            if let Some(next) = (first..=last)
                .find(|idx| !self.completed.contains(idx) && !self.prefetch_inflight.contains(idx))
            {
                cursors.push(next);
            }
        }

        cursors.sort_unstable();
        cursors.dedup();
        cursors
    }

    fn select_next_chunk(&self) -> Option<usize> {
        let chunk_sz = CHUNK_SIZE as u64;
        let mut best: Option<(usize, f64)> = None;

        for (start, end, _) in &self.subscribers {
            let first_chunk = (*start / chunk_sz) as usize;
            let last_chunk = ((*end / chunk_sz) as usize).min(self.chunk_count.saturating_sub(1));
            let range_len = (*end - *start).max(1) as f64;

            for idx in first_chunk..=last_chunk {
                if self.completed.contains(&idx) {
                    continue;
                }
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

    fn cleanup_prefetches(&mut self) {
        let cached = self.cached_chunks.lock_or_recover();
        self.prefetch_inflight
            .retain(|idx| !self.completed.contains(idx) && !cached.contains_key(idx));
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
        // Fast-path: existing actor is still alive.
        if let Some(entry) = self.map.get(&file_id) {
            if !entry.0.mailbox.is_closed() {
                return entry.0.clone();
            }
        }

        // Actor has exited or was never created — spawn new and replace.
        let chunk_count = (total_size as usize).div_ceil(CHUNK_SIZE);
        let (handle, join) = FileSessionActor::spawn(
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
        );
        self.map.insert(file_id, (handle.clone(), join));
        handle
    }

    pub fn remove(&self, file_id: i64) {
        self.map.remove(&file_id);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::{prefetch_budget, FileSessionActor, LockExt, SessionTable, CHUNK_SIZE};
    use crate::storage::StorageBackend;
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex as StdMutex};
    use tempfile::TempDir;

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
            3,
        ));
        // stash mailbox_tx so the channel stays open
        std::mem::forget(mailbox_tx);
        FileSessionActor {
            mailbox_rx,
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
            prefetch_inflight: HashSet::new(),
            cached_chunks: Arc::new(StdMutex::new(HashMap::new())),
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

    // ── select_next_chunk ─────────────────────────────────────

    #[test]
    fn select_next_chunk_picks_closest_to_subscriber_start() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);

        actor
            .subscribers
            .push((CHUNK_SIZE as u64, 4 * CHUNK_SIZE as u64 - 1, tx));

        assert_eq!(actor.select_next_chunk(), Some(1));

        actor.completed.insert(1);
        assert_eq!(actor.select_next_chunk(), Some(2));
    }

    #[test]
    fn select_next_chunk_returns_none_when_all_done() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 9 * CHUNK_SIZE as u64 - 1, tx));

        for i in 0..10 {
            actor.completed.insert(i);
        }
        assert_eq!(actor.select_next_chunk(), None);
    }

    #[test]
    fn select_next_chunk_includes_prefetch_inflight() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 9 * CHUNK_SIZE as u64 - 1, tx));

        // Chunks 0 and 1 are being prefetched — select_next_chunk still picks them
        actor.prefetch_inflight.insert(0);
        actor.prefetch_inflight.insert(1);

        // Chunk 0 is closest to subscriber start, should be selected regardless of prefetch
        assert_eq!(actor.select_next_chunk(), Some(0));
    }

    #[test]
    fn select_next_chunk_handles_two_subscribers() {
        let mut actor = test_actor();
        actor.chunk_count = 20;
        let (tx1, _rx1) = tokio::sync::mpsc::channel(32);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(32);

        actor.subscribers.push((0, 4 * CHUNK_SIZE as u64 - 1, tx1));
        actor
            .subscribers
            .push((10 * CHUNK_SIZE as u64, 14 * CHUNK_SIZE as u64 - 1, tx2));

        assert_eq!(actor.select_next_chunk(), Some(0));

        for i in 0..5 {
            actor.completed.insert(i);
        }
        assert_eq!(actor.select_next_chunk(), Some(10));
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

        let cursors = actor.compute_active_cursors();
        assert_eq!(cursors, vec![0, 6]);

        actor.completed.insert(0);
        let cursors = actor.compute_active_cursors();
        assert_eq!(cursors, vec![1, 6]);
    }

    #[test]
    fn compute_active_cursors_excludes_prefetch_inflight() {
        let mut actor = test_actor();
        actor.chunk_count = 10;
        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 9 * CHUNK_SIZE as u64 - 1, tx));

        actor.prefetch_inflight.insert(0);
        let cursors = actor.compute_active_cursors();
        assert_eq!(cursors, vec![1]);
    }

    // ── schedule_prefetches ────────────────────────────────────

    #[tokio::test]
    async fn schedule_prefetches_skips_already_cached_chunks() {
        let mut actor = test_actor();
        actor.chunk_count = 20;

        // Seed cached_chunks — simulate chunks 1,2,3 already downloaded
        {
            let mut cc = actor.cached_chunks.lock_or_recover();
            cc.insert(1, "abc123".into());
            cc.insert(2, "def456".into());
            cc.insert(3, "ghi789".into());
        }

        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 19 * CHUNK_SIZE as u64 - 1, tx));

        actor.schedule_prefetches();

        // Cached chunks must NOT be scheduled for prefetch
        assert!(!actor.prefetch_inflight.contains(&1));
        assert!(!actor.prefetch_inflight.contains(&2));
        assert!(!actor.prefetch_inflight.contains(&3));

        // Non-cached chunks after the cursor must still be scheduled
        assert!(actor.prefetch_inflight.contains(&4));
        assert!(actor.prefetch_inflight.contains(&5));
    }

    #[tokio::test]
    async fn schedule_prefetches_all_cached_is_noop() {
        let mut actor = test_actor();
        actor.chunk_count = 10;

        // All chunks are cached
        {
            let mut cc = actor.cached_chunks.lock_or_recover();
            for i in 0..10 {
                cc.insert(i, format!("sha{i}"));
            }
        }

        let (tx, _rx) = tokio::sync::mpsc::channel(32);
        actor.subscribers.push((0, 9 * CHUNK_SIZE as u64 - 1, tx));

        actor.schedule_prefetches();

        assert!(
            actor.prefetch_inflight.is_empty(),
            "no chunks should be prefetched when all are cached"
        );
    }

    // ── forward_chunk ─────────────────────────────────────────

    #[tokio::test]
    async fn forward_chunk_sends_relevant_slice_to_each_subscriber() {
        let mut actor = test_actor();
        let (tx1, mut rx1) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(32);
        let (tx2, mut rx2) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        actor.subscribers.push((100, 299, tx1));
        actor.subscribers.push((500, 799, tx2));

        let chunk_data = Arc::new(Bytes::from(vec![0u8; CHUNK_SIZE]));
        actor.forward_chunk(0, &chunk_data).await;

        let received1 = rx1.try_recv().unwrap().unwrap();
        assert_eq!(received1.len(), 200);

        let received2 = rx2.try_recv().unwrap().unwrap();
        assert_eq!(received2.len(), 300);
    }

    #[tokio::test]
    async fn forward_chunk_skips_non_overlapping_subscribers() {
        let mut actor = test_actor();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        actor
            .subscribers
            .push((5 * CHUNK_SIZE as u64, 6 * CHUNK_SIZE as u64 - 1, tx));

        let chunk_data = Arc::new(Bytes::from(vec![0u8; CHUNK_SIZE]));
        actor.forward_chunk(0, &chunk_data).await;

        assert!(rx.try_recv().is_err());
    }

    // ── fetch_chunk retry tests ──────────────────────────────

    use super::ChunkReader;
    use axum::{extract::State, http::StatusCode, response::Response, routing::get, Router};
    use dashmap::DashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Clone)]
    struct RetryTestServer {
        attempt: Arc<AtomicU32>,
        fail_count: u32,
        data: Arc<Vec<u8>>,
    }

    async fn retry_handler(
        State(state): State<RetryTestServer>,
    ) -> Result<Response<axum::body::Body>, (StatusCode, String)> {
        let prev = state.attempt.fetch_add(1, Ordering::SeqCst);
        if prev < state.fail_count {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-length", "999999")
                .body(axum::body::Body::from("short"))
                .unwrap());
        }
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(axum::body::Body::from(state.data.as_ref().clone()))
            .unwrap())
    }

    async fn retry_handler_403(
        State(state): State<RetryTestServer>,
    ) -> Result<Response<axum::body::Body>, (StatusCode, String)> {
        state.attempt.fetch_add(1, Ordering::SeqCst);
        Ok(Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(axum::body::Body::empty())
            .unwrap())
    }

    fn chunk_reader(dir: &TempDir, chunk_retries: u32) -> ChunkReader {
        let (event_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let backend: Arc<dyn StorageBackend> = Arc::new(crate::storage::local::LocalBackend::new(
            dir.path().join("chunks"),
            crate::storage::Compression::None,
        ));
        ChunkReader {
            http_client: reqwest::Client::new(),
            backend,
            event_tx,
            fetched_bytes: Arc::new(AtomicU64::new(0)),
            verify_sha256: false,
            chunk_retries,
            write_locks: DashMap::new(),
        }
    }

    #[tokio::test]
    async fn fetch_chunk_retries_on_body_read_error_then_succeeds() {
        let dir = TempDir::new().unwrap();
        let test_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let data_len = test_data.len() as u64;

        let server = RetryTestServer {
            attempt: Arc::new(AtomicU32::new(0)),
            fail_count: 2,
            data: Arc::new(test_data.clone()),
        };
        let attempt_counter = server.attempt.clone();

        let app = Router::new()
            .route("/test", get(retry_handler))
            .with_state(server);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reader = chunk_reader(&dir, 3);
        let url = format!("http://{addr}/test");
        let result = reader
            .fetch_chunk(
                url,
                1,
                0,
                0,
                data_len - 1,
                data_len,
                1,
                None,
                Arc::new(StdMutex::new(HashMap::new())),
            )
            .await;

        assert!(
            result.is_ok(),
            "expected success after retries, got: {result:?}"
        );
        assert_eq!(
            result.unwrap().as_ref(),
            test_data.as_slice(),
            "downloaded data should match"
        );
        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            3,
            "expected 3 attempts (2 failures + 1 success)"
        );
    }

    #[tokio::test]
    async fn fetch_chunk_fails_after_body_read_errors_exhausted() {
        let dir = TempDir::new().unwrap();
        let test_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let data_len = test_data.len() as u64;

        let server = RetryTestServer {
            attempt: Arc::new(AtomicU32::new(0)),
            fail_count: 10,
            data: Arc::new(test_data.clone()),
        };
        let attempt_counter = server.attempt.clone();

        let app = Router::new()
            .route("/test", get(retry_handler))
            .with_state(server);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reader = chunk_reader(&dir, 3);
        let url = format!("http://{addr}/test");
        let result = reader
            .fetch_chunk(
                url,
                1,
                0,
                0,
                data_len - 1,
                data_len,
                1,
                None,
                Arc::new(StdMutex::new(HashMap::new())),
            )
            .await;

        assert!(result.is_err(), "expected failure after max retries");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("after 3 attempts"),
            "error should mention attempt count, got: {err_msg}"
        );
        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            3,
            "expected exactly 3 attempts"
        );
    }

    async fn retry_handler_length_mismatch(
        State(state): State<RetryTestServer>,
    ) -> Result<Response<axum::body::Body>, (StatusCode, String)> {
        let prev = state.attempt.fetch_add(1, Ordering::SeqCst);
        if prev < state.fail_count {
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .body(axum::body::Body::from("short"))
                .unwrap());
        }
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(axum::body::Body::from(state.data.as_ref().clone()))
            .unwrap())
    }

    #[tokio::test]
    async fn fetch_chunk_retries_on_length_mismatch_then_succeeds() {
        let dir = TempDir::new().unwrap();
        let test_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let data_len = test_data.len() as u64;

        let server = RetryTestServer {
            attempt: Arc::new(AtomicU32::new(0)),
            fail_count: 2,
            data: Arc::new(test_data.clone()),
        };
        let attempt_counter = server.attempt.clone();

        let app = Router::new()
            .route("/test", get(retry_handler_length_mismatch))
            .with_state(server);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reader = chunk_reader(&dir, 3);
        let url = format!("http://{addr}/test");
        let result = reader
            .fetch_chunk(
                url,
                1,
                0,
                0,
                data_len - 1,
                data_len,
                1,
                None,
                Arc::new(StdMutex::new(HashMap::new())),
            )
            .await;

        assert!(
            result.is_ok(),
            "expected success after retries on length mismatch, got: {result:?}"
        );
        assert_eq!(
            result.unwrap().as_ref(),
            test_data.as_slice(),
            "downloaded data should match"
        );
        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            3,
            "expected 3 attempts (2 length mismatches + 1 success)"
        );
    }

    #[tokio::test]
    async fn fetch_chunk_fails_after_length_mismatch_exhausted() {
        let dir = TempDir::new().unwrap();
        let test_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let data_len = test_data.len() as u64;

        let server = RetryTestServer {
            attempt: Arc::new(AtomicU32::new(0)),
            fail_count: 10,
            data: Arc::new(test_data.clone()),
        };
        let attempt_counter = server.attempt.clone();

        let app = Router::new()
            .route("/test", get(retry_handler_length_mismatch))
            .with_state(server);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reader = chunk_reader(&dir, 3);
        let url = format!("http://{addr}/test");
        let result = reader
            .fetch_chunk(
                url,
                1,
                0,
                0,
                data_len - 1,
                data_len,
                1,
                None,
                Arc::new(StdMutex::new(HashMap::new())),
            )
            .await;

        assert!(
            result.is_err(),
            "expected failure after max retries on length mismatch"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("after 3 attempts"),
            "error should mention attempt count, got: {err_msg}"
        );
        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            3,
            "expected exactly 3 attempts"
        );
    }

    #[tokio::test]
    async fn fetch_chunk_does_not_retry_on_4xx() {
        let dir = TempDir::new().unwrap();
        let test_data: Vec<u8> = (0..100).map(|i| i as u8).collect();
        let data_len = test_data.len() as u64;

        let server = RetryTestServer {
            attempt: Arc::new(AtomicU32::new(0)),
            fail_count: 0,
            data: Arc::new(test_data.clone()),
        };
        let attempt_counter = server.attempt.clone();

        let app = Router::new()
            .route("/test", get(retry_handler_403))
            .with_state(server);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let reader = chunk_reader(&dir, 3);
        let url = format!("http://{addr}/test");
        let result = reader
            .fetch_chunk(
                url,
                1,
                0,
                0,
                data_len - 1,
                data_len,
                1,
                None,
                Arc::new(StdMutex::new(HashMap::new())),
            )
            .await;

        assert!(result.is_err(), "4xx should fail immediately");
        assert_eq!(
            attempt_counter.load(Ordering::SeqCst),
            1,
            "4xx should not be retried"
        );
    }
}
