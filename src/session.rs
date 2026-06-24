use crate::chunker;
use crate::metadata::{File, MetadataStore};
use crate::storage::StorageBackend;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

pub const CHUNK_SIZE: usize = crate::service::CHUNK_SIZE;

// ── TrunkSession ──────────────────────────────────────────────

pub struct TrunkSession {
    pub tx: broadcast::Sender<Arc<Bytes>>,
    _task: JoinHandle<()>,
}

pub struct SessionTable {
    map: DashMap<(i64, i64), Arc<TrunkSession>>,
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
            map: DashMap::new(),
            http_client,
            backend,
            metadata,
            fetched_bytes,
        }
    }

    pub fn subscribe(
        &self,
        file_id: i64,
        chunk_idx: i64,
        url: &str,
        start: u64,
        end: u64,
        total_size: u64,
        chunk_count: usize,
    ) -> broadcast::Receiver<Arc<Bytes>> {
        let key = (file_id, chunk_idx);

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

        tokio::spawn(async move {
            match Self::download_and_store(
                client, backend, metadata, url, fetched_bytes,
                file_id, chunk_idx, start, end, total_size, chunk_count,
            )
            .await
            {
                Ok(Some(data)) => {
                    let _ = tx.send(Arc::new(data));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!("chunk {} download failed: {}", chunk_idx, e);
                }
            }
        });

        self.map.insert(
            key,
            Arc::new(TrunkSession {
                tx: tx2,
                _task: tokio::spawn(async {}), // placeholder, actual task is spawned above
            }),
        );
        rx
    }

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
        let data = client
            .get(&url)
            .header("Range", &range_header)
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
        metadata.ensure_trunk(&sha256, "local", &path, data.len() as i64, stored_size)?;
        metadata.link_file_trunk(file_id, &sha256, chunk_idx, data.len() as i64)?;

        tracing::info!(
            "[f{}] chunk {}/{} done ({} bytes)",
            file_id,
            chunk_idx + 1,
            chunk_count,
            data.len()
        );

        Ok(Some(Bytes::from(data)))
    }
}

// ── FileDownloadSession ───────────────────────────────────────

#[derive(PartialEq, Eq)]
struct TrunkPriority {
    index: i64,
    priority: usize,
}

impl PartialOrd for TrunkPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TrunkPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.index.cmp(&other.index))
    }
}

pub struct FileDownloadSession {
    file_id: i64,
    name: String,
    repo: String,
    url: String,
    total_size: u64,
    chunk_count: usize,

    subscriber_count: AtomicUsize,
    subscribers: StdMutex<Vec<((u64, u64), mpsc::Sender<Result<Bytes, anyhow::Error>>)>>,

    session_table: Arc<SessionTable>,
    metadata: Arc<MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    head_client: reqwest::Client,
    served_bytes: Arc<AtomicU64>,

    task: StdMutex<Option<JoinHandle<()>>>,
    state: AtomicU8,
    file_ready_tx: StdMutex<Option<oneshot::Sender<(File, u64)>>>,
}

impl FileDownloadSession {
    #[allow(clippy::too_many_arguments)]
    fn new(
        file_id: i64,
        name: String,
        repo: String,
        url: String,
        total_size: u64,
        chunk_count: usize,
        session_table: Arc<SessionTable>,
        metadata: Arc<MetadataStore>,
        backend: Arc<dyn StorageBackend>,
        head_client: reqwest::Client,
        served_bytes: Arc<AtomicU64>,
    ) -> Self {
        let (ftx, _) = oneshot::channel();
        Self {
            file_id,
            name,
            repo,
            url,
            total_size,
            chunk_count: chunk_count.max(1),
            subscriber_count: AtomicUsize::new(0),
            subscribers: StdMutex::new(Vec::new()),
            session_table,
            metadata,
            backend,
            head_client,
            served_bytes,
            task: StdMutex::new(None),
            state: AtomicU8::new(0),
            file_ready_tx: StdMutex::new(Some(ftx)),
        }
    }

    fn signal_file_ready(&self, file: File, total_size: u64) {
        if let Some(tx) = self.file_ready_tx.lock().unwrap().take() {
            let _ = tx.send((file, total_size));
        }
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
        self.ensure_running();

        let (file, cl) = self.wait_for_file_ready(content_length).await?;
        Ok((file, cl, tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    async fn wait_for_file_ready(&self, content_length: u64) -> anyhow::Result<(File, u64)> {
        let rx = {
            let mut guard = self.file_ready_tx.lock().unwrap();
            if guard.is_some() {
                let (tx, rx) = oneshot::channel();
                *guard = Some(tx);
                Some(rx)
            } else {
                None
            }
        };

        if let Some(rx) = rx {
            rx.await
                .map_err(|_| anyhow::anyhow!("file session closed"))
        } else {
            let f = self
                .metadata
                .get_file_by_name(&self.name)?
                .ok_or_else(|| {
                    anyhow::anyhow!("file {} not found after session start", self.name)
                })?;
            Ok((f, content_length))
        }
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
        if let Err(e) = self.ensure_file_metadata().await {
            tracing::error!("Failed to get file metadata for {}: {}", self.name, e);
            return;
        }

        tracing::info!(
            "[f{}] {}: session started, {} trunks total, metadata in {}ms",
            self.file_id,
            self.name,
            self.chunk_count,
            session_start.elapsed().as_millis()
        );

        let chunk_sz = CHUNK_SIZE as u64;

        loop {
            let client_ranges: Vec<(u64, u64)> = {
                let subs = self.subscribers.lock().unwrap();
                subs.iter().map(|(r, _)| *r).collect()
            };

            if client_ranges.is_empty() {
                self.state.store(2, Ordering::Relaxed);
                break;
            }

            let mut trunk_prio: HashMap<i64, usize> = HashMap::new();
            for (s, e) in &client_ranges {
                let first = (*s / chunk_sz) as i64;
                let last = ((*e / chunk_sz) as i64).min(self.chunk_count as i64 - 1);
                for i in first..=last {
                    *trunk_prio.entry(i).or_insert(0) += 1;
                }
            }

            let mut heap = BinaryHeap::new();
            for (idx, prio) in trunk_prio {
                heap.push(TrunkPriority {
                    index: idx,
                    priority: prio,
                });
            }

            if let Some(next) = heap.pop() {
                let i = next.index as usize;
                let start = (i * CHUNK_SIZE) as u64;
                let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, self.total_size - 1);

                let trunk_start = std::time::Instant::now();
                let mut rx = self.session_table.subscribe(
                    self.file_id,
                    i as i64,
                    &self.url,
                    start,
                    end,
                    self.total_size,
                    self.chunk_count,
                );

                match rx.recv().await {
                    Ok(data) => {
                        let elapsed_ms = trunk_start.elapsed().as_millis();
                        let chunk_start = i as u64 * chunk_sz;
                        self.forward_chunk(chunk_start, &data).await;
                        self.served_bytes
                            .fetch_add(data.len() as u64, Ordering::Relaxed);

                        tracing::info!(
                            "[f{}] {} trunk {}/{}: {} bytes in {}ms",
                            self.file_id,
                            self.name,
                            i + 1,
                            self.chunk_count,
                            data.len(),
                            elapsed_ms,
                        );
                        if elapsed_ms > 5_000 {
                            tracing::warn!(
                                "[f{}] {} trunk {}/{}: SLOW — {} bytes in {}ms",
                                self.file_id,
                                self.name,
                                i + 1,
                                self.chunk_count,
                                data.len(),
                                elapsed_ms,
                            );
                        }

                        let step = self.prefetch_step();
                        for j in (i + 1)..(i + 1 + step).min(self.chunk_count) {
                            if !self.is_trunk_cached(j).await {
                                let pstart = (j * CHUNK_SIZE) as u64;
                                let pend = std::cmp::min(
                                    pstart + CHUNK_SIZE as u64 - 1,
                                    self.total_size - 1,
                                );
                                let _ = self.session_table.subscribe(
                                    self.file_id,
                                    j as i64,
                                    &self.url,
                                    pstart,
                                    pend,
                                    self.total_size,
                                    self.chunk_count,
                                );
                            }
                        }
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
                break;
            }

            self.clean_subscribers();
        }

        if let Ok(Some(f)) = self.metadata.get_file_by_name(&self.name) {
            self.signal_file_ready(f.clone(), f.total_size as u64);
        }

        tracing::info!(
            "[f{}] {}: session finished in {}ms",
            self.file_id,
            self.name,
            session_start.elapsed().as_millis()
        );
    }

    async fn ensure_file_metadata(&self) -> anyhow::Result<File> {
        if let Some(f) = self.metadata.get_file_by_name(&self.name)? {
            if f.x_repo_commit.is_some() && f.total_size > 0 {
                self.signal_file_ready(f.clone(), f.total_size as u64);
                return Ok(f);
            }
        }

        let head_resp = self.head_client.head(&self.url).send().await?;
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
            let resp2 = self.head_client.head(&location).send().await?;
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

        self.metadata.delete_file(&self.name)?;
        self.metadata
            .add_file(&self.name, &self.repo, size as i64, "pull")?;
        self.metadata.set_file_headers(
            &self.name,
            etag.as_deref(),
            x_repo_commit.as_deref(),
            x_linked_size,
            x_linked_etag.as_deref(),
            content_type.as_deref(),
        )?;
        self.metadata.touch_repo(&self.repo)?;

        let file = self
            .metadata
            .get_file_by_name(&self.name)?
            .ok_or_else(|| anyhow::anyhow!("file disappeared after creation"))?;
        self.signal_file_ready(file.clone(), size);
        Ok(file)
    }

    fn prefetch_step(&self) -> usize {
        let count = self.subscriber_count.load(Ordering::Relaxed).max(1);
        (16 / count).max(4)
    }

    async fn is_trunk_cached(&self, i: usize) -> bool {
        match self.metadata.is_chunk_linked(self.file_id, i) {
            Ok(Some(sha)) => self.backend.exists(&sha).await.unwrap_or(false),
            _ => false,
        }
    }

    async fn forward_chunk(&self, chunk_start: u64, data: &[u8]) {
        let targets: Vec<((u64, u64), mpsc::Sender<Result<Bytes, anyhow::Error>>)> = {
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
        self.subscriber_count
            .store(subs.len(), Ordering::Relaxed);
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

    pub fn get_or_create(
        &self,
        file_id: i64,
        name: &str,
        repo: &str,
        url: &str,
        total_size: u64,
    ) -> Arc<FileDownloadSession> {
        self.map
            .entry(file_id)
            .or_insert_with(|| {
                let chunk_count = (total_size as usize).div_ceil(CHUNK_SIZE);
                Arc::new(FileDownloadSession::new(
                    file_id,
                    name.to_string(),
                    repo.to_string(),
                    url.to_string(),
                    total_size,
                    chunk_count,
                    self.session_table.clone(),
                    self.metadata.clone(),
                    self.backend.clone(),
                    self.head_client.clone(),
                    self.served_bytes.clone(),
                ))
            })
            .clone()
    }

    pub fn remove(&self, file_id: i64) {
        self.map.remove(&file_id);
    }
}
