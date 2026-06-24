# Server Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor server handlers into two unified categories (file proxy + API proxy), replace lock/semaphore with single-flight SessionTable, and add `/api/models/{org}/{repo}` endpoint.

**Architecture:** New `src/session.rs` module with `SessionTable` (trunk-level single-flight) and `FileSessionManager` (file-level coordination). `CacheService` integrates both, removing all semaphores/locks. `server.rs` merges route handlers. FileDownloadSession drives adaptive prefetch internally.

**Tech Stack:** Rust, tokio, axum, reqwest, rusqlite, dashmap

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `src/session.rs` | **Create** | `SessionTable`, `TrunkSession`, `FileSessionManager`, `FileDownloadSession` |
| `src/service.rs` | Modify | Add session fields, remove locks, refactor `stream_from_upstream` |
| `src/server.rs` | Modify | Merge handlers, add new route, simplify |
| `src/hf.rs` | Modify | Add `build_stream_client()` without timeout |
| `src/main.rs` | Modify | Initialize `SessionTable` + `FileSessionManager` |
| `tests/streaming_tests.rs` | Modify | Adapt to new API |
| `tests/e2e_tests.rs` | Modify | Adapt to new API + handler names |

---

### Task 1: Add `dashmap` dependency and `build_stream_client` in hf.rs

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/hf.rs:77-103`

- [ ] **Step 1: Add dashmap to Cargo.toml**

Read `Cargo.toml` then add:
```toml
dashmap = "6"
```

- [ ] **Step 2: Add `build_stream_client()` to hf.rs**

In `src/hf.rs`, after `build_head_client` (line 103), add:

```rust
pub fn build_stream_client(config: &Config) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.huggingface.connect_timeout_secs));

    if let Some(ref proxy_url) = config.huggingface.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)?;
        builder = builder.proxy(proxy);
    }

    Ok(builder.build()?)
}
```

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml src/hf.rs
git commit -m "feat: add dashmap dep and streaming HTTP client without timeout"
```

---

### Task 2: Create `src/session.rs` — SessionTable + TrunkSession

**Files:**
- Create: `src/session.rs`
- Modify: `src/lib.rs` (add `pub mod session;`)

- [ ] **Step 1: Create src/session.rs with SessionTable**

```rust
use crate::chunker;
use crate::metadata::MetadataStore;
use crate::service::CHUNK_SIZE;
use crate::storage::StorageBackend;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

pub struct TrunkSession {
    pub tx: broadcast::Sender<Arc<Bytes>>,
    _task: JoinHandle<()>,
    completed: AtomicBool,
}

impl TrunkSession {
    pub fn is_done(&self) -> bool {
        self.completed.load(Ordering::Relaxed)
    }
}

pub struct SessionTable {
    map: DashMap<(i64, usize), Arc<TrunkSession>>,
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
        chunk_idx: usize,
        url: &str,
        start: u64,
        end: u64,
        total_size: u64,
        chunk_count: usize,
    ) -> broadcast::Receiver<Arc<Bytes>> {
        let key = (file_id, chunk_idx as i64 as usize);

        // Fast path: reuse existing session
        if let Some(session) = self.map.get(&key) {
            if session.is_done() {
                self.map.remove(&key);
                drop(session);
            } else {
                return session.tx.subscribe();
            }
        }

        let (tx, _) = broadcast::channel::<Arc<Bytes>>(4);
        let rx = tx.subscribe();

        let backend = self.backend.clone();
        let metadata = self.metadata.clone();
        let client = self.http_client.clone();
        let url = url.to_string();
        let tx2 = tx.clone();
        let fetched_bytes = self.fetched_bytes.clone();
        let cidx = chunk_idx;

        let completed = AtomicBool::new(false);
        let task = tokio::spawn(async move {
            let downloaded = match Self::download_and_store(
                client,
                backend,
                metadata,
                url,
                fetched_bytes,
                file_id,
                cidx,
                start,
                end,
                total_size,
                chunk_count,
            )
            .await
            {
                Ok(Some(data)) => data,
                Ok(None) => return,
                Err(e) => {
                    tracing::warn!("chunk {} download failed: {}", cidx, e);
                    return;
                }
            };
            let _ = tx.send(Arc::new(downloaded));
        });

        let session = Arc::new(TrunkSession {
            tx: tx2,
            _task: task,
            completed: AtomicBool::new(false),
        });
        self.map.insert(key, session);
        rx
    }

    async fn download_and_store(
        client: reqwest::Client,
        backend: Arc<dyn StorageBackend>,
        metadata: Arc<MetadataStore>,
        url: String,
        fetched_bytes: Arc<AtomicU64>,
        file_id: i64,
        chunk_idx: usize,
        start: u64,
        end: u64,
        total_size: u64,
        chunk_count: usize,
    ) -> anyhow::Result<Option<Bytes>> {
        // Check if already linked (someone else finished first)
        if let Ok(Some(sha)) = metadata.is_chunk_linked(file_id, chunk_idx) {
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
        metadata.link_file_trunk(file_id, &sha256, chunk_idx as i64, data.len() as i64)?;

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
```

- [ ] **Step 2: Add `pub mod session;` to src/lib.rs**

Read `src/lib.rs`, find where other modules are listed, add:
```rust
pub mod session;
```

- [ ] **Step 3: Commit**

```bash
git add src/session.rs src/lib.rs
git commit -m "feat: add SessionTable with trunk-level single-flight"
```

---

### Task 3: Create FileSessionManager + FileDownloadSession in session.rs

**File:** Modify `src/session.rs`

- [ ] **Step 1: Add FileDownloadSession struct and implementation after TrunkSession**

Append to `src/session.rs`:

```rust
use crate::metadata::File;
use crate::service::ByteStream;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::ops::Range;
use std::sync::Mutex as StdMutex;
use tokio::sync::{mpsc, oneshot};

type ClientRange = (u64, u64);

struct TrunkPriority {
    index: usize,
    priority: usize, // higher = more clients need it
}

impl PartialEq for TrunkPriority {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.index == other.index
    }
}
impl Eq for TrunkPriority {}
impl PartialOrd for TrunkPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TrunkPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority).then_with(|| self.index.cmp(&other.index))
    }
}

pub struct FileDownloadSession {
    file_id: i64,
    name: String,
    repo: String,
    url: String,
    total_size: u64,
    chunk_count: usize,

    subscriber_count: Arc<std::sync::atomic::AtomicUsize>,
    subscribers: StdMutex<Vec<(ClientRange, mpsc::Sender<Result<Bytes, anyhow::Error>>)>>,

    session_table: Arc<SessionTable>,
    metadata: Arc<MetadataStore>,
    backend: Arc<dyn StorageBackend>,
    head_client: reqwest::Client,
    served_bytes: Arc<AtomicU64>,

    task: StdMutex<Option<JoinHandle<()>>>,
    state: std::sync::atomic::AtomicU8, // 0=Created, 1=Downloading, 2=Satisfied
    file_ready_tx: StdMutex<Option<oneshot::Sender<(File, u64)>>>,
    file_ready_rx: StdMutex<Option<oneshot::Receiver<(File, u64)>>>,
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
        file: File,
        session_table: Arc<SessionTable>,
        metadata: Arc<MetadataStore>,
        backend: Arc<dyn StorageBackend>,
        head_client: reqwest::Client,
        served_bytes: Arc<AtomicU64>,
    ) -> Self {
        let (ftx, frx) = oneshot::channel();
        Self {
            file_id,
            name,
            repo,
            url,
            total_size,
            chunk_count,
            subscriber_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            subscribers: StdMutex::new(Vec::new()),
            session_table,
            metadata,
            backend,
            head_client,
            served_bytes,
            task: StdMutex::new(None),
            state: std::sync::atomic::AtomicU8::new(0),
            file_ready_tx: StdMutex::new(Some(ftx)),
            file_ready_rx: StdMutex::new(Some(frx)),
        }
    }

    fn signal_file_ready(&self, file: File, content_length: u64) {
        if let Some(tx) = self.file_ready_tx.lock().unwrap().take() {
            let _ = tx.send((file, content_length));
        }
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        range: Option<(u64, Option<u64>)>,
    ) -> anyhow::Result<(File, u64, ByteStream)> {
        let total_size = self.total_size;
        let req_start = range.map(|r| r.0).unwrap_or(0);
        let req_end = range
            .and_then(|r| r.1)
            .unwrap_or(total_size.saturating_sub(1))
            .min(total_size.saturating_sub(1));

        if req_start > req_end || req_start >= total_size {
            anyhow::bail!("invalid range: bytes={}-{}/{}", req_start, req_end, total_size);
        }
        let content_length = req_end - req_start + 1;

        let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(32);

        {
            let mut subs = self.subscribers.lock().unwrap();
            subs.push(((req_start, req_end), tx));
        }
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);

        // Ensure download loop is running
        self.ensure_running();

        // Wait for file metadata to be available
        let (file, cl) = {
            let mut rx_guard = self.file_ready_rx.lock().unwrap();
            match rx_guard.take() {
                Some(rx) => rx.await.map_err(|_| anyhow::anyhow!("file session closed"))?,
                None => {
                    // Already consumed by another subscribe; reconstruct from db
                    let f = self.metadata.get_file_by_name(&self.name)?
                        .ok_or_else(|| anyhow::anyhow!("file {} not found after session start", self.name))?;
                    (f, content_length)
                }
            }
        };

        Ok((file, cl, tokio_stream::wrappers::ReceiverStream::new(rx)))
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
        // Fetch file metadata from upstream if not already populated
        let file = match self.ensure_file_metadata().await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to get file metadata for {}: {}", self.name, e);
                return;
            }
        };

        let chunk_size_u64 = CHUNK_SIZE as u64;

        loop {
            // Collect current client ranges
            let client_ranges: Vec<ClientRange> = {
                let subs = self.subscribers.lock().unwrap();
                subs.iter().map(|(r, _)| *r).collect()
            };

            if client_ranges.is_empty() {
                self.state.store(2, Ordering::Relaxed); // Satisfied
                break;
            }

            // Determine which trunks are needed + their priority
            let mut trunk_prio: HashMap<usize, usize> = HashMap::new();
            for (s, e) in &client_ranges {
                let first = (*s / chunk_size_u64) as usize;
                let last = ((*e / chunk_size_u64) as usize).min(self.chunk_count.saturating_sub(1));
                for i in first..=last {
                    *trunk_prio.entry(i).or_insert(0) += 1;
                }
            }

            // Build priority queue
            let mut heap = BinaryHeap::new();
            for (idx, prio) in trunk_prio {
                heap.push(TrunkPriority { index: idx, priority: prio });
            }

            // Process highest priority trunk
            if let Some(next) = heap.pop() {
                let i = next.index;
                let start = (i * CHUNK_SIZE) as u64;
                let end = std::cmp::min(start + CHUNK_SIZE as u64 - 1, self.total_size - 1);

                let mut rx = self.session_table.subscribe(
                    self.file_id,
                    i,
                    &self.url,
                    start,
                    end,
                    self.total_size,
                    self.chunk_count,
                );

                match rx.recv().await {
                    Ok(data) => {
                        let chunk_start = i as u64 * chunk_size_u64;
                        let chunk_end = chunk_start + data.len() as u64 - 1;
                        self.forward_chunk(chunk_start, chunk_end, &data).await;
                        self.served_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);

                        // Prefetch upcoming trunks
                        let step = self.prefetch_step();
                        for j in (i + 1)..(i + 1 + step).min(self.chunk_count) {
                            if !self.is_trunk_cached(j).await {
                                let pstart = (j * CHUNK_SIZE) as u64;
                                let pend = std::cmp::min(pstart + CHUNK_SIZE as u64 - 1, self.total_size - 1);
                                let _rx2 = self.session_table.subscribe(
                                    self.file_id, j, &self.url,
                                    pstart, pend, self.total_size, self.chunk_count,
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
                // No trunks needed, all client ranges satisfied
                self.state.store(2, Ordering::Relaxed);
                break;
            }

            // Remove satisfied subscribers
            self.clean_subscribers();
        }

        // Signal file ready to late subscribers
        if let Ok(f) = self.metadata.get_file_by_name(&self.name) {
            if let Some(f) = f {
                self.signal_file_ready(f.clone(), f.total_size as u64);
            }
        }
    }

    async fn ensure_file_metadata(&self) -> anyhow::Result<File> {
        if let Some(f) = self.metadata.get_file_by_name(&self.name)? {
            if f.x_repo_commit.is_some() && f.total_size > 0 {
                let cl = if f.total_size > 0 { f.total_size as u64 } else { self.total_size };
                self.signal_file_ready(f.clone(), cl);
                return Ok(f);
            }
        }

        // HEAD upstream
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
            let et = h.get("etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
            let ct = h.get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
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

        // Create or update file record
        self.metadata.delete_file(&self.name)?;
        self.metadata.add_file(&self.name, &self.repo, size as i64, "pull")?;
        self.metadata.set_file_headers(
            &self.name,
            etag.as_deref(),
            x_repo_commit.as_deref(),
            x_linked_size,
            x_linked_etag.as_deref(),
            content_type.as_deref(),
        )?;
        self.metadata.touch_repo(&self.repo)?;

        let file = self.metadata.get_file_by_name(&self.name)?
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

    async fn forward_chunk(&self, chunk_start: u64, chunk_end: u64, data: &[u8]) {
        let subs = self.subscribers.lock().unwrap();
        for ((s, e), tx) in subs.iter() {
            let s = *s;
            let e = *e;
            if chunk_end < s || chunk_start > e {
                continue; // chunk not in this client's range
            }
            let sl_start = if s > chunk_start { (s - chunk_start) as usize } else { 0 };
            let sl_end = if e < chunk_end { (e - chunk_start + 1) as usize } else { data.len() };
            let slice = Bytes::copy_from_slice(&data[sl_start..sl_end]);
            if tx.send(Ok(slice)).await.is_err() {
                // Client disconnected
            }
        }
    }

    fn clean_subscribers(&self) {
        let mut subs = self.subscribers.lock().unwrap();
        subs.retain(|(_, tx)| !tx.is_closed());
        let count = subs.len();
        self.subscriber_count.store(count, Ordering::Relaxed);
    }
}
```

- [ ] **Step 2: Add FileSessionManager after FileDownloadSession**

Append to `src/session.rs`:

```rust
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
        file: File,
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
                    chunk_count.max(1),
                    file,
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
```

- [ ] **Step 3: Add needed imports at top of session.rs**

Ensure these imports exist at the top:
```rust
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
```

- [ ] **Step 4: Commit**

```bash
git add src/session.rs
git commit -m "feat: add FileSessionManager and FileDownloadSession"
```

---

### Task 4: Refactor `CacheService` in service.rs — remove locks, add session fields

**Files:**
- Modify: `src/service.rs`

- [ ] **Step 1: Remove lock/semaphore fields and add session fields**

In `src/service.rs`, modify the `CacheService` struct (lines 17-29):

**Remove these lines:**
```rust
download_locks: Arc<StdMutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
download_sem: Arc<Semaphore>,
```

**Remove the import at top:**
```rust
use tokio::sync::{mpsc, oneshot, Semaphore};
```
Change to:
```rust
use tokio::sync::mpsc;
```

**Add fields to the struct:**
```rust
#[derive(Clone)]
pub struct CacheService {
    pub metadata: Arc<MetadataStore>,
    pub backend: Arc<dyn StorageBackend>,
    max_size: Option<u64>,
    pub http_client: reqwest::Client,
    pub head_client: reqwest::Client,
    prefetch_depth: usize,
    verify_sha256: bool,
    fetched_bytes: Arc<AtomicU64>,
    served_bytes: Arc<AtomicU64>,
    session_table: Arc<crate::session::SessionTable>,
    fs_manager: Arc<crate::session::FileSessionManager>,
}
```

- [ ] **Step 2: Update `CacheService::new()`**

Replace the `new()` function (lines 38-69) with:

```rust
impl CacheService {
    pub fn new(
        metadata: Arc<MetadataStore>,
        backend: Arc<dyn StorageBackend>,
        max_size: Option<u64>,
        http_client: reqwest::Client,
        head_client: reqwest::Client,
        prefetch_depth: usize,
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
            verify_sha256,
            fetched_bytes,
            served_bytes,
            session_table,
            fs_manager,
        }
    }
}
```

- [ ] **Step 3: Add `stream_file()` method to replace `stream_from_upstream`**

Add a new public method (can be placed after `stream_from_upstream` or replace it):

```rust
pub async fn stream_file(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    range: Option<(u64, Option<u64>)>,
) -> anyhow::Result<(File, u64, ByteStream)> {
    let file = self.ensure_file_metadata(url, name, repo).await?;
    let total_size = file.total_size as u64;
    let session = self.fs_manager.get_or_create(
        file.id,
        name,
        repo,
        url,
        total_size,
        file,
    );
    session.subscribe(range).await
}
```

Wait, we need `ensure_file_metadata` to be a helper. Actually, the metadata fetching already exists in `stream_from_upstream` (HEAD upstream, follow redirects, create file record). Let me extract it.

Actually, for the first pass, the `stream_file` method can just use the existing HEAD logic from `stream_from_upstream`. Let me keep `stream_from_upstream` but refactor it to use the session:

Actually, I think the cleanest approach is:
1. Keep the HEAD metadata logic in a helper `ensure_file_headers_from_upstream()`
2. `stream_file()` calls the helper, then creates session, then subscribes

But to minimize the diff, let me just modify `stream_from_upstream` to use the session while keeping the same signature.

Actually, looking at the test code, `stream_from_upstream` is called directly by tests. I should keep this function as the public API but change its internals.

Let me replace the body of `stream_from_upstream`:

```rust
pub async fn stream_from_upstream(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> anyhow::Result<(File, u64, ByteStream)> {
    // 1. HEAD upstream for metadata (same as before)
    let head_resp = self.head_client.head(url).send().await?;
    // ... (same HEAD + redirect logic as current lines 745-806)
    // ...
    
    // 2. Create/update file record (same as before)
    // ... (same logic as current lines 808-830)
    
    // 3. Create session and subscribe (NEW)
    let file = self.metadata.get_file_by_name(name)?
        .ok_or_else(|| anyhow::anyhow!("file disappeared after creation"))?;
    let total_size = file.total_size as u64;
    let session = self.fs_manager.get_or_create(file.id, name, repo, url, total_size, file);
    session.subscribe(Some((range_start.unwrap_or(0), range_end))).await
}
```

Hmm, but this keeps the HEAD logic duplicated in `stream_from_upstream`. Let me extract it.

Actually, for the plan I need to be concrete. Let me write the actual replacement code. The key observation is: the HEAD metadata logic in `stream_from_upstream` (lines ~745-806) is ~60 lines of code that I need to extract. Let me put it in a helper.

Given the complexity, let me just write the replacement for the entire `stream_from_upstream` function and the helper, showing the exact code.

OK let me write a simpler plan. The approach will be:
1. Keep `stream_from_upstream` as the public API (same signature)
2. Replace the body: extract HEAD metadata logic into `fetch_file_metadata`, then use session
3. Remove the old chunk download loop (lines ~862-1088)

This is a big diff. Let me write it step by step.

Actually, let me simplify the plan. I'll explain what to do and show the key code changes rather than copying all 200+ lines. The implementer can use search/replace.

Let me focus on the most impactful changes and write them precisely.

- [ ] **Step 4: Add `fetch_file_metadata` helper and refactor `stream_from_upstream`**

In `src/service.rs`, after `stream_cached_file` (around line 720), add:

```rust
async fn fetch_file_metadata(
    &self,
    url: &str,
    name: &str,
    repo: &str,
) -> anyhow::Result<(u64, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>)> {
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

    let (upstream_size, etag, content_type) = if status.is_redirection() {
        let location = first_headers
            .get("location")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let location = crate::server::resolve_redirect(url, location);
        tracing::info!("fetch_file_metadata following redirect: {}", location);
        match self.head_client.head(&location).send().await {
            Ok(resp2) => {
                let h = resp2.headers();
                let cl: u64 = h.get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok()).unwrap_or(0);
                let et = h.get("etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
                let ct = h.get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
                (cl, et, ct)
            }
            Err(e) => anyhow::bail!("redirect follow failed for {}: {}", url, e),
        }
    } else {
        let cl: u64 = first_headers.get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok()).unwrap_or(0);
        let et = first_headers.get("etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
        let ct = first_headers.get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
        (cl, et, ct)
    };

    let size = if upstream_size > 0 { upstream_size } else { x_linked_size.unwrap_or(0) as u64 };
    if size == 0 {
        anyhow::bail!("cannot determine file size for {}", url);
    }

    let existing = self.metadata.get_file_by_name(name)?;
    if existing.as_ref().map(|f| f.total_size as u64 != size).unwrap_or(true) {
        self.metadata.delete_file(name)?;
    }
    if self.metadata.get_file_by_name(name)?.is_none() {
        self.metadata.add_file(name, repo, size as i64, "pull")?;
    }
    self.metadata.set_file_headers(
        name,
        etag.as_deref(),
        x_repo_commit.as_deref(),
        x_linked_size,
        x_linked_etag.as_deref(),
        content_type.as_deref(),
    )?;
    self.metadata.touch_repo(repo)?;

    Ok((size, etag, content_type, x_repo_commit, x_linked_size, x_linked_etag))
}
```

Then replace the entire `stream_from_upstream` function body (keeping signature):

```rust
pub async fn stream_from_upstream(
    &self,
    url: &str,
    name: &str,
    repo: &str,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> anyhow::Result<(File, u64, ByteStream)> {
    let (total_size, _etag, _content_type, _x_repo_commit, _x_linked_size, _x_linked_etag) =
        self.fetch_file_metadata(url, name, repo).await?;

    let file = self.metadata.get_file_by_name(name)?
        .ok_or_else(|| anyhow::anyhow!("file disappeared after creation"))?;

    if total_size <= CHUNK_SIZE as u64 {
        return self.stream_small_file_via_session(url, name, &file).await;
    }

    let session = self.fs_manager.get_or_create(file.id, name, repo, url, total_size, file);
    session.subscribe(Some((range_start.unwrap_or(0), range_end))).await
}
```

And add `stream_small_file_via_session`:

```rust
async fn stream_small_file_via_session(
    &self,
    url: &str,
    name: &str,
    file: &File,
) -> anyhow::Result<(File, u64, ByteStream)> {
    if self.is_file_complete(name).await? {
        return self.stream_cached_file(name, None, None).await;
    }
    let total = file.total_size as u64;
    let (tx, rx) = mpsc::channel::<Result<Bytes, anyhow::Error>>(1);
    let client = self.http_client.clone();
    let url = url.to_string();
    let svc = self.clone();
    let fname = name.to_string();
    let frepo = file.repo.clone();
    let fetched_bytes = self.fetched_bytes.clone();
    let served_bytes = self.served_bytes.clone();

    let tx2 = tx.clone();
    tokio::spawn(async move {
        let data = match client.get(&url).send().await.and_then(|r| r.bytes().await) {
            Ok(d) => d,
            Err(e) => { let _ = tx2.send(Err(anyhow::anyhow!("{}", e))).await; return; }
        };
        fetched_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        if let Err(e) = svc.upload(&fname, &frepo, data.to_vec()).await {
            let _ = tx2.send(Err(e)).await;
            return;
        }
        served_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        let _ = tx2.send(Ok(data)).await;
    });

    Ok((file.clone(), total, ReceiverStream::new(rx)))
}
```

- [ ] **Step 5: Clean up removed code**

Remove old code that's no longer needed:
- Remove `download_locks` field reference from struct (Step 1 above)
- Remove `download_sem` field reference from struct (Step 1 above)
- Remove the old `DownloadedChunk` struct (line 31-36)
- Remove the unused import `use tokio::sync::Semaphore;`
- Remove `use tokio::sync::oneshot;` (was used in old stream_from_upstream)
- Remove `use std::collections::{HashMap, HashSet, VecDeque};` if no longer used elsewhere
- Remove `StreamExt` import if only used in removed code

- [ ] **Step 6: Remove old `stream_small_file` method**

Replace the old `stream_small_file` (lines 1093-1140) with the new one from Step 4.

- [ ] **Step 7: Remove old `do_download_chunk` method**

Remove lines 1143-1211 (the old `do_download_chunk` method). This logic is now in `SessionTable::download_and_store`.

- [ ] **Step 8: Build and fix any remaining compilation issues**

```bash
cargo build 2>&1
```

Check for:
- Removed imports that are still needed elsewhere
- Unused variable warnings
- Any remaining references to removed fields

- [ ] **Step 9: Commit**

```bash
git add src/service.rs
git commit -m "refactor: replace semaphore/lock with SessionTable in CacheService"
```

---

### Task 5: Rewrite server.rs — merge file handlers, add simple API proxy

**Files:**
- Modify: `src/server.rs`

**Design note**: `/api/models/{org}/{repo}` is just a simple proxy + http_cache. JSON is just a small file. No special etag freshness check. `http_cache` is just a cache, nothing special.

- [ ] **Step 1: Update route definitions**

Replace the route definitions in `run()` (lines 31-49):

```rust
let app = Router::new()
    .route("/", get(root))
    .route("/api/whoami-v2", get(whoami))
    .route(
        "/api/models/{org}/{repo}",
        get(handle_api_proxy).head(handle_api_proxy),
    )
    .route(
        "/api/models/{org}/{repo}/revision/{revision}",
        get(handle_api_proxy).head(handle_api_proxy),
    )
    .route(
        "/{org}/{repo}/resolve/{revision}/{*path}",
        get(handle_file_proxy).head(handle_file_proxy),
    )
    .route(
        "/api/resolve-cache/{repo_type}/{org}/{repo}/{revision}/{*path}",
        get(handle_file_proxy).head(handle_file_proxy),
    )
    .route("/api/stats", get(stats))
    .route("/api/agent-harnesses", get(agent_harnesses))
    .layer(middleware::from_fn(log_request))
    .with_state(app_state);
```

- [ ] **Step 2: Add `handle_file_proxy` handler**

Add after the `AppState` struct (after line 68):

```rust
async fn handle_file_proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    let url = format!(
        "{}/{}/resolve/{}/{}",
        state.config.huggingface.endpoint, repo_id, revision, path
    );

    if method == Method::HEAD {
        let service = state.service.lock().await;
        if let Ok(Some(file)) = service.info(&cache_name).await {
            if file.x_repo_commit.is_some() {
                return build_head_response(&file, &path);
            }
        }
        drop(service);

        let resp = state
            .head_client
            .head(&url)
            .send()
            .await
            .map_err(|e| AppError::Anyhow(e.into()))?;
        let status = resp.status();
        let first_headers = resp.headers();

        let x_repo_commit = first_headers
            .get("x-repo-commit")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let xl_size: Option<i64> = first_headers
            .get("x-linked-size")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        let x_linked_etag = first_headers
            .get("x-linked-etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let (total_size, etag, content_type) = if status.is_redirection() {
            let location = first_headers
                .get("location")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let location = resolve_redirect(&url, location);
            match state.http_client.head(location).send().await {
                Ok(resp2) => {
                    let h = resp2.headers();
                    (h.get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0),
                     h.get("etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
                     h.get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string()))
                }
                Err(_) => (0u64, None, None),
            }
        } else {
            (first_headers.get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0),
             first_headers.get("etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
             first_headers.get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string()))
        };

        let size = if total_size > 0 { total_size } else { xl_size.unwrap_or(0) as u64 };
        if size > 0 {
            let service = state.service.lock().await;
            let _ = service.ensure_file_headers(
                &cache_name, &repo_id, size,
                etag.as_deref(), x_repo_commit.as_deref(),
                xl_size, x_linked_etag.as_deref(),
                content_type.as_deref(),
            );
        }

        let mut builder = Response::builder().status(StatusCode::OK);
        if let Some(ref ct) = content_type { builder = builder.header("Content-Type", ct.as_str()); }
        if size > 0 { builder = builder.header("Content-Length", size); }
        builder = builder.header("Accept-Ranges", "bytes");
        if let Some(ref et) = etag { builder = builder.header("ETag", et.as_str()); }
        if let Some(ref commit) = x_repo_commit { builder = builder.header("X-Repo-Commit", commit.as_str()); }
        if let Some(sz) = xl_size { builder = builder.header("X-Linked-Size", sz); }
        if let Some(ref le) = x_linked_etag { builder = builder.header("X-Linked-ETag", le.as_str()); }
        return builder.body(axum::body::Body::empty()).map_err(|e| AppError::Anyhow(e.into()));
    }

    // GET
    let range = parse_range(&headers);
    let service = state.service.lock().await;
    let (file, content_length, stream) = service
        .stream_from_upstream(&url, &cache_name, &repo_id, range.map(|r| r.0), range.and_then(|r| r.1))
        .await
        .map_err(AppError::Anyhow)?;
    drop(service);

    build_stream_response(file, content_length, stream, &path, range)
}
```

Wait, this is basically duplicating the HEAD logic that was in `serve_file`. The HEAD logic for file serving should stay in server.rs since it's HTTP response construction logic, not business logic. Actually, I could move it to service, but that's a separate concern.

Actually, looking at the existing code, the HEAD handling in `serve_file` does:
1. Check metadata cache → hit → return cached headers
2. Cache miss → HEAD upstream → cache metadata → return

This logic should stay in server.rs since it's about constructing HTTP responses. The new `handle_file_proxy` just combines the HEAD and GET paths that were already in `serve_file`.

- [ ] **Step 2 (continued): Note about GET path**

For the GET path of `handle_file_proxy`, the code calls `service.stream_from_upstream()` which now internally uses `FileSessionManager`. This is the bridge between the old API and the new session-based internals.

- [ ] **Step 3: Add `handle_api_proxy` handler**

Add after `handle_file_proxy`:

```rust
async fn handle_api_proxy(
    State(state): State<AppState>,
    Path(params): Path<Vec<String>>,
) -> Result<Response, AppError> {
    // Params: [org, repo] or [org, repo, revision, revision_value]
    let (org, repo, revision) = if params.len() == 2 {
        (params[0].clone(), params[1].clone(), "main".to_string())
    } else if params.len() == 4 {
        (params[0].clone(), params[1].clone(), params[3].clone())
    } else {
        return Err(AppError::Anyhow(anyhow::anyhow!("invalid path")));
    };

    let repo_id = format!("{}/{}", org, repo);
    let url = format!(
        "{}/api/models/{}/revision/{}",
        state.config.huggingface.endpoint, repo_id, revision
    );

    // Check http_cache for this URL
    {
        let service = state.service.lock().await;
        if let Ok(Some((status, headers, body))) = service.get_http_cache(&url) {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
            let mut builder = Response::builder().status(status);
            for line in headers.lines() {
                if let Some(col) = line.find(':') {
                    builder = builder.header(line[..col].trim(), line[col + 1..].trim());
                }
            }
            return builder.body(body.into()).map_err(|e| AppError::Anyhow(e.into()));
        }
        drop(service);
    }

    let mut req = state.http_client.get(&url);
    if let Some(ref token) = state.config.huggingface.token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    let resp = req.send().await.map_err(|e| AppError::Anyhow(e.into()))?;
    let status = resp.status();
    let upstream_headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .filter(|(n, _)| *n != "transfer-encoding")
        .map(|(n, v)| (n.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let body = resp.text().await.map_err(|e| AppError::Anyhow(e.into()))?.into_bytes();

    let headers_text = upstream_headers
        .iter()
        .map(|(n, v)| format!("{}: {}", n, v))
        .collect::<Vec<_>>()
        .join("\n");

    {
        let service = state.service.lock().await;
        let _ = service.set_http_cache(&url, status.as_u16(), &headers_text, &body);
        drop(service);
    }

    let mut builder = Response::builder().status(status);
    for (name, value) in &upstream_headers {
        builder = builder.header(name, value);
    }
    builder.body(body.into()).map_err(|e| AppError::Anyhow(e.into()))
}
```

Wait, the route params for `/api/models/{org}/{repo}` would be different from `/api/models/{org}/{repo}/revision/{revision}`. Let me think about how axum handles these.

In axum, `/api/models/{org}/{repo}` matches 2 path segments after `api/models/`, while `/api/models/{org}/{repo}/revision/{revision}` matches 4. We need to handle both.

Actually, looking at how the current `model_info_revision` works:
```rust
Path((org, repo, revision)): Path<(String, String, String)>
```

For the new route `/api/models/{org}/{repo}`, we'd use:
```rust
Path((org, repo)): Path<(String, String)>
```

But the route pattern `/api/models/{org}/{repo}` and `/api/models/{org}/{repo}/revision/{revision}` have different arity. In axum, we can register them as separate routes but use the same handler function. The handler would need to handle both signatures.

Actually, a simpler approach: use two separate route registrations, each mapping to `handle_api_proxy` but with different Path tuple sizes. Wait, they'd need different handler signatures then... Or use a catch-all with `{*rest}`.

Hmm, this is getting complex. Let me simplify: use `{*rest}` for the revision route:

```rust
.route("/api/models/{org}/{repo}", get(handle_api_proxy_simple).head(handle_api_proxy_simple))
.route("/api/models/{org}/{repo}/revision/{revision}", get(handle_api_proxy).head(handle_api_proxy))
```

Where `handle_api_proxy_simple` calls through to the logic with `revision = "main"`.

Actually, even simpler: just have two separate handlers that both call a shared `proxy_model_info(org, repo, revision)` function.

Let me revise:

```rust
async fn handle_api_proxy(
    State(state): State<AppState>,
    Path((org, repo, revision)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    proxy_model_info(state, org, repo, revision).await
}

async fn handle_api_proxy_simple(
    State(state): State<AppState>,
    Path((org, repo)): Path<(String, String)>,
) -> Result<Response, AppError> {
    proxy_model_info(state, org, repo, "main".to_string()).await
}

async fn proxy_model_info(
    state: AppState,
    org: String,
    repo: String,
    revision: String,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let url = format!(
        "{}/api/models/{}/revision/{}",
        state.config.huggingface.endpoint, repo_id, revision
    );
    // ... rest of logic
}
```

OK let me simplify the plan for server.rs and use two handler functions that share a helper.

- [ ] **Step 3: Implement handlers**

Replace `model_info_revision` (lines 104-172) with:

```rust
async fn handle_api_proxy_simple(
    State(state): State<AppState>,
    Path((org, repo)): Path<(String, String)>,
) -> Result<Response, AppError> {
    proxy_model_info(state, org, repo, "main".to_string()).await
}

async fn handle_api_proxy(
    State(state): State<AppState>,
    Path((org, repo, revision)): Path<(String, String, String)>,
) -> Result<Response, AppError> {
    proxy_model_info(state, org, repo, revision).await
}

async fn proxy_model_info(
    state: AppState,
    org: String,
    repo: String,
    revision: String,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let url = format!(
        "{}/api/models/{}/revision/{}",
        state.config.huggingface.endpoint, repo_id, revision
    );

    {
        let service = state.service.lock().await;
        if let Ok(Some((status, headers, body))) = service.get_http_cache(&url) {
            tracing::info!("model_info cache hit: {}", url);
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
            let mut builder = Response::builder().status(status);
            for line in headers.lines() {
                if let Some(col) = line.find(':') {
                    let name = line[..col].trim();
                    let value = line[col + 1..].trim();
                    builder = builder.header(name, value);
                }
            }
            return builder.body(body.into()).map_err(|e| AppError::Anyhow(e.into()));
        }
        drop(service);
    }

    tracing::info!("model_info proxy to: {}", url);
    let mut req = state.http_client.get(&url);
    if let Some(ref token) = state.config.huggingface.token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    let resp = req.send().await.map_err(|e| AppError::Anyhow(e.into()))?;
    let status = resp.status();
    let upstream_headers: Vec<(String, String)> = resp
        .headers()
        .iter()
        .filter(|(n, _)| *n != "transfer-encoding")
        .map(|(n, v)| (n.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    let body = resp.text().await.map_err(|e| AppError::Anyhow(e.into()))?.into_bytes();

    let headers_text = upstream_headers
        .iter()
        .map(|(n, v)| format!("{}: {}", n, v))
        .collect::<Vec<_>>()
        .join("\n");

    {
        let service = state.service.lock().await;
        let _ = service.set_http_cache(&url, status.as_u16(), &headers_text, &body);
        drop(service);
    }

    let mut builder = Response::builder().status(status);
    for (name, value) in &upstream_headers {
        builder = builder.header(name, value);
    }
    builder.body(body.into()).map_err(|e| AppError::Anyhow(e.into()))
}
```

- [ ] **Step 4: Replace `file_resolve` and `resolve_cache` with `handle_file_proxy`**

Replace the two handlers `file_resolve` (line 174) and `resolve_cache` (line 195) with the single `handle_file_proxy`. This handler combines the HEAD logic from `serve_file` (HEAD branch) and the GET logic (call `stream_from_upstream`).

The HEAD logic stays in server.rs since it's HTTP response construction. The GET logic delegates to `service.stream_from_upstream()`.

Write `handle_file_proxy` replacing both old handlers:

```rust
pub async fn handle_file_proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    Path((org, repo, revision, path)): Path<(String, String, String, String)>,
) -> Result<Response, AppError> {
    let repo_id = format!("{}/{}", org, repo);
    let cache_name = format!("{}/{}", repo_id, path);
    let range = parse_range(&headers);
    let url = format!(
        "{}/{}/resolve/{}/{}",
        state.config.huggingface.endpoint, repo_id, revision, path
    );

    if method == Method::HEAD {
        let service = state.service.lock().await;
        if let Ok(Some(file)) = service.info(&cache_name).await {
            if file.x_repo_commit.is_some() {
                tracing::debug!("HEAD cache hit (metadata): {}", cache_name);
                return build_head_response(&file, &path);
            }
            tracing::debug!(
                "HEAD cache hit but missing x_repo_commit, refreshing from upstream: {}",
                cache_name
            );
        }
        drop(service);

        tracing::info!("HEAD proxy to upstream: {}", url);
        let resp = state.head_client.head(&url).send().await
            .map_err(|e| AppError::Anyhow(e.into()))?;
        let status = resp.status();
        let first_headers = resp.headers();
        tracing::info!("HEAD upstream response: status={}", status);

        let x_repo_commit = first_headers.get("x-repo-commit").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
        let xl_size: Option<i64> = first_headers.get("x-linked-size").and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok());
        let x_linked_etag = first_headers.get("x-linked-etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string());

        let (total_size, etag, content_type) = if status.is_redirection() {
            let location = first_headers.get("location").and_then(|v| v.to_str().ok()).unwrap_or("");
            let location = resolve_redirect(&url, location);
            tracing::info!("HEAD following redirect: {}", location);
            match state.http_client.head(location).send().await {
                Ok(resp2) => {
                    let h = resp2.headers();
                    (h.get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0),
                     h.get("etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
                     h.get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string()))
                }
                Err(e) => { tracing::warn!("HEAD redirect failed: {}", e); (0u64, None, None) }
            }
        } else {
            (first_headers.get("content-length").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0),
             first_headers.get("etag").and_then(|v| v.to_str().ok()).map(|s| s.to_string()),
             first_headers.get("content-type").and_then(|v| v.to_str().ok()).map(|s| s.to_string()))
        };

        let size = if total_size > 0 { total_size } else { xl_size.unwrap_or(0) as u64 };
        if size > 0 {
            let service = state.service.lock().await;
            let _ = service.ensure_file_headers(
                &cache_name, &repo_id, size,
                etag.as_deref(), x_repo_commit.as_deref(),
                xl_size, x_linked_etag.as_deref(),
                content_type.as_deref(),
            );
            tracing::info!("cached HEAD metadata for {} ({} bytes)", cache_name, size);
        }

        let mut builder = Response::builder().status(StatusCode::OK);
        if let Some(ref ct) = content_type { builder = builder.header("Content-Type", ct.as_str()); }
        if size > 0 { builder = builder.header("Content-Length", size); }
        builder = builder.header("Accept-Ranges", "bytes");
        if let Some(ref et) = etag { builder = builder.header("ETag", et.as_str()); }
        if let Some(ref commit) = x_repo_commit { builder = builder.header("X-Repo-Commit", commit.as_str()); }
        if let Some(sz) = xl_size { builder = builder.header("X-Linked-Size", sz); }
        if let Some(ref le) = x_linked_etag { builder = builder.header("X-Linked-ETag", le.as_str()); }
        tracing::info!("HEAD returning 200 (size={})", size);
        return builder.body(axum::body::Body::empty()).map_err(|e| AppError::Anyhow(e.into()));
    }

    // GET
    {
        let service = state.service.lock().await;
        if service.is_file_complete(&cache_name).await.unwrap_or(false) {
            tracing::debug!("GET cache hit (streaming): {}", cache_name);
            let (file, content_length, stream) = service
                .stream_cached_file(&cache_name, range.map(|r| r.0), range.and_then(|r| r.1))
                .await?;
            return build_stream_response(file, content_length, stream, &path, range);
        }
    }

    tracing::info!("cache miss, streaming via session: {}", cache_name);
    let service = state.service.lock().await;
    let (file, content_length, stream) = service
        .stream_from_upstream(&url, &cache_name, &repo_id, range.map(|r| r.0), range.and_then(|r| r.1))
        .await?;
    drop(service);

    build_stream_response(file, content_length, stream, &path, range)
}
```

- [ ] **Step 5: Remove old handler functions**

Remove:
- `model_info_revision` (lines 104-172) — replaced by `handle_api_proxy_simple` + `handle_api_proxy`
- `file_resolve` (lines 174-193) — replaced by `handle_file_proxy`
- `resolve_cache` (lines 195-213) — replaced by `handle_file_proxy`
- `serve_file` (lines 216-394) — logic moved into `handle_file_proxy`

- [ ] **Step 6: Remove unused import**

In `server.rs`, remove the unused import if `serve_file` was the only user:
- `use std::sync::Arc;` — keep if still used by `AppState`
- Check for any other now-unused imports

- [ ] **Step 7: Build and verify**

```bash
cargo build 2>&1
```

- [ ] **Step 8: Commit**

```bash
git add src/server.rs
git commit -m "refactor: merge server handlers into file_proxy and api_proxy, add /api/models/{org}/{repo}"
```

---

### Task 6: Update main.rs for new CacheService signature

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Build streaming client and pass to CacheService**

In `src/main.rs`, after `let head_client = hf::build_head_client(&config)?;` (line 53), add:

```rust
let stream_client = hf::build_stream_client(&config)?;
```

Then update the `CacheService::new()` call (lines 54-62) to include the new parameter:

```rust
let service = CacheService::new(
    metadata,
    backend,
    config.storage.max_size,
    http_client,
    head_client,
    config.storage.prefetch_depth,
    config.storage.verify_sha256,
    stream_client,
);
```

- [ ] **Step 2: Build and verify**

```bash
cargo build 2>&1
```

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git commit -m "refactor: pass stream_client to CacheService"
```

---

### Task 7: Update tests for new API

**Files:**
- Modify: `tests/streaming_tests.rs`
- Modify: `tests/e2e_tests.rs`

- [ ] **Step 1: Update `make_service` helper in e2e_tests.rs**

In `tests/e2e_tests.rs`, update `make_service` (lines 88-100):

```rust
fn make_service(dir: &TempDir, db_name: &str) -> CacheService {
    let metadata = Arc::new(MetadataStore::new(&dir.path().join(db_name)).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("trunks"),
        Compression::None,
    ));
    let http = reqwest::Client::new();
    let head = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let stream = reqwest::Client::new();
    CacheService::new(metadata, backend, None, http, head, 0, true, stream)
}
```

- [ ] **Step 2: Update `build_hugrs_router` to use new handler names**

In `tests/e2e_tests.rs`, update the route registration (line 154-155) to:

```rust
.get(hugrs::server::handle_file_proxy).head(hugrs::server::handle_file_proxy),
```

- [ ] **Step 3: Update streaming_tests.rs `CacheService::new()` calls**

In `tests/streaming_tests.rs`, find all `CacheService::new(...)` calls and add the `stream_client` parameter. There are multiple occurrences:

For test `test_multiple_gets_no_duplicate_downloads` (around line 114):
```rust
let stream_client = reqwest::Client::new();
let service = Arc::new(CacheService::new(
    metadata,
    backend,
    None,
    http_client,
    head_client,
    0,
    true,
    stream_client,
));
```

For test `test_partial_cache_no_redundant_download` (around line 216):
```rust
let stream_client = reqwest::Client::new();
let service = CacheService::new(
    metadata.clone(),
    backend.clone(),
    None,
    http_client,
    head_client,
    0,
    true,
    stream_client,
);
```

- [ ] **Step 4: Update service_tests.rs `CacheService::new()` calls**

In `tests/service_tests.rs`, update all `CacheService::new(...)` calls to include the extra parameter. There are 4 calls in tests:
- `test_upload_and_download` (line 17)
- `test_delete_and_gc` (line 54)
- `test_stats` (line 86)
- `test_upload_duplicate_file_overwrites` (line 117)
- `test_lru_eviction` (line 149)
- `test_lru_eviction_by_repo` (line 183)

Each needs `reqwest::Client::new()` added as the last parameter.

- [ ] **Step 5: Run tests**

```bash
cargo test 2>&1
```

- [ ] **Step 6: Commit**

```bash
git add tests/
git commit -m "test: update tests for new CacheService API and handler names"
```

---

### Task 8: Final verification

- [ ] **Step 1: Run all tests**

```bash
cargo test 2>&1
```

- [ ] **Step 2: Run lints**

```bash
cargo clippy -- -D warnings 2>&1
```

- [ ] **Step 3: Check format**

```bash
cargo fmt -- --check
```

- [ ] **Step 4: Fix any issues and commit**

```bash
git add -A && git commit -m "fix: resolve test failures and lint warnings"
```
