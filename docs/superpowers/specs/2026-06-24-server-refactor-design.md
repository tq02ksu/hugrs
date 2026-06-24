# Server Refactor Design

## Problem Statement

1. Timeout: `download_sem` (128) and per-file `file_sem` (16) cause requests to queue for semaphore permits; reqwest timeout (60s) fires before queued requests ever start.
2. Route handler duplication: `file_resolve` and `resolve_cache` are identical except path parsing. `model_info_revision` uses a separate `http_cache` mechanism disjoint from trunk-based caching.
3. Missing endpoint: `/api/models/{org}/{repo}` not implemented.
4. No file-level coordination: concurrent requests for different ranges of the same file lack shared scheduling.

## Architecture Overview

```
Client Request
      │
      ▼
Server Route Layer (handle_file_proxy / handle_api_proxy)
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│ FileSessionManager (DashMap<file_id, FileDownloadSession>)│
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │ FileDownloadSession (per file_id)               │    │
│  │  state: Created → Downloading → Satisfied → Drop│    │
│  │  pending_ranges: client-requested byte ranges   │    │
│  │  trunk_priority_queue: BinaryHeap<(idx, prio)>  │    │
│  │  subscribers: Vec<(client_range, mpsc::Sender)> │    │
│  │  session_broadcast: broadcast::Sender<Chunk>    │    │
│  │  subscriber_count → determines prefetch step    │    │
│  └───────────┬─────────────────────────────────────┘    │
│              │                                           │
│  For each trunk in priority_queue:                      │
│              │                                           │
│  ┌───────────▼─────────────────────────────────────┐    │
│  │ SessionTable (DashMap<trunk_key, Session>)       │    │
│  │  trunk_key = (file_id, chunk_idx)                │    │
│  │                                                  │    │
│  │  Session (per trunk):                            │    │
│  │   state: AtomicState {Downloading, Done, Failed} │    │
│  │   broadcast: broadcast::Sender<Arc<Bytes>>       │    │
│  │   task: JoinHandle                               │    │
│  └───────────┬─────────────────────────────────────┘    │
│              │ fan-out                                   │
│     ┌────────┼────────┐                                 │
│     ▼        ▼        ▼                                 │
│  client    client   cache writer                         │
│  stream    stream   (hash → store → link metadata)       │
└─────────────────────────────────────────────────────────┘
```

## Component Boundaries

### FileSessionManager
- `DashMap<i64, FileDownloadSession>` — lock-free lookup
- `get_or_create(file_id)`: creates session if first request, returns existing otherwise
- Removes session when state → Dropped

### FileDownloadSession (per file)
- **Lifecycle**:
  - `Created`: first client request arrives
  - `Downloading`: trunk priority queue is processed
  - `Satisfied`: all client ranges covered → removed from manager
  - `Dropped`: client disconnects before Satisfied
- **Ranges**: merges overlapping client ranges into minimal set of pending ranges
- **Priority**: trunk prio = number of clients whose range covers that trunk
- **Internal loop**: pop highest-prio trunk → subscribe to SessionTable for trunk → wait → forward chunk data to clients whose range includes the chunk
- **Client subscribe**: ONE subscribe per client, returns a broadcast Receiver that streams chunks in order

### SessionTable (per trunk)
- `DashMap<(i64, usize), Session>` — trunk-level single-flight
- `subscribe(trunk_key) → broadcast::Receiver<Arc<Bytes>>`
  - Existing session → return receiver immediately
  - New session → spawn download task, register, return receiver
- `Session.download_task`:
  1. GET `{url}` Range:`bytes={start}-{end}`
  2. Read response body as bytes
  3. Send `Arc::new(bytes)` to broadcast channel
  4. Set state → Done

### Cache Writer
- One receiver per started session, or a shared mpsc receiver from all sessions
- Receives `Arc<Bytes>`, computes SHA256, stores to backend, links in metadata
- Runs as background task, decoupled from client streaming

### Prefetch (within FileDownloadSession)
- **NOT a separate scheduler scanning metadata**
- Driven by `FileDownloadSession` during trunk processing:
  - After processing trunk N, check trunks N+1 to N+step
  - If not cached + not already queued → submit to SessionTable (as low-prio, marks new session)
- **Adaptive step**:
  - `subscriber_count == 1` → step = 16 (aggressive push)
  - `subscriber_count > 1`  → step = 4 (clients collectively cover more)
  - Formula: `step = max(4, 16 / subscriber_count)`

## Request Flow

### Category A: File Proxy (trunk-based)

Routes:
- `GET|HEAD /{org}/{repo}/resolve/{revision}/{*path}`
- `GET|HEAD /api/resolve-cache/{repo_type}/{org}/{repo}/{revision}/{*path}`

```
handle_file_proxy(method, headers, org, repo, revision, path):
  build upstream URL
  cache_name = repo_id/path

  if method == HEAD:
    check metadata cache for file
    if hit and has x_repo_commit → return 200 with cached headers
    if miss → HEAD upstream → cache metadata → return 200

  if method == GET:
    ensure file metadata (HEAD upstream if not cached)
    range = parse Range header
    session = FileSessionManager.get_or_create(file_id)
    stream = session.subscribe(range)  // ONE subscribe
    return stream_response(stream, headers)
```

### Category B: API Proxy (http_cache with etag freshness)

Routes:
- `GET|HEAD /api/models/{org}/{repo}` (NEW)
- `GET|HEAD /api/models/{org}/{repo}/revision/{revision}`

```
handle_api_proxy(method, org, repo, revision?):
  upstream_url = build HF API URL
  revision = revision.unwrap_or("main")

  if method == HEAD or cache check:
    HEAD upstream to get etag
    cached = metadata.get_http_cache(upstream_url)
    if cached and cached.etag == upstream.etag → return cached
    if mismatch → fetch full response → update http_cache → return

  fetch upstream → cache headers + body in http_cache → return
```

Note: `http_cache` is used only for API responses (JSON), not file downloads. Trunk-based caching is for files.

### Other Routes

| Route | Handler |
|-------|---------|
| `GET /` | static JSON (unchanged) |
| `GET /api/whoami-v2` | static JSON (unchanged) |
| `GET /api/stats` | local DB query (unchanged) |
| `GET /api/agent-harnesses` | simple proxy, no cache (unchanged) |

## Lock/Semaphore Removal

| Removed | Replaced By |
|---------|-------------|
| `download_sem: Semaphore(128)` | Nothing. reqwest connection pool provides natural backpressure |
| `download_locks: Mutex<HashMap<String, Mutex>>` | `FileSessionManager` singleton-per-file; clients subscribe, not compete |
| `file_sem: Semaphore(16)` | `FileDownloadSession` serializes trunk download internally (one trunk at a time per file session) |

## Error Handling

- Trunk download failure → Session state → Failed. Propagate error to all subscribers for that trunk.
- FileDownloadSession sees a Failed trunk → skip it (hole in file). Client receives truncated stream or error depending on whether the missing trunk was in its range.
- http_cache etag HEAD failure → fall back to full fetch (treat as etag mismatch).
- Prefetch queue submission failure (try_send) → silent discard; prefetch is best-effort.

## New/Modified Files

| File | Action |
|------|--------|
| `src/server.rs` | Merge handlers: `handle_file_proxy`, `handle_api_proxy`. Remove `serve_file`, `file_resolve`, `resolve_cache`. Add `/api/models/{org}/{repo}` route. |
| `src/service.rs` | Add `FileSessionManager`, `SessionTable` fields. Remove `download_sem`, `download_locks`. `stream_from_upstream` refactored. Add `get_or_create_file_session()`. |
| `src/session.rs` | **New**. `SessionTable`, `Session`, `FileSessionManager`, `FileDownloadSession`. |
| `src/hf.rs` | Optionally build a separate client without timeout for streaming downloads. |
| `src/main.rs` | Initialize `FileSessionManager` + `SessionTable` in `CacheService`. |

## Data Flow Summary

1. Client sends `GET /org/repo/resolve/main/model.bin` with `Range: bytes=0-4194303`
2. `handle_file_proxy` parses and calls `CacheService.get_or_create_file_session(file_id)`
3. Client subscribes to `FileDownloadSession` via `session.subscribe(range)` — instant, non-blocking
4. `FileDownloadSession` internal loop downloads trunk 0 via `SessionTable.subscribe((file_id, 0))`
5. If another client requests same file: subscribes to same `FileDownloadSession`, gets a new broadcast receiver
6. Trunk 0 data arrives → forwarded to all clients whose range includes trunk 0
7. After trunk 0, prefetch step calculated → trunks 1–step submitted to SessionTable
8. When all client ranges satisfied → `FileDownloadSession` transitions to `Satisfied` → removed
