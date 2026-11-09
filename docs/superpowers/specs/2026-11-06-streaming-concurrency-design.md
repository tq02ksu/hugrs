# Streaming Concurrency Correctness Design

## Goal

Every streaming response must carry exactly the bytes of the requested range, in
order, and exactly `Content-Length` bytes. A client joining an in-flight download
of the same file must still receive its range from the first chunk, and a fully
cached file must never be served through shared per-file session state.

Success: two simultaneous full downloads of the same file, and N simultaneous
ranges of the same file, all produce bytes identical to the upstream content.

## Root cause

`FileSessionActor` (`src/session.rs`) keeps `completed: HashSet<usize>` and
`subscribers: Vec<(u64, u64, Sender)>` per file, not per request.

- `completed` records "this file has downloaded chunk N", not "this request has
  sent chunk N".
- `select_next_chunk()` skips `completed`, so a late subscriber's range is
  entered mid-way.
- `forward_chunk()` fans out only to subscribers registered at download time, so
  chunks completed before the subscriber joined are never sent.

`Content-Length` is still the full range length, so the client receives a short
body (scenario A) or shifted bytes once the client inserts them at its own
offset (scenario B).

The cache-hit path never used `stream_cached_file`: `src/server.rs` served even
a 100%-cached file through `stream_http_file` → `FileSessionManager` → the same
shared actor. Clean cache + corrupt download are therefore consistent.

## Non-goals

- Fixing the unreachable `HUGRS_HF_PROXY` endpoint (separate issue).
- Changing storage compression or content-type/header behavior.
- Background migration of already-corrupt rows; they self-heal on the next
  request once the completeness predicate is strict.
- Slow-client backpressure policy (a blocked subscriber still stalls the actor,
  as today).
- Zero-byte files (currently rejected upstream by `probe_file_metadata`).

## Approaches considered

1. **Per-subscriber cursor inside the actor (chosen).** Keeps one driver per
   file, upstream concurrency bounded by `prefetch_budget`, and one storage read
   shared by all subscribers.
2. **Per-request streaming tasks, delete the actor.** Simplest and obviously
   correct, but N concurrent readers cause N storage reads and N hash
   verifications of the same chunks, and per-request prefetch multiplies
   upstream concurrency.
3. **Only the complete-file fast path.** Minimal diff, fixes the reported
   reproductions, leaves first-time concurrent downloads corrupt.

## Design

### 1. Per-subscriber cursor

Replace the subscriber tuple with:

```rust
struct Subscriber {
    start: u64,
    end: u64,
    next_idx: usize,
    last_idx: usize,
    tx: mpsc::Sender<Result<Bytes, anyhow::Error>>,
}
```

`next_idx = start / CHUNK_SIZE`, `last_idx = min(end / CHUNK_SIZE, chunk_count - 1)`.
Delete `completed` entirely. The only "do we already have it" truth is
`cached_chunks` (`chunk_idx -> sha256`), which `ChunkReader::fetch_chunk` updates
after `backend.put` and which is seeded from `file_chunks` at actor creation.

Actor loop:

```text
drain mailbox -> push Subscriber
clean_subscribers()                       // drop closed receivers
cleanup_prefetches()
if subscribers.is_empty() && prefetch_inflight.is_empty() { break }
match select_next_chunk() {
    Some(idx) => serve_chunk(idx).await?, // serve or propagate error + break
    None => break,                        // no subscriber needs more
}
schedule_prefetches()
```

- `select_next_chunk()` returns the `next_idx` of the subscriber whose next
  chunk is most urgent (`byte_distance / range_len`), tie-broken by smallest
  index. Only `next_idx` values are candidates — never an arbitrary missing
  chunk — so no subscriber can be served out of order.
- `serve_chunk(idx)` awaits `SessionTable::subscribe`, which reads the chunk
  from storage when `cached_chunks` has it and otherwise waits on the existing
  per-chunk download (dedup is already keyed by `(file_id, chunk_idx)`). This is
  the "backfill" from the bug report: a late subscriber replaying cached chunks
  needs no special path.
- Before forwarding, `serve_chunk` asserts `data.len() == end - start + 1` for
  the chunk. A size mismatch is a stream error, never forwarded bytes.
- `forward_chunk(idx, chunk_start, data)` sends a slice only to subscribers with
  `next_idx == idx`, advances their `next_idx`, and counts `served_bytes` per
  slice. Subscribers with `next_idx > last_idx` are dropped, which closes their
  channel and ends the body.
- `compute_active_cursors()` returns each subscriber's `next_idx`.
- `schedule_prefetches()` prefetches `next_idx + 1 ..= next_idx + per_cursor`,
  skipping indexes already cached.

`prefetch_inflight` becomes `HashMap<usize, JoinHandle<()>>` and
`cleanup_prefetches()` retains entries that are neither finished nor cached.
Today a failed prefetch stays in the set forever and is never retried.

### 2. Complete-file fast path

`src/server.rs` GET cache-hit branch calls `stream_cached_file` instead of
`stream_http_file`. Change its signature to take the already-validated
`&File` (avoids a second lookup and a race with concurrent invalidation):

```rust
stream_cached_file(&self, file: &File, range_start: Option<u64>, range_end: Option<u64>)
```

Each request then streams independently: no session, no shared cursor, no
cross-request interaction. This is the path taken by essentially every hot
download.

`stream_cached_file` additionally validates `data.len()` against the chunk's
expected size before slicing, and on missing blob, size mismatch, or sha256
mismatch unlinks that `(file_id, chunk_index)` row so the file becomes
incomplete and the next request re-fetches through the actor.

### 3. Strict completeness predicate

Add to `src/service.rs`:

```rust
pub fn file_is_complete(total_size: i64, chunks: &[FileChunk]) -> bool
```

True only when: `total_size > 0`, `chunks.len() == ceil(total_size / CHUNK_SIZE)`,
`chunks[i].chunk_index == i` for every i, and
`chunks[i].chunk_size == min(CHUNK_SIZE, total_size - i * CHUNK_SIZE)`.

`get_complete_file` uses it instead of `SUM(chunk_size) >= total_size`. The
control-plane `complete` flags (`src/server.rs` file list and file show) use the
same predicate so `hugrsctl file` no longer reports a duplicated/oversized cache
as complete.

This also inverts the two-full-file-copies case: both rows fail the size check,
the file is treated as incomplete, and the actor re-downloads it.

### 4. Integrity guard independent of verification flag

In `ChunkReader::read_cached_chunk`, run the expected-size check before the
`verify_sha256` early return. Size is free to check and a wrong-sized chunk
corrupts all downstream offsets whether or not hashing is enabled.

### 5. Chunk handoff ordering

`SessionTable`'s fetch task sends the downloaded chunk on the broadcast channel
and then removes its map entry. A subscriber that attaches between those two
calls receives a channel that never carries the chunk and later faults with
"download closed unexpectedly". `cached_chunks` is already populated when the
fetch returns, so removing the map entry before sending guarantees every later
subscriber takes the cache path. This must be fixed for the cursor design,
where a late subscriber routinely attaches at exactly that moment.

## Invariants

- A subscriber only ever receives chunk `next_idx`, `next_idx + 1`, ...,
  `last_idx`, each sliced to its range.
- Total forwarded bytes per response equal the requested range length.
- No cached chunk is served whose stored length differs from its expected
  length, regardless of `verify_sha256`.
- No response depends on another response's progress.

## Testing

Regression tests first, expected to fail on the current code:

- `tests/streaming_tests.rs`: late joiner. Start a full download, consume two
  chunks, then subscribe a second full-range reader on the same file while the
  first is still open. Both must equal the upstream bytes.
- `tests/streaming_tests.rs`: N concurrent unaligned ranges covering the file;
  each response must equal its byte slice.
- `tests/streaming_tests.rs`: after a complete download, two concurrent
  `stream_cached_file` readers must both match the content (fast path).
- `tests/service_tests.rs`: completeness predicate rejects a file with two
  full-size chunk rows, rejects an index gap whose sizes sum to `total_size`,
  accepts a correctly chunked file including a short last chunk.

Unit tests in `src/session.rs`: cursor-based `select_next_chunk`,
`forward_chunk` advancing only the matching subscriber, prefetch cursor derived
from `next_idx`.

Gates: `cargo fmt`, `cargo clippy --all-features --tests`, `cargo test`.

Manual verification on the target deployment: rebuild the image, rerun the
two-client full download and the 8-way `hf_transfer`-style range script against
the DeepSeek-V4-Flash-0731 revision, compare sha256 per file and per segment.
