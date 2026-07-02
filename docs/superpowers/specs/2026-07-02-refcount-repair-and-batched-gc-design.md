# Refcount Repair and Batched GC Design

## Overview

This design covers three related integrity and operations changes:

1. make file deletion metadata updates transactional
2. add a control-plane repair endpoint for chunk reference-count consistency
3. change service GC from one large execution into repeated server-side batches driven by `hugrsctl`

These belong together because they all deal with chunk lifecycle correctness and long-running maintenance behavior.

## Problem Statement

### 1. File deletion is not fully transactional

Current chunk-link creation uses an explicit SQLite transaction in `ensure_chunk_and_link(...)`, but file deletion does not wrap the full lifecycle in one transaction.

The delete path currently performs, in sequence:

- load chunk references for the file
- decrement `chunks.ref_count`
- mark zero-ref chunks orphaned
- delete `file_chunks`
- delete `files`

If any step fails partway through, the database can be left in a partially updated state.

### 2. Historical chunk ref-count drift exists

Observed database evidence shows at least one chunk where:

- `chunks.ref_count` is higher than the actual number of `file_chunks` rows referencing that chunk

This means the system needs a repair path even if new code reduces the chance of future drift.

### 3. GC is currently too monolithic

The current GC control-plane flow is a single request that performs one whole execution pass on the server.

That makes long-running cleanup harder to control operationally and harder to surface incrementally to users.

## Goals

- Make file deletion metadata mutations atomic.
- Add an explicit admin repair operation for `chunks.ref_count` consistency.
- Preserve the existing `dry_run` model for maintenance operations.
- Change GC execution into one server-side batch per request, with a small default batch size.
- Let `hugrsctl` drive repeated GC execution with visible incremental progress and a one-second pause between batches.

## Non-Goals

- No automatic background self-healing at daemon startup for ref-count drift.
- No change to user-visible file proxy protocol behavior.
- No expansion into broader database repair beyond chunk ref-count and orphan state.
- No server-side streaming HTTP response for GC.

## Design

### 1. Transactional file deletion

The file delete lifecycle should be wrapped in a single SQLite transaction.

Affected logical steps:

1. select all chunk references for the target file
2. decrement `chunks.ref_count`
3. mark `orphaned_at` when `ref_count` becomes zero
4. delete `file_chunks`
5. delete `files`

Required property:

- all steps succeed together or none of them take effect

This applies to:

- `delete_file(name, source)`
- repo-scoped delete paths that iterate file deletion

The recommended implementation is to move the delete lifecycle into a helper that operates on a transaction, similar in spirit to `ensure_chunk_and_link(...)`.

### 2. Ref-count repair endpoint

Add a new admin endpoint under `/_hugrs/service/...` for ref-count reconciliation.

Recommended route:

- `POST /_hugrs/service/reconsile`

This endpoint should follow the same request style as current GC:

- support `dry_run: true`
- support `dry_run: false`

### Repair source of truth

The repair logic should treat `file_chunks` as the source of truth for actual chunk references.

For each row in `chunks`, compute:

- `actual_refs = COUNT(*) FROM file_chunks WHERE file_chunks.sha256 = chunks.sha256`

### Repair rules

If `actual_refs > 0`:

- set `chunks.ref_count = actual_refs`
- set `chunks.orphaned_at = NULL`

If `actual_refs = 0`:

- set `chunks.ref_count = 0`
- if `orphaned_at IS NULL`, set `orphaned_at = datetime('now')`
- if `orphaned_at` already has a value, keep the existing timestamp

This preserves useful orphan timing information while still repairing state.

### Response shape

Use one response shape for both dry-run and apply.

Recommended fields:

- `scanned_chunks`
- `mismatched_chunks`
- `refcount_fixed`
- `orphaned_marked`
- `orphaned_cleared`

Behavior:

- `dry_run = true`: compute and return counts, do not modify DB
- `dry_run = false`: apply the repair and return the same summary fields

### 3. Batched GC execution

Current GC request shape is too small for batch execution control.

Extend the GC request to support:

- `dry_run: bool`
- `batch_size: Option<usize>`

Behavior:

- `dry_run = true` keeps current dry-run semantics
- `dry_run = false` executes only one server-side batch per request

Default batch size:

- `32`

### GC response shape

Current GC response already returns:

- `deleted_chunks`
- `reclaimed_bytes`
- `skipped_chunks`

Add:

- `has_more: bool`

Meaning:

- `true` means there are still orphan candidates remaining after this batch
- `false` means the server-side cleanup pass is complete for now

### 4. `hugrsctl service gc` loop behavior

Keep the current CLI shape:

- `hugrsctl service gc --dry-run`
- `hugrsctl service gc`

Behavior changes only for execution mode.

#### Dry run

No change in behavior:

- one request
- one dry-run response

#### Execute mode

New behavior:

1. client calls GC execute endpoint with one batch request
2. client prints that batch result
3. if `has_more == true`, sleep for one second
4. call the endpoint again
5. repeat until `has_more == false`
6. print a final aggregate summary

This gives the user a stream-like operational experience without requiring the server to keep one HTTP response open.

### CLI output behavior

Recommended default human-readable output:

- one line per batch
- one final summary line

Example:

```text
batch 1: deleted 32 chunks, reclaimed 128 MB, skipped 0
batch 2: deleted 32 chunks, reclaimed 126 MB, skipped 1
batch 3: deleted 5 chunks, reclaimed 18 MB, skipped 0
done: deleted 69 chunks, reclaimed 272 MB, skipped 1
```

For `--json`, the simplest acceptable behavior is:

- emit only the final aggregate JSON result

This avoids defining a streaming JSON protocol in this change.

## Affected Areas

### `src/metadata.rs`

- wrap file deletion lifecycle in a transaction
- add internal helper(s) to reconcile chunk ref-counts against `file_chunks`

### `src/service.rs`

- add service-level repair entry point that delegates to metadata reconciliation
- update GC execution to accept a batch size and return `has_more`

### `src/control.rs`

- extend `GcRequest`
- extend `GcResultResponse`
- add request/response types for chunk ref reconciliation

### `src/server.rs`

- add `/_hugrs/service/reconsile`
- update `/_hugrs/service/gc` to batch one pass per request

### `src/admin_client.rs`

- add methods for the new repair endpoint
- update GC execute method to send batch-size-aware requests

### `src/hugrsctl_cli.rs`

- update `service gc` execute flow to loop with one-second pauses
- add output formatting for per-batch progress and final aggregate summary
- add CLI wiring for `hugrsctl service reconsile --dry-run|--apply` style usage

## Error Handling

### Transactional delete

- if delete fails mid-flight, transaction must roll back completely

### Ref-count repair

- `dry_run` must never mutate state
- `apply` must be all-or-nothing for one repair run

### Batched GC

- a failed batch ends the client loop
- already completed earlier batches remain committed
- client should report partial progress before failure if some batches already succeeded

## Testing

### Transactional delete tests

- prove that a failure inside delete does not leave ref-counts or links half-updated

### Ref-count repair tests

- drifted `ref_count > actual_refs` is corrected
- drifted `ref_count < actual_refs` is corrected
- `actual_refs = 0` sets `ref_count = 0`
- `actual_refs > 0` clears orphan state
- `dry_run` reports changes without applying them

### Batched GC tests

- one execute request deletes at most one batch
- `has_more` is correct when more orphan chunks remain
- aggregate CLI loop stops when `has_more == false`
- CLI loop sleeps between calls in execute mode
- `dry_run` behavior remains unchanged

## Risks

### Repair operation on large databases

Full chunk-to-file reference reconciliation may be expensive on large databases.

This is acceptable as an admin operation, but implementation should use SQL that is easy to reason about and test.

### Batch GC behavior change

Users who expect one `hugrsctl service gc` call to finish everything in one server request will now see multiple client-driven calls.

This is intentional, but the CLI output should make the loop behavior obvious.

## Decision Summary

- file deletion becomes transactional
- chunk ref-count repair is exposed as an admin operation with `dry_run` and `apply`
- `file_chunks` is the source of truth for actual refs
- GC execution becomes one server-side batch per request
- default GC batch size is `32`
- `hugrsctl service gc` loops with a one-second pause until `has_more == false`
