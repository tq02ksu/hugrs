# Metadata Reconcile and Singleflight Design

## Overview

HugRS currently uses cached file metadata aggressively on `HEAD` and `GET` paths, but the reuse rule is too weak for HuggingFace snapshot-based clients.

The current logic treats matching `ETag` as sufficient proof that cached metadata is still valid. That is incorrect for clients such as `huggingface_hub`, because file content identity and snapshot commit identity are not the same thing.

Observed production evidence shows the failure mode clearly:

- one repo revision resolves to a new git commit
- many files keep the same content and therefore the same `ETag`
- cached metadata continues to serve an older `X-Repo-Commit`
- the client assembles one logical revision into multiple local snapshot directories

This design changes metadata reuse from a boolean `ETag` validation step into a full metadata reconcile step, and adds per-file singleflight so concurrent requests share one upstream metadata probe.

## Problem Statement

Current issues:

1. metadata validation is modeled as `can_reuse_cached_etag -> bool`
2. the validation path already probes upstream and learns fresh metadata, but only the `ETag` is used as the decision input
3. concurrent requests for the same file can trigger repeated upstream metadata probes
4. there is no dedicated per-file concurrency control for upstream metadata lookup

Consequences:

- stale `X-Repo-Commit` can survive even when upstream revision identity changed
- concurrent `HEAD` requests can duplicate upstream load
- metadata correctness depends on path identity alone, which is not enough for HF snapshot semantics

## Goals

- Replace boolean metadata reuse with explicit metadata reconcile behavior.
- Preserve file content reuse when `ETag` is unchanged.
- Refresh metadata fields, especially `X-Repo-Commit`, whenever upstream metadata changes.
- Deduplicate concurrent upstream metadata probes per file.
- Keep provider-specific probe transport rules separate from provider-neutral reconcile logic.

## Non-Goals

- No schema redesign in this change.
- No commit-aware persistent metadata identity in this change.
- No redesign of file chunk storage.
- No global serialization of all metadata probes.

## Current Behavior

### HEAD path

Today, the `HEAD` route does roughly this:

1. check whether cached metadata exists
2. if cached `x_repo_commit` exists and cached `etag` exists, call `validate_file_etag(...)`
3. `validate_file_etag(...)` probes upstream metadata through `fetch_file_metadata(...)`
4. `fetch_file_metadata(...)` writes the latest metadata into SQLite
5. `validate_file_etag(...)` returns `true` if upstream `etag == cached_etag`
6. `HEAD` serves cached metadata if validation returned `true`

The main flaw is step 5: it ignores upstream `X-Repo-Commit` even though that value has already been fetched.

### Probe transport

The project already distinguishes first-hop probe method by provider and route shape:

- HuggingFace normally uses `HEAD`
- ModelScope `/repo?...` metadata probing may use first-hop `GET`

This difference is already encoded in the probe layer and should stay there.

## Proposed Design

### 1. Replace boolean validation with metadata reconcile

Instead of asking whether cached metadata is reusable, HugRS should always reconcile the latest upstream metadata against local metadata.

The new conceptual flow is:

1. fetch upstream metadata
2. load current DB metadata, if any
3. compare upstream and local state
4. decide whether cached file record can stay or must be deleted
5. write the latest metadata into DB
6. return the final DB-backed metadata for the response

The reconcile function becomes the source of truth for `HEAD` metadata freshness.

### 2. Reconcile rules

The compare rules for one file are:

#### Case A: upstream `ETag` is missing

This is treated as non-reusable content identity.

Action:

- delete the local file record for `name + source`
- recreate or reinitialize metadata using the latest upstream probe result

Reason:

- without upstream `ETag`, current logic cannot safely decide whether cached content identity still matches

#### Case B: cached `ETag` is missing

This is also treated as non-reusable content identity.

Action:

- delete the local file record for `name + source`
- recreate or reinitialize metadata from the latest upstream probe result

#### Case C: `ETag` changed

This means content identity changed.

Action:

- delete the local file record for `name + source`
- recreate or reinitialize metadata from the latest upstream probe result

#### Case D: `ETag` unchanged

This means cached content can remain reusable, but metadata must still be refreshed.

Action:

- keep the existing file content and chunk links
- update metadata fields from upstream, including:
  - `x_repo_commit`
  - `x_linked_size`
  - `x_linked_etag`
  - `content_type`
  - size-related metadata if needed

This is the key fix for the split-snapshot bug.

#### Case E: invalid or inconsistent size

If upstream size cannot be determined, or if the size conflicts in a way that makes cached metadata unsafe, treat the file record as stale.

Action:

- delete the local file record
- recreate or reinitialize metadata from upstream only if the final upstream metadata is sufficient

The exact handling should remain conservative.

### 3. Add per-file singleflight for metadata probe

Concurrent requests for the same file should share one upstream metadata probe.

This design adds a per-file inflight table keyed by file identity in request space:

- key: `source + cache_name`

Behavior:

1. first request for a key becomes the leader
2. leader performs the full reconcile flow
3. later requests for the same key become waiters
4. waiters do not query upstream again
5. when the leader finishes, all waiters observe the same result
6. all requests then load final metadata from DB for response construction

Properties:

- same file: one upstream probe at a time
- different files: no blocking between keys
- concurrency control is narrow and local, not global

### 4. Keep one reconcile flow, two probe transports

Provider-specific transport details should remain inside metadata probing, not inside reconcile decision logic.

Meaning:

- one shared reconcile function for both HuggingFace and ModelScope
- provider-specific first-hop method selection stays in probe code
- provider-specific URL patterns stay in route or probe construction

This avoids duplicating deletion and refresh policy while still respecting provider differences.

## Architecture

### New conceptual helper

Introduce a single metadata entry point along the lines of:

- `reconcile_file_metadata(url, name, repo, source, user_agent)`

Responsibilities:

1. enter singleflight for `source + cache_name`
2. if leader:
   - fetch upstream metadata
   - compare against DB
   - delete stale file record if needed
   - ensure metadata row exists
   - write latest metadata
   - publish result to waiters
3. if waiter:
   - wait for leader result
4. load final metadata from DB and return it to caller

This replaces the current boolean-style `validate_file_etag(...)` role in the `HEAD` path.

### Data model impact

No schema change is required in this iteration.

The existing `files` table remains path-identity-based by `UNIQUE(name, source)`.

This means the change improves correctness under the current model, but does not yet redesign persistent identity to be commit-aware.

## Error Handling

### Upstream probe failure

If the leader fails during upstream metadata fetch:

- the reconcile call fails
- all waiters for the same key receive the same failure
- the inflight key must be removed

Do not silently fall back to stale metadata and claim success in this new reconcile path.

### Database write failure

If the leader cannot delete or persist metadata:

- return failure to leader and waiters
- remove inflight state
- do not publish a success result based on incomplete persistence

### Inflight cleanup

Inflight state must always be removed on both success and failure.

This is required to avoid permanently stuck keys.

## Testing Requirements

Add focused tests for both correctness and concurrency.

### Required correctness tests

1. same `ETag`, different `X-Repo-Commit`
   - expected: metadata is updated to the new commit
   - expected: file content is not discarded just because commit changed

2. changed `ETag`
   - expected: file record is deleted and metadata is rebuilt

3. missing upstream `ETag`
   - expected: local file record is treated as non-reusable and rebuilt conservatively

4. missing cached `ETag`
   - expected: local file record is treated as non-reusable and rebuilt conservatively

### Required concurrency tests

1. two concurrent `HEAD` requests for the same file
   - expected: exactly one upstream metadata probe
   - expected: both requests receive the same final metadata

2. concurrent requests for different files
   - expected: no shared blocking between keys

3. failed leader probe with waiters
   - expected: all waiters fail consistently
   - expected: inflight state is cleaned up

### Provider coverage

At least one probe-path test should cover:

- HuggingFace first-hop `HEAD`
- ModelScope `repo?...` first-hop `GET`

The reconcile logic itself should remain provider-neutral.

## Risks

### Keeping path-based persistent identity

This design fixes the immediate stale-metadata bug, but it does not eliminate all semantic limits of `UNIQUE(name, source)`.

If future behavior requires persistent coexistence of multiple commit views for the same path, an additional schema redesign may still be needed.

### Over-broad delete behavior

Deleting the file record on missing `ETag` is intentionally conservative. Review implementation carefully to ensure this only removes file metadata and chunk links, not backend chunk data itself.

### Async coordination complexity

The singleflight helper must avoid lock-order mistakes and must not hold broad locks across network I/O.

## Decision Summary

- metadata reuse changes from boolean `ETag` validation to explicit reconcile
- same `ETag` does not mean metadata can be reused unchanged
- `X-Repo-Commit` must be refreshed from upstream even when content is reusable
- concurrent metadata probes are deduplicated per `source + cache_name`
- probe transport remains provider-specific, reconcile policy remains shared
