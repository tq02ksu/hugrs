# HuggingFace Protocol Notes

This document is a protocol-facing reference for HuggingFace compatibility in HugRS.

It focuses on three things:

- which HuggingFace-compatible routes HugRS exposes
- what upstream HuggingFace endpoints return at the HTTP layer
- what client-visible behavior HugRS should preserve for different classes of clients

It intentionally does not describe internal cache, database, or storage design unless a client-visible rule depends on it.

## Purpose

This document describes HuggingFace upstream response behavior and the client-visible behavior HugRS should preserve.

This document is intentionally limited to protocol and interface behavior:

- what upstream endpoints return
- what HugRS returns to clients
- which headers matter for compatibility
- which request patterns are expected from common clients

This document does not describe internal cache or storage implementation.

## Client Usage Modes

HuggingFace-compatible usage currently falls into these client categories:

### 1. Hub API clients

- `huggingface_hub`
- `hf download`
- `snapshot_download`
- `hf_hub_download`

These are the most important compatibility targets for revision resolution, `HEAD`, `ETag`, and `X-Repo-Commit` behavior.

### 2. Frameworks layered on top of `huggingface_hub`

- `transformers`
- `diffusers`
- `sentence-transformers`
- `vllm`
- TEI / TGI style model-serving startup flows

These often inherit the HTTP behavior of `huggingface_hub`, but should still be treated as separate compatibility consumers when real traces differ.

### 3. Direct HTTP clients

- `curl`
- `wget`
- `aria2`
- custom HTTP downloaders

These care most about visible `HEAD`, `GET`, `Range`, `Content-Length`, `Content-Range`, `ETag`, and `Content-Type` semantics.

### 4. Git and Git LFS clients

- `git clone`
- `git lfs pull`

These depend on git smart HTTP passthrough and LFS URL rewriting back through HugRS.

### 5. Helper scripts and wrappers

- `hfd.sh`

These are often thin wrappers around HuggingFace-compatible file APIs, but may have their own request sequencing.

## Route Surface

HugRS exposes these HuggingFace-compatible routes:

- `GET/HEAD /api/models/{org}/{repo}`
- `GET/HEAD /api/models/{org}/{repo}/revision/{revision}`
- `GET/HEAD /api/models/{org}/{repo}/{*suffix}`
- `GET/HEAD /{org}/{repo}/resolve/{revision}/{*path}`
- `GET/HEAD /api/resolve-cache/{repo_type}/{org}/{repo}/{revision}/{*path}`

It also exposes explicit `/hf/...` equivalents.

## Upstream-Facing Behavior

### Model metadata endpoints

Typical endpoint shape:

- `/api/models/{org}/{repo}/revision/{revision}`

Observed purpose:

- resolve symbolic revisions such as `main` to a concrete repository commit

HugRS client-visible rule:

- forward upstream status, headers, and body as-is except for normal hop-by-hop header cleanup
- do not rewrite the JSON body

## File Endpoints

### Core file route

Typical endpoint shape:

- `/{org}/{repo}/resolve/{revision}/{path}`

For HuggingFace compatibility, HugRS must preserve these visible semantics:

- client can send `HEAD`
- client can send `GET`
- client can send `Range GET`
- upstream redirects must not leak to the client

### HEAD behavior

Observed HuggingFace behavior:

- `HEAD` on a file route can return metadata on the first hop
- the first hop may be a redirecting response rather than the final file host
- that first hop can include headers such as:
  - `X-Repo-Commit`
  - `X-Linked-Size`
  - `X-Linked-ETag`
- the final file host can provide headers such as:
  - `Content-Length`
  - `ETag`
  - `Content-Type`

HugRS client-visible rule:

- do not return the upstream `302` to the client
- follow the redirect internally
- return `200` to the client when upstream metadata lookup succeeds
- merge the metadata from the redirect hop and the final file hop into the returned response

Expected visible response headers from HugRS `HEAD`:

- `Content-Length` when upstream provides a reliable size
- `ETag` when upstream provides it
- `Content-Type` when upstream provides it
- `X-Repo-Commit` when upstream provides it
- `X-Linked-Size` when upstream provides it
- `X-Linked-ETag` when upstream provides it
- `Accept-Ranges: bytes`

### Meaning of `X-Repo-Commit`

In current observed HuggingFace flows, `X-Repo-Commit` should be treated as the resolved repository git commit SHA for the requested revision.

Example:

- client resolves `main`
- upstream identifies a commit such as `4f8aa297f7866eaca7d955ee2960273d23010cd3`
- file metadata returned for that revision is expected to carry the same commit identity through `X-Repo-Commit`

This matters because some clients assemble local snapshot directories keyed by repository commit SHA.

### GET behavior

Observed HuggingFace behavior:

- upstream file GET may redirect before the final file content is served
- clients may request either the full file or a byte range

HugRS client-visible rule:

- follow redirects internally
- return final file bytes to the client
- preserve upstream-visible metadata instead of inventing replacements

Expected visible response properties from HugRS `GET`:

- final file body
- `ETag` when upstream provides it
- `Content-Type` when upstream provides it
- `Content-Length` or `Content-Range`, depending on request type
- `Accept-Ranges: bytes`
- `X-Repo-Commit` when that metadata is available from the upstream resolve flow

### Range GET behavior

Expected user-visible behavior:

- `Range` requests should succeed when upstream supports them
- clients should receive `206 Partial Content`
- `Content-Range` should be correct
- `Accept-Ranges: bytes` should be present

### Upstream full-file response on Range requests

Some HuggingFace mirrors and CDN endpoints may ignore the `Range` header
and return `200 OK` with the entire file body.  When this happens:

- the response status is `200` instead of `206`
- the response body contains the full file, not just the requested range

HugRS internal behavior when this occurs:

- detect the mismatch (status `200` + body larger than the requested range)
- extract only the requested byte range from the full body
- store the extracted chunk normally

This does not change client-visible behavior. Clients still receive the
correct partial data through the normal chunk-serving path.

## Error Handling

HugRS client-visible rule:

- if upstream returns a non-success, non-redirect response, return that upstream status to the client
- preserve upstream response headers when possible
- do not replace an upstream `404` or similar response with a generic local success or redirect

This rule applies to both metadata and file routes.

## Client Patterns

### `huggingface_hub`

Observed pattern from traces:

1. call model revision API to resolve `main`
2. issue many `HEAD` requests to `resolve/{resolved_commit}/...`
3. issue `GET` only when bytes are needed locally

Compatibility requirement for HugRS:

- `HEAD` must return stable metadata for the resolved revision
- `X-Repo-Commit` and `ETag` must reflect the same upstream file view from the client perspective

### Other HF-based clients

Clients such as `hf download`, `huggingface_hub`, and tools layered on top of them are expected to rely on the same visible HuggingFace semantics above.

### `hfd.sh`

Project history shows explicit support for `hfd.sh`.

Expected compatibility rule:

- when `HF_ENDPOINT` points to HugRS, `hfd.sh` should remain a normal HuggingFace-compatible consumer of the file routes above

`TODO`:

- capture a full modern `hfd.sh` trace and document its exact `HEAD` vs `GET` pattern

## Git and Git LFS

HugRS also exposes HuggingFace-compatible git and git-lfs routes.

Visible behavior rules:

- git smart HTTP responses are proxied through
- LFS batch responses are rewritten so download URLs point back to HugRS
- the final file download behavior must still match the ordinary HuggingFace file rules above

## TODO

- document exact HuggingFace first-hop and final-hop header sets from more real traces
- document observed differences, if any, between `hf download`, `snapshot_download`, and other Hub clients at the HTTP layer
