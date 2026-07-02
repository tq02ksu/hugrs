# ModelScope Protocol Notes

This document is a protocol-facing reference for ModelScope compatibility in HugRS.

It focuses on three things:

- which ModelScope-compatible routes HugRS exposes
- what upstream ModelScope endpoints return at the HTTP layer
- what client-visible behavior HugRS should preserve for different classes of clients

It intentionally does not describe internal cache, database, or storage design unless a client-visible rule depends on it.

## Purpose

This document describes ModelScope upstream response behavior and the client-visible behavior HugRS should preserve.

This document is intentionally limited to protocol and interface behavior:

- what upstream endpoints return
- what HugRS returns to clients
- which headers and methods matter for compatibility
- where ModelScope differs from HuggingFace at the HTTP layer

This document does not describe internal cache or storage implementation.

## Client Usage Modes

ModelScope-compatible usage currently falls into these client categories:

### 1. Official CLI and SDK

- `modelscope download`
- ModelScope Python SDK

These are the main compatibility targets for `/ms/...` route shape, `/repo?...` file access, and internal first-hop `GET` metadata probing where upstream `HEAD` is insufficient.

### 2. Frameworks layered on top of ModelScope

- `swift`
- inference or training tools that fetch models through ModelScope
- project-specific wrappers around the ModelScope SDK

These may inherit the same HTTP behavior as the official SDK, but should still be verified with traces when they are important HugRS consumers.

### 3. Direct HTTP clients

- `curl`
- `wget`
- `aria2`
- custom HTTP downloaders

These care most about visible `HEAD`, `GET`, `Range`, `Content-Length`, `Content-Range`, `ETag`, and `Content-Type` semantics.

### 4. Git and Git LFS clients

- `git clone`
- `git lfs pull`

These depend on git smart HTTP passthrough and LFS URL rewriting back through HugRS under `/ms/...`.

## Route Surface

HugRS exposes these ModelScope-compatible routes:

- `GET/HEAD /ms/api/v1/models/{org}/{repo}`
- `GET/HEAD /ms/api/v1/models/{org}/{repo}/revision/{revision}`
- `GET/HEAD /ms/api/v1/models/{org}/{repo}/{*suffix}`
- `GET/HEAD /ms/api/v1/models/{org}/{repo}/repo?Revision={revision}&FilePath={path}`
- `GET/HEAD /ms/{org}/{repo}/resolve/{revision}/{*path}`

It also exposes ModelScope-compatible git and git-lfs routes under `/ms/...`.

## Upstream-Facing Behavior

### Model metadata endpoints

Typical endpoint shapes:

- `/ms/api/v1/models/{org}/{repo}`
- `/ms/api/v1/models/{org}/{repo}/revision/{revision}`

HugRS client-visible rule:

- forward upstream status, headers, and body as-is except for normal hop-by-hop header cleanup
- do not rewrite the JSON body

## File Endpoints

### Two file-access shapes

ModelScope compatibility requires two request shapes:

1. `/ms/{org}/{repo}/resolve/{revision}/{path}`
2. `/ms/api/v1/models/{org}/{repo}/repo?Revision={revision}&FilePath={path}`

The `/repo?...` shape is important because some ModelScope client flows use it instead of a path-shaped `resolve` route.

### HEAD support differences

Important difference from HuggingFace:

- some ModelScope file-access paths do not provide a usable `HEAD` response
- in particular, the `/repo?...` path may not support `HEAD` in a way that returns usable file metadata

Current observed pattern:

- `HEAD /repo?...` may not be sufficient
- `GET /repo?...` can be needed as the first hop to obtain redirect metadata

HugRS client-visible rule:

- clients may still send `HEAD` to HugRS
- HugRS may internally use upstream `GET` for ModelScope metadata discovery on `/repo?...`
- this transport difference must not leak to the client
- when metadata lookup succeeds, HugRS still returns a normal `HEAD` response to the client

### Redirect behavior

Observed ModelScope pattern:

- the `/repo?...` endpoint can return a redirect response
- redirect metadata can include values such as `X-Linked-ETag`
- the final file host can then provide the final `ETag`, `Content-Length`, and `Content-Type`

HugRS client-visible rule:

- do not return the upstream redirect to the client
- follow the redirect internally
- return the final success response to the client
- preserve useful redirect metadata in the visible response when available

### HEAD behavior

For successful metadata lookup on ModelScope file routes, HugRS should return to the client:

- `Content-Length` when upstream provides a reliable size
- `ETag` when upstream provides it
- `Content-Type` when upstream provides it
- `X-Linked-ETag` when upstream provides it
- `Accept-Ranges: bytes`

`TODO`:

- identify whether ModelScope exposes a stable commit-equivalent response header on file routes

### GET behavior

Observed ModelScope-compatible expectation:

- clients may request the full file through `/repo?...` or `/resolve/...`
- upstream may redirect before final file bytes are served

HugRS client-visible rule:

- follow redirects internally
- return final file bytes to the client
- preserve upstream metadata rather than inventing replacements

Expected visible response properties from HugRS `GET`:

- final file body
- `ETag` when upstream provides it
- `Content-Type` when upstream provides it
- `Content-Length` or `Content-Range`, depending on request type
- `Accept-Ranges: bytes`

### Range GET behavior

Expected user-visible behavior:

- `Range` requests should succeed when upstream supports them
- clients should receive `206 Partial Content`
- `Content-Range` should be correct
- `Accept-Ranges: bytes` should be present

## User-Agent Behavior

This is a client-visible compatibility rule for ModelScope.

Observed issue:

- some ModelScope CDN paths may reject requests without an acceptable `User-Agent`

HugRS user-visible rule:

- when the inbound client request contains `User-Agent`, HugRS forwards it on ModelScope file requests
- this includes the initial request and internally followed redirect requests
- HugRS must not invent a fallback `User-Agent` when the client did not send one

## Error Handling

HugRS client-visible rule:

- if upstream returns a non-success, non-redirect response, return that upstream status to the client
- preserve upstream response headers when possible
- do not hide ModelScope upstream errors behind a generic local success response

## Client Patterns

### `modelscope` CLI / SDK

Expected endpoint pattern in current project docs:

- `modelscope download ... --endpoint http://host:port/ms`

Compatibility requirement for HugRS:

- preserve the `/ms/...` protocol surface expected by ModelScope clients
- support `/repo?...` flows even when upstream metadata probing internally requires `GET` instead of `HEAD`

`TODO`:

- capture a full current `modelscope download` trace and document the exact request order
- verify whether CLI and SDK use identical HTTP patterns

### Git and Git LFS

Visible behavior rules:

- git smart HTTP responses are proxied through
- LFS batch responses are rewritten so download URLs point back to HugRS under `/ms/...`
- the final file download behavior must still match the ordinary ModelScope file rules above

## TODO

- document exact ModelScope redirect and CDN header sets from more real traces
- identify whether ModelScope exposes a stable file-level commit or revision identity header
