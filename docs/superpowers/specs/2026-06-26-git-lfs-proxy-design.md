# Git/LFS Proxy Design

## Scope

Add `git clone` support to HugRS for both HuggingFace and ModelScope upstreams.
Download-only (no push).

## Architecture

`git clone` involves two layers:

| Layer | Protocol | Endpoints | Handling |
|-------|----------|-----------|----------|
| Git metadata | git smart HTTP | `info/refs`, `git-upload-pack` | Pass-through proxy (binary, no transform) |
| LFS large files | git-lfs batch API | `lfs/objects/batch` | Pass-through proxy + URL rewrite |

Integrity: git uses SHA-1 on objects (unchanged), git-lfs uses SHA-256 on downloaded content (unchanged).
We rewrite only the download URL in the LFS batch response — not the content.

## New Module: `src/git.rs`

Three handlers:

### `git_info_refs(state, Path(org, repo), headers)`

- GET `/{org}/{repo}/info/refs?service=git-upload-pack`
- Forwards to upstream via `http_client.get()`, passes through response headers and body as-is.
- Content-Type: `application/x-git-upload-pack-advertisement`

### `git_upload_pack(state, Path(org, repo), headers, body)`

- POST `/{org}/{repo}/git-upload-pack`
- Forwards to upstream via `http_client.post()`, passes through body and response as-is.
- Content-Type: `application/x-git-upload-pack-result`

### `lfs_batch(state, Path(org, repo), headers, request, body)`

- POST `/{org}/{repo}/info/lfs/objects/batch`
- Forwards the JSON body to upstream.
- Parses upstream response JSON; for each object's `actions.download.href`, replaces the upstream base URL with the proxy URL derived from the incoming request (scheme + host).
- Returns modified JSON.

## URL Rewriting (LFS)

Upstream response example:
```json
{"objects":[{"oid":"sha256:abc","actions":{"download":{"href":"https://huggingface.co/org/repo/resolve/main/file.bin"}}}]}
```

Rewritten:
```json
{"objects":[{"oid":"sha256:abc","actions":{"download":{"href":"http://127.0.0.1:3000/org/repo/resolve/main/file.bin"}}}]}
```

Rewrite rules:
- Replace `{upstream_endpoint}/{org}/{repo}/resolve/` → `{proxy_base}/{org}/{repo}/resolve/`
- If upstream href is a CDN URL (e.g. lfs.huggingface.co, xet-bridge), rewrite it too using the same host replacement.
- The rewritten URL will hit the proxy's existing `/resolve/` routes, which handle caching + SHA256 verify-on-read.

## Routes (9 new routes in server.rs)

Three path prefixes, each with 3 git endpoints:

```
Legacy:   /{org}/{repo}/info/refs               GET  -> git_info_refs
          /{org}/{repo}/git-upload-pack          POST -> git_upload_pack
          /{org}/{repo}/info/lfs/objects/batch   POST -> lfs_batch

/hf/:     /hf/{org}/{repo}/info/refs             GET  -> git_info_refs
          /hf/{org}/{repo}/git-upload-pack       POST -> git_upload_pack
          /hf/{org}/{repo}/info/lfs/objects/batch POST -> lfs_batch

/ms/:     /ms/{org}/{repo}/info/refs             GET  -> git_info_refs
          /ms/{org}/{repo}/git-upload-pack       POST -> git_upload_pack
          /ms/{org}/{repo}/info/lfs/objects/batch POST -> lfs_batch
```

No conflict with existing `/{org}/{repo}/resolve/{revision}/{*path}` because axum matches most-specific route first (3rd segment differs: `info`, `git-upload-pack`, `resolve`).

## Client Usage

```bash
git clone http://127.0.0.1:3000/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/hf/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/ms/qwen/Qwen3.5-0.8B
```

Git with curl backend automatically sends LFS batch requests through the proxy since the remote URL points to it.

## Files Changed

- `src/git.rs` — new module (git + LFS handlers, URL rewrite logic)
- `src/lib.rs` — add `pub mod git;`
- `src/server.rs` — add 9 git routes, source-aware dispatch logic
- `README.md` / `README_zh.md` — add git clone client usage section
- `openapi.yaml` — add git/LFS endpoints
