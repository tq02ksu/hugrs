# Git/LFS Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `git clone` support via git smart HTTP + LFS batch API proxy for both HuggingFace and ModelScope.

**Architecture:** New `src/git.rs` module with three handlers (info/refs pass-through, git-upload-pack pass-through, LFS batch with URL rewrite). Nine new routes in server.rs across legacy, `/hf/`, and `/ms/` prefixes.

**Tech Stack:** axum, reqwest, serde_json

---

### Task 1: Create `src/git.rs` — git smart HTTP + LFS proxy module

**Files:**
- Create: `src/git.rs`

- [ ] **Step 1: Write the `rewrite_lfs_urls` function**

Write `src/git.rs` with the URL rewriting helper and three handler functions:

```rust
use axum::{
    extract::{OriginalUri, Path, Request, State},
    http::{HeaderMap, Method},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::server::{hub_config, AppState};

pub fn rewrite_lfs_urls(
    body: &str,
    proxy_base: &str,
    upstream_endpoint: &str,
) -> anyhow::Result<String> {
    let mut json: Value = serde_json::from_str(body)?;

    if let Some(objects) = json["objects"].as_array_mut() {
        for obj in objects {
            if let Some(actions) = obj["actions"].as_object_mut() {
                if let Some(download) = actions.get_mut("download") {
                    if let Some(href) = download["href"].as_str() {
                        let rewritten = rewrite_href(href, proxy_base, upstream_endpoint);
                        download["href"] = Value::String(rewritten);
                    }
                }
            }
        }
    }

    Ok(serde_json::to_string(&json)?)
}

fn rewrite_href(href: &str, proxy_base: &str, upstream_endpoint: &str) -> String {
    let endpoints = [
        upstream_endpoint.to_string(),
        "https://huggingface.co".to_string(),
        "https://hf-mirror.com".to_string(),
        "https://cdn-lfs.huggingface.co".to_string(),
        "https://cdn-lfs-us-1.huggingface.co".to_string(),
        "https://lfs.huggingface.co".to_string(),
        "https://www.modelscope.cn".to_string(),
    ];

    for ep in &endpoints {
        if let Some(rest) = href.strip_prefix(ep) {
            return format!("{}{}", proxy_base, rest);
        }
    }

    href.to_string()
}

pub async fn git_info_refs(
    State(state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Response, crate::server::AppError> {
    let source = git_source_from_path(uri.path());
    let (endpoint, client, _) = hub_config(&state, source);
    let query = uri.query().unwrap_or("");
    let upstream_url = format!(
        "{}/{}/{}.git/info/refs?{}",
        endpoint, owner, repo, query
    );
    git_proxy_pass(client, &upstream_url, Method::GET, headers, None).await
}

pub async fn git_upload_pack(
    State(state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    request: Request,
) -> Result<Response, crate::server::AppError> {
    let source = git_source_from_path(uri.path());
    let (_, client, _) = hub_config(&state, source);
    let endpoint = match source {
        "ms" => &state.config.modelscope.endpoint,
        _ => &state.config.huggingface.endpoint,
    };
    let upstream_url = format!(
        "{}/{}/{}.git/git-upload-pack",
        endpoint, owner, repo
    );

    let body = axum::body::to_bytes(request.into_body(), 50 * 1024 * 1024)
        .await
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))?;

    git_proxy_pass(client, &upstream_url, Method::POST, headers, Some(body.to_vec())).await
}

pub async fn lfs_batch(
    State(state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    request: Request,
) -> Result<Response, crate::server::AppError> {
    let source = git_source_from_path(uri.path());
    let (endpoint, client, _) = hub_config(&state, source);
    let upstream_url = format!(
        "{}/{}/{}.git/info/lfs/objects/batch",
        endpoint, owner, repo
    );

    let body = axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024)
        .await
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))?;

    let mut req = client.post(&upstream_url);
    req = req.header("Content-Type", "application/vnd.git-lfs+json");
    req = req.header("Accept", "application/vnd.git-lfs+json");

    let token = match source {
        "ms" => &state.config.modelscope.token,
        _ => &state.config.huggingface.token,
    };
    if let Some(ref token) = token {
        req = req.header("Authorization", format!("Bearer {}", token));
    }
    if let Some(ua) = headers.get("user-agent").and_then(|v| v.to_str().ok()) {
        req = req.header("User-Agent", ua);
    }

    let resp = req.body(body).send().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("LFS upstream error: {}", e))
    })?;

    let status = resp.status();
    let resp_text = resp.text().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("LFS read error: {}", e))
    })?;

    let scheme = "http";
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1:3000");
    let proxy_base = format!("{}://{}", scheme, host);

    let rewritten = rewrite_lfs_urls(&resp_text, &proxy_base, endpoint)
        .map_err(|e| crate::server::AppError::from(e))?;

    axum::response::Response::builder()
        .status(status)
        .header("Content-Type", "application/vnd.git-lfs+json")
        .body(axum::body::Body::from(rewritten))
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))
}

async fn git_proxy_pass(
    client: &reqwest::Client,
    url: &str,
    method: Method,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
) -> Result<Response, crate::server::AppError> {
    let mut req = client.request(method.clone(), url);

    for (key, value) in headers.iter() {
        let key_str = key.as_str().to_lowercase();
        if key_str != "host" && key_str != "content-length" && key_str != "transfer-encoding" {
            req = req.header(key, value);
        }
    }

    if let Some(ref b) = body {
        req = req.body(b.clone());
    }

    let resp = req.send().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("git upstream error: {}", e))
    })?;

    let status = resp.status();
    let resp_headers = resp.headers().clone();
    let resp_body = resp.bytes().await.map_err(|e| {
        crate::server::AppError::from(anyhow::anyhow!("git read error: {}", e))
    })?;

    let mut builder = axum::response::Response::builder().status(status);
    for (key, value) in resp_headers.iter() {
        let key_str = key.as_str().to_lowercase();
        if key_str != "transfer-encoding" && key_str != "content-length" {
            builder = builder.header(key, value);
        }
    }

    builder
        .body(axum::body::Body::from(resp_body))
        .map_err(|e| crate::server::AppError::from(anyhow::anyhow!("{}", e)))
}

fn git_source_from_path(path: &str) -> &str {
    if path.starts_with("/ms/") {
        "ms"
    } else {
        "hf"
    }
}
```

- [ ] **Step 2: Build check**

Run: `cargo build`
Expected: Should compile (modulo missing `hub_config` visibility fix — see Task 2).

---

### Task 2: Wire `src/git.rs` into `src/lib.rs` and `src/server.rs`

**Files:**
- Modify: `src/lib.rs`
- Modify: `src/server.rs`

- [ ] **Step 1: Register module in `src/lib.rs`**

Add after line 3 (`pub mod hf;`):

```rust
pub mod git;
```

- [ ] **Step 2: Fix visibility — make `hub_config`, `AppState`, `AppError` public (they are already `pub`)**

Check that `hub_config` is `pub` in `server.rs`. It's currently `fn hub_config(...)` (non-pub). Change it to `pub fn hub_config(...)`.

Edit `src/server.rs` line 146:

Old:
```rust
fn hub_config<'a>(
```
New:
```rust
pub fn hub_config<'a>(
```

- [ ] **Step 3: Add `post` import in `src/server.rs`**

Edit `src/server.rs` line 9, add `post`:

Old:
```rust
use axum::{
    extract::{OriginalUri, Path, Query, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
```
New:
```rust
use axum::{
    extract::{OriginalUri, Path, Query, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
```

- [ ] **Step 4: Add git routes in `src/server.rs` `run()` function**

Add git routes after the legacy resolve route (line 62) and before `/hf/` prefix routes (line 63). Insert after line 62:

```rust
        // Git/LFS proxy (legacy)
        .route(
            "/{org}/{repo}/info/refs",
            get(git::git_info_refs),
        )
        .route(
            "/{org}/{repo}/git-upload-pack",
            post(git::git_upload_pack),
        )
        .route(
            "/{org}/{repo}/info/lfs/objects/batch",
            post(git::lfs_batch),
        )
```

Add after `/hf/` resolve route (after line 79), before `/ms/` routes (line 80):

```rust
        // Git/LFS proxy (/hf/)
        .route(
            "/hf/{org}/{repo}/info/refs",
            get(git::git_info_refs),
        )
        .route(
            "/hf/{org}/{repo}/git-upload-pack",
            post(git::git_upload_pack),
        )
        .route(
            "/hf/{org}/{repo}/info/lfs/objects/batch",
            post(git::lfs_batch),
        )
```

Add after `/ms/` resolve route (after line 100), before `/api/stats` (line 101):

```rust
        // Git/LFS proxy (/ms/)
        .route(
            "/ms/{org}/{repo}/info/refs",
            get(git::git_info_refs),
        )
        .route(
            "/ms/{org}/{repo}/git-upload-pack",
            post(git::git_upload_pack),
        )
        .route(
            "/ms/{org}/{repo}/info/lfs/objects/batch",
            post(git::lfs_batch),
        )
```

- [ ] **Step 5: Build check**

Run: `cargo build`
Expected: Compiles successfully.

- [ ] **Step 6: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No warnings.

- [ ] **Step 7: Commit**

```bash
git add src/git.rs src/lib.rs src/server.rs
git commit -m "feat: add git/LFS proxy (info/refs, upload-pack, lfs batch)"
```

---

### Task 3: Update README and openapi.yaml

**Files:**
- Modify: `README.md`
- Modify: `README_zh.md`
- Modify: `openapi.yaml`

- [ ] **Step 1: Add git clone section to `README.md`**

Add after `modelscope download` block and before the proxy explanation line (after line 74):

```markdown
### git clone

```bash
git clone http://127.0.0.1:3000/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/hf/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/ms/qwen/Qwen3.5-0.8B
```
```

- [ ] **Step 2: Add git clone section to `README_zh.md`**

Same change, Chinese equivalent after line 75:

```markdown
### git clone

```bash
git clone http://127.0.0.1:3000/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/hf/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/ms/qwen/Qwen3.5-0.8B
```
```

- [ ] **Step 3: Add git/LFS endpoints to `openapi.yaml`**

Add after the `/api/agent-harnesses` path block and before `components:`, under the App tag but git routes belong to new tags. Add after the `/api/agent-harnesses` block:

```yaml
  # ═══════════════════════════════════════════════════════════════════════
  # Git/LFS (HuggingFace — legacy)
  # ═══════════════════════════════════════════════════════════════════════

  /{org}/{repo}/info/refs:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
      - name: service
        in: query
        required: true
        schema:
          type: string
          example: git-upload-pack
    get:
      tags: [HuggingFace, Git]
      summary: Git smart HTTP info/refs (legacy)
      operationId: git_info_refs
      responses:
        "200":
          description: Git upload-pack advertisement
          content:
            application/x-git-upload-pack-advertisement:
              schema:
                type: string
                format: binary

  /{org}/{repo}/git-upload-pack:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
    post:
      tags: [HuggingFace, Git]
      summary: Git smart HTTP upload-pack (legacy)
      operationId: git_upload_pack
      requestBody:
        content:
          application/x-git-upload-pack-request:
            schema:
              type: string
              format: binary
      responses:
        "200":
          description: Git packfile data
          content:
            application/x-git-upload-pack-result:
              schema:
                type: string
                format: binary

  /{org}/{repo}/info/lfs/objects/batch:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
    post:
      tags: [HuggingFace, Git]
      summary: Git LFS batch API (legacy)
      description: |
        Proxies LFS batch request to upstream, rewrites download URLs
        in the response to point back to the proxy.
      operationId: lfs_batch
      requestBody:
        required: true
        content:
          application/vnd.git-lfs+json:
            schema:
              type: object
      responses:
        "200":
          description: LFS batch response with rewritten download URLs
          content:
            application/vnd.git-lfs+json:
              schema:
                type: object

  /hf/{org}/{repo}/info/refs:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
      - name: service
        in: query
        required: true
        schema:
          type: string
          example: git-upload-pack
    get:
      tags: [HuggingFace, Git]
      summary: Git smart HTTP info/refs (/hf prefix)
      operationId: hf_prefixed_git_info_refs
      responses:
        "200":
          description: Git upload-pack advertisement

  /hf/{org}/{repo}/git-upload-pack:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
    post:
      tags: [HuggingFace, Git]
      summary: Git smart HTTP upload-pack (/hf prefix)
      operationId: hf_prefixed_git_upload_pack
      responses:
        "200":
          description: Git packfile data

  /hf/{org}/{repo}/info/lfs/objects/batch:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
    post:
      tags: [HuggingFace, Git]
      summary: Git LFS batch API (/hf prefix)
      operationId: hf_prefixed_lfs_batch
      requestBody:
        required: true
        content:
          application/vnd.git-lfs+json:
            schema:
              type: object
      responses:
        "200":
          description: LFS batch response with rewritten download URLs

  /ms/{org}/{repo}/info/refs:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
      - name: service
        in: query
        required: true
        schema:
          type: string
          example: git-upload-pack
    get:
      tags: [ModelScope, Git]
      summary: Git smart HTTP info/refs (/ms prefix)
      operationId: ms_git_info_refs
      responses:
        "200":
          description: Git upload-pack advertisement

  /ms/{org}/{repo}/git-upload-pack:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
    post:
      tags: [ModelScope, Git]
      summary: Git smart HTTP upload-pack (/ms prefix)
      operationId: ms_git_upload_pack
      responses:
        "200":
          description: Git packfile data

  /ms/{org}/{repo}/info/lfs/objects/batch:
    parameters:
      - $ref: "#/components/parameters/Org"
      - $ref: "#/components/parameters/Repo"
    post:
      tags: [ModelScope, Git]
      summary: Git LFS batch API (/ms prefix)
      operationId: ms_lfs_batch
      requestBody:
        required: true
        content:
          application/vnd.git-lfs+json:
            schema:
              type: object
      responses:
        "200":
          description: LFS batch response with rewritten download URLs
```

- [ ] **Step 4: Commit**

```bash
git add README.md README_zh.md openapi.yaml
git commit -m "docs: add git clone usage and git/LFS openapi endpoints"
```

---

### Task 4: Format and final verification

- [ ] **Step 1: Format code**

```bash
cargo fmt
```

- [ ] **Step 2: Run clippy**

```bash
cargo clippy -- -D warnings
```

- [ ] **Step 3: Run tests**

```bash
cargo test
```

- [ ] **Step 4: Commit any formatting changes**

```bash
git add -u
git commit -m "chore: cargo fmt"
```
