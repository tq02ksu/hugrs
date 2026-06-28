# HugRS

High-performance HuggingFace & ModelScope model mirror. Prefetch-driven, content-addressed architecture with SHA256 integrity verification on read, chunk-level dedup & compression — purpose-built for LLM supply chain security and fast model delivery.

## Highlights

- **Multi-Platform** — supports both HuggingFace (`/hf`) and ModelScope (`/ms`) upstreams
- **Supply Chain Security** — SHA256 content-addressed, verify-on-read integrity
- **Storage Efficiency** — 4MB chunk dedup + compression, cross-file reuse
- **Fast Access** — prefetch-driven caching, local hits after first pull
- **Backup-Grade Integrity** — SQLite WAL transactions + resumable downloads, zero loss
- **Transparent Proxy** — full upstream header forwarding, HF Hub + ModelScope protocol compatible
- **Flexible Deployment** — single binary + Docker, local FS / S3 dual backend

## Docker

```bash
docker run -p 3000:3000 ghcr.io/tq02ksu/hugrs:0.3.1

# custom endpoint + persistent cache (named volume)
docker volume create hugrs-cache
docker run -p 3000:3000 \
  -v hugrs-cache:/home/hugrs/.cache/hugrs \
  -e HUGRS_HF_ENDPOINT=https://hf-mirror.com \
  ghcr.io/tq02ksu/hugrs:0.3.1
```

Runs as non-root `hugrs` on Debian 13 (trixie-slim).

## Quick Start

```bash
cargo build --release
cargo run
HUGRS_HF_ENDPOINT=https://hf-mirror.com cargo run
HUGRS_MS_ENDPOINT=https://modelscope.cn cargo run

# management client
cargo run --bin hugrsctl -- service
cargo run --bin hugrsctl -- repo
cargo run --bin hugrsctl -- file
cargo run --bin hugrsctl -- service gc --dry-run
```

`hugrs` is the daemon. `hugrsctl` is the management client. Cache management is limited to service status, repo/file inspection, delete operations, and GC; `chunk` remains an internal implementation detail.

## Client Usage

HugRS acts as a transparent proxy. Point popular download tools at it with an environment variable.

### hfd.sh

```bash
export HF_ENDPOINT=http://127.0.0.1:3000
hfd.sh Qwen/Qwen3.5-0.8B
```

### huggingface-cli / hf download

```bash
export HF_DEBUG=1 HF_HUB_DOWNLOAD_TIMEOUT=120 HF_ENDPOINT=http://127.0.0.1:3000
hf download Qwen/Qwen3.5-0.8B
```

### huggingface_hub SDK

```python
import os
os.environ["HF_ENDPOINT"] = "http://127.0.0.1:3000"
from huggingface_hub import snapshot_download
snapshot_download("Qwen/Qwen3.5-0.8B")
```

### modelscope download

```bash
modelscope download qwen/Qwen3.5-0.8B --endpoint http://127.0.0.1:3000/ms
```

### git clone

> [!WARNING]
> `git clone` + `git lfs pull` creates a full working copy **plus** a local proxy cache copy of every large file, roughly doubling disk usage. For model downloads prefer `hfd.sh`, `huggingface-cli`, or `modelscope` CLI — they download only the model files without git overhead.

```bash
git clone http://127.0.0.1:3000/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/hf/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/ms/qwen/Qwen3.5-0.8B
```

The proxy follows upstream 302 redirects internally and returns merged headers — all three tools work with zero special configuration beyond the endpoint.

### TEI (Text Embeddings Inference)

Point TEI at HugRS to cache model downloads:

```bash
docker run --rm --gpus all -p 8002:80 \
  -e HF_ENDPOINT=http://localhost:3000 \
  ghcr.io/huggingface/text-embeddings-inference:cpu-latest \
  --model-id Qwen/Qwen3-Embedding-0.6B
```

## HTTP API

[📖 OpenAPI Spec →](openapi.yaml)

## Storage Layout

4MB chunks, SHA256-addressed:

```
.cache/hugrs/chunks/{sha256[0..2]}/{sha256[2..4]}/{sha256}
```

## Configuration

Priority: env vars > `.env` > `hugrs.toml` > defaults

Management defaults:

- control API namespace: `/_hugrs/...`
- admin token file: `~/.cache/hugrs/admin.token`

`hugrsctl` defaults to `http://127.0.0.1:3000`, can override the server address with `--endpoint` or `HUGRS_CONTROL_ENDPOINT`, and resolves the admin token from `--admin-token`, `HUGRS_ADMIN_TOKEN`, or `~/.cache/hugrs/admin.token`. Delete removes file-cache references; `hugrsctl service gc` performs batched orphan chunk reclamation.

[📖 Full Configuration Docs →](docs/CONFIG.md)

## License

MIT
