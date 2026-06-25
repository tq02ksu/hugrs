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
docker run -p 3000:3000 ghcr.io/tq02ksu/hugrs:0.2.0

# custom endpoint + persistent cache (named volume)
docker volume create hugrs-cache
docker run -p 3000:3000 \
  -v hugrs-cache:/home/hugrs/.cache/hugrs \
  ghcr.io/tq02ksu/hugrs:0.2.0 \
  serve --hf-endpoint https://hf-mirror.com
```

Runs as non-root `hugrs` on Debian 13 (trixie-slim).

## Quick Start

```bash
cargo build --release
cargo run -- serve
cargo run -- serve --hf-endpoint https://hf-mirror.com
cargo run -- serve --ms-endpoint https://modelscope.cn
cargo run -- pull bert-base-uncased
cargo run -- list
cargo run -- stats
cargo run -- gc
```

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
modelscope download qwen/Qwen3.5-0.6B --endpoint http://127.0.0.1:3000/ms
```

The proxy follows upstream 302 redirects internally and returns merged headers — all three tools work with zero special configuration beyond the endpoint.

## HTTP API

[📖 OpenAPI Spec →](openapi.yaml)

## Storage Layout

4MB trunks, SHA256-addressed:

```
.cache/hugrs/trunks/{sha256[0..2]}/{sha256[2..4]}/{sha256}
```

## Configuration

Priority: CLI flags > env vars > `.env` > `hugrs.toml` > defaults

[📖 Full Configuration Docs →](docs/CONFIG.md)

## License

MIT
