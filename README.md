# HugRS

[![CI](https://github.com/tq02ksu/hugrs/actions/workflows/ci.yml/badge.svg)](https://github.com/tq02ksu/hugrs/actions/workflows/ci.yml)
[![Security](https://github.com/tq02ksu/hugrs/actions/workflows/security.yml/badge.svg)](https://github.com/tq02ksu/hugrs/actions/workflows/security.yml)
[![Release](https://github.com/tq02ksu/hugrs/actions/workflows/release.yml/badge.svg)](https://github.com/tq02ksu/hugrs/actions/workflows/release.yml)
[![GitHub release](https://img.shields.io/github/v/release/tq02ksu/hugrs)](https://github.com/tq02ksu/hugrs/releases)
[![GHCR](https://img.shields.io/badge/GHCR-hugrs-2496ED?logo=docker&logoColor=white)](https://github.com/tq02ksu/hugrs/pkgs/container/hugrs)
[![License](https://img.shields.io/github/license/tq02ksu/hugrs)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-2ea44f)](https://github.com/tq02ksu/hugrs/releases)
[![Rust](https://img.shields.io/badge/rust-1.96%2B-orange?logo=rust)](https://github.com/tq02ksu/hugrs/blob/master/Cargo.toml)
[![Homebrew](https://img.shields.io/badge/Homebrew-tq02ksu%2Ftap-FBB040?logo=homebrew&logoColor=white)](https://github.com/tq02ksu/homebrew-tap)

High-performance HuggingFace & ModelScope model mirror service. Prefetch-driven, content-addressed architecture with SHA256 integrity verification on read, chunk-level dedup & compression — purpose-built for LLM supply chain security and fast model delivery.

![HugRS Overview](docs/poster.png)

## Highlights

- **Multi-Platform** — supports both HuggingFace (`/hf`) and ModelScope (`/ms`) upstreams
- **Integrity & Security** — SHA256 content-addressed, verify-on-read integrity, SQLite WAL + resumable downloads
- **Storage Efficiency** — 4MB chunk dedup + compression, cross-file reuse
- **Async Architecture** — fully async pipeline with event-driven prefetch and fast local hits after first pull
- **Easy Operations** — built-in `hugrsctl` for service status, repo/file inspection, delete, and GC
- **Transparent Proxy** — full upstream header forwarding, HF Hub + ModelScope protocol compatible
- **Flexible Deployment** — single binary + Docker, local FS / S3 dual backend

## Docker

```bash
docker run -p 3000:3000 ghcr.io/tq02ksu/hugrs:0.7.1

# custom endpoint + persistent cache (named volume)
docker volume create hugrs-cache
docker run -p 3000:3000 \
  -v hugrs-cache:/home/hugrs/.cache/hugrs \
  -e HUGRS_HF_ENDPOINT=https://hf-mirror.com \
  ghcr.io/tq02ksu/hugrs:0.7.1
```

## Homebrew

```bash
brew tap tq02ksu/tap
brew install tq02ksu/tap/hugrs

# rollback to 0.6.1
brew install tq02ksu/tap/hugrs@0.6.1
```

## Quick Start

```bash
# start the daemon
hugrs

# optional upstream overrides
# HUGRS_HF_ENDPOINT=https://hf-mirror.com hugrs
# HUGRS_MS_ENDPOINT=https://modelscope.cn hugrs
```

```
# inspect cache state
hugrsctl service
hugrsctl repo
hugrsctl file
hugrsctl service gc --dry-run
```

`hugrs` is the daemon. `hugrsctl` is the management client. Cache management is limited to service status, repo/file inspection, delete operations, and GC; `chunk` remains an internal implementation detail.

[📖 Full CLI Docs →](docs/CLI.md)

## Client Usage

HugRS acts as a transparent proxy. Point popular download tools at it with an environment variable.

### hfd.sh

```bash
export HF_ENDPOINT=http://127.0.0.1:3000
hfd.sh Qwen/Qwen3.5-0.8B
```

### huggingface-cli / hf download

```bash
export HF_DEBUG=1 HF_HUB_DOWNLOAD_TIMEOUT=300 HF_ENDPOINT=http://127.0.0.1:3000
hf download Qwen/Qwen3.5-0.8B
```

**setup venv**

```bash
# install uv : curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
uv pip install huggingface-hub
export HF_DEBUG=1 HF_HUB_DOWNLOAD_TIMEOUT=120 HF_HUB_DOWNLOAD_NUM_THREADS=1 HF_ENDPOINT=http://127.0.0.1:3000
uv run hf download Qwen/Qwen3.5-0.8B
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

**setup venv**

```bash
# install uv : curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
uv pip install modelscope
uv run modelscope download qwen/Qwen3.5-0.8B --endpoint http://127.0.0.1:3000/ms
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

Default local runtime data uses the platform data directory:

- macOS: `~/Library/Application Support/hugrs/chunks`
- Linux: `~/.local/share/hugrs/chunks`

4MB chunks, SHA256-addressed:

```
.cache/hugrs/chunks/{sha256[0..2]}/{sha256[2..4]}/{sha256}
```

## Configuration

Priority: env vars > `.env` > `hugrs.toml` > defaults

Management defaults:

- control API namespace: `/_hugrs/...`
- admin token file:
  macOS: `~/Library/Application Support/hugrs/admin.token`
  Linux: `~/.local/share/hugrs/admin.token`

Other default runtime paths:

- config file:
  macOS: `~/Library/Application Support/hugrs/hugrs.toml`
  Linux: `~/.config/hugrs/hugrs.toml`
  system-wide: `/etc/hugrs/hugrs.toml`
- metadata DB:
  macOS: `~/Library/Application Support/hugrs/hugrs.db`
  Linux: `~/.local/share/hugrs/hugrs.db`

`hugrsctl` defaults to `http://127.0.0.1:3000`. Override the server address with `--endpoint` or `HUGRS_CONTROL_ENDPOINT`. The admin token is resolved from `--admin-token`, `HUGRS_ADMIN_TOKEN`, or the default token file for the current platform. Delete removes file-cache references; `hugrsctl service gc` performs batched orphan chunk reclamation.

[📖 Full Configuration Docs →](docs/CONFIG.md)

## Development

Start the daemon from source:

```bash
cargo run
cargo run -- --server-port 3001
cargo run -- --config ./hugrs.toml
HUGRS_HF_ENDPOINT=https://hf-mirror.com cargo run
HUGRS_MS_ENDPOINT=https://modelscope.cn cargo run
```

Use the management client from source:

```bash
cargo run --bin hugrsctl -- service
cargo run --bin hugrsctl -- repo
cargo run --bin hugrsctl -- file
cargo run --bin hugrsctl -- service gc --dry-run
```

## Using Installed Binaries

After installation, run the daemon and management client directly:

```bash
hugrs
hugrs --server-port 3001
hugrs --config ./hugrs.toml
hugrsctl service
hugrsctl repo
hugrsctl file
```

## License

MIT
