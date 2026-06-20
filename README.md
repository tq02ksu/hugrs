# HugRS

Content-addressed caching service for HuggingFace model files. Files are split into 4MB trunks, each keyed by SHA256. Deduplicates identical chunks across files.

## Features

- **Content-addressed storage**: 4MB fixed-size chunks, SHA256 keys, automatic deduplication
- **SQLite metadata**: Tracks files, trunks, and their mappings
- **Pluggable backends**: Local filesystem and S3-compatible storage
- **CLI management**: Upload, pull, list, stats, garbage collection
- **HTTP API**: RESTful access for upload/download/query
- **HuggingFace Hub integration**: Pull models from huggingface.co or hf-mirror.com
- **Proxy support**: HTTP proxy for corporate environments

## Quick Start

```bash
# Build
cargo build --release

# Start HTTP server
cargo run -- serve

# Start HTTP server with custom endpoint
cargo run -- serve --hf-endpoint https://hf-mirror.com

# Upload a file
cargo run -- upload model.safetensors

# Pull from HuggingFace
cargo run -- pull bert-base-uncased

# List cached files
cargo run -- list

# Show stats
cargo run -- stats

# Garbage collect orphaned trunks
cargo run -- gc
```

## Configuration

[📖 完整配置文档 →](docs/CONFIG.md)

Config priority (highest to lowest): CLI flags > env vars > `.env` file > `hugrs.toml` > defaults

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/files` | Upload file (multipart) |
| GET | `/files/:name` | Download assembled file |
| GET | `/files/:name/info` | File metadata |
| POST | `/files/pull` | Pull from HF Hub (`{"repo":"..."}`) |
| DELETE | `/files/:name` | Delete file |
| GET | `/stats` | Cache statistics |

## Storage Layout

Local backend stores trunks under:
```
.cache/hugrs/trunks/{sha256[0..2]}/{sha256[2..4]}/{sha256}
```
Default at `~/.cache/hugrs/`.

## License

MIT
