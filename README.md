# HugRS

High-performance HuggingFace model mirror. Prefetch-driven, content-addressed architecture with SHA256 integrity verification on read, chunk-level dedup & compression — purpose-built for LLM supply chain security and fast model delivery.

## Highlights

- **Supply Chain Security** — SHA256 content-addressed, verify-on-read integrity
- **Storage Efficiency** — 4MB chunk dedup + compression, cross-file reuse
- **Fast Access** — prefetch-driven caching, local hits after first pull
- **Backup-Grade Integrity** — SQLite WAL transactions + resumable downloads, zero loss
- **Transparent Proxy** — full upstream header forwarding, HF Hub protocol compatible
- **Flexible Deployment** — single binary + Docker, local FS / S3 dual backend

## Docker

```bash
docker run -p 3000:3000 ghcr.io/tq02ksu/hugrs:0.1.0

# custom endpoint + persistent cache (named volume)
docker volume create hugrs-cache
docker run -p 3000:3000 \
  -v hugrs-cache:/home/hugrs/.cache/hugrs \
  ghcr.io/tq02ksu/hugrs:0.1.0 \
  serve --hf-endpoint https://hf-mirror.com
```

Runs as non-root `hugrs` on Debian 13 (trixie-slim).

## Quick Start

```bash
cargo build --release
cargo run -- serve
cargo run -- serve --hf-endpoint https://hf-mirror.com
cargo run -- pull bert-base-uncased
cargo run -- list
cargo run -- stats
cargo run -- gc
```

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/files` | Upload file (multipart) |
| GET | `/files/:name` | Download assembled file |
| GET | `/files/:name/info` | File metadata |
| POST | `/files/pull` | Pull from HF Hub |
| DELETE | `/files/:name` | Delete file |
| GET | `/stats` | Cache statistics |

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
