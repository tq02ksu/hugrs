# HugRS Design Spec

## Overview

HugRS is a content-addressed caching service for HuggingFace model files. Files are split into 4MB fixed-size trunks, each identified by its SHA256 hash. The service provides CLI management and HTTP API access, with SQLite metadata and pluggable storage backends (local filesystem, S3).

## Architecture

5-layer design:

```
CLI (clap)  |  HTTP API (axum)     ← Access Layer
Service Layer                      ← Business Logic
Metadata (SQLite) | Storage Trait  ← Core Layer
Local FS / S3 Backend              ← Storage Backends
Trunk (sha256 keyed)               ← Chunk I/O
```

## Data Model

```sql
CREATE TABLE files (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    total_size    INTEGER NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
    source        TEXT NOT NULL  -- 'upload' | 'pull'
);

CREATE TABLE trunks (
    sha256    TEXT PRIMARY KEY,
    backend   TEXT NOT NULL,   -- 'local' | 's3'
    path      TEXT NOT NULL,   -- relative path within backend
    size      INTEGER NOT NULL,
    ref_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE file_trunks (
    file_id      INTEGER REFERENCES files(id),
    sha256       TEXT REFERENCES trunks(sha256),
    chunk_index  INTEGER NOT NULL,
    chunk_size   INTEGER NOT NULL,
    PRIMARY KEY (file_id, sha256)
);
```

## Storage Backend Trait

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, sha256: &str, data: &[u8]) -> Result<()>;
    async fn get(&self, sha256: &str) -> Result<Vec<u8>>;
    async fn exists(&self, sha256: &str) -> bool;
    async fn delete(&self, sha256: &str) -> Result<()>;
}
```

### Local Backend
- Directory structure: `{root}/{sha256[0..2]}/{sha256[2..4]}/{sha256}`
- Two-level directory sharding to avoid flat directory with many files

### S3 Backend
- Key: `{prefix}/{sha256}`
- Configurable bucket, region, prefix

## Chunking Strategy

- Fixed 4MB chunks (4 * 1024 * 1024 = 4,194,304 bytes)
- Last chunk variable length (no padding)
- Each chunk hashed with SHA256 for content addressing
- Dedup: before writing a chunk, check if SHA256 already exists in `trunks` table; if so, increment ref_count and skip write

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/files` | Upload file (multipart) |
| GET | `/files/:name` | Download file (assembled from trunks, streaming) |
| GET | `/files/:name/info` | Get file metadata |
| POST | `/files/pull` | Pull from HuggingFace Hub |
| DELETE | `/files/:name` | Delete file (decrements trunk ref_counts) |
| GET | `/stats` | Cache statistics |

## CLI

| Command | Description |
|---------|-------------|
| `hugrs upload <path>` | Upload a local file |
| `hugrs pull <hf-repo>` | Pull a model from HuggingFace Hub |
| `hugrs list` | List cached files |
| `hugrs info <name>` | Show file metadata |
| `hugrs stats` | Show cache statistics |
| `hugrs gc` | Garbage collect trunks with ref_count=0 |
| `hugrs serve` | Start HTTP server |

## Cache Eviction

- Configurable `max_size` limit (bytes) for total cache disk usage
- LRU eviction: when total_size exceeds max_size after upload/pull, evict least recently accessed files
- `last_accessed` timestamp updated on every file read (download) and write (upload/pull)
- Eviction removes whole files, decrements trunk ref_counts, deletes orphan trunks
- GC command also checks and enforces max_size limit

## Concurrency

- SQLite in WAL mode for concurrent reads
- File-level write lock (tokio::sync::Mutex per file name) to prevent concurrent uploads of same file
- Read operations are lock-free (WAL readers don't block writers)

## Technology Stack

- **Runtime**: tokio (async)
- **HTTP**: axum
- **SQLite**: rusqlite with bundled feature
- **S3**: aws-sdk-s3 or rusoto_s3
- **Hashing**: sha2
- **CLI**: clap (derive mode)
- **HTTP client**: reqwest (for HF Hub pulls)
- **HF Hub**: huggingface-hub crate or raw API calls

## Error Handling

- `anyhow` for application errors
- `thiserror` for library-style error types in core/backend crates

## Project Structure

```
hugrs/
├── Cargo.toml
├── src/
│   ├── main.rs           # CLI entry point
│   ├── cli.rs            # CLI command definitions
│   ├── config.rs         # Configuration
│   ├── server.rs         # HTTP server (axum)
│   ├── service.rs        # Business logic
│   ├── chunker.rs        # File splitting / assembly
│   ├── metadata.rs       # SQLite operations
│   ├── storage/
│   │   ├── mod.rs        # StorageBackend trait
│   │   ├── local.rs      # Local filesystem backend
│   │   └── s3.rs         # S3 backend
│   └── hf.rs             # HuggingFace Hub integration
└── docs/
    └── superpowers/
        └── specs/
            └── 2026-06-20-hugrs-design.md
```

## HuggingFace Hub Integration

- Supports configurable endpoint: `https://huggingface.co` or `https://hf-mirror.com`
- HTTP proxy support for corporate network environments
- API-based file listing and download via HF Hub API

## Open Questions (decided)

- Chunk size: 4MB fixed, last chunk variable ✓
- Auth: none required ✓
- Storage: local + S3 ✓
- Metadata: SQLite ✓
- Concurrency: WAL mode, file-level write lock ✓
- Proxy support: yes ✓
- Multi-endpoint: huggingface.co and hf-mirror.com ✓
- Config sources: TOML file, .env, env vars, CLI args (priority: CLI > env > .env > file > default) ✓
- Max disk size with LRU eviction ✓
