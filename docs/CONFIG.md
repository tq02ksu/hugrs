# HugRS Configuration

## Loading Priority

Config is loaded in this order, **later overrides earlier**:

```
defaults  →  hugrs.toml  →  .env  →  env vars
(lowest)                                   (highest)
```

Default cache directory:

- macOS: `~/Library/Caches`
- Linux: `~/.cache`

## Configuration Methods

| Method | Format | Notes |
|--------|--------|-------|
| Defaults | — | Works out of the box |
| `hugrs.toml` | TOML | Tries `./hugrs.toml`, then `~/.config/hugrs/hugrs.toml` |
| `.env` | KEY=VALUE | Environment file in the current directory |
| Env vars | `HUGRS_*` | System environment variables |

### Example: 3 ways to set `max_size`

```bash
# 1. hugrs.toml
[storage]
max_size = 10737418240  # 10GB

# 2. .env
HUGRS_MAX_SIZE=10737418240

# 3. Env var
export HUGRS_MAX_SIZE=10737418240
```

---

## All Configuration Options

### `[storage]` — Storage

| Key | Type | Default | Env Var | Description |
|-----|------|---------|---------|-------------|
| `backend` | string | `"local"` | `HUGRS_STORAGE_BACKEND` | Storage backend: `local` or `s3` |
| `local_root` | path | `$CACHE_DIR/hugrs/chunks` | `HUGRS_LOCAL_ROOT` | Local storage root directory. macOS default: `~/Library/Caches/hugrs/chunks`; Linux default: `~/.cache/hugrs/chunks` |
| `s3_bucket` | string | — | `HUGRS_S3_BUCKET` | S3 bucket name (required for `backend=s3`) |
| `s3_region` | string | — | `HUGRS_S3_REGION` | S3 region (required for `backend=s3`) |
| `s3_prefix` | string | — | `HUGRS_S3_PREFIX` | S3 key prefix, e.g. `"hugrs/cache"` |
| `s3_endpoint` | string | — | `HUGRS_S3_ENDPOINT` | S3-compatible endpoint URL (MinIO, etc.) |
| `max_size` | integer | — | `HUGRS_MAX_SIZE` | Max disk usage in bytes. Triggers LRU eviction when exceeded |
| `compression` | string | `"zstd"` | `HUGRS_COMPRESSION` | Chunk compression: `zstd` or `none` |
| `prefetch_depth` | integer | `0` (auto=CPU cores) | `HUGRS_PREFETCH_DEPTH` | Cache read prefetch depth. `0`=auto (max 16). Range 1–16 |
| `prefetch_budget_base` | integer | `8` | `HUGRS_PREFETCH_BUDGET_BASE` | Base chunk prefetch budget for streaming sessions. Effective budgets are `base`, `base/2`, `base/4` for 1, 2, and 3+ active cursors |
| `verify_sha256` | boolean | `true` | `HUGRS_VERIFY_SHA256` | Validate SHA256 on cached reads. Disable for higher throughput |

### `[database]` — Database

| Key | Type | Default | Env Var | Description |
|-----|------|---------|---------|-------------|
| `path` | path | `$CACHE_DIR/hugrs/hugrs.db` | `HUGRS_DB_PATH` | SQLite database path. macOS default: `~/Library/Caches/hugrs/hugrs.db`; Linux default: `~/.cache/hugrs/hugrs.db` |

### `[server]` — HTTP Server

| Key | Type | Default | Env Var | Description |
|-----|------|---------|---------|-------------|
| `host` | string | `"127.0.0.1"` | `HUGRS_SERVER_HOST` | Listen address |
| `port` | integer | `3000` | `HUGRS_SERVER_PORT` | Listen port |

### `[admin]` — Control Plane

| Key | Type | Default | Env Var | Description |
|-----|------|---------|---------|-------------|
| `token` | string | auto-generated | `HUGRS_ADMIN_TOKEN` | Fixed admin token for `/_hugrs` APIs |
| `token_file` | path | `$CACHE_DIR/hugrs/admin.token` | `HUGRS_ADMIN_TOKEN_FILE` | Admin token file. macOS default: `~/Library/Caches/hugrs/admin.token`; Linux default: `~/.cache/hugrs/admin.token` |

### `[huggingface]` — HuggingFace Hub

| Key | Type | Default | Env Var | Description |
|-----|------|---------|---------|-------------|
| `endpoint` | string | `"https://huggingface.co"` | `HUGRS_HF_ENDPOINT` | HF Hub URL, e.g. `https://hf-mirror.com` |
| `token` | string | — | `HUGRS_HF_TOKEN` | HF API token for private/gated models |
| `proxy` | string | — | `HUGRS_HF_PROXY` | HTTP proxy, e.g. `http://proxy:8080` |
| `timeout_secs` | integer | `60` | `HUGRS_HF_TIMEOUT` | Request timeout in seconds |
| `connect_timeout_secs` | integer | `15` | `HUGRS_HF_CONNECT_TIMEOUT` | Connect timeout in seconds |

### `[modelscope]` — ModelScope Hub

| Key | Type | Default | Env Var | Description |
|-----|------|---------|---------|-------------|
| `endpoint` | string | `"https://modelscope.cn"` | `HUGRS_MS_ENDPOINT` | ModelScope Hub URL |
| `token` | string | — | `HUGRS_MS_TOKEN` | ModelScope API token for private models |
| `proxy` | string | — | `HUGRS_MS_PROXY` | HTTP proxy, e.g. `http://proxy:8080` |
| `timeout_secs` | integer | `60` | `HUGRS_MS_TIMEOUT` | Request timeout in seconds |
| `connect_timeout_secs` | integer | `15` | `HUGRS_MS_CONNECT_TIMEOUT` | Connect timeout in seconds |

---

## Config Templates

### Local storage (minimal — works with defaults)

```toml
# hugrs.toml
[storage]
backend = "local"
local_root = "~/.cache/hugrs/chunks"

[database]
path = "~/.cache/hugrs/hugrs.db"

[server]
host = "127.0.0.1"
port = 3000

[huggingface]
endpoint = "https://huggingface.co"

[modelscope]
endpoint = "https://modelscope.cn"
```

### Production (S3 + mirror + proxy + capacity limit)

```toml
# hugrs.toml
[storage]
backend = "s3"
s3_bucket = "my-hugrs-bucket"
s3_region = "us-east-1"
s3_prefix = "hugrs/prod"
max_size = 107374182400     # 100GB

[database]
path = "/data/hugrs/hugrs.db"

[server]
host = "0.0.0.0"
port = 3000

[huggingface]
endpoint = "https://hf-mirror.com"
proxy = "http://proxy.internal:8080"

[modelscope]
endpoint = "https://modelscope.cn"
```

### High-performance local cache

```toml
# hugrs.toml
[storage]
backend = "local"
compression = "none"
prefetch_depth = 16
prefetch_budget_base = 8
verify_sha256 = false
max_size = 107374182400
```

### MinIO / self-hosted S3

```toml
[storage]
backend = "s3"
s3_bucket = "hugrs"
s3_region = "us-east-1"
s3_endpoint = "http://localhost:9000"
s3_prefix = "cache"
```

### Env vars only (Docker-friendly)

```bash
HUGRS_STORAGE_BACKEND=s3
HUGRS_S3_BUCKET=my-bucket
HUGRS_S3_REGION=us-east-1
HUGRS_MAX_SIZE=53687091200       # 50GB
HUGRS_COMPRESSION=none
HUGRS_PREFETCH_DEPTH=8
HUGRS_PREFETCH_BUDGET_BASE=8
HUGRS_VERIFY_SHA256=false
HUGRS_SERVER_HOST=0.0.0.0
HUGRS_SERVER_PORT=8080
HUGRS_HF_ENDPOINT=https://hf-mirror.com
HUGRS_HF_PROXY=http://proxy:3128
HUGRS_HF_TIMEOUT=60
HUGRS_HF_CONNECT_TIMEOUT=15
HUGRS_MS_ENDPOINT=https://modelscope.cn
HUGRS_MS_PROXY=http://proxy:3128
```

---

## `.env` File Example

```bash
# .env
HUGRS_STORAGE_BACKEND=local
HUGRS_LOCAL_ROOT=/data/hugrs/chunks
HUGRS_DB_PATH=/data/hugrs/hugrs.db
HUGRS_COMPRESSION=none
HUGRS_PREFETCH_DEPTH=8
HUGRS_PREFETCH_BUDGET_BASE=8
HUGRS_VERIFY_SHA256=true
HUGRS_MAX_SIZE=107374182400
HUGRS_SERVER_HOST=0.0.0.0
HUGRS_SERVER_PORT=3000
HUGRS_HF_ENDPOINT=https://hf-mirror.com
HUGRS_HF_PROXY=http://proxy:8080
HUGRS_HF_TIMEOUT=60
HUGRS_HF_CONNECT_TIMEOUT=15
HUGRS_MS_ENDPOINT=https://modelscope.cn
HUGRS_MS_TIMEOUT=60
HUGRS_MS_CONNECT_TIMEOUT=15
```
