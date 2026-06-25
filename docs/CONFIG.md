# HugRS Configuration

## Loading Priority

Config is loaded in this order, **later overrides earlier**:

```
defaults  →  hugrs.toml  →  .env  →  env vars  →  CLI flags
(lowest)                                                (highest)
```

## Configuration Methods

| Method | Format | Notes |
|--------|--------|-------|
| Defaults | — | Works out of the box |
| `hugrs.toml` | TOML | Tries `./hugrs.toml`, then `~/.config/hugrs/hugrs.toml`. Use `-c` to override |
| `.env` | KEY=VALUE | Environment file in the current directory |
| Env vars | `HUGRS_*` | System environment variables |
| CLI flags | `--xxx` | Global flags, apply to all subcommands |

### Example: 4 ways to set `max_size`

```bash
# 1. hugrs.toml
[storage]
max_size = 10737418240  # 10GB

# 2. .env
HUGRS_MAX_SIZE=10737418240

# 3. Env var
export HUGRS_MAX_SIZE=10737418240

# 4. CLI flag
hugrs --max-size 10737418240 serve
```

---

## All Configuration Options

### `[storage]` — Storage

| Key | Type | Default | Env Var | CLI Flag | Description |
|-----|------|---------|---------|----------|-------------|
| `backend` | string | `"local"` | `HUGRS_STORAGE_BACKEND` | `--storage-backend` | Storage backend: `local` or `s3` |
| `local_root` | path | `~/.cache/hugrs/trunks` | `HUGRS_LOCAL_ROOT` | `--local-root` | Local storage root directory |
| `s3_bucket` | string | — | `HUGRS_S3_BUCKET` | `--s3-bucket` | S3 bucket name (required for `backend=s3`) |
| `s3_region` | string | — | `HUGRS_S3_REGION` | `--s3-region` | S3 region (required for `backend=s3`) |
| `s3_prefix` | string | — | `HUGRS_S3_PREFIX` | `--s3-prefix` | S3 key prefix, e.g. `"hugrs/cache"` |
| `s3_endpoint` | string | — | `HUGRS_S3_ENDPOINT` | `--s3-endpoint` | S3-compatible endpoint URL (MinIO, etc.) |
| `max_size` | integer | — | `HUGRS_MAX_SIZE` | `--max-size` | Max disk usage in bytes. Triggers LRU eviction when exceeded |
| `compression` | string | `"zstd"` | `HUGRS_COMPRESSION` | `--compression` | Trunk compression: `zstd` or `none` |
| `prefetch_depth` | integer | `0` (auto=CPU cores) | `HUGRS_PREFETCH_DEPTH` | `--prefetch-depth` | Cache read prefetch depth. `0`=auto (max 16). Range 1–16 |
| `prefetch_budget_base` | integer | `8` | `HUGRS_PREFETCH_BUDGET_BASE` | `--prefetch-budget-base` | Base chunk prefetch budget for streaming sessions. Effective budgets are `base`, `base/2`, `base/4` for 1, 2, and 3+ active cursors |
| `verify_sha256` | boolean | `true` | `HUGRS_VERIFY_SHA256` | `--enable-sha256-verify` | Validate SHA256 on cached reads. Disable for higher throughput |

### `[database]` — Database

| Key | Type | Default | Env Var | CLI Flag | Description |
|-----|------|---------|---------|----------|-------------|
| `path` | path | `~/.cache/hugrs/hugrs.db` | `HUGRS_DB_PATH` | `--db-path` | SQLite database path |

### `[server]` — HTTP Server

| Key | Type | Default | Env Var | CLI Flag | Description |
|-----|------|---------|---------|----------|-------------|
| `host` | string | `"127.0.0.1"` | `HUGRS_SERVER_HOST` | `--server-host` | Listen address |
| `port` | integer | `3000` | `HUGRS_SERVER_PORT` | `--server-port` | Listen port |

### `[huggingface]` — HuggingFace Hub

| Key | Type | Default | Env Var | CLI Flag | Description |
|-----|------|---------|---------|----------|-------------|
| `endpoint` | string | `"https://huggingface.co"` | `HUGRS_HF_ENDPOINT` | `--hf-endpoint` | HF Hub URL, e.g. `https://hf-mirror.com` |
| `token` | string | — | `HUGRS_HF_TOKEN` | `--hf-token` | HF API token for private/gated models |
| `proxy` | string | — | `HUGRS_HF_PROXY` | `--hf-proxy` | HTTP proxy, e.g. `http://proxy:8080` |
| `timeout_secs` | integer | `60` | `HUGRS_HF_TIMEOUT` | `--hf-timeout` | Request timeout in seconds |
| `connect_timeout_secs` | integer | `15` | `HUGRS_HF_CONNECT_TIMEOUT` | `--hf-connect-timeout` | Connect timeout in seconds |

### `[modelscope]` — ModelScope Hub

| Key | Type | Default | Env Var | CLI Flag | Description |
|-----|------|---------|---------|----------|-------------|
| `endpoint` | string | `"https://modelscope.cn"` | `HUGRS_MS_ENDPOINT` | `--ms-endpoint` | ModelScope Hub URL |
| `token` | string | — | `HUGRS_MS_TOKEN` | `--ms-token` | ModelScope API token for private models |
| `proxy` | string | — | `HUGRS_MS_PROXY` | `--ms-proxy` | HTTP proxy, e.g. `http://proxy:8080` |
| `timeout_secs` | integer | `60` | `HUGRS_MS_TIMEOUT` | `--ms-timeout` | Request timeout in seconds |
| `connect_timeout_secs` | integer | `15` | `HUGRS_MS_CONNECT_TIMEOUT` | `--ms-connect-timeout` | Connect timeout in seconds |

---

## Config Templates

### Local storage (minimal — works with defaults)

```toml
# hugrs.toml
[storage]
backend = "local"
local_root = "~/.cache/hugrs/trunks"

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

## CLI Global Flags

All subcommands accept these global flags:

```
hugrs [GLOBAL FLAGS] <SUBCOMMAND>

Global Flags:
  -c, --config <FILE>          Config file path (default: hugrs.toml)
      --db-path <PATH>         Database path
      --storage-backend <BE>   Storage backend: local | s3
      --local-root <DIR>       Local storage directory
      --s3-bucket <NAME>       S3 bucket
      --s3-region <REGION>     S3 region
      --s3-prefix <PREFIX>     S3 key prefix
      --s3-endpoint <URL>      S3 endpoint URL
      --compression <MODE>     Trunk compression: zstd | none
      --max-size <BYTES>       Max disk usage
      --prefetch-depth <N>     Cache read prefetch depth (0=auto)
      --prefetch-budget-base <N>  Base chunk prefetch budget for streaming sessions
      --enable-sha256-verify <BOOL>  Enable SHA256 validation on cached reads
      --server-host <HOST>     Listen address
      --server-port <PORT>     Listen port
      --hf-endpoint <URL>      HF Hub URL
      --hf-token <TOKEN>       HF API token
      --hf-proxy <URL>         HTTP proxy
      --hf-timeout <SECS>      HF request timeout
      --hf-connect-timeout <SECS>  HF connect timeout
      --ms-endpoint <URL>      ModelScope Hub URL
      --ms-token <TOKEN>       ModelScope API token
      --ms-proxy <URL>         ModelScope HTTP proxy
      --ms-timeout <SECS>      ModelScope request timeout
      --ms-connect-timeout <SECS>  ModelScope connect timeout

Subcommands:
  upload     Upload a file
  pull       Pull a model from HuggingFace
  list       List cached files
  info       Show file details
  stats      Show cache statistics
  gc         Garbage-collect orphaned trunks
  serve      Start HTTP server
```

## `.env` File Example

```bash
# .env
HUGRS_STORAGE_BACKEND=local
HUGRS_LOCAL_ROOT=/data/hugrs/trunks
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
