# HugRS 配置文档

## 配置加载优先级

配置按以下顺序加载，**后者覆盖前者**：

```
默认值  →  hugrs.toml  →  .env  →  环境变量  →  hugrs 参数
（最低）                                                        （最高）
```

默认运行目录：

- 配置文件：
  macOS：`~/Library/Application Support/hugrs/hugrs.toml`
  Linux：`~/.config/hugrs/hugrs.toml`
  系统级：`/etc/hugrs/hugrs.toml`
- 持久数据：
  macOS：`~/Library/Application Support/hugrs`
  Linux：`~/.local/share/hugrs`

## 各组件职责

这些组件是配合关系，不是替代关系：

- `clap`：解析命令行参数
- `dotenvy`：把 `.env` 加载到进程环境变量
- `figment`：合并默认值、配置文件、环境变量和命令行覆盖项

真正的设计选择，是“自己手写 merge 逻辑”还是“交给 `figment` 统一处理”。HugRS 当前使用 `figment` 作为配置合并引擎。

## 配置方式一览

| 方式 | 格式 | 说明 |
|------|------|------|
| 默认值 | — | 开箱即用，无需任何配置 |
| `hugrs.toml` | TOML | 先找 `./hugrs.toml`，再找平台用户配置路径，最后找 `/etc/hugrs/hugrs.toml` |
| `.env` | KEY=VALUE | 当前目录下的环境文件 |
| 环境变量 | `HUGRS_*` | 系统环境变量 |
| `hugrs` 参数 | `--xxx` | 守护进程启动覆盖项，例如 `--config`、`--server-port` |

### 示例：max_size 的四种配置方式

```bash
# 1. hugrs.toml
[storage]
max_size = 10737418240  # 10GB

# 2. .env
HUGRS_MAX_SIZE=10737418240

# 3. 环境变量
export HUGRS_MAX_SIZE=10737418240

# 4. hugrs 参数
hugrs --max-size 10737418240
```

---

## 全部配置项

### `[storage]` — 存储配置

| 配置项 | 类型 | 默认值 | 环境变量 | 说明 |
|--------|------|--------|----------|------|
| `backend` | string | `"local"` | `HUGRS_STORAGE_BACKEND` | 存储后端：`local` 或 `s3` |
| `local_root` | path | `$DATA_DIR/hugrs/chunks` | `HUGRS_LOCAL_ROOT` | 本地存储根目录。macOS 默认：`~/Library/Application Support/hugrs/chunks`；Linux 默认：`~/.local/share/hugrs/chunks` |
| `s3_bucket` | string | — | `HUGRS_S3_BUCKET` | S3 bucket 名称（backend=s3 时必填） |
| `s3_region` | string | — | `HUGRS_S3_REGION` | S3 区域（backend=s3 时必填） |
| `s3_prefix` | string | — | `HUGRS_S3_PREFIX` | S3 key 前缀，如 `"hugrs/cache"` |
| `s3_endpoint` | string | — | `HUGRS_S3_ENDPOINT` | S3 兼容端点 URL（MinIO 等） |
| `max_size` | integer | — | `HUGRS_MAX_SIZE` | 最大磁盘占用（字节），超出触发 LRU 淘汰 |
| `compression` | string | `"zstd"` | `HUGRS_COMPRESSION` | chunk 压缩方式：`zstd` 或 `none` |
| `prefetch_depth` | integer | `0`（自动=CPU核数） | `HUGRS_PREFETCH_DEPTH` | 缓存读取预读深度，`0`=自动（最多16），范围 1–16 |
| `prefetch_budget_base` | integer | `8` | `HUGRS_PREFETCH_BUDGET_BASE` | 流式下载 session 的 chunk 预取预算基数。1 个活跃游标时用 `base`，2 个时用 `base/2`，3 个及以上时用 `base/4` |
| `verify_sha256` | boolean | `true` | `HUGRS_VERIFY_SHA256` | 缓存读取时是否校验 SHA256，关闭可提升缓存读取速度 |
| `etag_validation_timeout_secs` | integer | `5` | `HUGRS_ETAG_VALIDATION_TIMEOUT` | 上游 ETag 校验 HEAD 请求的超时秒数，设为 `0` 可禁用 |
| `chunk_retries` | integer | `3` | `HUGRS_CHUNK_RETRIES` | 分块下载失败时的最大重试次数（网络错误、5xx、响应体读取错误） |

### `[database]` — 数据库配置

| 配置项 | 类型 | 默认值 | 环境变量 | 说明 |
|--------|------|--------|----------|------|
| `path` | path | `$DATA_DIR/hugrs/hugrs.db` | `HUGRS_DB_PATH` | SQLite 数据库文件路径。macOS 默认：`~/Library/Application Support/hugrs/hugrs.db`；Linux 默认：`~/.local/share/hugrs/hugrs.db` |

### `[server]` — HTTP 服务配置

| 配置项 | 类型 | 默认值 | 环境变量 | 说明 |
|--------|------|--------|----------|------|
| `host` | string | `"127.0.0.1"` | `HUGRS_SERVER_HOST` | 监听地址 |
| `port` | integer | `3000` | `HUGRS_SERVER_PORT` | 监听端口 |

### `[admin]` — 控制面配置

| 配置项 | 类型 | 默认值 | 环境变量 | 说明 |
|--------|------|--------|----------|------|
| `token` | string | 自动生成 | `HUGRS_ADMIN_TOKEN` | `/_hugrs` 管理 API 的固定 admin token |
| `token_file` | path | `$DATA_DIR/hugrs/admin.token` | `HUGRS_ADMIN_TOKEN_FILE` | admin token 文件。macOS 默认：`~/Library/Application Support/hugrs/admin.token`；Linux 默认：`~/.local/share/hugrs/admin.token` |

### `[huggingface]` — HuggingFace Hub 配置

| 配置项 | 类型 | 默认值 | 环境变量 | 说明 |
|--------|------|--------|----------|------|
| `endpoint` | string | `"https://huggingface.co"` | `HUGRS_HF_ENDPOINT` | HF Hub 地址，可设为 `https://hf-mirror.com` |
| `token` | string | — | `HUGRS_HF_TOKEN` | HF API Token（访问私有/受限模型） |
| `proxy` | string | — | `HUGRS_HF_PROXY` | HTTP 代理地址，如 `http://proxy:8080` |
| `timeout_secs` | integer | `60` | `HUGRS_HF_TIMEOUT` | 请求超时（秒） |
| `connect_timeout_secs` | integer | `15` | `HUGRS_HF_CONNECT_TIMEOUT` | 连接超时（秒） |

### `[modelscope]` — ModelScope Hub 配置

| 配置项 | 类型 | 默认值 | 环境变量 | 说明 |
|--------|------|--------|----------|------|
| `endpoint` | string | `"https://modelscope.cn"` | `HUGRS_MS_ENDPOINT` | ModelScope Hub 地址 |
| `token` | string | — | `HUGRS_MS_TOKEN` | ModelScope API Token（访问私有模型） |
| `proxy` | string | — | `HUGRS_MS_PROXY` | HTTP 代理地址，如 `http://proxy:8080` |
| `timeout_secs` | integer | `60` | `HUGRS_MS_TIMEOUT` | 请求超时（秒） |
| `connect_timeout_secs` | integer | `15` | `HUGRS_MS_CONNECT_TIMEOUT` | 连接超时（秒） |

---

## 配置模板

### 本地存储（最小配置，什么都不写也行）

```toml
# hugrs.toml
[storage]
backend = "local"
local_root = "~/.local/share/hugrs/chunks"

[database]
path = "~/.local/share/hugrs/hugrs.db"

[server]
host = "127.0.0.1"
port = 3000

[huggingface]
endpoint = "https://huggingface.co"

[modelscope]
endpoint = "https://modelscope.cn"
```

### 生产环境（S3 + 镜像站 + 代理 + 容量限制）

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

### MinIO / 自建 S3 兼容存储

```toml
[storage]
backend = "s3"
s3_bucket = "hugrs"
s3_region = "us-east-1"
s3_endpoint = "http://localhost:9000"
s3_prefix = "cache"
```

### 高性能本地缓存

```toml
[storage]
backend = "local"
compression = "none"
prefetch_depth = 16
prefetch_budget_base = 8
verify_sha256 = false
max_size = 107374182400
```

### 仅用环境变量（适合 Docker）

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

## .env 文件示例

```bash
# .env
HUGRS_STORAGE_BACKEND=local
HUGRS_LOCAL_ROOT=/data/hugrs/chunks
HUGRS_DB_PATH=/data/hugrs/hugrs.db
HUGRS_MAX_SIZE=107374182400
HUGRS_PREFETCH_DEPTH=8
HUGRS_PREFETCH_BUDGET_BASE=8
HUGRS_VERIFY_SHA256=true
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

## 常用 `hugrs` 参数

这些参数作用于守护进程本身，不是 `hugrsctl` 参数。

```bash
hugrs --config ./hugrs.toml
hugrs --server-host 0.0.0.0 --server-port 3001
hugrs --db-path /data/hugrs.db
hugrs --local-root /data/chunks
hugrs --hf-endpoint https://hf-mirror.com
```
