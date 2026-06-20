# HugRS 配置文档

## 配置加载优先级

配置按以下顺序加载，**后者覆盖前者**：

```
默认值  →  hugrs.toml  →  .env  →  环境变量  →  CLI 参数
（最低）                                                      （最高）
```

## 配置方式一览

| 方式 | 格式 | 说明 |
|------|------|------|
| 默认值 | — | 开箱即用，无需任何配置 |
| `hugrs.toml` | TOML | 先找 `./hugrs.toml`，没有则找 `~/.config/hugrs/hugrs.toml`，可通过 `-c` 强制指定 |
| `.env` | KEY=VALUE | 当前目录下的环境文件 |
| 环境变量 | `HUGRS_*` | 系统环境变量 |
| CLI 参数 | `--xxx` | 命令行全局参数，作用于所有子命令 |

### 示例：max_size 的四种配置方式

```bash
# 1. hugrs.toml
[storage]
max_size = 10737418240  # 10GB

# 2. .env
HUGRS_MAX_SIZE=10737418240

# 3. 环境变量
export HUGRS_MAX_SIZE=10737418240

# 4. CLI 参数
hugrs --max-size 10737418240 serve
```

---

## 全部配置项

### `[storage]` — 存储配置

| 配置项 | 类型 | 默认值 | 环境变量 | CLI 参数 | 说明 |
|--------|------|--------|----------|----------|------|
| `backend` | string | `"local"` | `HUGRS_STORAGE_BACKEND` | `--storage-backend` | 存储后端：`local` 或 `s3` |
| `local_root` | path | `~/.cache/hugrs/trunks` | `HUGRS_LOCAL_ROOT` | `--local-root` | 本地存储根目录 |
| `s3_bucket` | string | — | `HUGRS_S3_BUCKET` | `--s3-bucket` | S3 bucket 名称（backend=s3 时必填） |
| `s3_region` | string | — | `HUGRS_S3_REGION` | `--s3-region` | S3 区域（backend=s3 时必填） |
| `s3_prefix` | string | — | `HUGRS_S3_PREFIX` | `--s3-prefix` | S3 key 前缀，如 `"hugrs/cache"` |
| `s3_endpoint` | string | — | `HUGRS_S3_ENDPOINT` | `--s3-endpoint` | S3 兼容端点 URL（MinIO 等） |
| `max_size` | integer | — | `HUGRS_MAX_SIZE` | `--max-size` | 最大磁盘占用（字节），超出触发 LRU 淘汰 |

### `[database]` — 数据库配置

| 配置项 | 类型 | 默认值 | 环境变量 | CLI 参数 | 说明 |
|--------|------|--------|----------|----------|------|
| `path` | path | `~/.cache/hugrs/hugrs.db` | `HUGRS_DB_PATH` | `--db-path` | SQLite 数据库文件路径 |

### `[server]` — HTTP 服务配置

| 配置项 | 类型 | 默认值 | 环境变量 | CLI 参数 | 说明 |
|--------|------|--------|----------|----------|------|
| `host` | string | `"127.0.0.1"` | `HUGRS_SERVER_HOST` | `--server-host` | 监听地址 |
| `port` | integer | `3000` | `HUGRS_SERVER_PORT` | `--server-port` | 监听端口 |

### `[huggingface]` — HuggingFace Hub 配置

| 配置项 | 类型 | 默认值 | 环境变量 | CLI 参数 | 说明 |
|--------|------|--------|----------|----------|------|
| `endpoint` | string | `"https://huggingface.co"` | `HUGRS_HF_ENDPOINT` | `--hf-endpoint` | HF Hub 地址，可设为 `https://hf-mirror.com` |
| `token` | string | — | `HUGRS_HF_TOKEN` | `--hf-token` | HF API Token（访问私有/受限模型） |
| `proxy` | string | — | `HUGRS_HF_PROXY` | `--hf-proxy` | HTTP 代理地址，如 `http://proxy:8080` |

---

## 配置模板

### 本地存储（最小配置，什么都不写也行）

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

### 仅用环境变量（适合 Docker）

```bash
HUGRS_STORAGE_BACKEND=s3
HUGRS_S3_BUCKET=my-bucket
HUGRS_S3_REGION=us-east-1
HUGRS_MAX_SIZE=53687091200       # 50GB
HUGRS_SERVER_HOST=0.0.0.0
HUGRS_SERVER_PORT=8080
HUGRS_HF_ENDPOINT=https://hf-mirror.com
HUGRS_HF_PROXY=http://proxy:3128
```

---

## CLI 全局参数速查

所有子命令均接受以下全局参数：

```
hugrs [全局参数] <子命令>

全局参数:
  -c, --config <FILE>          配置文件路径（默认 hugrs.toml）
      --db-path <PATH>         数据库路径
      --storage-backend <BE>   存储后端: local | s3
      --local-root <DIR>       本地存储目录
      --s3-bucket <NAME>       S3 bucket
      --s3-region <REGION>     S3 region
      --s3-prefix <PREFIX>     S3 key 前缀
      --s3-endpoint <URL>      S3 端点 URL
      --server-host <HOST>     服务监听地址
      --server-port <PORT>     服务监听端口
      --hf-endpoint <URL>      HF Hub 地址
      --hf-token <TOKEN>       HF API Token
      --hf-proxy <URL>         HTTP 代理
      --max-size <BYTES>       最大磁盘占用

子命令:
  upload   上传文件
  pull     从 HuggingFace 拉取模型
  list     列出缓存文件
  info     查看文件详情
  stats    查看缓存统计
  gc       垃圾回收
  serve    启动 HTTP 服务
```

## .env 文件示例

```bash
# .env
HUGRS_STORAGE_BACKEND=local
HUGRS_LOCAL_ROOT=/data/hugrs/trunks
HUGRS_DB_PATH=/data/hugrs/hugrs.db
HUGRS_MAX_SIZE=107374182400
HUGRS_SERVER_HOST=0.0.0.0
HUGRS_SERVER_PORT=3000
HUGRS_HF_ENDPOINT=https://hf-mirror.com
HUGRS_HF_PROXY=http://proxy:8080
```
