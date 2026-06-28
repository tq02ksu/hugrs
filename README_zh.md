# HugRS

高性能 HuggingFace & ModelScope 模型镜像服务。基于 prefetch + 内容寻址架构，读时 SHA256 校验数据完整性，内置分块去重与压缩，保障大模型供应链安全与高速访问。

## 核心亮点

- **多平台支持** — 同时支持 HuggingFace (`/hf`) 和 ModelScope (`/ms`) 上游
- **供应链安全** — SHA256 内容寻址，读时校验，杜绝篡改
- **高存储效率** — 4MB 分块去重 + 压缩，跨文件复用
- **高速访问** — prefetch 智能缓存，首次拉取后本地极速命中
- **备份级完整性** — SQLite WAL 事务 + 断点续传，零丢失
- **透明代理** — 完整转发上游 headers，兼容 HF Hub + ModelScope 协议
- **弹性部署** — 单二进制 + Docker，本地 FS / S3 双后端

## Docker

```bash
docker run -p 3000:3000 ghcr.io/tq02ksu/hugrs:0.3.1

# 指定镜像源 + 持久化缓存（使用命名卷）
docker volume create hugrs-cache
docker run -p 3000:3000 \
  -v hugrs-cache:/home/hugrs/.cache/hugrs \
  -e HUGRS_HF_ENDPOINT=https://hf-mirror.com \
  ghcr.io/tq02ksu/hugrs:0.3.1
```

运行在 Debian 13 (trixie-slim)，非 root 用户 `hugrs`。

## 快速开始

```bash
cargo build --release
cargo run                                   # 启动服务
HUGRS_HF_ENDPOINT=https://hf-mirror.com cargo run
HUGRS_MS_ENDPOINT=https://modelscope.cn cargo run

# 管理客户端
cargo run --bin hugrsctl -- service
cargo run --bin hugrsctl -- repo
cargo run --bin hugrsctl -- file
cargo run --bin hugrsctl -- service gc --dry-run
```

`hugrs` 是守护进程，`hugrsctl` 是管理客户端。管理面只暴露服务状态、repo/file 查看、删除和 GC；`chunk` 保持为内部实现细节，不面向用户。

## 客户端使用

HugRS 作为透明代理运行，通过环境变量即可接入常用下载工具。

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
> `git clone` + `git lfs pull` 会同时创建完整工作副本和本地代理缓存，磁盘占用约翻倍。推荐使用 `hfd.sh`、`huggingface-cli` 或 `modelscope` CLI 下载模型，仅拉取模型文件，无 git 额外开销。

```bash
git clone http://127.0.0.1:3000/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/hf/Qwen/Qwen3.5-0.8B
git clone http://127.0.0.1:3000/ms/qwen/Qwen3.5-0.8B
```

代理内部跟随上游 30x 跳转并合并响应头 — 以上工具除端点外无需额外配置。

### TEI (Text Embeddings Inference)

将 TEI 指向 HugRS 以缓存模型下载：

```bash
docker run --rm --gpus all -p 8002:80 \
  -e HF_ENDPOINT=http://your-hugrs-host:3000 \
  ghcr.io/huggingface/text-embeddings-inference:cpu-latest \
  --model-id Qwen/Qwen3-Embedding-0.6B
```

## HTTP API

[📖 OpenAPI Spec →](openapi.yaml)

## 存储架构

4MB 分块，SHA256 寻址：

```
.cache/hugrs/chunks/{sha256[0..2]}/{sha256[2..4]}/{sha256}
```

## 配置

优先级: env vars > `.env` > `hugrs.toml` > defaults

管理默认值：

- 控制面路径前缀：`/_hugrs/...`
- admin token 文件：`~/.cache/hugrs/admin.token`

`hugrsctl` 默认连接 `http://127.0.0.1:3000`，也可通过 `--endpoint` 或 `HUGRS_CONTROL_ENDPOINT` 覆盖服务地址，admin token 则按 `--admin-token`、`HUGRS_ADMIN_TOKEN`、`~/.cache/hugrs/admin.token` 的顺序解析。删除只移除文件缓存引用；`hugrsctl service gc` 负责按批回收 orphan chunk。

[📖 完整配置文档 →](docs/CONFIG_zh.md)

## License

MIT
