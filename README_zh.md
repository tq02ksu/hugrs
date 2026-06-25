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
docker run -p 3000:3000 ghcr.io/tq02ksu/hugrs:0.2.0

# 指定镜像源 + 持久化缓存（使用命名卷）
docker volume create hugrs-cache
docker run -p 3000:3000 \
  -v hugrs-cache:/home/hugrs/.cache/hugrs \
  ghcr.io/tq02ksu/hugrs:0.2.0 \
  serve --hf-endpoint https://hf-mirror.com
```

运行在 Debian 13 (trixie-slim)，非 root 用户 `hugrs`。

## 快速开始

```bash
cargo build --release
cargo run -- serve                          # 启动服务
cargo run -- serve --hf-endpoint https://hf-mirror.com
cargo run -- serve --ms-endpoint https://modelscope.cn
cargo run -- pull bert-base-uncased         # 从 HF Hub 拉取
cargo run -- list                           # 列出缓存
cargo run -- stats                          # 缓存统计
cargo run -- gc                             # 垃圾回收
```

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
modelscope download qwen/Qwen3.5-0.6B --endpoint http://127.0.0.1:3000/ms
```

代理内部跟随上游 30x 跳转并合并响应头 — 以上工具除端点外无需额外配置。

## HTTP API

[📖 OpenAPI Spec →](openapi.yaml)

## 存储架构

4MB 分块，SHA256 寻址：

```
.cache/hugrs/trunks/{sha256[0..2]}/{sha256[2..4]}/{sha256}
```

## 配置

优先级: CLI flags > env vars > `.env` > `hugrs.toml` > defaults

[📖 完整配置文档 →](docs/CONFIG.md)

## License

MIT
