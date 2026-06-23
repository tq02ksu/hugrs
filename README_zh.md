# HugRS

基于内容寻址的 HuggingFace 模型文件缓存代理。文件按 4MB 分块，每块以 SHA256 为键，跨文件自动去重。

## 功能特性

- **内容寻址存储**：4MB 定长分块，SHA256 键值，自动去重
- **SQLite 元数据**：跟踪文件、trunk、映射关系
- **可插拔后端**：本地文件系统和 S3 兼容存储
- **CLI 管理**：上传、拉取、列出、统计、垃圾回收
- **HTTP API**：RESTful 接口用于上传/下载/查询
- **HuggingFace Hub 集成**：从 huggingface.co 或 hf-mirror.com 拉取模型
- **透明缓存代理**：直接替换 HF_ENDPOINT，无需修改客户端代码

## Docker

```bash
# 默认配置启动
docker run -p 3000:3000 ghcr.io/tq02ksu/hugrs:0.1.0

# 指定镜像站 + 持久化缓存
docker run -p 3000:3000 \
  -v ./cache:/home/hugrs/.cache/hugrs \
  ghcr.io/tq02ksu/hugrs:0.1.0 \
  serve --hf-endpoint https://hf-mirror.com
```

镜像以非 root 用户 `hugrs` 运行，基础镜像 Debian 13 (trixie-slim)。缓存数据默认落 `~/.cache/hugrs/`。

## 快速开始

```bash
# 构建
cargo build --release

# 启动 HTTP 服务
cargo run -- serve

# 使用镜像站
cargo run -- serve --hf-endpoint https://hf-mirror.com

# 从 HuggingFace 拉取
cargo run -- pull bert-base-uncased

# 列出缓存文件
cargo run -- list

# 查看统计
cargo run -- stats

# 垃圾回收
cargo run -- gc
```

## 作为 HuggingFace 缓存代理使用

最常用的场景：让 `huggingface-cli` 和 `huggingface_hub` 库走本地缓存。

```bash
# 1. 启动代理
cargo run -- serve --hf-endpoint https://hf-mirror.com

# 2. 通过代理下载
HF_ENDPOINT=http://localhost:3000 hf download Qwen/Qwen3-Embedding-0.6B

# Python 代码中
import os
os.environ["HF_ENDPOINT"] = "http://localhost:3000"
from huggingface_hub import snapshot_download
snapshot_download("Qwen/Qwen3-Embedding-0.6B")
```

首次通过代理下载后，所有 chunk 已缓存到本地。再次下载同一模型或相同 chunk 的文件时，直接从本地读取，速度由网络速度变为磁盘速度。

## 配置

[📖 完整配置文档 →](docs/CONFIG_zh.md)

配置优先级（从高到低）：CLI 参数 > 环境变量 > `.env` > `hugrs.toml` > 默认值

### 关键配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `prefetch_depth` | 0（自动=CPU核数） | 缓存读取预读深度，范围 1~16 |
| `verify_sha256` | true | 缓存读取时是否 SHA256 校验，关闭可提速 |
| `max_size` | 无限制 | 最大磁盘占用，超出按 LRU 淘汰 |
| `compression` | zstd | trunk 存储压缩方式：`zstd` / `none` |

## HTTP API

作为 HuggingFace 透明代理时，支持以下端点：

| Method | Path | Description |
|--------|------|-------------|
| GET / HEAD | `/{org}/{repo}/resolve/{revision}/{*path}` | 下载/获取文件（支持 Range） |
| GET | `/api/models/{org}/{repo}/revision/{revision}` | 模型文件列表 |

## 存储布局

本地后端的 trunk 文件存储路径：
```
.cache/hugrs/trunks/{sha256[0..2]}/{sha256[2..4]}/{sha256}
```
默认路径 `~/.cache/hugrs/`。

## 性能调优

- **提高缓存读取速度**：`compression = "none"`（关闭压缩）+ `verify_sha256 = false`（关闭校验）
- **提高下载并发**：`prefetch_depth = 16`（最大预读）
- **限制磁盘占用**：`max_size = 107374182400`（100GB），超出自动淘汰最早访问的仓库

```toml
# hugrs.toml — 高性能配置
[storage]
compression = "none"
prefetch_depth = 16
verify_sha256 = false
max_size = 107374182400
```

## License

MIT
