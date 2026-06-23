# Benchmark

## 测试环境

| 项目 | 说明 |
|------|------|
| 操作系统 | Debian 13 |
| CPU | Intel Core i5-7500 @ 3.40GHz |
| 内存 | — |
| 磁盘 | — |
| HugRS 版本 | `main` 分支 |
| 上游 | `hf-mirror.com` |

## 测试配置

| 配置项 | 值 |
|--------|-----|
| `prefetch_depth` | 4 |
| `verify_sha256` | `true`（开启读时校验） |
| `compression` | `zstd`（默认） |

## 测试模型

**Qwen/Qwen3-Embedding-0.6B** — 12 个文件，总计 1.21 GB。

| 文件 | 大小 |
|------|------|
| model.safetensors | 1.20 GB (286 chunks × 4MB) |
| tokenizer.json | 11 MB |
| vocab.json | 2.7 MB |
| merges.txt | 1.7 MB |
| 其他 8 个小文件 | <50 KB |

## 测试结果

### 首次下载（走上游，同时写缓存）

```
Download complete: 1.21G/1.21G [00:21<00:00, 61.3MB/s]
```

- 总耗时：**21 秒**
- 平均速度：**~57 MB/s**
- 大文件 `model.safetensors` 速度：**61.3 MB/s**

### 文件完整性验证

```
$ diff -qr models--Qwen--Qwen3-Embedding-0.6B ~/.cache/huggingface/hub/models--Qwen--Qwen3-Embedding-0.6B/
```

**输出为空** — 所有 12 个文件字节级一致，零差异。

## 说明

- 首次下载即达到 61 MB/s，小文件（<50KB）受 HTTP 延迟影响速度较低，但不影响整体吞吐
- 预读深度 4 + 开启 SHA256 校验时，读磁盘验 hash 与 TCP 发送并行，未成为瓶颈
- 文件完整性验证通过，代理转发的数据与直接从镜像站下载完全一致
