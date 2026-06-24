# 服务端重构设计方案

## 问题陈述

1. **超时问题**: `download_sem` (128) 和 per-file `file_sem` (16) 导致请求在 semaphore 上排队等许可；reqwest 的 60s 超时在排队请求真正发起 HTTP 调用之前就已触发。
2. **路由 handler 重复**: `file_resolve` 和 `resolve_cache` 除了路径参数解析不同外完全一样。`model_info_revision` 使用独立的 `http_cache` 机制，与 trunk 缓存体系割裂。
3. **缺失接口**: `/api/models/{org}/{repo}` 未实现。
4. **缺乏文件级协调**: 同一文件的多个并发请求（不同 range）没有共享的调度视角。

## 整体架构

```
客户端请求
      │
      ▼
Server Route 层 (handle_file_proxy / handle_api_proxy)
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│ FileSessionManager (DashMap<file_id, FileDownloadSession>)│
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │ FileDownloadSession（每个文件一个）               │    │
│  │  状态机: Created → Downloading → Satisfied → Drop│    │
│  │  pending_ranges: 客户端请求的字节范围并集           │    │
│  │  trunk_priority_queue: BinaryHeap<(idx, 优先级)>  │    │
│  │  subscribers: Vec<(client_range, mpsc::Sender)>  │    │
│  │  session_broadcast: broadcast::Sender<Chunk>     │    │
│  │  subscriber_count → 决定 prefetch 步长            │    │
│  └───────────┬─────────────────────────────────────┘    │
│              │ 按优先级队列逐个处理 trunk                    │
│              │                                           │
│  ┌───────────▼─────────────────────────────────────┐    │
│  │ SessionTable (DashMap<trunk_key, Session>)       │    │
│  │  trunk_key = (file_id, chunk_idx)                │    │
│  │                                                  │    │
│  │  Session（每个 trunk 一个）:                      │    │
│  │   state: Atomic {Downloading, Done, Failed}      │    │
│  │   broadcast: broadcast::Sender<Arc<Bytes>>       │    │
│  │   task: JoinHandle                               │    │
│  └───────────┬─────────────────────────────────────┘    │
│              │ fan-out: 一个下载结果分发给多个订阅者      │
│     ┌────────┼────────┐                                 │
│     ▼        ▼        ▼                                 │
│  客户端A   客户端B   cache writer                         │
│  流式返回  流式返回  (hash → store → link metadata)       │
└─────────────────────────────────────────────────────────┘
```

## 组件边界

### FileSessionManager
- `DashMap<i64, FileDownloadSession>` —— 无锁查找
- `get_or_create(file_id)`: 首个请求创建 session，后续请求返回已有 session
- session 状态转 Dropped 后自动从 map 中移除

### FileDownloadSession（文件级）
- **生命周期**:
  - `Created`: 首个客户端请求到达，创建 session
  - `Downloading`: 处理 trunk 优先级队列
  - `Satisfied`: 所有客户端请求的 range 均已覆盖 → 从 manager 移除
  - `Dropped`: 客户端在 Satisfied 之前全部断开 → 清理
- **范围合并**: 将多个客户端的请求 range 合并为最小的 pending ranges 集合
- **优先级**: trunk 的优先级 = 覆盖该 trunk 的客户端数量（被越多人需要越优先下载）
- **内部循环**: 弹出最高优先级 trunk → 向 SessionTable 订阅该 trunk → 等待数据 → 将 chunk 转发给覆盖该 trunk 的客户端
- **客户端订阅**: 每个客户端 **只订阅一次**，获取一个按序输出 chunk 的 broadcast Receiver

### SessionTable（trunk 级）
- `DashMap<(i64, usize), Session>` —— trunk 级 single-flight 去重
- `subscribe(trunk_key) → broadcast::Receiver<Arc<Bytes>>`
  - 已有 session → 立即返回 receiver（复用已有下载）
  - 无 session → 创建 session、启动下载任务、注册、返回 receiver
- `Session.download_task` 内部流程：
  1. GET `{url}` 带 `Range: bytes={start}-{end}`
  2. 读取响应 body 为 bytes
  3. 广播 `Arc::new(bytes)` 到 broadcast channel
  4. 状态置为 Done

### Cache Writer
- 作为 broadcast 的一个 Receiver，独立于客户端流
- 收到 `Arc<Bytes>` → 计算 SHA256 → 写入 StorageBackend → 在 MetadataStore 中 link file_trunk
- 作为后台任务运行，与客户端流式返回解耦

### Prefetch（FileDownloadSession 内部驱动）
- **不是独立的扫描 metadata 的 scheduler**
- 由 FileDownloadSession 在处理每个 trunk 时同步驱动：
  - 处理完 trunk N 后，检查 N+1 到 N+step 的 trunks
  - 如果未缓存 + 未在队列中 → 提交到 SessionTable（标记为低优先级）
- **自适应步长**:
  - `subscriber_count == 1` → step = 16（单客户端，主动往前推）
  - `subscriber_count > 1`  → step = 4（多客户端 range 交叉，覆盖已经够广）
  - 公式: `step = max(4, 16 / subscriber_count)`
- Prefetch 只是往 SessionTable 创建低优先级 session；SessionTable 本身的 single-flight 机制保证不重复下载

## 请求流程

### 类型 A：文件代理（trunk 缓存）

路由：
- `GET|HEAD /{org}/{repo}/resolve/{revision}/{*path}`
- `GET|HEAD /api/resolve-cache/{repo_type}/{org}/{repo}/{revision}/{*path}`

```
handle_file_proxy(method, headers, org, repo, revision, path):
  构建 upstream URL
  cache_name = "{repo_id}/{path}"

  HEAD 请求:
    ├── 查 metadata 缓存：有 x_repo_commit → 返回 200 + 缓存头
    └── 缓存缺失 → HEAD 上游 → 缓存 metadata → 返回 200

  GET 请求:
    ├── 确保文件 metadata 已缓存（未缓存则 HEAD 上游获取）
    ├── 解析 Range header
    ├── session = FileSessionManager.get_or_create(file_id)
    ├── stream = session.subscribe(client_range)  // 只订阅一次
    └── 返回流式 response
```

### 类型 B：API 代理（http_cache + etag 新鲜度）

路由：
- `GET|HEAD /api/models/{org}/{repo}` （新增）
- `GET|HEAD /api/models/{org}/{repo}/revision/{revision}`

```
handle_api_proxy(method, org, repo, revision?):
  upstream_url = 构建 HF API URL
  revision 为空时默认用 "main"

  HEAD 上游获取 etag
  从 http_cache 查缓存的 etag
  ├── 缓存命中 + etag 一致 → 直接返回缓存
  └── 缓存缺失或 etag 不一致 → 从上游重新拉取 → 更新 http_cache → 返回
```

注意: `http_cache` 仅用于 API 响应（JSON），不用于文件下载。文件下载走 trunk 缓存。

### 其他路由（不变）

| 路由 | 处理方式 |
|------|---------|
| `GET /` | 静态 JSON |
| `GET /api/whoami-v2` | 静态 JSON |
| `GET /api/stats` | 本地 DB 查询 |
| `GET /api/agent-harnesses` | 简单透传，无缓存 |

## 锁/Semaphore 移除

| 被移除 | 替代方案 |
|--------|---------|
| `download_sem: Semaphore(128)` | 不需要。reqwest 连接池 + OS socket 提供自然背压 |
| `download_locks: Mutex<HashMap<String, Mutex>>` | `FileSessionManager` 保证每个文件只有一个 session 实例；客户端只订阅，不竞争 |
| `file_sem: Semaphore(16)` | `FileDownloadSession` 内部 trunk 逐个串行下载（一个文件 session 同一时间只处理一个 trunk），无需额外限制 |

## 错误处理

- Trunk 下载失败 → Session 状态转 Failed → 错误传播给该 trunk 的所有 subscriber
- FileDownloadSession 遇到 Failed trunk → 跳过（文件出现空洞）。客户端收到截断流或错误，取决于缺失的 trunk 是否在其请求 range 内
- http_cache etag HEAD 失败 → 回退到全量拉取（视为 etag 不匹配）
- Prefetch 提交失败 → 静默丢弃；prefetch 是最佳努力，失败不影响主流程

## 涉及文件

| 文件 | 变更 |
|------|------|
| `src/server.rs` | 合并 handler: `handle_file_proxy` + `handle_api_proxy`。删除 `serve_file`、`file_resolve`、`resolve_cache`。新增 `/api/models/{org}/{repo}` 路由 |
| `src/service.rs` | 新增 `FileSessionManager`、`SessionTable` 字段。移除 `download_sem`、`download_locks`、`file_sem`。重构 `stream_from_upstream`。新增 `get_or_create_file_session()` |
| `src/session.rs` | **新文件**。`SessionTable`、`Session`、`FileSessionManager`、`FileDownloadSession` |
| `src/hf.rs` | 可选: 为流式下载构建一个不带 timeout 的独立 client |
| `src/main.rs` | 在 `CacheService` 初始化时传入 `FileSessionManager` + `SessionTable` |

## 数据流示例

1. 客户端发起 `GET /org/repo/resolve/main/model.bin` + `Range: bytes=0-4194303`
2. `handle_file_proxy` 解析，调用 `CacheService.get_or_create_file_session(file_id)`
3. 客户端调用 `session.subscribe(range)` —— 即时返回，无阻塞
4. `FileDownloadSession` 内部循环：通过 `SessionTable.subscribe((file_id, 0))` 获取 trunk 0
5. 此时如有第二个客户端请求同一文件：订阅同一个 `FileDownloadSession`，获取新的 broadcast receiver
6. trunk 0 数据到达 → 转发给所有覆盖 trunk 0 的客户端
7. trunk 0 处理完后，计算 prefetch 步长 → 将 trunk 1 到 step 提交到 SessionTable
8. 所有客户端 range 均已覆盖 → `FileDownloadSession` 状态转 `Satisfied` → 从 manager 移除
