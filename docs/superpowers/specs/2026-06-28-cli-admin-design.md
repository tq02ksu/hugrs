# HugRS 管理命令与控制面设计

## 目标

重设计 HugRS 的管理命令，使其符合以下原则：

1. 用户只管理 `service`、`repo`、`file` 三类对象。
2. `chunk` 是内部实现细节，不进入用户命令面。
3. `hugrs` 是服务进程，`hugrsctl` 是独立管理客户端。
4. 管理操作统一经由 `hugrs` 服务执行，`hugrsctl` 不直接打开 SQLite 或存储后端。
5. 在不打断正常读请求的前提下，尽量满足删除与空间回收需求。

## 非目标

以下内容不纳入本轮设计：

1. 下载/预热/拉取类管理命令。
2. 基于用户体系、RBAC、会话表的复杂权限模型。
3. 让用户直接管理 chunk、session、backend、内部调度状态。
4. 追求强线性一致的复杂并发控制。

## 二进制职责

### `hugrs`

- 默认行为即启动服务。
- `hugrs serve` 保留为兼容别名，可视为等价入口。
- 负责：
  - 代理 HuggingFace / ModelScope 请求
  - 管理 API
  - 缓存元数据维护
  - chunk 生命周期维护

### `hugrsctl`

- 独立二进制。
- 只作为管理客户端调用 `hugrs` 的控制面 API。
- 不直接访问：
  - SQLite
  - 本地 chunk 存储
  - S3 backend

## 自动发现与认证

### 默认路径

控制面相关默认文件与现有缓存目录统一放在 `~/.cache/hugrs/` 下：

- `hugrs.db`
- `chunks/`
- `admin.token`

### admin token

- 若显式提供 `HUGRS_ADMIN_TOKEN` 或 `HUGRS_ADMIN_TOKEN_FILE`，服务端使用该 token。
- 否则服务启动时自动生成高熵 token，并写入 `~/.cache/hugrs/admin.token`。
- `hugrsctl` 默认按以下优先级获取 token：
  1. `--admin-token`
  2. `HUGRS_ADMIN_TOKEN`
  3. `HUGRS_ADMIN_TOKEN_FILE`
  4. `~/.cache/hugrs/admin.token`

要求：

- token 不写日志
- token 文件写入原子化
- token 文件权限收紧

## 命令风格

采用资源分组风格，而不是动词平铺风格。

### 顶层资源

- `service`
- `repos`
- `files`

### 默认动作

- `hugrsctl service` = `hugrsctl service status`
- `hugrsctl repos` = `hugrsctl repos list`
- `hugrsctl files` = `hugrsctl files list`

### 统一动作命名

- 查看单对象统一使用 `show`
- 删除统一使用 `delete`

## 命令集

### service

- `hugrsctl service`
- `hugrsctl service status`
- `hugrsctl service stats`
- `hugrsctl service gc --dry-run`
- `hugrsctl service gc`

### repos

- `hugrsctl repos`
- `hugrsctl repos list`
- `hugrsctl repos show <repo>`
- `hugrsctl repos delete <repo>`

### files

- `hugrsctl files`
- `hugrsctl files list`
- `hugrsctl files show --repo <repo> --file <file>`
- `hugrsctl files delete --repo <repo> --file <file>`

## source 规则

`source` 只是可选筛选参数，不是主用户心智。

- 可选值：`hf`、`ms`
- 不指定 `source` 时：
  - 查看类命令返回聚合视图
  - 删除类命令对所有 source 生效

聚合视图用 `sources: ["hf", "ms"]` 表示对象存在于哪些来源中，不展开成多份顶层对象。

## 用户对象模型

### service

用户关注服务状态、配置摘要、缓存统计。

### repo

用户关注某个 repo 是否已缓存、包含多少文件、逻辑大小、出现在哪些 source。

### file

用户关注某个文件是否已缓存、属于哪个 repo、逻辑大小、出现在哪些 source。

### chunk

chunk 不属于文件，不进入用户对象模型。它只属于服务内部的去重存储实现。

## 输出与格式化

API 返回原始机器数据，展示格式由 `hugrsctl` 负责。

规则：

- bytes 用整数返回
- 百分比用数值返回
- 时间用标准时间字符串返回
- `hugrsctl` 默认转成人类可读格式
- `hugrsctl --json` 原样输出 API 数据

## 控制面路径

管理 API 放在保留前缀 `/_hugrs` 下，避免与上游 HuggingFace / ModelScope 路由冲突。

### service

- `GET /_hugrs/service`
- `GET /_hugrs/service/stats`
- `POST /_hugrs/service/gc`

### repos

- `GET /_hugrs/repos`
- `GET /_hugrs/repos/{repo}`
- `DELETE /_hugrs/repos/{repo}`

### files

- `GET /_hugrs/files`
- `GET /_hugrs/files/show`
- `DELETE /_hugrs/files`

说明：

- `gc --dry-run` 通过请求参数或请求体表达 dry-run 语义
- `files show` 与 `files delete` 继续使用 `repo + file` 二元组定位文件

## 返回语义

### 聚合视图

不指定 `source` 时：

- `repos show` 中的文件列表按文件聚合，每个文件带 `sources`
- `files show` 返回单文件聚合对象，带 `sources`

### 逻辑大小

repo / file / service 面向用户的大小统计均使用逻辑大小，不做 chunk 去重分摊。

chunk 级节省量只体现在服务统计里，不向 repo / file 维度分摊。

## 删除语义

删除不是“删除 chunk”，而是“删除文件缓存引用”。

### `files delete`

执行内容：

1. 删除对应文件元数据
2. 删除文件与 chunk 的关联关系
3. 对相关 chunk 执行 `ref_count` 递减
4. 将 `ref_count = 0` 的 chunk 标记为 orphan

### `repos delete`

语义是对 repo 下所有文件执行同样的缓存引用删除逻辑。

### 删除后的保证

- 不影响已经开始的读取
- 删除后新请求不可再通过该文件元数据命中缓存
- 删除不保证立刻释放物理空间

## GC 语义

GC 负责真正的物理空间回收。

### `service gc --dry-run`

- 只统计候选 orphan chunk 与预计可回收空间
- 不改元数据
- 不删物理 chunk

### `service gc`

- 仅处理 orphan chunk
- 负责物理删除 chunk 数据
- 删除完成后更新相应内部状态

### 关键约束

- 删除负责“摘引用”
- GC 负责“回收空间”

两者语义分离，避免在 delete 中混入复杂的物理回收逻辑。

## 最小并发模型

本轮不引入复杂任务系统，也不追求复杂并发控制。

只要求满足以下最小约束：

1. 不打断正常读请求
2. 不把元数据与 chunk 生命周期弄乱
3. 尽量满足删除与回收需求

### 读请求

- 已开始的读取可继续完成
- 删除不强行中断活跃读

### GC

- GC 不持有覆盖整个执行期的全局锁
- GC 采用 batch 处理
- 每个 batch 短暂进入写协调区
- 若发现与用户请求冲突，则跳过冲突对象，留待下一轮 GC

### 设计目标

GC 是“尽力回收”，不是“必须一次清空全部 orphan”。

## 与当前分层的关系

本设计不改变当前三层职责方向：

- `metadata.rs`：持久元数据真相源
- `session.rs`：读会话与 chunk 数据面
- `service.rs`：编排与管理操作入口

本轮设计只要求：

- 用户命令不暴露 chunk
- 删除与 GC 的语义围绕文件引用与 orphan chunk 展开
- 管理操作不再绕过服务进程直接改元数据

## 总结

本设计将 HugRS 控制面收敛为：

1. 资源分组式 CLI：`service` / `repos` / `files`
2. `hugrs` 负责服务与管理 API，`hugrsctl` 负责远程管理
3. 删除只删除文件缓存引用
4. GC 负责 chunk 物理回收
5. `source` 仅为可选筛选参数
6. chunk 完全留在服务内部，不进入用户心智

这使控制面更符合模型文件缓存的真实使用场景，也避免将内部去重存储细节错误暴露给用户。
