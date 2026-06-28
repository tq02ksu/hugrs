# HugRS CLI 文档

## 概览

HugRS 提供两个二进制：

- `hugrs`：启动代理守护进程
- `hugrsctl`：用于查看和管理 service、repo、file

`hugrsctl` 只面向缓存管理，不暴露 chunk 级内部实现。

## 连接默认值

- endpoint 默认值：`http://127.0.0.1:3000`
- 可通过 `--endpoint` 或 `HUGRS_CONTROL_ENDPOINT` 覆盖 endpoint
- admin token 解析顺序：
  1. `--admin-token`
  2. `HUGRS_ADMIN_TOKEN`
  3. 当前平台默认 token 文件

默认 admin token 文件：

- macOS：`~/Library/Caches/hugrs/admin.token`
- Linux：`~/.cache/hugrs/admin.token`

## 资源模型

顶层命令：

- `service`
- `repo`
- `file`

兼容别名：

- `repos` = `repo`
- `files` = `file`

默认动作：

- `hugrsctl service` = `hugrsctl service status`
- `hugrsctl repo` = `hugrsctl repo list`
- `hugrsctl file` = `hugrsctl file list`

## 输出

- 默认输出：人类可读文本
- `--json`：格式化 JSON

默认文本输出会把大小字段转成人类可读格式；JSON 输出保留 API 返回的原始数值。

## 全局参数

```bash
hugrsctl [--json] [--source <SOURCE>] [--endpoint <URL>] [--admin-token <TOKEN>] <COMMAND>
```

- `--json`：输出 JSON，而不是文本
- `--source <SOURCE>`：只操作某一个来源，例如 `hf` 或 `ms`
- `--endpoint <URL>`：控制面 API 地址
- `--admin-token <TOKEN>`：覆盖 admin token

## 命令

### `service`

```bash
hugrsctl service
hugrsctl service status
hugrsctl service stats
hugrsctl service gc --dry-run
hugrsctl service gc
hugrsctl service gc --batch-size 100
```

- `status`：查看守护进程状态和实际 endpoint
- `stats`：查看 repo/file 数量与缓存体积摘要
- `gc`：回收 orphan chunk

### `repo`

```bash
hugrsctl repo
hugrsctl repo list
hugrsctl repo show Qwen/Qwen3-8B
hugrsctl repo delete Qwen/Qwen3-8B
hugrsctl repo --source hf
```

- `list`：列出已缓存 repo
- `show <repo>`：查看 repo 摘要和缓存文件
- `delete <repo>`：删除该 repo 的文件缓存元数据

如果不设置 `--source`，操作默认覆盖所有来源。删除时不带 `--source` 会删除该 repo 的全部来源缓存记录。

### `file`

```bash
hugrsctl file
hugrsctl file list
hugrsctl file show --repo Qwen/Qwen3-8B --file config.json
hugrsctl file delete --repo Qwen/Qwen3-8B --file config.json
hugrsctl file --source ms
```

- `list`：列出已缓存文件
- `show`：查看单个缓存文件
- `delete`：删除文件缓存元数据

如果不设置 `--source`，操作默认覆盖所有来源。删除时不带 `--source` 会删除该文件的全部来源缓存记录。

## 说明

- delete 只删除文件缓存引用
- chunk 数据由 `hugrsctl service gc` 负责真正回收
- 控制面 API 路径前缀为 `/_hugrs/...`
