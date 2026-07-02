# HugRS CLI

## Overview

HugRS has two binaries:

- `hugrs`: starts the proxy daemon and accepts daemon startup overrides
- `hugrsctl`: management client for service, repo, and file inspection

`hugrsctl` focuses on cache management only. Chunk-level internals are not exposed as user-facing resources.

## `hugrs` Daemon Flags

Common examples:

```bash
hugrs
hugrs --config ./hugrs.toml
hugrs --server-host 0.0.0.0 --server-port 3001
hugrs --db-path /data/hugrs.db
hugrs --local-root /data/chunks
```

Configuration precedence is documented in [CONFIG.md](CONFIG.md).

## Connection Defaults

- endpoint default: `http://127.0.0.1:3000`
- override endpoint with `--endpoint` or `HUGRS_CONTROL_ENDPOINT`
- admin token resolution order:
  1. `--admin-token`
  2. `HUGRS_ADMIN_TOKEN`
  3. default token file for the current platform

Default admin token file:

- macOS: `~/Library/Application Support/hugrs/admin.token`
- Linux: `~/.local/share/hugrs/admin.token`

## Resource Model

Top-level commands:

- `service`
- `repo`
- `file`

Compatibility aliases:

- `repos` = `repo`
- `files` = `file`

Default actions:

- `hugrsctl service` = `hugrsctl service status`
- `hugrsctl repo` = `hugrsctl repo list`
- `hugrsctl file` = `hugrsctl file list`

## Output

- default output: human-readable text
- `--json`: pretty JSON

Size-related fields are rendered in human-readable form by default. JSON output keeps the original numeric values returned by the API.

## Global Options

```bash
hugrsctl [--json] [--source <SOURCE>] [--endpoint <URL>] [--admin-token <TOKEN>] <COMMAND>
```

- `--json`: output JSON instead of text
- `--source <SOURCE>`: limit operations to one source, such as `hf` or `ms`
- `--endpoint <URL>`: control API endpoint
- `--admin-token <TOKEN>`: admin token override

## Commands

### `service`

```bash
hugrsctl service
hugrsctl service status
hugrsctl service stats
hugrsctl service gc --dry-run
hugrsctl service gc
```

- `status`: daemon status and effective endpoints
- `stats`: repo/file counts and cache size summary
- `gc`: reclaim orphan chunks

### `repo`

```bash
hugrsctl repo
hugrsctl repo list
hugrsctl repo show Qwen/Qwen3-8B
hugrsctl repo delete Qwen/Qwen3-8B
hugrsctl repo --source hf
```

- `list`: list cached repos
- `show <repo>`: show repo summary and cached files
- `delete <repo>`: remove cached file metadata for that repo

If `--source` is not set, operations work across all sources. Delete without `--source` removes all cached entries for that repo.

### `file`

```bash
hugrsctl file
hugrsctl file list
hugrsctl file show --repo Qwen/Qwen3-8B --file config.json
hugrsctl file delete --repo Qwen/Qwen3-8B --file config.json
hugrsctl file --source ms
```

- `list`: list cached files
- `show`: show one cached file
- `delete`: remove cached file metadata

If `--source` is not set, operations work across all sources. Delete without `--source` removes all cached entries for that file.

## Notes

- delete removes file-cache references only
- chunk data is reclaimed by `hugrsctl service gc`
- control API namespace is `/_hugrs/...`
