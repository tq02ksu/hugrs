# Packaging And Paths Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make HugRS package-friendly for Homebrew and Linux/macOS installs by standardizing default path semantics without breaking existing CLI, env, or config overrides.

**Architecture:** Keep install-time concerns separate from runtime state. `hugrs` and `hugrsctl` remain relocatable binaries; runtime config, cache, and persistent state use platform-appropriate defaults. Deliver this in two phases: Phase 1 updates default path logic plus docs/tests; Phase 2 adds package-manager and service integration artifacts.

**Tech Stack:** Rust, `dirs`, clap, figment, GitHub Actions, Markdown docs

---

## File Map

**Phase 1 must modify:**
- `src/config.rs` — default config/data/cache path helpers and config-file lookup order
- `src/admin_client.rs` — `hugrsctl` default admin token file path resolution
- `tests/config_tests.rs` — config loading and default path behavior
- `docs/CONFIG.md` — English config docs
- `docs/CONFIG_zh.md` — Chinese config docs
- `docs/CLI.md` — English CLI docs
- `docs/CLI_zh.md` — Chinese CLI docs
- `README.md` — English top-level install/runtime notes
- `README_zh.md` — Chinese top-level install/runtime notes

**Phase 2 should create or modify:**
- `packaging/homebrew/hugrs.rb` or tap repo formula — Homebrew install entrypoint
- `packaging/systemd/hugrs.service` — Linux service unit
- `packaging/launchd/io.github.anomalyco.hugrs.plist` — macOS service unit
- `.github/workflows/release.yml` — attach packaging artifacts if needed

## Phase 1: Default Path Semantics

### Task 1: Define path policy boundaries

**Files:**
- Modify: `src/config.rs`
- Modify: `src/admin_client.rs`
- Test: `tests/config_tests.rs`

- [ ] **Step 1: Write failing tests for default-path intent**

Add tests that lock these expectations:
- cache chunks default under platform cache dir
- DB default under persistent data dir, not cache dir
- admin token default under persistent data dir, not cache dir
- config file lookup includes platform config dir

- [ ] **Step 2: Run targeted tests to verify failure**

Run: `cargo test config_tests -- --nocapture`

Expected: failures because current defaults place DB and token under cache semantics and config lookup is not yet fully standardized.

- [ ] **Step 3: Implement shared default-path helpers**

In `src/config.rs`, split the current single cache-based default into three helpers:
- config dir helper
- cache dir helper
- data dir helper

Use them to derive:
- chunk root: cache dir
- DB path: data dir
- admin token file: data dir
- config file lookup: config dir

Preserve all existing override precedence:
- defaults
- config file
- `.env`
- env vars
- CLI flags

- [ ] **Step 4: Align `hugrsctl` token discovery with daemon defaults**

Update `src/admin_client.rs` so `default_admin_token_file()` resolves to the same default location as the daemon-side admin token file.

- [ ] **Step 5: Run targeted tests to verify pass**

Run:
- `cargo test config_tests -- --nocapture`
- `cargo test admin_client -- --nocapture`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/config.rs src/admin_client.rs tests/config_tests.rs
git commit -m "refactor: standardize runtime default paths"
```

### Task 2: Document the new default path model

**Files:**
- Modify: `docs/CONFIG.md`
- Modify: `docs/CONFIG_zh.md`
- Modify: `docs/CLI.md`
- Modify: `docs/CLI_zh.md`
- Modify: `README.md`
- Modify: `README_zh.md`

- [ ] **Step 1: Update config docs**

Document the three-way split:
- config file lives under config dir
- chunk cache lives under cache dir
- DB and admin token live under persistent data dir

Call out platform defaults explicitly for:
- macOS
- Linux

- [ ] **Step 2: Update CLI docs**

Document where `hugrsctl` looks for the default admin token file after the change, and make sure the precedence order text matches implementation.

- [ ] **Step 3: Update README install/runtime notes**

Keep README concise:
- installed binaries are relocatable
- runtime state is not stored beside the executable
- users can override paths with config/env/CLI

- [ ] **Step 4: Verify bilingual sync**

Check each pair remains equivalent:
- `README.md` / `README_zh.md`
- `docs/CONFIG.md` / `docs/CONFIG_zh.md`
- `docs/CLI.md` / `docs/CLI_zh.md`

- [ ] **Step 5: Commit**

```bash
git add README.md README_zh.md docs/CONFIG.md docs/CONFIG_zh.md docs/CLI.md docs/CLI_zh.md
git commit -m "docs: clarify package-friendly runtime paths"
```

### Task 3: Full verification for Phase 1

**Files:**
- No new files

- [ ] **Step 1: Run formatting**

Run: `cargo fmt -- --check`

Expected: PASS

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -- -D warnings`

Expected: PASS

- [ ] **Step 3: Run full tests**

Run: `cargo test`

Expected: PASS

- [ ] **Step 4: Commit verification-safe state**

If any verification-driven touchups were required:

```bash
git add -A
git commit -m "test: verify package-friendly path defaults"
```

## Phase 2: Packaging Integration

### Task 4: Add Homebrew installation path

**Files:**
- Create: `packaging/homebrew/hugrs.rb` or publish to a tap repo
- Modify: `README.md`
- Modify: `README_zh.md`

- [ ] **Step 1: Choose formula strategy**

Pick one:
- in-repo formula template for manual tap publication
- dedicated tap repository for direct `brew install`

Recommended: dedicated tap repo, because Homebrew formulas are typically maintained outside the main source repo.

- [ ] **Step 2: Author formula**

Formula should:
- install both `hugrs` and `hugrsctl`
- point to GitHub release tarballs
- verify sha256
- include a `service do` block for `hugrs`

- [ ] **Step 3: Document install command**

Add user-facing examples for:
- `brew tap ...`
- `brew install ...`
- `brew services start ...`

- [ ] **Step 4: Commit**

```bash
git add packaging/homebrew/hugrs.rb README.md README_zh.md
git commit -m "packaging: add homebrew distribution path"
```

### Task 5: Add service templates

**Files:**
- Create: `packaging/systemd/hugrs.service`
- Create: `packaging/launchd/io.github.anomalyco.hugrs.plist`
- Modify: `README.md`
- Modify: `README_zh.md`

- [ ] **Step 1: Add `systemd` unit**

Use:
- explicit `ExecStart`
- non-root service user if documented
- environment/config file override guidance
- writable runtime paths outside install tree

- [ ] **Step 2: Add `launchd` plist**

Use:
- installed binary path
- config override argument
- standard macOS user or system agent conventions

- [ ] **Step 3: Document service startup**

Add examples for:
- `systemctl enable --now hugrs`
- `launchctl bootstrap ...`

- [ ] **Step 4: Commit**

```bash
git add packaging/systemd/hugrs.service packaging/launchd/io.github.anomalyco.hugrs.plist README.md README_zh.md
git commit -m "packaging: add service templates"
```

### Task 6: Release and packaging alignment

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Decide whether to publish packaging artifacts**

Optional artifacts:
- Homebrew formula snapshot
- service templates alongside release archives

- [ ] **Step 2: Keep release payload minimal**

Do not block binary release on package-manager extras unless they are stable and tested.

- [ ] **Step 3: Verify release matrix still includes both binaries**

Confirm:
- `hugrs`
- `hugrsctl`
- checksums

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: align release artifacts with packaging support"
```

## Recommendation

Implement only **Phase 1** first. That is the smallest set of changes that improves professional packaging posture without introducing distribution maintenance overhead.

Phase 2 is valuable, but it should come after the default-path semantics are cleaned up and documented.
