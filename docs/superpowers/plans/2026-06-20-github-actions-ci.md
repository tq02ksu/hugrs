# GitHub Actions CI/CD Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two GitHub Actions workflows: CI checks on push/PR and release automation on version tags.

**Architecture:** Two standalone workflow files under `.github/workflows/`. `ci.yml` runs build/test/lint/format/audit on push and PR. `release.yml` runs on `v*` tags to publish a binary GitHub Release and a Docker image to ghcr.io.

**Tech Stack:** GitHub Actions, `dtolnay/rust-toolchain`, `Swatinem/rust-cache`, `rustsec/audit-check`, `softprops/action-gh-release`, `docker/build-push-action`

---

### Task 1: Create CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: ["**"]
  pull_request:
    branches: [main]

jobs:
  check:
    runs-on: debian-latest
    steps:
      - uses: actions/checkout@v4

      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt

      - uses: Swatinem/rust-cache@v2

      - name: Build
        run: cargo build

      - name: Test
        run: cargo test

      - name: Clippy
        run: cargo clippy -- -D warnings

      - name: Format
        run: cargo fmt -- --check

      - name: Security audit
        uses: rustsec/audit-check@v2
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 2: Verify the file was created**

Run: `ls -la .github/workflows/ci.yml`
Expected: file exists

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add CI workflow (build, test, clippy, fmt, audit)"
```

---

### Task 2: Create Release workflow

**Files:**
- Create: `.github/workflows/release.yml`

- [ ] **Step 1: Create `.github/workflows/release.yml`**

```yaml
name: Release

on:
  push:
    tags: ["v*"]

jobs:
  binary:
    runs-on: debian-latest
    steps:
      - uses: actions/checkout@v4

      - uses: dtolnay/rust-toolchain@stable

      - uses: Swatinem/rust-cache@v2

      - name: Build release binary
        run: cargo build --release

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          files: target/release/hugrs
          generate_release_notes: true

  docker:
    runs-on: debian-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4

      - name: Extract tag version
        id: meta
        run: echo "version=${GITHUB_REF#refs/tags/v}" >> "$GITHUB_OUTPUT"

      - name: Login to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Build and push Docker image
        uses: docker/build-push-action@v6
        with:
          context: .
          push: true
          tags: |
            ghcr.io/${{ github.repository }}:${{ steps.meta.outputs.version }}
            ghcr.io/${{ github.repository }}:latest
```

> **Note:** The docker job requires a `Dockerfile` at the repository root. Dockerfile creation is out of scope for this plan. If absent, the docker job will fail.

- [ ] **Step 2: Verify the file was created**

Run: `ls -la .github/workflows/release.yml`
Expected: file exists

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add release workflow (binary + docker)"
```

---

## Verification

After both workflows are committed and pushed to GitHub:

1. **CI check**: Push any commit or open a PR — the CI job should appear under Actions tab and run all steps
2. **Release check**: Push a tag `v0.1.0-test` — the release workflow should trigger (docker job will fail without Dockerfile, binary job should succeed)
3. **Cleanup**: Delete the test tag after verification: `git push origin --delete v0.1.0-test`

Run locally to validate YAML syntax:
```bash
# If yamllint is available
yamllint .github/workflows/
```
