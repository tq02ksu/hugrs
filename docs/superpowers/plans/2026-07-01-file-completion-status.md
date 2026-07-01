# File Completion Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose per-file downloaded bytes and completion state in the control-plane file APIs and `hugrsctl file` output while preserving the meaning of `size`.

**Architecture:** Add a metadata helper that sums linked chunk sizes per file id, extend file response DTOs with `downloaded_size` and `complete`, and reuse that data in server aggregation and CLI rendering. Keep the implementation narrow to control-plane file endpoints and avoid changing core cache behavior.

**Tech Stack:** Rust, axum, rusqlite, clap

---

### Task 1: Add failing control-plane tests for incomplete and complete files

**Files:**
- Modify: `tests/e2e_tests.rs`
- Reference: `src/server.rs`, `src/control.rs`

- [ ] **Step 1: Write the failing tests**

Add one test that seeds a file whose metadata size is larger than the linked chunk total and asserts file list/show report `downloaded_size < size` and `complete == false`. Add one test that seeds a fully linked file and asserts `downloaded_size == size` and `complete == true`.

- [ ] **Step 2: Run targeted tests to verify they fail**

Run: `cargo test --test e2e_tests file_completion -- --nocapture`

Expected: FAIL because the current file responses do not expose `downloaded_size` or `complete`.

- [ ] **Step 3: Commit the red test**

```bash
git add tests/e2e_tests.rs
git commit -m "test: cover file completion status"
```

### Task 2: Add metadata support for downloaded byte totals

**Files:**
- Modify: `src/metadata.rs`
- Test via: `tests/metadata_tests.rs` or `tests/e2e_tests.rs`

- [ ] **Step 1: Add a helper to sum chunk bytes by file id**

Add a metadata method that returns `COALESCE(SUM(file_chunks.chunk_size), 0)` for a given file id.

- [ ] **Step 2: Run targeted tests**

Run: `cargo test --test e2e_tests file_completion -- --nocapture`

Expected: still FAIL because the API layer does not use the new metadata helper yet.

### Task 3: Extend control-plane file response models and server aggregation

**Files:**
- Modify: `src/control.rs`
- Modify: `src/server.rs`

- [ ] **Step 1: Extend response DTOs**

Add `downloaded_size: i64` and `complete: bool` to `FileListItem` and `FileShowResponse`.

- [ ] **Step 2: Populate the new fields in server aggregation**

Update the server-side file aggregation logic to calculate downloaded bytes per file entry, then aggregate grouped rows by taking the maximum downloaded bytes and deriving `complete` from `downloaded_size >= size`.

- [ ] **Step 3: Run targeted tests**

Run: `cargo test --test e2e_tests file_completion -- --nocapture`

Expected: PASS.

### Task 4: Update `hugrsctl` rendering

**Files:**
- Modify: `src/hugrsctl_cli.rs`

- [ ] **Step 1: Render completion data in table and detail views**

Keep `SIZE` as total file size, add `DOWNLOADED` and `COMPLETE` to the file table, and print `downloaded` / `complete` in `file show`.

- [ ] **Step 2: Run targeted CLI-adjacent tests if present, otherwise full targeted suite**

Run: `cargo test --test e2e_tests file_completion -- --nocapture`

Expected: PASS.

### Task 5: Final verification

**Files:**
- Verify only

- [ ] **Step 1: Run formatting**

Run: `cargo fmt -- --check`
Expected: PASS

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-features`
Expected: PASS

- [ ] **Step 3: Run full tests**

Run: `cargo test`
Expected: PASS
