use hugrs::metadata::MetadataStore;
use tempfile::TempDir;

#[test]
fn test_init_schema() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let row = store
        .raw_conn()
        .unwrap()
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='files'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert_eq!(row, "files");

    let version: i64 = store
        .raw_conn()
        .unwrap()
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 7);
}

#[test]
fn test_add_and_get_file() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let file = store
        .add_file("model.bin", "my-model", 1024, "upload")
        .unwrap();
    assert_eq!(file.name, "model.bin");
    assert_eq!(file.repo, "my-model");
    assert_eq!(file.total_size, 1024);
    assert_eq!(file.source, "upload");

    let got = store.get_file_by_name("model.bin", "upload").unwrap();
    assert!(got.is_some());
}

#[test]
fn test_add_chunk_and_link() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store
        .ensure_chunk("abc123", "local", "ab/c3/abc123", 100, 100)
        .unwrap();
    let chunk = store.get_chunk("abc123").unwrap().unwrap();
    assert_eq!(chunk.size, 100);
    assert_eq!(chunk.ref_count, 0);
    assert!(chunk.orphaned_at.is_some());

    let file = store.add_file("test.bin", "repo-x", 100, "upload").unwrap();
    store.link_file_chunk(file.id, "abc123", 0, 100).unwrap();

    let chunk = store.get_chunk("abc123").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 1);
    assert_eq!(chunk.orphaned_at, None);
}

#[test]
fn test_unlink_and_gc() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store
        .ensure_chunk("def456", "local", "de/f4/def456", 200, 200)
        .unwrap();
    let file = store.add_file("x.bin", "repo-z", 200, "upload").unwrap();
    store.link_file_chunk(file.id, "def456", 0, 200).unwrap();

    store.delete_file("x.bin", "upload").unwrap();

    let orphans = store.get_orphan_chunks().unwrap();
    assert_eq!(orphans.len(), 1);
    assert_eq!(orphans[0].sha256, "def456");
    assert!(orphans[0].orphaned_at.is_some());
}

#[test]
fn test_delete_chunks_batch_removes_only_unreferenced_candidates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store
        .ensure_chunk("orphan-a", "local", "or/ph/orphan-a", 100, 100)
        .unwrap();
    store
        .ensure_chunk("orphan-b", "local", "or/ph/orphan-b", 200, 200)
        .unwrap();
    store
        .ensure_chunk("live-c", "local", "li/ve/live-c", 300, 300)
        .unwrap();
    let file = store.add_file("live.bin", "repo", 300, "hf").unwrap();
    store.link_file_chunk(file.id, "live-c", 0, 300).unwrap();

    let deleted = store
        .delete_chunks_batch(&[
            "orphan-a".to_string(),
            "live-c".to_string(),
            "orphan-b".to_string(),
        ])
        .unwrap();

    assert_eq!(deleted.len(), 2);
    assert!(deleted.contains(&"orphan-a".to_string()));
    assert!(deleted.contains(&"orphan-b".to_string()));
    assert!(!deleted.contains(&"live-c".to_string()));
    assert!(store.get_chunk("orphan-a").unwrap().is_none());
    assert!(store.get_chunk("orphan-b").unwrap().is_none());
    assert!(store.get_chunk("live-c").unwrap().is_some());
}

#[test]
fn test_list_files() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store.add_file("a.bin", "repo-1", 100, "upload").unwrap();
    store.add_file("b.bin", "repo-2", 200, "pull").unwrap();

    let files = store.list_files().unwrap();
    assert_eq!(files.len(), 2);
}

#[test]
fn test_stats() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let stats = store.get_stats().unwrap();
    assert_eq!(stats.file_count, 0);
    assert_eq!(stats.repo_count, 0);
    assert_eq!(stats.original_bytes, 0);
    assert_eq!(stats.stored_bytes, 0);
    assert_eq!(stats.bytes_saved, 0);
    assert_eq!(stats.saved_percent, 0.0);

    let file = store.add_file("f.bin", "r", 500, "upload").unwrap();
    store.ensure_chunk("s1", "local", "s/1", 500, 500).unwrap();
    store.link_file_chunk(file.id, "s1", 0, 500).unwrap();

    let stats = store.get_stats().unwrap();
    assert_eq!(stats.file_count, 1);
    assert_eq!(stats.repo_count, 1);
    assert_eq!(stats.original_bytes, 500);
    assert_eq!(stats.stored_bytes, 500);
    assert_eq!(stats.bytes_saved, 0);
    assert_eq!(stats.saved_percent, 0.0);
}

#[test]
fn test_stats_ignores_orphan_chunks() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let file = store.add_file("f.bin", "r", 1000, "upload").unwrap();
    store.ensure_chunk("s1", "local", "s/1", 500, 400).unwrap();
    store.ensure_chunk("s2", "local", "s/2", 500, 400).unwrap();
    store.link_file_chunk(file.id, "s1", 0, 500).unwrap();
    store.link_file_chunk(file.id, "s2", 1, 500).unwrap();

    let stats = store.get_stats().unwrap();
    assert_eq!(stats.file_count, 1);
    assert_eq!(stats.original_bytes, 1000);
    assert_eq!(stats.stored_bytes, 800);

    store.delete_file("f.bin", "upload").unwrap();

    let stats = store.get_stats().unwrap();
    assert_eq!(stats.file_count, 0);
    assert_eq!(stats.original_bytes, 0);
    assert_eq!(stats.stored_bytes, 0);
    assert_eq!(stats.bytes_saved, 0);
    assert_eq!(stats.saved_percent, 0.0);
}

#[test]
fn test_reconsile_chunk_refs_dry_run_reports_without_mutating() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let file = store.add_file("f.bin", "repo", 100, "hf").unwrap();
    store
        .ensure_chunk("sha-a", "local", "sh/a", 100, 100)
        .unwrap();
    store.link_file_chunk(file.id, "sha-a", 0, 100).unwrap();

    {
        let conn = store.raw_conn().unwrap();
        conn.execute("UPDATE chunks SET ref_count = 4 WHERE sha256 = 'sha-a'", [])
            .unwrap();
    }

    let result = store.reconsile_chunk_refs(true).unwrap();
    assert_eq!(result.scanned_chunks, 1);
    assert_eq!(result.mismatched_chunks, 1);
    assert_eq!(result.refcount_fixed, 1);

    let chunk = store.get_chunk("sha-a").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 4);
}

#[test]
fn test_reconsile_chunk_refs_apply_repairs_refcount_and_orphan_state() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    let file = store.add_file("f.bin", "repo", 100, "hf").unwrap();
    store
        .ensure_chunk("sha-a", "local", "sh/a", 100, 100)
        .unwrap();
    store.link_file_chunk(file.id, "sha-a", 0, 100).unwrap();
    store.mark_chunk_orphaned("sha-a").unwrap();

    {
        let conn = store.raw_conn().unwrap();
        conn.execute("UPDATE chunks SET ref_count = 4 WHERE sha256 = 'sha-a'", [])
            .unwrap();
    }

    let result = store.reconsile_chunk_refs(false).unwrap();
    assert_eq!(result.mismatched_chunks, 1);
    assert_eq!(result.refcount_fixed, 1);
    assert_eq!(result.orphaned_cleared, 1);

    let chunk = store.get_chunk("sha-a").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 1);
    assert_eq!(chunk.orphaned_at, None);
}

#[test]
fn test_delete_file_transaction_rolls_back_on_failure() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store
        .ensure_chunk("sha-a", "local", "sh/a", 100, 100)
        .unwrap();
    let file = store.add_file("f.bin", "repo", 100, "hf").unwrap();
    store.link_file_chunk(file.id, "sha-a", 0, 100).unwrap();

    {
        let conn = store.raw_conn().unwrap();
        conn.execute_batch(
            "CREATE TRIGGER fail_file_chunk_delete
             BEFORE DELETE ON file_chunks
             BEGIN
               SELECT RAISE(FAIL, 'boom');
             END;",
        )
        .unwrap();
    }

    let err = store.delete_file("f.bin", "hf").unwrap_err();
    assert!(err.to_string().contains("boom"));

    let file = store.get_file_by_name("f.bin", "hf").unwrap();
    assert!(file.is_some(), "file row should still exist after rollback");

    let chunk = store.get_chunk("sha-a").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 1, "ref_count should roll back");
    assert_eq!(chunk.orphaned_at, None, "orphan marker should roll back");

    let links = store.get_file_chunks(file.unwrap().id).unwrap();
    assert_eq!(
        links.len(),
        1,
        "file_chunks should still exist after rollback"
    );
}

#[test]
fn test_delete_file_handles_duplicate_chunk_refs_in_single_file() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store
        .ensure_chunk("dup-sha", "local", "du/ps/dup-sha", 100, 100)
        .unwrap();
    let file = store.add_file("dup.bin", "repo", 200, "hf").unwrap();
    store.link_file_chunk(file.id, "dup-sha", 0, 100).unwrap();
    store.link_file_chunk(file.id, "dup-sha", 1, 100).unwrap();

    let chunk = store.get_chunk("dup-sha").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 2);

    let deleted = store.delete_file("dup.bin", "hf").unwrap();
    assert!(deleted);

    let chunk = store.get_chunk("dup-sha").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 0);
    assert!(chunk.orphaned_at.is_some());
}

#[test]
fn test_orphan_stats_count_chunks_and_bytes() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store
        .ensure_chunk("orphan-1", "local", "or/ph/orphan-1", 100, 80)
        .unwrap();
    store
        .ensure_chunk("orphan-2", "local", "or/ph/orphan-2", 200, 150)
        .unwrap();
    store.mark_chunk_orphaned("orphan-1").unwrap();
    store.mark_chunk_orphaned("orphan-2").unwrap();

    let (count, bytes) = store.list_orphan_chunks_stats().unwrap();
    assert_eq!(count, 2);
    assert_eq!(bytes, 230);
}

#[test]
fn test_orphan_batch_query_uses_covering_index() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();
    let conn = store.raw_conn().unwrap();

    let mut stmt = conn
        .prepare(
            "EXPLAIN QUERY PLAN
             SELECT sha256, backend, path, size, compressed_size, ref_count, orphaned_at
             FROM chunks
             WHERE ref_count = 0 AND orphaned_at IS NOT NULL
             ORDER BY orphaned_at ASC
             LIMIT 32",
        )
        .unwrap();
    let details: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert!(
        details.iter().any(|detail| {
            detail.contains("idx_chunks_ref_count_orphaned_at")
                || detail.contains("idx_chunks_gc_orphaned")
        }),
        "expected orphan GC query to use a dedicated orphan index, got {details:?}"
    );
    assert!(
        !details.iter().any(|detail| detail.contains("TEMP B-TREE")),
        "expected orphan GC query to avoid temp sorting, got {details:?}"
    );
}

#[test]
fn test_touch_repo() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store
        .add_file("f1.bin", "test-repo", 100, "upload")
        .unwrap();
    store
        .add_file("f2.bin", "test-repo", 200, "upload")
        .unwrap();
    store.touch_repo("test-repo").unwrap();

    let f1 = store.get_file_by_name("f1.bin", "upload").unwrap().unwrap();
    let f2 = store.get_file_by_name("f2.bin", "upload").unwrap().unwrap();
    assert!(!f1.last_accessed.is_empty());
    assert!(!f2.last_accessed.is_empty());
}

#[test]
fn test_list_repos_by_access() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store.add_file("old.bin", "repo-a", 100, "upload").unwrap();
    store.add_file("new.bin", "repo-b", 200, "pull").unwrap();

    let repos = store.list_repos_by_access(10).unwrap();
    assert!(repos.len() >= 2);
}

#[test]
fn test_delete_files_by_repo() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store.add_file("a.txt", "repo-del", 100, "upload").unwrap();
    store.add_file("b.txt", "repo-del", 200, "upload").unwrap();
    store.add_file("c.txt", "repo-keep", 300, "upload").unwrap();

    let deleted = store.delete_files_by_repo("repo-del").unwrap();
    assert_eq!(deleted, 2);

    let files = store.list_files().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].name, "c.txt");
}

#[test]
fn test_same_name_different_source() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let store = MetadataStore::new(&db_path).unwrap();

    store.add_file("model.bin", "repo", 100, "hf").unwrap();
    store.add_file("model.bin", "repo", 200, "ms").unwrap();

    let hf = store.get_file_by_name("model.bin", "hf").unwrap().unwrap();
    let ms = store.get_file_by_name("model.bin", "ms").unwrap().unwrap();

    assert_eq!(hf.total_size, 100);
    assert_eq!(ms.total_size, 200);
    assert_ne!(hf.id, ms.id);
}

#[test]
fn test_migration_from_old_unique_name_schema() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");

    // Create v0.2.0-style DB manually
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE files (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                name          TEXT NOT NULL UNIQUE,
                repo          TEXT NOT NULL DEFAULT '',
                total_size    INTEGER NOT NULL,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
                source        TEXT NOT NULL,
                etag          TEXT,
                x_repo_commit TEXT,
                x_linked_size INTEGER,
                x_linked_etag TEXT,
                content_type  TEXT
            );
            CREATE TABLE trunks (
                sha256           TEXT PRIMARY KEY,
                backend          TEXT NOT NULL,
                path             TEXT NOT NULL,
                size             INTEGER NOT NULL,
                ref_count        INTEGER NOT NULL DEFAULT 0,
                compressed_size  INTEGER
            );
            CREATE TABLE file_trunks (
                file_id      INTEGER NOT NULL REFERENCES files(id),
                sha256       TEXT NOT NULL REFERENCES trunks(sha256),
                chunk_index  INTEGER NOT NULL,
                chunk_size   INTEGER NOT NULL,
                PRIMARY KEY (file_id, chunk_index)
            );
            CREATE TABLE http_cache (
                url        TEXT PRIMARY KEY,
                status     INTEGER NOT NULL,
                headers    TEXT NOT NULL,
                body       BLOB NOT NULL,
                cached_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );
            PRAGMA user_version = 1;
            INSERT INTO files (name, repo, total_size, source, etag, content_type) VALUES ('model.bin', 'repo', 100, 'pull', 'abc', 'application/octet-stream');"
        ).unwrap();
    }

    // Open with MetadataStore — should trigger migration
    let store = MetadataStore::new(&db_path).unwrap();

    // Old row should now have source='hf' (migrated from 'pull')
    let file = store.get_file_by_name("model.bin", "hf").unwrap().unwrap();
    assert_eq!(file.total_size, 100);

    // Should be able to add same name with different source now
    store.add_file("model.bin", "repo", 200, "ms").unwrap();
    let ms = store.get_file_by_name("model.bin", "ms").unwrap().unwrap();
    assert_eq!(ms.total_size, 200);
}

#[test]
fn test_ensure_chunk_and_link_replaces_old_sha_on_re_download() {
    let dir = TempDir::new().unwrap();
    let store = MetadataStore::new(&dir.path().join("test.db")).unwrap();
    let file = store.add_file("f.bin", "r", 4194304, "hf").unwrap();

    let old_sha = "oldsha";
    let new_sha = "newsha";
    store
        .ensure_chunk("oldsha", "local", "ol/ds/oldsha", 100, 100)
        .unwrap();
    store
        .ensure_chunk("newsha", "local", "ne/ws/newsha", 200, 200)
        .unwrap();

    // First link with old_sha
    store
        .ensure_chunk_and_link(
            old_sha,
            "local",
            "ol/ds/oldsha",
            100,
            100,
            file.id,
            0,
            4194304,
        )
        .unwrap();
    let chunks = store.get_file_chunks(file.id).unwrap();
    assert_eq!(chunks[0].sha256, "oldsha");

    // Re-download with different sha256 should replace the old one
    store
        .ensure_chunk_and_link(
            new_sha,
            "local",
            "ne/ws/newsha",
            200,
            200,
            file.id,
            0,
            4194304,
        )
        .unwrap();
    let chunks = store.get_file_chunks(file.id).unwrap();
    assert_eq!(
        chunks[0].sha256, "newsha",
        "BUG: ensure_chunk_and_link with new sha256 must replace old entry"
    );

    let old = store.get_chunk(old_sha).unwrap().unwrap();
    assert_eq!(
        old.ref_count, 0,
        "BUG: old sha256 ref_count should be 0 after replacement"
    );
    let new = store.get_chunk(new_sha).unwrap().unwrap();
    assert_eq!(new.ref_count, 1, "BUG: new sha256 ref_count should be 1");
}
