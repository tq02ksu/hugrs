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
    assert_eq!(version, 5);
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

    let file = store.add_file("test.bin", "repo-x", 100, "upload").unwrap();
    store.link_file_chunk(file.id, "abc123", 0, 100).unwrap();

    let chunk = store.get_chunk("abc123").unwrap().unwrap();
    assert_eq!(chunk.ref_count, 1);
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
    assert_eq!(orphans[0], "def456");
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

    store.add_file("f.bin", "r", 500, "upload").unwrap();
    store.ensure_chunk("s1", "local", "s/1", 500, 500).unwrap();

    let stats = store.get_stats().unwrap();
    assert_eq!(stats.file_count, 1);
    assert_eq!(stats.repo_count, 1);
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
