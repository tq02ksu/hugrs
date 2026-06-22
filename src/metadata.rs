use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct File {
    pub id: i64,
    pub name: String,
    pub repo: String,
    pub total_size: i64,
    pub created_at: String,
    pub last_accessed: String,
    pub source: String,
    pub etag: Option<String>,
    pub x_repo_commit: Option<String>,
    pub x_linked_size: Option<i64>,
    pub x_linked_etag: Option<String>,
    pub content_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Trunk {
    pub sha256: String,
    pub backend: String,
    pub path: String,
    pub size: i64,
    pub compressed_size: Option<i64>,
    pub ref_count: i64,
}

#[derive(Debug, Clone)]
pub struct FileTrunk {
    pub file_id: i64,
    pub sha256: String,
    pub chunk_index: i64,
    pub chunk_size: i64,
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub repo_count: i64,
    pub file_count: i64,
    pub trunk_count: i64,
    pub total_size: i64,
    pub unique_size: i64,
}

pub struct MetadataStore {
    conn: Arc<Mutex<Connection>>,
}

impl MetadataStore {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL")?;
        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init_schema()?;
        Ok(store)
    }

    pub fn raw_conn(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Connection>> {
        Ok(self.conn.lock().unwrap())
    }

    fn init_schema(&self) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS files (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                name          TEXT NOT NULL UNIQUE,
                repo          TEXT NOT NULL DEFAULT '',
                total_size    INTEGER NOT NULL,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
                source        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trunks (
                sha256    TEXT PRIMARY KEY,
                backend   TEXT NOT NULL,
                path      TEXT NOT NULL,
                size      INTEGER NOT NULL,
                ref_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS file_trunks (
                file_id      INTEGER NOT NULL REFERENCES files(id),
                sha256       TEXT NOT NULL REFERENCES trunks(sha256),
                chunk_index  INTEGER NOT NULL,
                chunk_size   INTEGER NOT NULL,
                PRIMARY KEY (file_id, chunk_index)
            );

            CREATE TABLE IF NOT EXISTS http_cache (
                url        TEXT PRIMARY KEY,
                status     INTEGER NOT NULL,
                headers    TEXT NOT NULL,
                body       BLOB NOT NULL,
                cached_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )?;
        // Migration: add repo column if upgrading from older schema
        let has_repo: bool = conn.prepare("SELECT repo FROM files LIMIT 0").is_ok();
        if !has_repo {
            conn.execute_batch("ALTER TABLE files ADD COLUMN repo TEXT NOT NULL DEFAULT ''")?;
        }
        let has_etag: bool = conn.prepare("SELECT etag FROM files LIMIT 0").is_ok();
        if !has_etag {
            conn.execute_batch("ALTER TABLE files ADD COLUMN etag TEXT")?;
        }
        let has_x_repo_commit: bool = conn
            .prepare("SELECT x_repo_commit FROM files LIMIT 0")
            .is_ok();
        if !has_x_repo_commit {
            conn.execute_batch("ALTER TABLE files ADD COLUMN x_repo_commit TEXT")?;
        }
        let has_x_linked_size: bool = conn
            .prepare("SELECT x_linked_size FROM files LIMIT 0")
            .is_ok();
        if !has_x_linked_size {
            conn.execute_batch("ALTER TABLE files ADD COLUMN x_linked_size INTEGER")?;
        }
        let has_x_linked_etag: bool = conn
            .prepare("SELECT x_linked_etag FROM files LIMIT 0")
            .is_ok();
        if !has_x_linked_etag {
            conn.execute_batch("ALTER TABLE files ADD COLUMN x_linked_etag TEXT")?;
        }
        let has_content_type: bool = conn
            .prepare("SELECT content_type FROM files LIMIT 0")
            .is_ok();
        if !has_content_type {
            conn.execute_batch("ALTER TABLE files ADD COLUMN content_type TEXT")?;
        }
        let has_compressed_size: bool = conn
            .prepare("SELECT compressed_size FROM trunks LIMIT 0")
            .is_ok();
        if !has_compressed_size {
            conn.execute_batch("ALTER TABLE trunks ADD COLUMN compressed_size INTEGER")?;
        }
        Ok(())
    }

    pub fn add_file(
        &self,
        name: &str,
        repo: &str,
        total_size: i64,
        source: &str,
    ) -> anyhow::Result<File> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO files (name, repo, total_size, source, last_accessed) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            params![name, repo, total_size, source],
        )?;
        let id = conn.last_insert_rowid();
        Ok(File {
            id,
            name: name.to_string(),
            repo: repo.to_string(),
            total_size,
            created_at: String::new(),
            last_accessed: String::new(),
            source: source.to_string(),
            etag: None,
            x_repo_commit: None,
            x_linked_size: None,
            x_linked_etag: None,
            content_type: None,
        })
    }

    pub fn get_file_by_name(&self, name: &str) -> anyhow::Result<Option<File>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type FROM files WHERE name = ?1",
        )?;
        let mut rows = stmt.query_map(params![name], |row| {
            Ok(File {
                id: row.get(0)?,
                name: row.get(1)?,
                repo: row.get(2)?,
                total_size: row.get(3)?,
                created_at: row.get(4)?,
                last_accessed: row.get(5)?,
                source: row.get(6)?,
                etag: row.get(7)?,
                x_repo_commit: row.get(8)?,
                x_linked_size: row.get(9)?,
                x_linked_etag: row.get(10)?,
                content_type: row.get(11)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn set_file_headers(
        &self,
        name: &str,
        etag: Option<&str>,
        x_repo_commit: Option<&str>,
        x_linked_size: Option<i64>,
        x_linked_etag: Option<&str>,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET etag = ?1, x_repo_commit = ?2, x_linked_size = ?3, x_linked_etag = ?4, content_type = ?5 WHERE name = ?6",
            params![etag, x_repo_commit, x_linked_size, x_linked_etag, content_type, name],
        )?;
        Ok(())
    }

    pub fn touch_repo(&self, repo: &str) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET last_accessed = datetime('now') WHERE repo = ?1",
            params![repo],
        )?;
        Ok(())
    }

    pub fn delete_file(&self, name: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let file_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM files WHERE name = ?1",
                params![name],
                |row| row.get(0),
            )
            .ok();

        match file_id {
            Some(id) => {
                Self::delete_file_by_id_internal(&conn, id)?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn delete_file_by_id_internal(conn: &Connection, file_id: i64) -> anyhow::Result<()> {
        let mut stmt = conn.prepare("SELECT sha256 FROM file_trunks WHERE file_id = ?1")?;
        let trunks: Vec<String> = stmt
            .query_map(params![file_id], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        for sha256 in &trunks {
            conn.execute(
                "UPDATE trunks SET ref_count = ref_count - 1 WHERE sha256 = ?1",
                params![sha256],
            )?;
        }
        conn.execute(
            "DELETE FROM file_trunks WHERE file_id = ?1",
            params![file_id],
        )?;
        conn.execute("DELETE FROM files WHERE id = ?1", params![file_id])?;
        Ok(())
    }

    pub fn delete_files_by_repo(&self, repo: &str) -> anyhow::Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id FROM files WHERE repo = ?1")?;
        let file_ids: Vec<i64> = stmt
            .query_map(params![repo], |row| row.get::<_, i64>(0))?
            .filter_map(|r| r.ok())
            .collect();

        for id in &file_ids {
            Self::delete_file_by_id_internal(&conn, *id)?;
        }
        Ok(file_ids.len())
    }

    pub fn list_repos_by_access(&self, limit: usize) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT repo, MIN(last_accessed) as oldest_access
             FROM files
             GROUP BY repo
             ORDER BY oldest_access ASC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn ensure_trunk(
        &self,
        sha256: &str,
        backend: &str,
        path: &str,
        size: i64,
        compressed_size: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO trunks (sha256, backend, path, size, compressed_size) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![sha256, backend, path, size, compressed_size],
        )?;
        Ok(())
    }

    pub fn get_trunk(&self, sha256: &str) -> anyhow::Result<Option<Trunk>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT sha256, backend, path, size, compressed_size, ref_count FROM trunks WHERE sha256 = ?1",
        )?;
        let mut rows = stmt.query_map(params![sha256], |row| {
            Ok(Trunk {
                sha256: row.get(0)?,
                backend: row.get(1)?,
                path: row.get(2)?,
                size: row.get(3)?,
                compressed_size: row.get(4)?,
                ref_count: row.get(5)?,
            })
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn link_file_trunk(
        &self,
        file_id: i64,
        sha256: &str,
        chunk_index: i64,
        chunk_size: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO file_trunks (file_id, sha256, chunk_index, chunk_size) VALUES (?1, ?2, ?3, ?4)",
            params![file_id, sha256, chunk_index, chunk_size],
        )?;
        conn.execute(
            "UPDATE trunks SET ref_count = ref_count + 1 WHERE sha256 = ?1",
            params![sha256],
        )?;
        Ok(())
    }

    pub fn get_file_trunks(&self, file_id: i64) -> anyhow::Result<Vec<FileTrunk>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT file_id, sha256, chunk_index, chunk_size FROM file_trunks WHERE file_id = ?1 ORDER BY chunk_index",
        )?;
        let rows = stmt.query_map(params![file_id], |row| {
            Ok(FileTrunk {
                file_id: row.get(0)?,
                sha256: row.get(1)?,
                chunk_index: row.get(2)?,
                chunk_size: row.get(3)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn list_files(&self) -> anyhow::Result<Vec<File>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type FROM files ORDER BY repo, name",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(File {
                id: row.get(0)?,
                name: row.get(1)?,
                repo: row.get(2)?,
                total_size: row.get(3)?,
                created_at: row.get(4)?,
                last_accessed: row.get(5)?,
                source: row.get(6)?,
                etag: row.get(7)?,
                x_repo_commit: row.get(8)?,
                x_linked_size: row.get(9)?,
                x_linked_etag: row.get(10)?,
                content_type: row.get(11)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn get_orphan_trunks(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT sha256 FROM trunks WHERE ref_count = 0")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn get_stats(&self) -> anyhow::Result<Stats> {
        let conn = self.conn.lock().unwrap();
        let file_count: i64 = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;
        let repo_count: i64 =
            conn.query_row("SELECT COUNT(DISTINCT repo) FROM files", [], |row| {
                row.get(0)
            })?;
        let trunk_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM trunks", [], |row| row.get(0))?;
        let total_size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(total_size), 0) FROM files",
            [],
            |row| row.get(0),
        )?;
        let unique_size: i64 =
            conn.query_row("SELECT COALESCE(SUM(size), 0) FROM trunks", [], |row| {
                row.get(0)
            })?;
        Ok(Stats {
            repo_count,
            file_count,
            trunk_count,
            total_size,
            unique_size,
        })
    }

    pub fn get_http_cache(&self, url: &str) -> anyhow::Result<Option<(u16, String, Vec<u8>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT status, headers, body FROM http_cache WHERE url = ?1")?;
        let mut rows = stmt.query_map(params![url], |row| {
            Ok((
                row.get::<_, i64>(0)? as u16,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        Ok(rows.next().transpose()?)
    }

    pub fn set_http_cache(
        &self,
        url: &str,
        status: u16,
        headers: &str,
        body: &[u8],
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO http_cache (url, status, headers, body, cached_at) VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            params![url, status as i64, headers, body],
        )?;
        Ok(())
    }
}
