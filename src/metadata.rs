use rusqlite::{params, Connection};
use rusqlite_migration::{Migrations, M};
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
pub struct Chunk {
    pub sha256: String,
    pub backend: String,
    pub path: String,
    pub size: i64,
    pub compressed_size: Option<i64>,
    pub ref_count: i64,
}

#[derive(Debug, Clone)]
pub struct FileChunk {
    pub file_id: i64,
    pub sha256: String,
    pub chunk_index: i64,
    pub chunk_size: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Stats {
    pub repo_count: i64,
    pub file_count: i64,
    pub chunk_count: i64,
    pub total_size: i64,
    pub unique_size: i64,
    pub compression_ratio: f64,
    pub fetched_bytes: u64,
    pub served_bytes: u64,
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
        let mut conn = self.conn.lock().unwrap();

        let migrations = Migrations::new(vec![
            M::up(include_str!("migrations/001_initial_schema.sql")),
            M::up(include_str!("migrations/002_rename_trunk_to_chunk.sql")),
            M::up(include_str!("migrations/003_drop_http_cache.sql")),
        ]);

        migrations.to_latest(&mut conn)?;

        Self::run_legacy_migrations(&conn)?;

        conn.execute_batch(include_str!("migrations/004_add_indexes.sql"))?;

        Ok(())
    }

    fn run_legacy_migrations(conn: &Connection) -> anyhow::Result<()> {
        let has_files = conn.prepare("SELECT name FROM files LIMIT 0").is_ok();
        if !has_files {
            return Ok(());
        }
        let legacy_cols = [
            ("repo", "TEXT NOT NULL DEFAULT ''"),
            ("source", "TEXT NOT NULL DEFAULT 'hf'"),
            ("etag", "TEXT"),
            ("x_repo_commit", "TEXT"),
            ("x_linked_size", "INTEGER"),
            ("x_linked_etag", "TEXT"),
            ("content_type", "TEXT"),
        ];
        for (col, def) in &legacy_cols {
            let exists = conn
                .prepare(&format!("SELECT {} FROM files LIMIT 0", col))
                .is_ok();
            if !exists {
                let _ =
                    conn.execute_batch(&format!("ALTER TABLE files ADD COLUMN {} {}", col, def));
            }
        }

        let needs_source_migration: bool = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='files'",
                [],
                |row| {
                    let sql: String = row.get(0)?;
                    Ok(!sql.contains("UNIQUE(name, source)"))
                },
            )
            .unwrap_or(false);

        if needs_source_migration {
            conn.execute_batch("PRAGMA foreign_keys = OFF")?;
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS files_mig (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    name          TEXT NOT NULL,
                    repo          TEXT NOT NULL DEFAULT '',
                    total_size    INTEGER NOT NULL,
                    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                    last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
                    source        TEXT NOT NULL DEFAULT 'hf',
                    etag          TEXT,
                    x_repo_commit TEXT,
                    x_linked_size INTEGER,
                    x_linked_etag TEXT,
                    content_type  TEXT,
                    UNIQUE(name, source)
                );
                INSERT OR IGNORE INTO files_mig (id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type)
                    SELECT id, name, repo, total_size, created_at, last_accessed,
                           CASE WHEN source IN ('pull', 'upload') THEN 'hf' ELSE source END,
                           etag, x_repo_commit, x_linked_size, x_linked_etag, content_type
                    FROM files;
                DROP TABLE files;
                ALTER TABLE files_mig RENAME TO files;
            ")?;
            conn.execute_batch("PRAGMA foreign_keys = ON")?;
        }

        let table = if conn.prepare("SELECT 1 FROM chunks LIMIT 0").is_ok() {
            "chunks"
        } else if conn.prepare("SELECT 1 FROM trunks LIMIT 0").is_ok() {
            "trunks"
        } else {
            return Ok(());
        };
        let has_col = conn
            .prepare(&format!("SELECT compressed_size FROM {} LIMIT 0", table))
            .is_ok();
        if !has_col {
            let _ = conn.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN compressed_size INTEGER",
                table
            ));
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

    pub fn get_file_by_name(&self, name: &str, source: &str) -> anyhow::Result<Option<File>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type FROM files WHERE name = ?1 AND source = ?2",
        )?;
        let mut rows = stmt.query_map(params![name, source], |row| {
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

    #[allow(clippy::too_many_arguments)]
    pub fn set_file_headers(
        &self,
        name: &str,
        source: &str,
        etag: Option<&str>,
        x_repo_commit: Option<&str>,
        x_linked_size: Option<i64>,
        x_linked_etag: Option<&str>,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE files SET etag = ?1, x_repo_commit = ?2, x_linked_size = ?3, x_linked_etag = ?4, content_type = ?5 WHERE name = ?6 AND source = ?7",
            params![etag, x_repo_commit, x_linked_size, x_linked_etag, content_type, name, source],
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

    pub fn delete_file(&self, name: &str, source: &str) -> anyhow::Result<bool> {
        let conn = self.conn.lock().unwrap();
        let file_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM files WHERE name = ?1 AND source = ?2",
                params![name, source],
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
        let mut stmt = conn.prepare("SELECT sha256 FROM file_chunks WHERE file_id = ?1")?;
        let chunks: Vec<String> = stmt
            .query_map(params![file_id], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        for sha256 in &chunks {
            conn.execute(
                "UPDATE chunks SET ref_count = ref_count - 1 WHERE sha256 = ?1",
                params![sha256],
            )?;
        }
        conn.execute(
            "DELETE FROM file_chunks WHERE file_id = ?1",
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

    pub fn ensure_chunk(
        &self,
        sha256: &str,
        backend: &str,
        path: &str,
        size: i64,
        compressed_size: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO chunks (sha256, backend, path, size, compressed_size) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![sha256, backend, path, size, compressed_size],
        )?;
        Ok(())
    }

    pub fn get_chunk(&self, sha256: &str) -> anyhow::Result<Option<Chunk>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT sha256, backend, path, size, compressed_size, ref_count FROM chunks WHERE sha256 = ?1",
        )?;
        let mut rows = stmt.query_map(params![sha256], |row| {
            Ok(Chunk {
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

    pub fn link_file_chunk(
        &self,
        file_id: i64,
        sha256: &str,
        chunk_index: i64,
        chunk_size: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO file_chunks (file_id, sha256, chunk_index, chunk_size) VALUES (?1, ?2, ?3, ?4)",
            params![file_id, sha256, chunk_index, chunk_size],
        )?;
        conn.execute(
            "UPDATE chunks SET ref_count = ref_count + 1 WHERE sha256 = ?1",
            params![sha256],
        )?;
        Ok(())
    }

    pub fn get_file_chunks(&self, file_id: i64) -> anyhow::Result<Vec<FileChunk>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT file_id, sha256, chunk_index, chunk_size FROM file_chunks WHERE file_id = ?1 ORDER BY chunk_index",
        )?;
        let rows = stmt.query_map(params![file_id], |row| {
            Ok(FileChunk {
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

    pub fn is_chunk_linked(
        &self,
        file_id: i64,
        chunk_index: usize,
    ) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT sha256 FROM file_chunks WHERE file_id = ?1 AND chunk_index = ?2",
            params![file_id, chunk_index as i64],
            |row| row.get(0),
        );
        match result {
            Ok(sha) => Ok(Some(sha)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
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

    pub fn get_orphan_chunks(&self) -> anyhow::Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT sha256 FROM chunks WHERE ref_count = 0")?;
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
        let chunk_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
        let total_size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(total_size), 0) FROM files",
            [],
            |row| row.get(0),
        )?;
        let unique_size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(COALESCE(compressed_size, size)), 0) FROM chunks",
            [],
            |row| row.get(0),
        )?;
        let compression_ratio = if total_size > 0 {
            unique_size as f64 / total_size as f64
        } else {
            1.0
        };
        Ok(Stats {
            repo_count,
            file_count,
            chunk_count,
            total_size,
            unique_size,
            compression_ratio,
            fetched_bytes: 0,
            served_bytes: 0,
        })
    }


}
