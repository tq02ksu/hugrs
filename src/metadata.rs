use rusqlite::{params, Connection};
use rusqlite_migration::{Migrations, M};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

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
    pub orphaned_at: Option<String>,
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
    pub original_bytes: i64,
    pub stored_bytes: i64,
    pub bytes_saved: i64,
    pub saved_percent: f64,
    pub fetched_bytes: u64,
    pub served_bytes: u64,
}

pub struct MetadataStore {
    conn: Arc<Mutex<Connection>>,
}

impl MetadataStore {
    fn conn(&self) -> anyhow::Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|e| anyhow::anyhow!("metadata connection mutex poisoned: {e}"))
    }

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
        self.conn()
    }

    fn init_schema(&self) -> anyhow::Result<()> {
        let mut conn = self.conn()?;

        conn.pragma_update(None, "foreign_keys", "OFF")?;
        let migrations = Migrations::new(vec![
            M::up(include_str!("migrations/001_initial_schema.sql")),
            M::up(include_str!("migrations/002_rename_trunk_to_chunk.sql")).foreign_key_check(),
            M::up(include_str!("migrations/003_drop_http_cache.sql")),
            M::up(include_str!("migrations/004_add_indexes.sql")),
            M::up(include_str!("migrations/005_cleanup_headerless_files.sql")),
            M::up(include_str!("migrations/006_add_chunk_orphaned_at.sql")),
        ]);

        let result = migrations.to_latest(&mut conn);
        conn.pragma_update(None, "foreign_keys", "ON")?;
        result?;

        Ok(())
    }

    pub fn add_file(
        &self,
        name: &str,
        repo: &str,
        total_size: i64,
        source: &str,
    ) -> anyhow::Result<File> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;
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
        let conn = self.conn()?;
        conn.execute(
            "UPDATE files SET etag = ?1, x_repo_commit = ?2, x_linked_size = ?3, x_linked_etag = ?4, content_type = ?5 WHERE name = ?6 AND source = ?7",
            params![etag, x_repo_commit, x_linked_size, x_linked_etag, content_type, name, source],
        )?;
        Ok(())
    }

    pub fn touch_repo(&self, repo: &str) -> anyhow::Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE files SET last_accessed = datetime('now') WHERE repo = ?1",
            params![repo],
        )?;
        Ok(())
    }

    pub fn delete_file(&self, name: &str, source: &str) -> anyhow::Result<bool> {
        let conn = self.conn()?;
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
            .filter_map(Result::ok)
            .collect();
        for sha256 in &chunks {
            conn.execute(
                "UPDATE chunks SET ref_count = ref_count - 1 WHERE sha256 = ?1",
                params![sha256],
            )?;
            conn.execute(
                "UPDATE chunks SET orphaned_at = datetime('now') WHERE sha256 = ?1 AND ref_count = 0",
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
        let conn = self.conn()?;
        let mut stmt = conn.prepare("SELECT id FROM files WHERE repo = ?1")?;
        let file_ids: Vec<i64> = stmt
            .query_map(params![repo], |row| row.get::<_, i64>(0))?
            .filter_map(Result::ok)
            .collect();

        for id in &file_ids {
            Self::delete_file_by_id_internal(&conn, *id)?;
        }
        Ok(file_ids.len())
    }

    pub fn list_repos_by_access(&self, limit: usize) -> anyhow::Result<Vec<String>> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;
        conn.execute(
            "INSERT OR IGNORE INTO chunks (sha256, backend, path, size, compressed_size, orphaned_at) VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))",
            params![sha256, backend, path, size, compressed_size],
        )?;
        Ok(())
    }

    pub fn get_chunk(&self, sha256: &str) -> anyhow::Result<Option<Chunk>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT sha256, backend, path, size, compressed_size, ref_count, orphaned_at FROM chunks WHERE sha256 = ?1",
        )?;
        let mut rows = stmt.query_map(params![sha256], |row| {
            Ok(Chunk {
                sha256: row.get(0)?,
                backend: row.get(1)?,
                path: row.get(2)?,
                size: row.get(3)?,
                compressed_size: row.get(4)?,
                ref_count: row.get(5)?,
                orphaned_at: row.get(6)?,
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
        let conn = self.conn()?;
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO file_chunks (file_id, sha256, chunk_index, chunk_size) VALUES (?1, ?2, ?3, ?4)",
            params![file_id, sha256, chunk_index, chunk_size],
        )?;
        if inserted > 0 {
            conn.execute(
                "UPDATE chunks SET ref_count = ref_count + 1, orphaned_at = NULL WHERE sha256 = ?1",
                params![sha256],
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ensure_chunk_and_link(
        &self,
        sha256: &str,
        backend: &str,
        path: &str,
        size: i64,
        compressed_size: i64,
        file_id: i64,
        chunk_index: i64,
        chunk_size: i64,
    ) -> anyhow::Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO chunks (sha256, backend, path, size, compressed_size, orphaned_at) VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))",
            params![sha256, backend, path, size, compressed_size],
        )?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO file_chunks (file_id, sha256, chunk_index, chunk_size) VALUES (?1, ?2, ?3, ?4)",
            params![file_id, sha256, chunk_index, chunk_size],
        )?;
        if inserted > 0 {
            tx.execute(
                "UPDATE chunks SET ref_count = ref_count + 1, orphaned_at = NULL WHERE sha256 = ?1",
                params![sha256],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn mark_chunk_orphaned(&self, sha256: &str) -> anyhow::Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE chunks SET orphaned_at = datetime('now') WHERE sha256 = ?1",
            params![sha256],
        )?;
        Ok(())
    }

    pub fn clear_chunk_orphaned(&self, sha256: &str) -> anyhow::Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE chunks SET orphaned_at = NULL WHERE sha256 = ?1",
            params![sha256],
        )?;
        Ok(())
    }

    pub fn get_file_chunks(&self, file_id: i64) -> anyhow::Result<Vec<FileChunk>> {
        let conn = self.conn()?;
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

    pub fn get_file_downloaded_size(&self, file_id: i64) -> anyhow::Result<i64> {
        let conn = self.conn()?;
        let downloaded = conn.query_row(
            "SELECT COALESCE(SUM(chunk_size), 0) FROM file_chunks WHERE file_id = ?1",
            params![file_id],
            |row| row.get(0),
        )?;
        Ok(downloaded)
    }

    pub fn is_chunk_linked(
        &self,
        file_id: i64,
        chunk_index: usize,
    ) -> anyhow::Result<Option<String>> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;
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

    pub fn get_orphan_chunks(&self) -> anyhow::Result<Vec<Chunk>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT sha256, backend, path, size, compressed_size, ref_count, orphaned_at
             FROM chunks
             WHERE ref_count = 0 AND orphaned_at IS NOT NULL
             ORDER BY orphaned_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Chunk {
                sha256: row.get(0)?,
                backend: row.get(1)?,
                path: row.get(2)?,
                size: row.get(3)?,
                compressed_size: row.get(4)?,
                ref_count: row.get(5)?,
                orphaned_at: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn list_orphan_chunks_batch(&self, limit: usize) -> anyhow::Result<Vec<Chunk>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT sha256, backend, path, size, compressed_size, ref_count, orphaned_at
             FROM chunks
             WHERE ref_count = 0 AND orphaned_at IS NOT NULL
             ORDER BY orphaned_at ASC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(Chunk {
                sha256: row.get(0)?,
                backend: row.get(1)?,
                path: row.get(2)?,
                size: row.get(3)?,
                compressed_size: row.get(4)?,
                ref_count: row.get(5)?,
                orphaned_at: row.get(6)?,
            })
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    pub fn list_orphan_chunks_stats(&self) -> anyhow::Result<(i64, i64)> {
        let conn = self.conn()?;
        let count = conn.query_row(
            "SELECT COUNT(*) FROM chunks WHERE ref_count = 0 AND orphaned_at IS NOT NULL",
            [],
            |row| row.get(0),
        )?;
        let bytes = conn.query_row(
            "SELECT COALESCE(SUM(COALESCE(compressed_size, size)), 0) FROM chunks WHERE ref_count = 0 AND orphaned_at IS NOT NULL",
            [],
            |row| row.get(0),
        )?;
        Ok((count, bytes))
    }

    pub fn delete_chunk(&self, sha256: &str) -> anyhow::Result<bool> {
        let conn = self.conn()?;
        let deleted = conn.execute(
            "DELETE FROM chunks WHERE sha256 = ?1 AND ref_count = 0",
            params![sha256],
        )?;
        Ok(deleted > 0)
    }

    pub fn get_stats(&self) -> anyhow::Result<Stats> {
        let conn = self.conn()?;
        let file_count: i64 = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;
        let repo_count: i64 =
            conn.query_row("SELECT COUNT(DISTINCT repo) FROM files", [], |row| {
                row.get(0)
            })?;
        let chunk_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?;
        let original_bytes: i64 = conn.query_row(
            "SELECT COALESCE(SUM(total_size), 0) FROM files",
            [],
            |row| row.get(0),
        )?;
        let stored_bytes: i64 = conn.query_row(
            "SELECT COALESCE(SUM(COALESCE(compressed_size, size)), 0) FROM chunks",
            [],
            |row| row.get(0),
        )?;
        let bytes_saved = original_bytes.saturating_sub(stored_bytes);
        let saved_percent = if original_bytes > 0 {
            bytes_saved as f64 * 100.0 / original_bytes as f64
        } else {
            0.0
        };
        Ok(Stats {
            repo_count,
            file_count,
            chunk_count,
            original_bytes,
            stored_bytes,
            bytes_saved,
            saved_percent,
            fetched_bytes: 0,
            served_bytes: 0,
        })
    }

    // TRANSITIONAL: remove in v0.X.0 ──────────────────────────
    pub fn list_files_with_missing_headers(&self) -> anyhow::Result<Vec<File>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT id, name, repo, total_size, created_at, last_accessed, source, etag, x_repo_commit, x_linked_size, x_linked_etag, content_type FROM files WHERE etag IS NULL OR content_type IS NULL")?;
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
        let mut files = Vec::new();
        for row in rows {
            files.push(row?);
        }
        Ok(files)
    }
    // TRANSITIONAL: end ───────────────────────────────────────
}
