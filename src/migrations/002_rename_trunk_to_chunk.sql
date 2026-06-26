ALTER TABLE trunks RENAME TO chunks;
ALTER TABLE file_trunks RENAME TO file_chunks;

CREATE TABLE files_new (
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

INSERT OR IGNORE INTO files_new (
    id,
    name,
    repo,
    total_size,
    created_at,
    last_accessed,
    source,
    etag,
    x_repo_commit,
    x_linked_size,
    x_linked_etag,
    content_type
)
SELECT
    id,
    name,
    repo,
    total_size,
    created_at,
    last_accessed,
    CASE WHEN source IN ('pull', 'upload') THEN 'hf' ELSE source END,
    etag,
    x_repo_commit,
    x_linked_size,
    x_linked_etag,
    content_type
FROM files;

DROP TABLE files;
ALTER TABLE files_new RENAME TO files;
