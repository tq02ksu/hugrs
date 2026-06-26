CREATE TABLE IF NOT EXISTS files (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL,
    total_size    INTEGER NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
    repo          TEXT NOT NULL DEFAULT '',
    source        TEXT NOT NULL,
    etag          TEXT,
    x_repo_commit TEXT,
    x_linked_size INTEGER,
    x_linked_etag TEXT,
    content_type  TEXT,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS trunks (
    sha256           TEXT PRIMARY KEY,
    backend          TEXT NOT NULL,
    path             TEXT NOT NULL,
    size             INTEGER NOT NULL,
    ref_count        INTEGER NOT NULL DEFAULT 0,
    compressed_size  INTEGER
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
);
