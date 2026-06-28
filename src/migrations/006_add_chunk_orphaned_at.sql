ALTER TABLE chunks ADD COLUMN orphaned_at TEXT;

UPDATE chunks
SET orphaned_at = datetime('now')
WHERE ref_count = 0;
