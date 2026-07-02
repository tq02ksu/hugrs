CREATE INDEX IF NOT EXISTS idx_chunks_ref_count_orphaned_at
ON chunks(ref_count, orphaned_at);
