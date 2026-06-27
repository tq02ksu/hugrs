-- Remove files that were cached without headers (etag + content_type) due to a bug
-- in the upload() path that deleted and recreated file rows without preserving metadata.
-- Next access will re-download these files with proper headers.

UPDATE chunks SET ref_count = ref_count - 1
WHERE sha256 IN (
    SELECT fc.sha256 FROM file_chunks fc
    INNER JOIN files f ON f.id = fc.file_id
    WHERE f.etag IS NULL AND f.content_type IS NULL
);

DELETE FROM file_chunks
WHERE file_id IN (
    SELECT id FROM files WHERE etag IS NULL AND content_type IS NULL
);

DELETE FROM files WHERE etag IS NULL AND content_type IS NULL;
