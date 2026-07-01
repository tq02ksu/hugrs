# File Completion Status Design

## Goal

Make `hugrsctl file` and `hugrsctl file show` distinguish between a file's metadata size and its actually downloaded bytes, and expose whether the file is complete.

## Requirements

- Keep the existing `size` field semantics unchanged: it remains the file's total size from metadata.
- Compute downloaded bytes from cached chunk metadata with `SUM(file_chunks.chunk_size)`.
- A file is complete when downloaded bytes are greater than or equal to the metadata size.
- Apply the same completion logic to:
  - control-plane JSON responses
  - `hugrsctl file` tabular output
  - `hugrsctl file show` text output
- When multiple sources exist for the same logical file, aggregate source names as today and report the maximum downloaded byte count across those source-specific records.

## Design

### API shape

Extend file-oriented control-plane responses with:

- `downloaded_size: i64`
- `complete: bool`

This keeps existing clients compatible while exposing completion status explicitly instead of overloading `size`.

### Metadata lookup

Add a metadata helper that returns the total linked chunk bytes for a file id. Service and server code should use that helper instead of inferring completion from chunk count or file existence.

### Aggregation behavior

`/_hugrs/files` groups multiple source-specific file records into one logical row today. That grouping should keep:

- `size` from the representative file record
- `downloaded_size` as the maximum downloaded bytes among grouped source entries
- `complete` from `downloaded_size >= size`

`/_hugrs/files/show` should do the same for its matched set.

### CLI presentation

For `hugrsctl file`:

- keep `SIZE` as the total file size
- add `DOWNLOADED`
- add `COMPLETE`

For `hugrsctl file show`:

- print `size`
- print `downloaded`
- print `complete`

## Testing

- Add a failing API aggregation test for an incomplete file where metadata size exceeds cached chunk bytes.
- Add a failing API aggregation test for a complete file where downloaded bytes equal total size.
- Verify both `file list` and `file show` response shapes.
