use hugrs::chunker;

const CHUNK_SIZE: usize = 4 * 1024 * 1024;

#[test]
fn test_chunk_and_assemble() {
    let mut data = vec![0u8; CHUNK_SIZE + 1024];
    data[CHUNK_SIZE] = 42;

    let chunks = chunker::chunk_data(&data, CHUNK_SIZE);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].len(), CHUNK_SIZE);
    assert_eq!(chunks[1].len(), 1024);

    let assembled = chunker::assemble_chunks(&chunks);
    assert_eq!(assembled, data);
}

#[test]
fn test_single_small_chunk() {
    let data = b"hello";
    let chunks = chunker::chunk_data(data, CHUNK_SIZE);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].len(), 5);
    let assembled = chunker::assemble_chunks(&chunks);
    assert_eq!(assembled, b"hello");
}

#[test]
fn test_sha256_chunk() {
    let data = b"hello world";
    let hash = chunker::sha256_hex(data);
    assert_eq!(
        hash,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
}

#[test]
fn test_chunk_with_hashes() {
    let data = vec![1u8; 10];
    let result = chunker::chunk_with_hashes(&data, CHUNK_SIZE);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].chunk_index, 0);
    assert_eq!(result[0].chunk_size, 10);
    assert!(!result[0].sha256.is_empty());
    assert_eq!(result[0].data, data);
}
