use sha2::{Digest, Sha256};

pub struct ChunkWithHash {
    pub chunk_index: usize,
    pub sha256: String,
    pub chunk_size: usize,
    pub data: Vec<u8>,
}

pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

pub fn chunk_data(data: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
    data.chunks(chunk_size).map(<[u8]>::to_vec).collect()
}

pub fn assemble_chunks(chunks: &[Vec<u8>]) -> Vec<u8> {
    let total_size: usize = chunks.iter().map(Vec::len).sum();
    let mut result = Vec::with_capacity(total_size);
    for chunk in chunks {
        result.extend_from_slice(chunk);
    }
    result
}

pub fn chunk_with_hashes(data: &[u8], chunk_size: usize) -> Vec<ChunkWithHash> {
    data.chunks(chunk_size)
        .enumerate()
        .map(|(i, chunk)| {
            let data = chunk.to_vec();
            let sha256 = sha256_hex(&data);
            ChunkWithHash {
                chunk_index: i,
                sha256,
                chunk_size: data.len(),
                data,
            }
        })
        .collect()
}
