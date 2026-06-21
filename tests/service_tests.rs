use hugrs::metadata::MetadataStore;
use hugrs::service::CacheService;
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_upload_and_download() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("trunks"),
        Compression::None,
    ));
    let service = CacheService::new(metadata, backend, None, reqwest::Client::new());

    let data = b"hello hugrs cache service";
    service
        .upload("test.bin", "test-repo", data.to_vec())
        .await
        .unwrap();

    let file = service.info("test.bin").await.unwrap().unwrap();
    assert_eq!(file.name, "test.bin");
    assert_eq!(file.repo, "test-repo");
    assert_eq!(file.total_size as usize, data.len());

    let downloaded = service.download("test.bin").await.unwrap();
    assert_eq!(downloaded, data);

    let files = service.list().await.unwrap();
    assert_eq!(files.len(), 1);
}

#[tokio::test]
async fn test_delete_and_gc() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("trunks"),
        Compression::None,
    ));
    let service = CacheService::new(metadata, backend, None, reqwest::Client::new());

    service
        .upload("x.bin", "repo-a", vec![1, 2, 3])
        .await
        .unwrap();
    assert!(service.info("x.bin").await.unwrap().is_some());

    service.delete("x.bin").await.unwrap();
    assert!(service.info("x.bin").await.unwrap().is_none());

    let count = service.gc().await.unwrap();
    assert!(count > 0);
}

#[tokio::test]
async fn test_stats() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("trunks"),
        Compression::None,
    ));
    let service = CacheService::new(metadata, backend, None, reqwest::Client::new());

    let stats = service.stats().await.unwrap();
    assert_eq!(stats.file_count, 0);

    service
        .upload("f.bin", "test-repo", vec![5; 100])
        .await
        .unwrap();

    let stats = service.stats().await.unwrap();
    assert_eq!(stats.file_count, 1);
    assert_eq!(stats.repo_count, 1);
}

#[tokio::test]
async fn test_upload_duplicate_file_overwrites() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("trunks"),
        Compression::None,
    ));
    let service = CacheService::new(metadata, backend, None, reqwest::Client::new());

    service
        .upload("dup.bin", "repo-a", vec![1, 2, 3])
        .await
        .unwrap();
    service
        .upload("dup.bin", "repo-a", vec![4, 5, 6])
        .await
        .unwrap();

    let downloaded = service.download("dup.bin").await.unwrap();
    assert_eq!(downloaded, vec![4, 5, 6]);
}

#[tokio::test]
async fn test_lru_eviction() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("trunks"),
        Compression::None,
    ));
    let service = CacheService::new(metadata, backend, Some(300), reqwest::Client::new());

    service
        .upload("big.bin", "repo-big", vec![0u8; 250])
        .await
        .unwrap();
    service
        .upload("small.bin", "repo-small", vec![1u8; 100])
        .await
        .unwrap();

    let files = service.list().await.unwrap();
    let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
    assert!(names.contains(&"small.bin"));
    assert!(!names.contains(&"big.bin"));
}

#[tokio::test]
async fn test_lru_eviction_by_repo() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("trunks"),
        Compression::None,
    ));
    let service = CacheService::new(metadata, backend, Some(250), reqwest::Client::new());

    service
        .upload("a.txt", "repo-a", vec![1u8; 100])
        .await
        .unwrap();
    service
        .upload("b.txt", "repo-a", vec![2u8; 100])
        .await
        .unwrap();
    service
        .upload("c.txt", "repo-b", vec![3u8; 100])
        .await
        .unwrap();

    let files = service.list().await.unwrap();
    let repos: std::collections::HashSet<&str> = files.iter().map(|f| f.repo.as_str()).collect();
    assert_eq!(repos.len(), 1);
    assert!(repos.contains("repo-b"));
}
