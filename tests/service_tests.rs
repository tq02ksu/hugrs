use hugrs::metadata::File;
use hugrs::metadata::MetadataStore;
use hugrs::service::CacheService;
use hugrs::service::CHUNK_SIZE;
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use std::sync::Arc;
use tempfile::TempDir;

async fn seed_file(svc: &CacheService, name: &str, repo: &str, source: &str, data: &[u8]) {
    let existing = svc.metadata.get_file_by_name(name, source).unwrap();
    svc.metadata.delete_file(name, source).ok();
    let file = svc
        .metadata
        .add_file(name, repo, data.len() as i64, source)
        .unwrap();
    if let Some(ref h) = existing {
        svc.metadata
            .set_file_headers(
                name,
                source,
                h.etag.as_deref(),
                h.x_repo_commit.as_deref(),
                h.x_linked_size,
                h.x_linked_etag.as_deref(),
                h.content_type.as_deref(),
            )
            .unwrap();
    }
    let chunks = hugrs::chunker::chunk_with_hashes(data, CHUNK_SIZE);
    for chunk in &chunks {
        svc.backend.put(&chunk.sha256, &chunk.data).await.unwrap();
        let path = format!(
            "{}\057{}\057{}",
            &chunk.sha256[0..2],
            &chunk.sha256[2..4],
            chunk.sha256
        );
        svc.metadata
            .ensure_chunk(
                &chunk.sha256,
                "local",
                &path,
                chunk.chunk_size as i64,
                chunk.chunk_size as i64,
            )
            .unwrap();
        svc.metadata
            .link_file_chunk(
                file.id,
                &chunk.sha256,
                chunk.chunk_index as i64,
                chunk.chunk_size as i64,
            )
            .unwrap();
    }
    svc.metadata.touch_repo(repo).unwrap();
    if let Some(limit) = svc.max_size {
        let _ = svc.evict_if_needed(limit).await;
    }
}

async fn read_seeded_file(svc: &CacheService, file: &File) -> Vec<u8> {
    let file_chunks = svc.metadata.get_file_chunks(file.id).unwrap();
    let mut chunks = Vec::with_capacity(file_chunks.len());
    for ft in &file_chunks {
        let data = svc.backend.get(&ft.sha256).await.unwrap();
        assert_eq!(hugrs::chunker::sha256_hex(&data), ft.sha256);
        chunks.push(data);
    }
    hugrs::chunker::assemble_chunks(&chunks)
}

#[tokio::test]
async fn test_seeded_file_chunks_reassemble_to_original_data() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata,
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    let data = b"hello hugrs cache service";
    seed_file(&service, "test.bin", "test-repo", "hf", data).await;

    let file = service.info("test.bin", "hf").await.unwrap().unwrap();
    assert_eq!(file.name, "test.bin");
    assert_eq!(file.repo, "test-repo");
    assert_eq!(file.total_size as usize, data.len());

    let downloaded = read_seeded_file(&service, &file).await;
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
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata,
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "x.bin", "repo-a", "hf", &[1, 2, 3]).await;
    assert!(service.info("x.bin", "hf").await.unwrap().is_some());

    service.delete("x.bin", "hf").await.unwrap();
    assert!(service.info("x.bin", "hf").await.unwrap().is_none());

    let result = service.gc_execute().await.unwrap();
    assert!(result.deleted_chunks > 0);
}

#[tokio::test]
async fn test_stats() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata,
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    let stats = service.stats().await.unwrap();
    assert_eq!(stats.file_count, 0);
    assert_eq!(stats.original_bytes, 0);
    assert_eq!(stats.stored_bytes, 0);

    seed_file(&service, "f.bin", "test-repo", "hf", &[5; 100]).await;

    let stats = service.stats().await.unwrap();
    assert_eq!(stats.file_count, 1);
    assert_eq!(stats.repo_count, 1);
    assert_eq!(stats.original_bytes, 100);
    assert_eq!(stats.bytes_saved, 0);
    assert_eq!(stats.saved_percent, 0.0);
}

#[tokio::test]
async fn test_overwriting_seeded_file_updates_chunk_mapping() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata,
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "dup.bin", "repo-a", "hf", &vec![1, 2, 3]).await;
    seed_file(&service, "dup.bin", "repo-a", "hf", &vec![4, 5, 6]).await;

    let file = service.info("dup.bin", "hf").await.unwrap().unwrap();
    let downloaded = read_seeded_file(&service, &file).await;
    assert_eq!(downloaded, vec![4, 5, 6]);
}

#[tokio::test]
async fn test_lru_eviction() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata,
        backend,
        Some(300),
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "big.bin", "repo-big", "hf", &vec![0u8; 250]).await;
    seed_file(&service, "small.bin", "repo-small", "hf", &vec![1u8; 100]).await;

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
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata,
        backend,
        Some(250),
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "a.txt", "repo-a", "hf", &vec![1u8; 100]).await;
    seed_file(&service, "b.txt", "repo-a", "hf", &vec![2u8; 100]).await;
    seed_file(&service, "c.txt", "repo-b", "hf", &vec![3u8; 100]).await;

    let files = service.list().await.unwrap();
    let repos: std::collections::HashSet<&str> = files.iter().map(|f| f.repo.as_str()).collect();
    assert_eq!(repos.len(), 1);
    assert!(repos.contains("repo-b"));
}

#[tokio::test]
async fn test_delete_marks_zero_ref_chunks_orphaned() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata.clone(),
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "x.bin", "repo-a", "hf", &vec![1, 2, 3, 4]).await;

    let deleted = service
        .delete_file_all_sources("repo-a", "x.bin", Some("hf"))
        .await
        .unwrap();
    assert_eq!(deleted.deleted_files, 1);

    let orphans = metadata.get_orphan_chunks().unwrap();
    assert_eq!(orphans.len(), 1);
    assert_eq!(orphans[0].ref_count, 0);
    assert!(orphans[0].orphaned_at.is_some());
}

#[tokio::test]
async fn test_delete_does_not_remove_backend_data_immediately() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata.clone(),
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "x.bin", "repo-a", "hf", &vec![1, 2, 3, 4]).await;

    let file = metadata.get_file_by_name("x.bin", "hf").unwrap().unwrap();
    let sha = metadata.get_file_chunks(file.id).unwrap()[0].sha256.clone();

    service
        .delete_file_all_sources("repo-a", "x.bin", Some("hf"))
        .await
        .unwrap();

    assert!(service.backend_exists(&sha).await.unwrap());
}

#[tokio::test]
async fn test_delete_without_source_removes_all_sources() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata.clone(),
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "x.bin", "repo-a", "hf", &vec![1, 2, 3, 4]).await;
    seed_file(&service, "x.bin", "repo-a", "ms", &vec![1, 2, 3, 4]).await;

    let deleted = service
        .delete_file_all_sources("repo-a", "x.bin", None)
        .await
        .unwrap();
    assert_eq!(deleted.deleted_files, 2);

    assert!(metadata.get_file_by_name("x.bin", "hf").unwrap().is_none());
    assert!(metadata.get_file_by_name("x.bin", "ms").unwrap().is_none());
}

#[tokio::test]
async fn test_gc_dry_run_reports_orphan_candidates() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata,
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "x.bin", "repo-a", "hf", &vec![1, 2, 3, 4]).await;
    service
        .delete_file_all_sources("repo-a", "x.bin", Some("hf"))
        .await
        .unwrap();

    let preview = service.gc_dry_run().await.unwrap();
    assert_eq!(preview.candidate_chunks, 1);
    assert!(preview.candidate_bytes > 0);
}

#[tokio::test]
async fn test_gc_execute_reclaims_orphan_backend_objects() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata.clone(),
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    seed_file(&service, "x.bin", "repo-a", "hf", &vec![1, 2, 3, 4]).await;

    let file = metadata.get_file_by_name("x.bin", "hf").unwrap().unwrap();
    let sha = metadata.get_file_chunks(file.id).unwrap()[0].sha256.clone();

    service
        .delete_file_all_sources("repo-a", "x.bin", Some("hf"))
        .await
        .unwrap();

    let result = service.gc_execute().await.unwrap();
    assert_eq!(result.deleted_chunks, 1);
    assert!(result.reclaimed_bytes > 0);
    assert!(!service.backend_exists(&sha).await.unwrap());
}

#[tokio::test]
async fn test_gc_execute_batch_limits_one_server_side_batch() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let metadata = Arc::new(MetadataStore::new(&db_path).unwrap());
    let backend: Arc<dyn hugrs::storage::StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));
    let service = CacheService::new(
        metadata.clone(),
        backend,
        None,
        reqwest::Client::new(),
        reqwest::Client::new(),
        0,
        8,
        true,
        reqwest::Client::new(),
        5,
    );

    for i in 0..40usize {
        let name = format!("f-{i}.bin");
        seed_file(&service, &name, "repo-a", "hf", &[i as u8 + 1]).await;
        service
            .delete_file_all_sources("repo-a", &name, Some("hf"))
            .await
            .unwrap();
    }

    let result = service.gc_execute_batch(32).await.unwrap();
    assert_eq!(result.deleted_chunks, 32);
    assert!(result.has_more);

    let remaining = metadata.get_orphan_chunks().unwrap();
    assert_eq!(remaining.len(), 8);
}
