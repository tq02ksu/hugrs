use hugrs::storage::local::LocalBackend;
use hugrs::storage::StorageBackend;
use tempfile::TempDir;

#[tokio::test]
async fn test_local_put_and_get() {
    let dir = TempDir::new().unwrap();
    let backend = LocalBackend::new(dir.path().to_path_buf());

    let data = b"hello world";
    let sha256 = "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447";

    backend.put(sha256, data).await.unwrap();
    assert!(backend.exists(sha256).await.unwrap());

    let got = backend.get(sha256).await.unwrap();
    assert_eq!(got, data);

    backend.delete(sha256).await.unwrap();
    assert!(!backend.exists(sha256).await.unwrap());
}
