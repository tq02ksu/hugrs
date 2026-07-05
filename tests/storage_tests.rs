#![allow(clippy::unwrap_used, clippy::expect_used)]
use hugrs::storage::local::LocalBackend;
use hugrs::storage::Compression;
use hugrs::storage::StorageBackend;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_local_put_and_get() {
    let dir = TempDir::new().unwrap();
    let backend = LocalBackend::new(dir.path().to_path_buf(), Compression::None);

    let data = b"hello world";
    let sha256 = "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447";

    backend.put(sha256, data).await.unwrap();
    assert!(backend.exists(sha256).await.unwrap());

    let got = backend.get(sha256).await.unwrap();
    assert_eq!(got, data);

    backend.delete(sha256).await.unwrap();
    assert!(!backend.exists(sha256).await.unwrap());
}

#[tokio::test]
async fn test_exists_must_not_return_true_before_put_completes() {
    let dir = TempDir::new().unwrap();
    let backend = Arc::new(LocalBackend::new(
        dir.path().to_path_buf(),
        Compression::None,
    ));

    let data = vec![42u8; 100 * 1024 * 1024]; // 100MB to make write observable
    let sha = hugrs::chunker::sha256_hex(&data);

    let be = backend.clone();
    let s = sha.clone();

    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel::<()>(1);

    let writer = tokio::spawn(async move {
        started_tx.send(()).await.ok();
        be.put(&s, &data).await
    });

    started_rx.recv().await;

    tokio::task::yield_now().await;

    let exists_during = backend.exists(&sha).await.unwrap();

    let result = writer.await.unwrap();

    assert!(result.is_ok(), "put must succeed");

    assert!(
        !exists_during,
        "BUG: exists() returned true while put() was still writing. \
         File was visible before write completed — TOCTOU window."
    );
}
