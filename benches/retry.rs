use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use hugrs::chunker;
use hugrs::storage::{local::LocalBackend, Compression, StorageBackend};
use std::sync::Arc;
use std::time::Duration;

fn make_data(mb: usize) -> Vec<u8> {
    let size = mb * 1024 * 1024;
    (0..size)
        .map(|i| i.wrapping_mul(0x9E37_79B9).wrapping_add(0x243F_6A88) as u8)
        .collect()
}

// ── SHA256 ────────────────────────────────────────────────────

fn bench_sha256(c: &mut Criterion) {
    let data_1m = make_data(1);
    let data_4m = make_data(4);
    let data_16m = make_data(16);

    let mut group = c.benchmark_group("sha256");
    group.throughput(Throughput::Bytes(1 * 1024 * 1024));
    group.bench_function("1MB", |b| {
        b.iter(|| criterion::black_box(chunker::sha256_hex(criterion::black_box(&data_1m))))
    });
    group.throughput(Throughput::Bytes(4 * 1024 * 1024));
    group.bench_function("4MB", |b| {
        b.iter(|| criterion::black_box(chunker::sha256_hex(criterion::black_box(&data_4m))))
    });
    group.throughput(Throughput::Bytes(16 * 1024 * 1024));
    group.bench_function("16MB", |b| {
        b.iter(|| criterion::black_box(chunker::sha256_hex(criterion::black_box(&data_16m))))
    });
    group.finish();
}

// ── Storage I/O ───────────────────────────────────────────────

fn bench_storage(c: &mut Criterion) {
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let dir = tempfile::TempDir::new().unwrap();
    let backend: Arc<dyn StorageBackend> = Arc::new(LocalBackend::new(
        dir.path().join("chunks"),
        Compression::None,
    ));

    let data_4m = make_data(4);
    let hash = chunker::sha256_hex(&data_4m);

    // ── Sequential write ──
    {
        let b = backend.clone();
        let d = data_4m.clone();
        let mut group = c.benchmark_group("storage_write");
        group.throughput(Throughput::Bytes(4 * 1024 * 1024));
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);

        let mut counter: u64 = 0;
        group.bench_function("seq_4mb_write", |bencher| {
            bencher.iter(|| {
                let key = format!("s_{counter:016x}");
                counter += 1;
                rt.block_on(async {
                    b.put(&key, &d).await.unwrap();
                    b.delete(&key).await.ok();
                });
            });
        });

        rt.block_on(async { b.put(&hash, &data_4m).await.unwrap() });
        group.bench_function("seq_4mb_dedup_hit", |bencher| {
            bencher.iter(|| {
                rt.block_on(async {
                    if !backend.exists(&hash).await.unwrap() {
                        backend.put(&hash, &data_4m).await.unwrap();
                    }
                });
            });
        });
        rt.block_on(async { b.delete(&hash).await.ok() });
        group.finish();
    }

    // ── Concurrent write ──
    {
        let mut group = c.benchmark_group("storage_concurrent_write");
        group.throughput(Throughput::Bytes(4 * 1024 * 1024));
        group.measurement_time(Duration::from_secs(15));
        group.sample_size(30);

        for &conc in &[4, 16, 64] {
            let b = backend.clone();
            let d = data_4m.clone();
            let r = rt.clone();
            let mut batch: u64 = 0;
            group.bench_function(
                BenchmarkId::new("tasks", format!("{conc}")),
                move |bencher| {
                    let r = r.clone();
                    bencher.iter(|| {
                        let key_prefix = format!("cw_{batch:04x}");
                        batch += 1;
                        r.block_on(async {
                            let mut tasks = Vec::with_capacity(conc);
                            for i in 0..conc {
                                let b = b.clone();
                                let d = d.clone();
                                let key = format!("{key_prefix}_{i:02x}");
                                let key2 = key.clone();
                                tasks.push(tokio::spawn(async move {
                                    b.put(&key, &d).await.unwrap();
                                    key2
                                }));
                            }
                            for t in tasks {
                                let key = t.await.unwrap();
                                b.delete(&key).await.ok();
                            }
                        });
                    });
                },
            );
        }
        group.finish();
    }

    // ── Concurrent read ──
    {
        let read_data = make_data(1);
        let read_hash = chunker::sha256_hex(&read_data);
        rt.block_on(async {
            backend.put(&read_hash, &read_data).await.unwrap();
        });

        let mut group = c.benchmark_group("storage_concurrent_read");
        group.throughput(Throughput::Bytes(1 * 1024 * 1024));
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);

        for &conc in &[4, 16, 64] {
            let b = backend.clone();
            let h = read_hash.clone();
            let r = rt.clone();
            group.bench_function(
                BenchmarkId::new("tasks", format!("{conc}")),
                move |bencher| {
                    let r = r.clone();
                    bencher.iter(|| {
                        r.block_on(async {
                            let mut tasks = Vec::with_capacity(conc);
                            for _ in 0..conc {
                                let b = b.clone();
                                let h = h.clone();
                                tasks.push(tokio::spawn(async move {
                                    b.get(&h).await.unwrap();
                                }));
                            }
                            for t in tasks {
                                t.await.unwrap();
                            }
                        });
                    });
                },
            );
        }
        group.finish();
    }
}

criterion_group!(sha256, bench_sha256);
criterion_group!(storage, bench_storage);
criterion_main!(sha256, storage);
