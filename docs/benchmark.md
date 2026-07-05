# Benchmark Results

Environment: GitHub Actions `ubuntu-latest` runner (2 vCPU, 7 GB RAM, SSD).

## SHA256 Hashing

| Input Size | Time    | Throughput |
|------------|---------|------------|
| 1 MB       | 4.75 ms | 211 MiB/s  |
| 4 MB       | 18.97 ms| 211 MiB/s  |
| 16 MB      | 75.9 ms | 211 MiB/s  |

Throughput is consistent across sizes. Hashing a typical 4 MB chunk takes ~19 ms.

## Storage I/O (Local Backend, No Compression)

### Sequential Write

| Scenario           | Time    | Throughput  |
|--------------------|---------|-------------|
| 4 MB new chunk     | 2.58 ms | 1.5 GiB/s   |
| 4 MB dedup hit     | 10.3 µs | 379 GiB/s   |

Dedup (exists-only) is ~250× faster than full write.

### Concurrent Write (4 MB per task)

| Tasks | Batch Time | Throughput (per batch) |
|-------|------------|------------------------|
| 4     | 7.84 ms    | 510 MiB/s              |
| 16    | 29.96 ms   | 133 MiB/s              |
| 64    | 123.89 ms  | 32 MiB/s               |

Write throughput degrades under high concurrency due to disk contention.

### Concurrent Read (1 MB per task)

| Tasks | Batch Time | Throughput (per batch) |
|-------|------------|------------------------|
| 4     | 397 µs     | 2.5 GiB/s              |
| 16    | 1.39 ms    | 717 MiB/s              |
| 64    | 4.56 ms    | 219 MiB/s              |

Read throughput is significantly higher than write and scales better with concurrency.

## How to Run

```bash
# All benchmarks
cargo bench --bench retry

# Specific group
cargo bench --bench retry -- sha256
cargo bench --bench retry -- storage_write
cargo bench --bench retry -- storage_concurrent_read
cargo bench --bench retry -- storage_concurrent_write
```

CI automatically runs these on every PR and push to master, comparing against the stored baseline.
