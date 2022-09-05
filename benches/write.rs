use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use disk_queue::DiskQueue;

const BENCHMARK_DB_PATH: &str = "benchmark.db";

fn cleanup_benchmark_db() {
    loop {
        std::fs::remove_file(BENCHMARK_DB_PATH).unwrap();
        if !Path::new(BENCHMARK_DB_PATH).exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

fn write(c: &mut Criterion) {
    let disk_queue = DiskQueue::new(BENCHMARK_DB_PATH);

    let record = "record".as_bytes().to_vec();

    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Elements(1));
    group.bench_function("enqueue", |b| b.iter(|| disk_queue.enqueue(record.clone())));
    group.finish();

    cleanup_benchmark_db();
}

criterion_group!(benches, write);
criterion_main!(benches);

