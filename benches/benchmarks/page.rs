use bytes::BytesMut;
use criterion::measurement::WallTime;
use criterion::Throughput::Bytes;
use criterion::{criterion_group, BenchmarkGroup, Criterion, Throughput};
use kisekifs::chunk::slice::{RSlice as RawRSlice, WSlice as RawWSlice};
use kisekifs::chunk::slice2::{RSlice, WSlice};
use kisekifs::chunk::Engine;
use tokio::runtime;

async fn unsafe_page(size: usize, read_cnt: usize, engine: Engine) {
    let mut ws = WSlice::new(1, engine.clone());
    let data = vec![0u8; size];
    ws.write_at(0, &data).unwrap();
    ws.finish(size).await.unwrap();

    let mut page = BytesMut::zeroed(size);
    let mut page = page.to_vec();
    for _ in 0..read_cnt {
        let v = RSlice::new(1, size, engine.clone())
            .read_at(0, &mut page)
            .unwrap();
        assert_eq!(v, size);
    }
}

async fn raw_page(size: usize, read_cnt: usize, engine: Engine) {
    let mut ws = RawWSlice::new(1, engine.clone());
    let data = vec![0u8; size];
    ws.write_at(0, &data).unwrap();
    ws.finish(size).await.unwrap();

    let mut page = BytesMut::zeroed(size);
    let mut page = page.to_vec();
    for _ in 0..read_cnt {
        let v = RawRSlice::new(1, size, engine.clone())
            .read_at(0, &mut page)
            .unwrap();
        assert_eq!(v, size);
    }
}

fn compare_page(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .thread_name("kiseki-bench-async-runtime")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();

    let engine1 = Engine::new_sled();
    let engine2 = Engine::new_sled();
    let mut group = c.benchmark_group("unsafe-page-throughput");

    let size = 1024;
    let cnt = 5;

    group.throughput(Throughput::Bytes(5 * 1024));
    group.bench_with_input("raw-page", &size, |b, &size| {
        b.to_async(&rt)
            .iter(|| raw_page(size, cnt, engine1.clone()))
    });

    group.throughput(Throughput::Bytes(5 * 1024));
    group.bench_with_input("unsafe-page", &size, |b, &size| {
        b.to_async(&rt)
            .iter(|| unsafe_page(size, cnt, engine2.clone()))
    });

    group.finish();
}

criterion_group!(benches, compare_page,);
