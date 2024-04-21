// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use kisekifs::{
    common::readable_size::ReadableSize,
    meta::types::{random_slice_id, SliceID},
    vfs::storage::{new_juice_builder, Cache},
};
use tokio::{io::AsyncReadExt, runtime};

// async fn juice_rw(c: &mut Criterion, rt: &runtime::Runtime) {
//     const BLOCK_SIZE: u64 = ReadableSize::mb(4).as_bytes();
//     let cache = make_juice_cache();
//
//     c.bench_function("write", |b| {
//         b.to_async(rt).iter(|| {
//             let block = Arc::new((0..BLOCK_SIZE).map(|_|
// rand::random::<u8>()).collect());             let slice_id =
// random_slice_id();             cache.cache(slice_id, block).unwrap();
//         })
//     });
//
//     cache.wait_on_all_flush_finish();
// }

fn make_juice_cache() -> Arc<dyn Cache> {
    let b = new_juice_builder();
    let c = b.build().unwrap();
    c
}

async fn read_cache(cache: Arc<dyn Cache>, req: &CacheReq) {
    let mut reader = cache.get(req.slice_id).await.unwrap();
    let mut buf = vec![0u8; req.block.len()];
    reader.read_exact(&mut buf).await.unwrap();
}

struct CacheReq {
    slice_id: SliceID,
    block: Arc<Vec<u8>>,
}

fn bench(c: &mut Criterion) {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .thread_name("cache-bench-async-runtime")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();

    let reqs = (0..block_cnt)
        .map(|_| CacheReq {
            slice_id: random_slice_id(),
            block: Arc::new(
                (0..block_size.as_bytes())
                    .map(|_| rand::random::<u8>())
                    .collect(),
            ),
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("simple cache rw");
    for i in reqs.iter() {
        group.throughput(Throughput::Bytes(block_size.as_bytes()));
        group.bench_with_input(
            BenchmarkId::new("write", block_size),
            &block_size,
            |b, s| {
                b.to_async(&rt)
                    .iter(|| cache.cache(i.slice_id, i.block.clone()));
            },
        );
        group.bench_with_input(BenchmarkId::new("read", block_size), &block_size, |b, s| {
            b.iter(|| async {
                let mut reader = cache.get(i.slice_id).await.unwrap();
                let mut buf = vec![0u8; block_size.as_bytes() as usize];
                reader.read_to_end(&mut buf).await.unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
