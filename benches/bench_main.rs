mod benchmarks;

use criterion::criterion_main;

criterion_main!(benchmarks::page::benches);