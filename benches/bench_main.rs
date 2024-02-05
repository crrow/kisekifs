mod cache;

use criterion::criterion_main;

criterion_main!(cache::juice::benches);
