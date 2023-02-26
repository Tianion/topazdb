use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::{TempDir, tempdir};
use topazdb::{table::{SsTable, SsTableBuilder, SsTableIterator}, iterators::StorageIterator};

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    1000
}

fn generate_sst() -> (TempDir, SsTable) {
    let mut builder = SsTableBuilder::new(4096);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(&key[..], &value[..]);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    (dir, builder.build(0, None, path).unwrap())
}

fn bench_iter_read(c: &mut Criterion) {
    let (_dir, sst) = generate_sst();
    let mut iter = SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
    c.bench_function("bench_iter_read", |b| b.iter(|| {
        while iter.is_valid() {
            iter.next().unwrap();
        }
    }));
}

fn bench_iter_create_and_read(c: &mut Criterion) {
    let (_dir, sst) = generate_sst();
    let sst = Arc::new(sst);
    c.bench_function("bench_iter_create_and_read", |b| b.iter(|| {
        let mut iter = SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap();
        while iter.is_valid() {
            iter.next().unwrap();
        }
    }));
}

criterion_group!(benches, bench_iter_read, bench_iter_create_and_read);
criterion_main!(benches);