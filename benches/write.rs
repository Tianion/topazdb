use std::{
    fs::{create_dir, remove_dir_all},
    sync::Arc,
};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;
use topazdb::opt::LsmOptions;

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    1000
}

const DIR: &str = "./benches_wirte_test";

fn generate_kvs() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..num_of_keys())
        .map(|idx| (key_of(idx), value_of(idx)))
        .collect()
}

fn bench_batch_write_real(c: &mut Criterion) {
    let mut opts = LsmOptions::default().path(DIR);
    opts.memtable_size = 4096 * 10;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>()
        .chunks(10)
        .map(|x| x.to_vec())
        .collect::<Vec<_>>();
    c.bench_function("bench_batch_write", |b| {
        b.iter(|| {
            for entries in &kvs {
                storage.batch_put(entries).unwrap();
            }
        })
    });
}

fn bench_write_real(c: &mut Criterion) {
    let mut opts = LsmOptions::default().path(DIR);
    opts.memtable_size = 4096 * 10;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>();
    c.bench_function("bench_write", |b| {
        b.iter(|| {
            for entry in &kvs {
                storage.put(&entry.0, &entry.1).unwrap();
            }
        })
    });
}

fn bench_batch_write_tmpfs(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut opts = LsmOptions::default().path(dir.path());
    opts.memtable_size = 4096 * 10;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>()
        .chunks(10)
        .map(|x| x.to_vec())
        .collect::<Vec<_>>();
    c.bench_function("bench_batch_write_tmpfs", |b| {
        b.iter(|| {
            for entries in &kvs {
                storage.batch_put(entries).unwrap();
            }
        })
    });
}

fn bench_write_tmpfs(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut opts = LsmOptions::default().path(dir.path());
    opts.memtable_size = 4096 * 10;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>();
    c.bench_function("bench_write_tmpfs", |b| {
        b.iter(|| {
            for entry in &kvs {
                storage.put(&entry.0, &entry.1).unwrap();
            }
        })
    });
}

fn create_test_dir(_: &mut Criterion) {
    create_dir(DIR).unwrap();
}

fn remove_test_dir(_: &mut Criterion) {
    remove_dir_all(DIR).unwrap();
}

criterion_group!(
    benches,
    create_test_dir,
    bench_batch_write_real,
    remove_test_dir,
    create_test_dir,
    bench_write_real,
    remove_test_dir,
    bench_write_tmpfs,
    bench_batch_write_tmpfs,
);
criterion_main!(benches);
