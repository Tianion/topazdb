use std::{
    fs::{create_dir, remove_dir_all},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tempfile::tempdir;
use topazdb::opt::LsmOptions;

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    500
}

const DIR: &str = "./benches_wirte_test";

fn generate_kvs() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..num_of_keys())
        .map(|idx| (key_of(idx), value_of(idx)))
        .collect()
}

fn bench_batch_write_real(c: &mut Criterion) {
    let mut opts = LsmOptions::default().path(DIR);
    opts.memtable_size = 4096 * 1000;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>()
        .chunks(100)
        .map(|x| x.to_vec())
        .collect::<Vec<_>>();
    let mut g = c.benchmark_group("bench write");
    g.bench_function(BenchmarkId::new("ext4", "batch"), |b| {
        b.iter(|| {
            for entries in &kvs {
                storage.batch_put(entries).unwrap();
            }
        })
    });
}

fn bench_write_real(c: &mut Criterion) {
    let mut opts = LsmOptions::default().path(DIR);
    opts.memtable_size = 4096 * 1000;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>();
    let mut g = c.benchmark_group("bench write");
    g.bench_function(BenchmarkId::new("ext4", "single"), |b| {
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
    opts.memtable_size = 4096 * 1000;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>()
        .chunks(100)
        .map(|x| x.to_vec())
        .collect::<Vec<_>>();
    let mut g = c.benchmark_group("bench write");
    g.bench_function(BenchmarkId::new("tmpfs", "batch"), |b| {
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
    opts.memtable_size = 4096 * 1000;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let kvs = generate_kvs()
        .into_iter()
        .map(|x| (Bytes::from(x.0), Bytes::from(x.1)))
        .collect::<Vec<_>>();
    let mut g = c.benchmark_group("bench write");
    g.bench_function(BenchmarkId::new("tmpfs", "single"), |b| {
        b.iter(|| {
            for entry in &kvs {
                storage.put(&entry.0, &entry.1).unwrap();
            }
        })
    });
}

fn bench_channel_write_tmpfs(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut opts = LsmOptions::default().path(dir.path());
    opts.memtable_size = 4096 * 1000;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let mut g = c.benchmark_group("bench write");
    g.bench_function(BenchmarkId::new("tmpfs", "channel"), |b| {
        b.iter_custom(|iter| {
            let kvs = generate_kvs()
                .into_iter()
                .map(|x| vec![(Bytes::from(x.0), Bytes::from(x.1))])
                .collect::<Vec<_>>();
            let mut rs = Vec::with_capacity(kvs.len());
            let input = vec![kvs; iter as usize];
            let start = Instant::now();
            for kvs in input {
                for entry in kvs {
                    let r = storage.put_to_channel(entry).unwrap();
                    rs.push(r);
                }
                for r in &rs {
                    let _ = r.recv();
                }
                rs.clear()
            }
            start.elapsed()
        })
    });
}

fn bench_channel_write(c: &mut Criterion) {
    let mut opts = LsmOptions::default().path(DIR);
    opts.memtable_size = 4096 * 1000;
    opts.block_size = 4096;
    opts.wait_entry_num = 100;
    let storage = Arc::new(opts.open().unwrap());
    let mut g = c.benchmark_group("bench write");
    g.bench_function(BenchmarkId::new("ext4", "channel"), |b| {
        b.iter_custom(|iter| {
            let kvs = generate_kvs()
                .into_iter()
                .map(|x| vec![(Bytes::from(x.0), Bytes::from(x.1))])
                .collect::<Vec<_>>();
            let mut rs = Vec::with_capacity(kvs.len());
            let input = vec![kvs; iter as usize];
            let start = Instant::now();
            for kvs in input {
                for entry in kvs {
                    let r = storage.put_to_channel(entry).unwrap();
                    rs.push(r);
                }
                for r in &rs {
                    let _ = r.recv();
                }
                rs.clear()
            }
            start.elapsed()
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
    create_test_dir,
    bench_channel_write,
    remove_test_dir,
    bench_write_tmpfs,
    bench_batch_write_tmpfs,
    bench_channel_write_tmpfs,
);
criterion_main!(benches);
