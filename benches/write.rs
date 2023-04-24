use std::{
    fs::{create_dir, remove_dir_all},
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
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

fn generate_kvs() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..num_of_keys())
        .map(|idx| (key_of(idx), value_of(idx)))
        .collect()
}

fn bench_write(c: &mut Criterion) {
    let path = "./benches_wirte_test";
    create_dir(path).unwrap();

    c.bench_function("bench_write", |b| {
        b.iter_custom(|iters| {
            let mut opts = LsmOptions::default().path(path);
            opts.memtable_size = 4096 * 10;
            opts.block_size = 4096;
            let storage = Arc::new(opts.open().unwrap());
            let kvs = generate_kvs();
            let kvs = vec![kvs; iters as usize];
            let start = Instant::now();
            for kvs in kvs {
                let mut handlers = Vec::with_capacity(kvs.len());
                for entry in kvs {
                    let storage = storage.clone();
                    let handler = std::thread::spawn(move || {
                        storage.put(&entry.0, &entry.1).unwrap();
                    });
                    handlers.push(handler);
                }
                for h in handlers {
                    h.join().unwrap();
                }
            }
            start.elapsed()
        })
    });
    remove_dir_all(path).unwrap();
}

fn bench_batch_write(c: &mut Criterion) {
    let path = "./benches_wirte_test";
    create_dir(path).unwrap();

    c.bench_function("bench_batch_write", |b| {
        b.iter_custom(|iters| {
            let mut opts = LsmOptions::default().path(path);
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
            let kvs = vec![kvs; iters as usize];
            let start = Instant::now();
            for kvs in kvs {
                let mut handlers = Vec::with_capacity(kvs.len());
                for entries in kvs {
                    let storage = storage.clone();
                    let handler = std::thread::spawn(move || {
                        storage.batch_put(entries).unwrap();
                    });
                    handlers.push(handler);
                }
                for h in handlers {
                    h.join().unwrap();
                }
            }
            start.elapsed()
        })
    });

    remove_dir_all(path).unwrap();
}

criterion_group!(benches, bench_write, bench_batch_write);
criterion_main!(benches);
