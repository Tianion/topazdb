use std::{
    fs::{create_dir, remove_dir_all},
    path::Path,
    sync::Arc,
};

use criterion::{criterion_group, criterion_main, Criterion};
use topazdb::{block::CompressOptions, opt::LsmOptions};

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    100
}

fn write_task(dir: impl AsRef<Path>) {
    let mut opts = LsmOptions::default().path(&dir);
    opts.block_size = 4096;
    opts.compress_option = CompressOptions::Uncompress;
    let storage = Arc::new(opts.open().unwrap());
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        storage.put(&key, &value).unwrap();
    }
}

const PATH: &str = "./disk_read";

fn start(_: &mut Criterion) {
    create_dir(PATH).unwrap();
    write_task(PATH);
}

fn end(_: &mut Criterion) {
    remove_dir_all(PATH).unwrap();
}

fn bench_read(c: &mut Criterion) {
    let mut opts = LsmOptions::default().path(PATH);
    opts.compress_option = CompressOptions::Uncompress;
    opts.block_size = 4096;
    let storage = Arc::new(opts.open().unwrap());
    let keys = (0..num_of_keys()).map(key_of).collect::<Vec<_>>();
    c.bench_function("bench_read", |b| {
        b.iter(|| {
            for key in &keys {
                storage.get(key).unwrap().unwrap();
            }
        })
    });
}

criterion_group!(benches, start, bench_read, end);
criterion_main!(benches);
