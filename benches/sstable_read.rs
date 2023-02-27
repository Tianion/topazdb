use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use tempfile::{TempDir, tempdir};
use topazdb::{table::{SsTable, SsTableBuilder, SsTableIterator}, iterators::StorageIterator, block::CompressOptions};

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:03}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    1000
}

fn generate_sst(compress_option: CompressOptions) -> (TempDir, SsTable) {
    let mut builder = SsTableBuilder::new(256, compress_option);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(&key[..], &value[..]).unwrap();
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    (dir, builder.build(0, None, path).unwrap())
}

fn bench_iter_read(c: &mut Criterion) {
    let opts = vec![CompressOptions::Uncompress, CompressOptions::Snappy, CompressOptions::Lz4];
    let mut group = c.benchmark_group("bench iter read");
    for opt in opts {
        let (_dir, sst) = generate_sst(opt);
        let mut iter = SsTableIterator::create_and_seek_to_first(Arc::new(sst)).unwrap();
        group.bench_function(
            BenchmarkId::new("iter_read", opt), 
            |b| b.iter(||{
                while iter.is_valid() {
                    iter.next().unwrap();
                }
            })
        );
    }
}

fn bench_iter_create_and_read(c: &mut Criterion) {
    let opts = vec![CompressOptions::Uncompress, CompressOptions::Snappy, CompressOptions::Lz4];
    let mut group = c.benchmark_group("bench iter create and read");
    for opt in opts {
        let (_dir, sst) = generate_sst(opt);
        let sst = Arc::new(sst);
        group.bench_function(
            BenchmarkId::new("create_and_read", opt),
            |b| b.iter(|| {
            let mut iter = SsTableIterator::create_and_seek_to_first(sst.clone()).unwrap();
            while iter.is_valid() {
                iter.next().unwrap();
            }
        }));
    }
}

criterion_group!(benches, bench_iter_read, bench_iter_create_and_read);
criterion_main!(benches);