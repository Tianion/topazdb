use tempfile::{tempdir, TempDir};

use super::MemTable;

use crate::iterators::StorageIterator;
use crate::opt::LsmOptions;
use crate::table::{SsTableBuilder, SsTableIterator};

fn create_for_test() -> (TempDir, MemTable) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    (dir, MemTable::create(path, 0).unwrap())
}

#[test]
fn test_memtable_get() {
    let (_dir, memtable) = create_for_test();
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value1");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value2");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value3");
}

#[test]
fn test_memtable_overwrite() {
    let (_dir, memtable) = create_for_test();
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();
    memtable.put(b"key1", b"value11").unwrap();
    memtable.put(b"key2", b"value22").unwrap();
    memtable.put(b"key3", b"value33").unwrap();
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value11");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value22");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value33");
}

#[test]
fn test_memtable_flush() {
    let (_dir, memtable) = create_for_test();
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(128));
    memtable.flush(&mut builder).unwrap();
    let dir = tempdir().unwrap();
    let sst = builder.build_for_test(dir.path().join("1.sst")).unwrap();
    let mut iter = SsTableIterator::create_and_seek_to_first(sst.into()).unwrap();
    assert_eq!(iter.key(), b"key1");
    assert_eq!(iter.value(), b"value1");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"key2");
    assert_eq!(iter.value(), b"value2");
    iter.next().unwrap();
    assert_eq!(iter.key(), b"key3");
    assert_eq!(iter.value(), b"value3");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}

#[test]
fn test_memtable_iter() {
    use std::ops::Bound;
    let (_dir, memtable) = create_for_test();
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();

    {
        let mut iter = memtable.scan(Bound::Unbounded, Bound::Unbounded);
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");
        iter.next().unwrap();
        assert_eq!(iter.key(), b"key2");
        assert_eq!(iter.value(), b"value2");
        iter.next().unwrap();
        assert_eq!(iter.key(), b"key3");
        assert_eq!(iter.value(), b"value3");
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.scan(Bound::Included(b"key1"), Bound::Included(b"key2"));
        assert_eq!(iter.key(), b"key1");
        assert_eq!(iter.value(), b"value1");
        iter.next().unwrap();
        assert_eq!(iter.key(), b"key2");
        assert_eq!(iter.value(), b"value2");
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.scan(Bound::Excluded(b"key1"), Bound::Excluded(b"key3"));
        assert_eq!(iter.key(), b"key2");
        assert_eq!(iter.value(), b"value2");
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
}

#[test]
fn test_memtable_replay() {
    let dir = tempdir().unwrap();
    let memtable = MemTable::create(dir.path(), 1).unwrap();
    memtable.put(b"key1", b"value1").unwrap();
    memtable.put(b"key2", b"value2").unwrap();
    memtable.put(b"key3", b"value3").unwrap();
    memtable.wal.save_file();
    drop(memtable);
    let memtable = MemTable::open(dir.path(), 1).unwrap();
    assert_eq!(&memtable.get(b"key1").unwrap()[..], b"value1");
    assert_eq!(&memtable.get(b"key2").unwrap()[..], b"value2");
    assert_eq!(&memtable.get(b"key3").unwrap()[..], b"value3");
}
