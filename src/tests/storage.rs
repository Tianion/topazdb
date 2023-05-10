use std::ops::Bound;

use bytes::Bytes;
use tempfile::tempdir;

use crate::{iterators::StorageIterator, opt::LsmOptions};

fn as_bytes(x: &[u8]) -> Bytes {
    Bytes::copy_from_slice(x)
}

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:04}", idx).into_bytes()
}

fn value_of(idx: usize, info: &str) -> Vec<u8> {
    format!("value_{:04}_{}", idx, info).into_bytes()
}

fn check_iter_result(iter: impl StorageIterator, expected: Vec<(Bytes, Bytes)>) {
    let mut iter = iter;
    for (k, v) in expected {
        assert!(iter.is_valid());
        assert_eq!(
            k,
            iter.key(),
            "expected key: {:?}, actual key: {:?}",
            k,
            as_bytes(iter.key()),
        );
        assert_eq!(
            v,
            iter.value(),
            "expected value: {:?}, actual value: {:?}",
            v,
            as_bytes(iter.value()),
        );
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}

#[test]
fn test_storage_get() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
}

#[test]
fn test_storage_channel_put() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    let mut rs = Vec::new();
    let kvs = (0..3)
        .map(|idx| (as_bytes(&key_of(idx)), as_bytes(&value_of(idx, ""))))
        .collect::<Vec<_>>();
    for kv in &kvs {
        let entries = vec![kv.clone()];
        let r = storage.put_to_channel(entries).unwrap();
        rs.push(r);
    }
    for r in rs {
        r.recv().unwrap().unwrap();
    }
    for kv in kvs {
        assert_eq!(storage.get(&kv.0).unwrap().unwrap(), kv.1);
    }
}

#[test]
fn test_storage_batch_put() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();

    let kvs = (0..3)
        .map(|idx| (as_bytes(&key_of(idx)), as_bytes(&value_of(idx, ""))))
        .collect::<Vec<_>>();
    storage.batch_put(&kvs).unwrap();
    for kv in kvs {
        assert_eq!(storage.get(&kv.0).unwrap().unwrap(), kv.1);
    }
}

#[test]
fn test_storage_scan_memtable_1() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"2").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("1"), Bytes::from("233"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![],
    );
}

#[test]
fn test_storage_scan_memtable_2() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"1").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
fn test_storage_get_after_sync() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.sync().unwrap();
    storage.put(b"3", b"23333").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    assert_eq!(&storage.get(b"2").unwrap().unwrap()[..], b"2333");
    assert_eq!(&storage.get(b"3").unwrap().unwrap()[..], b"23333");
    storage.delete(b"2").unwrap();
    assert!(storage.get(b"2").unwrap().is_none());
}

#[test]
fn test_storage_scan_memtable_1_after_sync() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.sync().unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.delete(b"2").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("1"), Bytes::from("233")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("1"), Bytes::from("233"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![],
    );
}

#[test]
fn test_storage_scan_memtable_2_after_sync() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    storage.put(b"2", b"2333").unwrap();
    storage.sync().unwrap();
    storage.put(b"3", b"23333").unwrap();
    storage.sync().unwrap();
    storage.delete(b"1").unwrap();
    check_iter_result(
        storage.scan(Bound::Unbounded, Bound::Unbounded).unwrap(),
        vec![
            (Bytes::from("2"), Bytes::from("2333")),
            (Bytes::from("3"), Bytes::from("23333")),
        ],
    );
    check_iter_result(
        storage
            .scan(Bound::Included(b"1"), Bound::Included(b"2"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
    check_iter_result(
        storage
            .scan(Bound::Excluded(b"1"), Bound::Excluded(b"3"))
            .unwrap(),
        vec![(Bytes::from("2"), Bytes::from("2333"))],
    );
}

#[test]
fn test_storage_close() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    drop(storage);
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
}

#[test]
fn test_storage_close2() {
    use crate::lsm_storage::LsmStorage;
    let dir = tempdir().unwrap();
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"1", b"233").unwrap();
    assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    drop(storage);
    let storage = LsmStorage::open(LsmOptions::default().path(&dir)).unwrap();
    storage.put(b"2", b"233").unwrap();
}
