use tempfile::TempDir;

use crate::util::memtable_file_path;

use super::Wal;

#[test]
fn test_replay() {
    let dir = TempDir::new().unwrap();
    let wal = Wal::create(memtable_file_path(dir.path(), 0)).unwrap();
    let input = vec![
        (&b"aaa"[..], &b"bbb"[..]),
        (&b"aaa"[..], &b"bbb"[..]),
        (&b"cccc"[..], &b"bbb"[..]),
        (&b"aaa"[..], &b"aaa"[..]),
        (&b"m"[..], &b"n"[..]),
    ];
    for (key, value) in &input {
        wal.add(key, value).unwrap();
    }
    drop(wal);
    let wal = Wal::open(memtable_file_path(dir.path(), 0)).unwrap();
    let mut iter = wal.iter().unwrap();
    for (key, value) in input {
        assert!(iter.is_valid());
        assert_eq!(iter.key(), key);
        assert_eq!(iter.value(), value);
        iter.next();
    }
}
