use std::{collections::BTreeMap, ops::Bound, path::Path, sync::Arc};

use bytes::Bytes;
use tempfile::TempDir;

use crate::{
    opt::LsmOptions,
    table::{SsTable, SsTableBuilder},
    util::sstable_file_path,
};

use super::{
    range::{RangeWithSize, RwsSlice},
    task::{Task, TaskPriority},
    LevelController,
};

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:04}", idx).into_bytes()
}

fn value_of(idx: usize, info: &str) -> Vec<u8> {
    format!("value_{:04}_{}", idx, info).into_bytes()
}

fn generate_sst(
    lower: usize,
    upper: usize,
    id: u64,
    path: impl AsRef<Path>,
    info: &str,
) -> SsTable {
    let mut builder = SsTableBuilder::new(LsmOptions::default());
    for idx in lower..upper {
        builder.add(&key_of(idx), &value_of(idx, info)).unwrap();
    }
    builder
        .build(id, None, sstable_file_path(path.as_ref(), id))
        .unwrap()
}

#[test]
fn create_ranges() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let mut task = Task {
        this_level_id: 0,
        next_level_id: 1,
        ..Default::default()
    };
    for i in 0..3 {
        let table = generate_sst(i * 20, i * 50 + 50, i as u64, path, "l0");
        task.this_tables.push(Arc::new(table));
    }
    for i in 4..9 {
        let j = i - 4;
        let table = generate_sst(j * 50, (j + 1) * 50, i as u64, path, "l1");
        task.next_tables.push(Arc::new(table));
    }
    let rws = RwsSlice::create(&task);
    let mut key = rws.ranges[0].smallest_key.clone();
    for item in &rws.ranges {
        assert_eq!(item.smallest_key, key);
        key = item.biggest_key.clone();
    }
}

#[test]
fn ranges_split() {
    let ranges = vec![
        RangeWithSize {
            smallest_key: Bytes::from(&b"001"[..]),
            biggest_key: Bytes::from(&b"002"[..]),
            size: 10,
        },
        RangeWithSize {
            smallest_key: Bytes::from(&b"002"[..]),
            biggest_key: Bytes::from(&b"003"[..]),
            size: 10,
        },
        RangeWithSize {
            smallest_key: Bytes::from(&b"003"[..]),
            biggest_key: Bytes::from(&b"004"[..]),
            size: 20,
        },
        RangeWithSize {
            smallest_key: Bytes::from(&b"004"[..]),
            biggest_key: Bytes::from(&b"005"[..]),
            size: 14,
        },
    ];
    let total_size = 10 + 10 + 20 + 14;
    let rws = RwsSlice { ranges, total_size };
    let mean = 15;
    let exp = vec![
        (
            Bound::Included(Bytes::from(&b"001"[..])),
            Bound::Excluded(Bytes::from(&b"003"[..])),
        ),
        (
            Bound::Included(Bytes::from(&b"003"[..])),
            Bound::Excluded(Bytes::from(&b"004"[..])),
        ),
        (
            Bound::Included(Bytes::from(&b"004"[..])),
            Bound::Included(Bytes::from(&b"005"[..])),
        ),
    ];
    let bounds = rws.split(mean);
    assert_eq!(exp, bounds)
}

fn lvctl_new(dir: &TempDir) -> LevelController {
    LevelController::open(LsmOptions::default().path(dir.path())).unwrap()
}

#[test]
fn get_simple_key() {
    let dir = TempDir::new().unwrap();
    let lvctl = lvctl_new(&dir);
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
    for i in 0..10 {
        builder.add(&key_of(i), &value_of(i, "")).unwrap();
    }
    lvctl.l0_push_sstable(builder).unwrap();
    for i in 0..10 {
        assert_eq!(value_of(i, ""), lvctl.get(&key_of(i)).unwrap().unwrap());
    }
}

#[test]
fn get_simple_not_exist() {
    let dir = TempDir::new().unwrap();
    let lvctl = lvctl_new(&dir);
    assert_eq!(None, lvctl.get(b"aaaaa").unwrap());
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
    for i in 0..10 {
        builder.add(&key_of(i), &value_of(i, "")).unwrap();
    }
    lvctl.l0_push_sstable(builder).unwrap();
    assert_eq!(None, lvctl.get(b"aaaaa").unwrap());
}

#[test]
fn get_key_new_old() {
    let dir = TempDir::new().unwrap();
    let lvctl = LevelController::open(LsmOptions::default().path(dir.path())).unwrap();
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
    for i in 0..10 {
        builder.add(&key_of(i), &value_of(i, "old")).unwrap();
    }
    lvctl.l0_push_sstable(builder).unwrap();
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
    for i in 0..10 {
        builder.add(&key_of(i), &value_of(i, "new")).unwrap();
    }
    lvctl.l0_push_sstable(builder).unwrap();
    for i in 0..10 {
        assert_eq!(
            Bytes::from(value_of(i, "new")),
            lvctl.get(&key_of(i)).unwrap().unwrap()
        );
    }
}

#[test]
fn get_key_delete() {
    let dir = TempDir::new().unwrap();
    let lvctl = lvctl_new(&dir);
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
    for i in 0..10 {
        builder.add(&key_of(i), &value_of(i, "old")).unwrap();
    }
    lvctl.l0_push_sstable(builder).unwrap();
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
    for i in 0..10 {
        builder.add(&key_of(i), b"").unwrap();
    }
    lvctl.l0_push_sstable(builder).unwrap();
    for i in 0..10 {
        assert_eq!(None, lvctl.get(&key_of(i)).unwrap());
    }
}

#[test]
fn get_key_drop() {
    let dir = TempDir::new().unwrap();
    let lvctl = lvctl_new(&dir);
    let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
    for i in 0..10 {
        builder.add(&key_of(i), &value_of(i, "")).unwrap();
    }
    lvctl.l0_push_sstable(builder).unwrap();
    lvctl.mark_save();
    drop(lvctl);
    let lvctl = lvctl_new(&dir);
    for i in 0..10 {
        assert_eq!(value_of(i, ""), lvctl.get(&key_of(i)).unwrap().unwrap());
    }
}

fn generate_lvctl(path: impl AsRef<Path>) -> (LevelController, BTreeMap<Bytes, Bytes>) {
    let lvctl = LevelController::open(LsmOptions::default().path(path)).unwrap();
    let mut map = BTreeMap::new();
    for i in 0..10 {
        let mut builder = SsTableBuilder::new(LsmOptions::default().block_size(64));
        for j in i * 50..i * 50 + 70 {
            let key = key_of(j);
            let val = value_of(j, &i.to_string());
            builder.add(&key, &val).unwrap();
            map.insert(Bytes::copy_from_slice(&key), Bytes::copy_from_slice(&val));
        }
        lvctl.l0_push_sstable(builder).unwrap();
    }
    (lvctl, map)
}

#[test]
fn open_drop() {
    let dir = TempDir::new().unwrap();
    let (lvctl, map) = generate_lvctl(dir.path());
    lvctl.mark_save();
    drop(lvctl);
    let lvctl = lvctl_new(&dir);
    for (key, val) in map.iter() {
        assert_eq!(lvctl.get(key).unwrap().unwrap(), val);
    }
}

#[test]
fn simple_compact() {
    let dir = TempDir::new().unwrap();
    let (lvctl, map) = generate_lvctl(dir.path());
    for (key, val) in map.iter() {
        assert_eq!(lvctl.get(key).unwrap().unwrap(), val);
    }
    lvctl
        .inner
        .do_compact(
            0,
            TaskPriority {
                level: 0,
                score: 1.0,
            },
        )
        .unwrap();
    for (key, val) in map.iter() {
        assert_eq!(lvctl.get(key).unwrap().unwrap(), val);
    }
}
