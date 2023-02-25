use std::collections::HashMap;

use tempfile::TempDir;

use crate::manifest::ManifestChangeSet;

use super::{Change, ManifestFile};

#[test]
fn create() {
    let dir = TempDir::new().unwrap();
    let (manifest, _) = ManifestFile::open(dir.path()).unwrap();
    manifest.apply_change(&Change::create(1, 1)).unwrap();
    let v = manifest.get_id_level();
    let exp = vec![(1, 1)].into_iter().collect::<HashMap<_, _>>();
    assert_eq!(exp, v);
}

#[test]
fn create_set() {
    let dir = TempDir::new().unwrap();
    let (manifest, _) = ManifestFile::open(dir.path()).unwrap();
    let mut set = Vec::new();
    for i in 1..5 {
        set.push(Change::create(i, i as usize));
    }
    let set = ManifestChangeSet { changes: set };
    manifest.apply_change_set(&set).unwrap();
    let v = manifest.get_id_level();
    let exp = vec![(1, 1), (2, 2), (3, 3), (4, 4)]
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(exp, v);
}

#[test]
fn delete() {
    let dir = TempDir::new().unwrap();
    let (manifest, _) = ManifestFile::open(dir.path()).unwrap();
    for i in 1..5 {
        manifest
            .apply_change(&Change::create(i, i as usize))
            .unwrap();
    }
    manifest.apply_change(&Change::delete(3)).unwrap();
    let v = manifest.get_id_level();
    let exp = vec![(1, 1), (2, 2), (4, 4)]
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(exp, v);
}

#[test]
fn replay() {
    let dir = TempDir::new().unwrap();
    let (manifest, _) = ManifestFile::open(dir.path()).unwrap();
    for i in 0..3 {
        manifest
            .apply_change(&Change::create(i, i as usize))
            .unwrap();
        manifest
            .apply_change(&Change::create(i + 10, i as usize))
            .unwrap();
    }
    manifest.apply_change(&Change::delete(1)).unwrap();
    drop(manifest);
    let (manifest, l0_ids) = ManifestFile::open(dir.path()).unwrap();
    let v = manifest.get_id_level();
    let exp = vec![(0, 0), (10, 0), (11, 1), (2, 2), (12, 2)]
        .into_iter()
        .collect::<HashMap<_, _>>();
    assert_eq!(exp, v);
    assert_eq!(l0_ids, vec![0, 10]);
}
