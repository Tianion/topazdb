use anyhow::{Ok, Result};
use bytes::{Buf, BufMut};
use parking_lot::Mutex;
use std::io::BufReader;
use std::path::Path;

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
};

struct ManifestFileInner {
    fs: File,
    map: HashMap<u64, usize>,
}

impl ManifestFileInner {
    fn delete(&mut self, id: u64) -> Result<()> {
        if !self.map.contains_key(&id) {
            return Err(anyhow::anyhow!("non-existent id"));
        }
        let mut buf = Vec::with_capacity(9);
        buf.put_u8(1);
        buf.put_u64(id);
        self.fs.write_all(&buf)?;
        self.map.remove(&id);
        Ok(())
    }

    fn add(&mut self, id: u64, level: usize) -> Result<()> {
        if self.map.contains_key(&id) {
            return Err(anyhow::anyhow!("repeated id"));
        }
        let mut buf = Vec::with_capacity(10);
        buf.put_u8(0);
        buf.put_u64(id);
        buf.put_u8(level as u8);
        self.fs.write_all(&buf)?;
        self.map.insert(id, level);
        Ok(())
    }
}

pub struct ManifestFile {
    inner: Mutex<ManifestFileInner>,
}

impl ManifestFile {
    pub fn get_id_level(&self) -> HashMap<u64, usize> {
        self.inner.lock().map.clone()
    }

    /// return Self and level0 ordered and unfiltered id
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, Vec<u64>)> {
        let manifest_path = path.as_ref().join("MANIFEST");
        if !manifest_path.exists() {
            fs::File::create(&manifest_path)?;
        }

        let mut reader = BufReader::new(File::open(&manifest_path)?);
        let mut buf = String::new();
        reader.read_to_string(&mut buf)?;
        let mut buf = buf.as_bytes();
        let mut map = HashMap::new();
        let mut ids = vec![];
        while !buf.is_empty() {
            let op = buf.get_u8();
            let id = buf.get_u64();
            match Operation::from(op) {
                Operation::Create => {
                    let level = buf.get_u8();
                    map.insert(id, level as usize);
                    if level == 0 {
                        ids.push(id);
                    }
                }
                Operation::Delete => {
                    map.remove(&id);
                }
            }
        }
        let fs = fs::File::options().append(true).open(&manifest_path)?;
        let inner = ManifestFileInner { fs, map };
        Ok((
            Self {
                inner: Mutex::new(inner),
            },
            ids,
        ))
    }

    pub fn apply_change_set(&self, change_set: &ManifestChangeSet) -> Result<()> {
        let mut w = self.inner.lock();
        for change in &change_set.changes {
            let id = change.table_id;
            let level = change.level;
            match change.op {
                Operation::Create => w.add(id, level)?,
                Operation::Delete => w.delete(id)?,
            }
        }
        w.fs.sync_all()?;
        Ok(())
    }

    pub fn apply_change(&self, change: &Change) -> Result<()> {
        let mut w = self.inner.lock();
        let id = change.table_id;
        let level = change.level;
        match change.op {
            Operation::Create => w.add(id, level)?,
            Operation::Delete => w.delete(id)?,
        }
        w.fs.sync_all()?;
        Ok(())
    }
}

pub struct ManifestChangeSet {
    pub changes: Vec<Change>,
}

#[repr(u8)]
enum Operation {
    Create = 0,
    Delete = 1,
}

impl From<u8> for Operation {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Create,
            1 => Self::Delete,
            _ => panic!("invalid value"),
        }
    }
}

pub struct Change {
    op: Operation,
    table_id: u64,
    level: usize,
}

impl Change {
    pub fn create(table_id: u64, level: usize) -> Self {
        Self {
            op: Operation::Create,
            table_id,
            level,
        }
    }

    pub fn delete(table_id: u64) -> Self {
        Self {
            op: Operation::Delete,
            table_id,
            level: 0,
        }
    }
}

#[cfg(test)]
mod test;
