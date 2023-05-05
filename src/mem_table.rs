use std::collections::VecDeque;
use std::fs;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam::skiplist as crossbeam_skiplist;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::opt::LsmOptions;
use crate::table::SsTableBuilder;
use crate::util::{memtable_file_path, MEMTABLE_FILE_EXT};
use crate::wal::Wal;

pub struct MemTables {
    pub memtable: Arc<MemTable>,
    pub imm_memtables: VecDeque<Arc<MemTable>>,
    pub next_mem_id: usize,
    opt: Arc<LsmOptions>,
}

impl MemTables {
    pub fn new(opt: Arc<LsmOptions>) -> Result<Self> {
        let (imm_memtables, next_mem_id) = Self::open_mem_tables(&opt)?;

        Ok(MemTables {
            memtable: Arc::new(MemTable::create(&opt.dir, next_mem_id)?),
            imm_memtables,
            next_mem_id: next_mem_id + 1,
            opt,
        })
    }

    fn open_mem_tables(opts: &LsmOptions) -> Result<(VecDeque<Arc<MemTable>>, usize)> {
        let mut fids = vec![];
        let mut mts = VecDeque::new();

        for file in fs::read_dir(&opts.dir)? {
            let file = file?;
            let filename_ = file.file_name();
            let filename = filename_.to_string_lossy();
            if filename.ends_with(MEMTABLE_FILE_EXT) {
                let end = filename.len() - MEMTABLE_FILE_EXT.len();
                let fid: usize = filename[end - 5..end].parse().unwrap();
                fids.push(fid);
            }
        }

        fids.sort_unstable();

        for fid in &fids {
            let memtable = MemTable::open(&opts.dir, *fid)?;
            mts.push_back(Arc::new(memtable));
        }

        let mut next_mem_fid = 0;

        if !fids.is_empty() {
            next_mem_fid = *fids.last().unwrap();
        }

        next_mem_fid += 1;

        Ok((mts, next_mem_fid))
    }

    pub fn view(&self) -> Vec<Arc<MemTable>> {
        let mut view = Vec::with_capacity(self.imm_memtables.len() + 1);
        for memtable in self.imm_memtables.iter() {
            view.push(memtable.clone());
        }
        view.push(self.memtable.clone());
        view
    }

    pub fn use_new_table(&mut self) -> Result<()> {
        let table = Arc::new(MemTable::create(&self.opt.dir, self.next_mem_id)?);
        self.next_mem_id += 1;
        let memtable = std::mem::replace(&mut self.memtable, table);
        self.imm_memtables.push_back(memtable);
        Ok(())
    }

    /// Put a key-value pair into the mutable mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.memtable.put(key, value)
    }

    pub fn put_entries(&self, entries: &[(Bytes, Bytes)]) -> Result<()> {
        self.memtable.put_entries(entries)
    }
}

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Value>>,
    size: AtomicUsize,
    wal: Wal,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(path: impl AsRef<Path>, id: usize) -> Result<Self> {
        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Wal::create(memtable_file_path(path, id))?,
            size: AtomicUsize::new(0),
        })
    }

    pub fn open(path: impl AsRef<Path>, id: usize) -> Result<Self> {
        let wal = Wal::open(memtable_file_path(path, id))?;
        let mut iter = wal.iter()?;
        let map = SkipMap::new();
        let mut size = 0;

        while iter.is_valid() {
            let key = Bytes::copy_from_slice(iter.key());
            let value = Bytes::copy_from_slice(iter.value());
            size += key.len() + value.len();
            map.insert(
                key,
                Value {
                    val: value,
                    version: 0,
                },
            );
            iter.next();
        }
        Ok(Self {
            map: map.into(),
            wal,
            size: AtomicUsize::new(size),
        })
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).map(|entry| entry.value().val.clone())
    }

    /// Put a key-value pair into the mem-table.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let version = self.wal.add(key, value)?;
        self.do_mem_put(key, value, version);
        Ok(())
    }

    fn put_entries(&self, entries: &[(Bytes, Bytes)]) -> Result<()> {
        let version = self.wal.add_entries(entries)?;
        for (key, value) in entries {
            self.do_mem_put(key, value, version);
        }
        Ok(())
    }

    fn do_mem_put(&self, key: &[u8], value: &[u8], version: u64) {
        let old_size = self
            .map
            .get(key)
            .map(|entry| entry.key().len() + entry.value().val.len())
            .unwrap_or(0);

        let val = Bytes::copy_from_slice(value);
        let insert_version = self
            .map
            .compare_insert(Bytes::copy_from_slice(key), Value { val, version }, |x| {
                x.version < version
            })
            .value()
            .version;

        if version != insert_version {
            return;
        }

        if key.len() + value.len() >= old_size {
            let add = key.len() + value.len() - old_size;
            self.size.fetch_add(add, Ordering::Relaxed);
        } else {
            let sub = old_size - key.len() + value.len();
            self.size.fetch_sub(sub, Ordering::Relaxed);
        }
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        fn bound_u8_to_bytes(bound: Bound<&[u8]>) -> Bound<Bytes> {
            match bound {
                Bound::Excluded(data) => Bound::Excluded(Bytes::copy_from_slice(data)),
                Bound::Included(data) => Bound::Included(Bytes::copy_from_slice(data)),
                Bound::Unbounded => Bound::Unbounded,
            }
        }

        let (lower, upper) = (bound_u8_to_bytes(lower), bound_u8_to_bytes(upper));

        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            item: (Bytes::new(), Bytes::new()),
            iter_builder: |map| map.range((lower, upper)),
        }
        .build();

        iter.with_mut(|x| *x.item = entry_to_item(x.iter.next()));
        iter
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key(), &entry.value().val)?;
        }
        Ok(())
    }
}

struct Value {
    val: Bytes,
    version: u64,
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Value>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Value>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

fn entry_to_item(entry: Option<Entry<Bytes, Value>>) -> (Bytes, Bytes) {
    entry
        .map(|x| (x.key().clone(), x.value().val.clone()))
        .unwrap_or((Bytes::new(), Bytes::new()))
}

impl StorageIterator for MemTableIterator {
    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|x| *x.item = entry_to_item(x.iter.next()));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
