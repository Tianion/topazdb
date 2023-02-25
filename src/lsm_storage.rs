use std::collections::BTreeMap;
use std::ops::Bound;

use std::sync::Arc;
use std::thread::spawn;
use std::time::Duration;


use anyhow::{Ok, Result};
use bytes::Bytes;

use crossbeam_channel::{tick, select};
use log::info;
use parking_lot::{Mutex, RwLock};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::level::LevelController;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::{MemTables};
use crate::opt::LsmOptions;
use crate::table::{SsTableBuilder, SsTableIterator};

pub struct LsmStorageInner {
    /// Memory table
    memtables: RwLock<MemTables>,
    lvctl: LevelController,
    opt: LsmOptions
}

impl LsmStorageInner {
    fn create(opt: LsmOptions) -> Result<Self> {
        Ok(Self {
            memtables: RwLock::new(MemTables::new(opt.clone())?),
            lvctl: LevelController::open(opt.clone())?,
            opt
        })
    }

    fn start_flush(self: Arc<Self>) {
        let inner = self.clone();
        spawn(move || {
            let run_once = || -> Result<()> {
                let mut imm_memtable = 
                    inner.memtables.read().imm_memtables.clone();
                if imm_memtable.len() < inner.opt.min_memtable_to_merge {
                    return Ok(());
                }
                let mut memtables = Vec::with_capacity(inner.opt.min_memtable_to_merge);
                while let Some(memtable) = imm_memtable.pop_front() {
                    memtables.push(memtable);
                } 

                
                let mut iter = MergeIterator::create(
                    memtables.into_iter().map(
                        |x| Box::new(x.scan(Bound::Unbounded, Bound::Unbounded))
                    ).collect()
                );

                let mut builder = SsTableBuilder::new(inner.opt.block_size);

                while iter.is_valid() {
                    builder.add(iter.key(), iter.value());
                    iter.next()?;
                }

                inner.lvctl.l0_push_sstable(builder)
            };

            let full_run = || {
                let len = 
                    self.memtables.read().imm_memtables.len();
                if len < self.opt.max_memtable_num - 1 {
                    return Ok(())
                }
                run_once()
            };
            let ticker_run = tick(Duration::from_millis(50));
            let ticker_check = tick(Duration::from_millis(5));
            loop {
                if let Err(e) = select! {
                    recv(ticker_run) -> _ => run_once(),
                    recv(ticker_check) -> _ => full_run(),
                }
                {
                    // TODO: err handling
                    info!("error {}", e)
                }
            }
        });
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<LsmStorageInner>,
    #[allow(dead_code)]
    opt: LsmOptions,
    flush_lock: Mutex<()>,
}

impl LsmStorage {
    pub fn open(opt: LsmOptions) -> Result<Self> {
        let inner = Arc::new(LsmStorageInner::create(opt.clone())?);
        inner.lvctl.start_compact();
        let flush_core = inner.clone();
        flush_core.start_flush();
        Ok(Self {
            inner,
            flush_lock: Mutex::new(()),
            opt
        })
    }

    /// Get a key from the storage. 
    // TODO: this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        assert!(!key.is_empty(), "key cannot be empty");

        let view = self.inner.memtables.read().view();

        for memtable in view.iter().rev() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        self.inner.lvctl.get(key)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");

        self.do_put(key, value)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        self.do_put(key, b"")
        
    }
    fn do_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.memtables.read().put(key, value)?;
        if self.inner.memtables.read().memtable.size() > self.opt.memtable_size {
            if let Some(mut guard) = self.inner.memtables.try_write() {
                guard.use_new_table()?;
            }
        }
        Ok(())
    }

    /// Persist data to disk.
    pub fn sync(&self) -> Result<()> {
        let _lock = self.flush_lock.lock();

        let len = {
            let mut guard = self.inner.memtables.write();
            guard.use_new_table()?;
            guard.imm_memtables.len()
        };

        let mut map = BTreeMap::new();
        for i in 0..len {
            let table = self.inner.memtables.read().imm_memtables[i].clone();
            let mut iter = table.scan(Bound::Unbounded, Bound::Unbounded);
            while iter.is_valid() {
                map.insert(iter.key().to_vec(), iter.value().to_vec());
                iter.next()?;
            }
        }

        let mut builder = SsTableBuilder::new(4096);
        for (key, value) in &map {
            builder.add(key, value);
        }

        self.inner.lvctl.l0_push_sstable(builder)?;

        {
            let mut guard = self.inner.memtables.write();
            for _ in 0..len {
                guard.imm_memtables.pop_front().unwrap();
            }
        }

        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let memtables = self.inner.memtables.read().view();
        let mem_iters = memtables
            .iter()
            .rev()
            .map(|table| Box::new(table.scan(lower, upper)))
            .collect::<Vec<_>>();
        let mem_iter = MergeIterator::create(mem_iters);

        let ssts = self.inner.lvctl.level_tables_sorted(lower, upper);
        let mut sst_iters = Vec::with_capacity(ssts.len());
        for table in ssts.iter() {
            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table.clone(), key)?
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table.clone())?,
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(table.clone(), key)?;
                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                }
            };
            sst_iters.push(Box::new(iter));
        }
        let sst_iter = MergeIterator::create(sst_iters);
        let iter = TwoMergeIterator::create(mem_iter, sst_iter)?;
        let end = match upper {
            Bound::Included(key) => Bound::Included(Bytes::copy_from_slice(key)),
            Bound::Unbounded => Bound::Unbounded,
            Bound::Excluded(key) => Bound::Excluded(Bytes::copy_from_slice(key)),
        };
        Ok(FusedIterator::new(LsmIterator::new(iter, end)?))
    }
}
