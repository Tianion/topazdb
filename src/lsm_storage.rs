use std::collections::BTreeMap;
use std::ops::Bound;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crossbeam_channel::{select, tick, Receiver, Sender};
use log::{debug, error, info};
use parking_lot::{Mutex, RwLock};
use yatp::task::callback::{Handle, TaskCell};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::level::LevelController;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::MemTables;
use crate::opt::LsmOptions;
use crate::table::{SsTableBuilder, SsTableIterator};

pub struct LsmStorageInner {
    /// Memory table
    memtables: RwLock<MemTables>,
    lvctl: LevelController,
    opt: LsmOptions,
}

impl LsmStorageInner {
    fn create(opt: LsmOptions) -> Result<Self> {
        Ok(Self {
            memtables: RwLock::new(MemTables::new(opt.clone())?),
            lvctl: LevelController::open(opt.clone())?,
            opt,
        })
    }

    //TODO: channel send task
    fn start_flush(self: Arc<Self>, pool: Arc<ThreadPool>, closer: Arc<Receiver<()>>) {
        let inner = self.clone();
        pool.spawn(move |_: &mut Handle| {
            let run_once = || -> Result<()> {
                let mut imm_memtable = inner.memtables.read().imm_memtables.clone();
                if imm_memtable.len() < inner.opt.min_memtable_to_merge {
                    return Ok(());
                }
                let mut memtables = Vec::with_capacity(inner.opt.min_memtable_to_merge);
                while let Some(memtable) = imm_memtable.pop_front() {
                    memtables.push(memtable);
                }

                let mut iter = MergeIterator::create(
                    memtables
                        .iter()
                        .map(|x| Box::new(x.scan(Bound::Unbounded, Bound::Unbounded)))
                        .collect(),
                );

                let mut builder = SsTableBuilder::new(self.opt.clone());

                while iter.is_valid() {
                    builder.add(iter.key(), iter.value())?;
                    iter.next()?;
                }

                inner.lvctl.l0_push_sstable(builder)?;
                {
                    let mut guard = inner.memtables.write();
                    for _ in 0..memtables.len() {
                        guard.imm_memtables.pop_front();
                    }
                }

                info!("push l0 sstable");
                Ok(())
            };

            let full_run = || {
                let len = self.memtables.read().imm_memtables.len();
                if len < self.opt.max_memtable_num - 1 {
                    return Ok(());
                }
                run_once()
            };
            let ticker_run = tick(Duration::from_millis(50));
            let ticker_check = tick(Duration::from_millis(5));
            info!("flush start");
            loop {
                if let Err(e) = select! {
                    recv(ticker_run) -> _ => run_once(),
                    recv(ticker_check) -> _ => full_run(),
                    recv(closer) -> _ => break,
                } {
                    // TODO: err handling
                    error!("error {}", e)
                }
            }
        });
    }
}

pub type ThreadPool = yatp::ThreadPool<TaskCell>;

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<LsmStorageInner>,
    opt: LsmOptions,
    closer: Option<Sender<()>>,
    pool: Arc<ThreadPool>,
    flush_lock: Mutex<()>,
}

impl LsmStorage {
    pub fn open(opt: LsmOptions) -> Result<Self> {
        let pool = yatp::Builder::new("topazdb")
            .max_thread_count(opt.compactor_num * 6 + 2)
            .min_thread_count(opt.compactor_num * 4 + 2)
            .build_callback_pool();

        let pool = Arc::new(pool);

        let inner = Arc::new(LsmStorageInner::create(opt.clone())?);

        let (sender, receiver) = crossbeam_channel::unbounded();

        let receiver = Arc::new(receiver);
        inner.lvctl.start_compact(pool.clone(), receiver.clone());

        let flush_core = inner.clone();
        flush_core.start_flush(pool.clone(), receiver);

        Ok(Self {
            inner,
            flush_lock: Mutex::new(()),
            closer: Some(sender),
            pool,
            opt,
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
        let size = {
            let guard = self.inner.memtables.read();
            guard.put(key, value)?;
            guard.memtable.size()
        };
        self.may_use_new_table(size)
    }

    // TODO: async by channel
    // 1. channel send entry to write core
    // 2. merge request
    // 3. batch write
    // ok... There may be several options here.
    // consider this case: A puts (k1, v1), B puts (k2, v2)
    // then A and B both receive OK
    pub fn put_to_channel(
        &self,
        _key: &[u8],
        _value: &[u8],
    ) -> Result<crossbeam_channel::Receiver<()>> {
        todo!()
    }

    fn may_use_new_table(&self, size: usize) -> Result<()> {
        if size <= self.opt.memtable_size {
            return Ok(());
        }

        let mut guard = self.inner.memtables.write();
        // secondary check
        if guard.memtable.size() > self.opt.memtable_size {
            guard.use_new_table()?;
            debug!("use new memtable");
        }

        Ok(())
    }

    pub fn batch_put(&self, entries: Vec<(&[u8], &[u8])>) -> Result<()> {
        let size = {
            let guard = self.inner.memtables.read();
            guard.put_entries(entries)?;
            guard.memtable.size()
        };

        self.may_use_new_table(size)
    }

    /// Persist data to disk.
    pub fn sync(&self) -> Result<()> {
        let _lock = self.flush_lock.lock();

        let mut guard = self.inner.memtables.write();
        guard.use_new_table()?;

        let len = guard.imm_memtables.len();

        let mut map = BTreeMap::new();
        for i in 0..len {
            let table = guard.imm_memtables[i].clone();
            let mut iter = table.scan(Bound::Unbounded, Bound::Unbounded);
            while iter.is_valid() {
                map.insert(iter.key().to_vec(), iter.value().to_vec());
                iter.next()?;
            }
        }

        if map.is_empty() {
            return Ok(());
        }

        let mut builder = SsTableBuilder::new(self.opt.clone());
        for (key, value) in &map {
            builder.add(key, value).unwrap();
        }

        self.inner.lvctl.l0_push_sstable(builder)?;

        for _ in 0..len {
            guard.imm_memtables.pop_front().unwrap();
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

impl Drop for LsmStorage {
    fn drop(&mut self) {
        self.closer.take();
        self.pool.shutdown();
        self.sync().unwrap();
        self.inner.lvctl.mark_save();
    }
}
