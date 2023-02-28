mod range;
#[allow(unused)]
#[allow(dead_code)]
mod task;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::spawn,
    time::Duration,
};

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_channel::{tick, unbounded};
use log::{error, info};
use parking_lot::{Mutex, RwLock};

use crate::{
    block::Block,
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    level::{
        range::RwsSlice,
        task::{Task, TaskPriority},
    },
    manifest::{Change, ManifestChangeSet, ManifestFile},
    opt::LsmOptions,
    table::{FileObject, SsTable, SsTableBuilder, SsTableIterator},
    util::sstable_file_path,
};

const MAX_LEVEL: usize = 6;
pub type BlockCache = moka::sync::Cache<(u64, usize), Arc<Block>>;

// TODO: opt or update size

struct LevelsControllerInner {
    next_sst_id: AtomicU64,
    levels: Vec<RwLock<Vec<Arc<SsTable>>>>,
    compact_job: Arc<Vec<Mutex<HashSet<u64>>>>,
    manifest: Arc<ManifestFile>,
    opt: LsmOptions,
}

impl LevelsControllerInner {
    fn max_level_byte(&self, level: usize) -> usize {
        let mut base_byte = self.opt.max_bytes_for_level_base;
        for _ in 1..=level {
            base_byte *= self.opt.max_bytes_for_level_multiplier;
        }
        base_byte
    }

    fn max_level_file(&self, level: usize) -> usize {
        let file_size_base = self.opt.max_bytes_for_level_base / self.opt.target_file_size_base;
        let mut num = file_size_base;
        for _ in 1..=level {
            num *= file_size_base;
        }
        num
    }

    fn new(opt: LsmOptions, block_cache: Arc<BlockCache>) -> Result<Self> {
        let path = &opt.dir;
        let (manifest, l0_ids) = ManifestFile::open(path)?;
        let id_level = manifest.get_id_level();
        let next_sst_id = AtomicU64::new(id_level.keys().copied().max().unwrap_or(0));
        let mut levels = vec![vec![]; opt.num_levels];

        for id in l0_ids {
            if id_level.contains_key(&id) {
                let file = FileObject::open(&sstable_file_path(path, id))?;
                let table = Arc::new(SsTable::open(id, Some(block_cache.clone()), file)?);
                levels[0].push(table);
            }
        }

        for (id, level) in id_level {
            if level == 0 {
                continue;
            }
            let file = FileObject::open(&sstable_file_path(path, id))?;
            let table = Arc::new(SsTable::open(id, Some(block_cache.clone()), file)?);
            levels[level].push(table);
        }
        let levels = levels.into_iter().map(RwLock::new).collect();
        let mut compact_job = Vec::with_capacity(MAX_LEVEL);
        for _ in 0..MAX_LEVEL {
            compact_job.push(Mutex::new(HashSet::new()))
        }
        let compact_job = Arc::new(compact_job);
        Ok(Self {
            opt,
            next_sst_id,
            levels,
            compact_job,
            manifest: Arc::new(manifest),
        })
    }

    fn level_size(&self, level: usize) -> usize {
        let tables = self.levels[level].read().clone();
        let job = self.compact_job[level].lock();
        let mut size = 0;
        for table in tables {
            if !job.contains(&table.id) {
                size += table.size;
            }
        }
        size
    }

    fn pick_compact_levels(&self) -> Vec<TaskPriority> {
        let mut prios = vec![];

        for i in 0..self.levels.len() {
            let size = self.level_size(i);
            let size_score = size as f64 / self.max_level_byte(i) as f64;
            let num_score = self.levels[i].read().len() as f64 / self.max_level_file(i) as f64;
            let pri = TaskPriority::new(i, size_score.max(num_score));
            prios.push(pri);
        }

        assert_eq!(prios.len(), self.levels.len());

        // Remove last level.
        prios.pop();
        let mut x: Vec<TaskPriority> = prios.into_iter().filter(|x| x.score > 1.0).collect();
        x.sort_by(|x, y| y.score.partial_cmp(&x.score).unwrap());
        x
    }

    /// tired compact, merge front 'size' tables.
    // unused
    #[allow(unused)]
    fn l0_tired_compact(&self, block_cache: Arc<BlockCache>) -> Result<()> {
        // unimplemented!();
        let tables = self.levels[0].read().clone();

        let mut iters = Vec::with_capacity(tables.len());

        for table in tables {
            let iter = Box::new(SsTableIterator::create_and_seek_to_first(table.clone())?);
            iters.push(iter);
        }

        let mut iter = MergeIterator::create(iters);

        let mut builder = SsTableBuilder::new(4096, self.opt.compress_option);
        while iter.is_valid() {
            builder.add(iter.key(), iter.value());
            iter.next()?;
        }

        let id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);

        // let sst = Arc::new(builder.build(id, Some(block_cache.clone()), self.sstable_file_path(id))?);
        // let front = vec![sst];

        // {
        //     // let mut manifest = self.manifest.lock();
        //     let mut guard = self.inner.levels[0].write();

        //     for _ in 0..size {
        //         let table = guard.pop_front().unwrap();
        //         // manifest.delete(table.id() as u64)?;
        //     }
        //     guard.push_front(sst.clone());
        //     // manifest.add(sst.id() as u64, 0)?;
        // }

        Ok(())
    }

    fn do_compact(self: &Arc<Self>, idx: usize, pri: TaskPriority) -> Result<()> {
        let level = pri.level;
        assert!(level + 1 < MAX_LEVEL);
        // TODO: 如果是level 判断是否要走l0的tired compaction

        let task = self.create_task(pri.level);

        info!("compactor {idx} creates task {}", task.is_some());

        if task.is_none() {
            return Ok(());
        }

        let task = Arc::new(task.unwrap());

        let rws = RwsSlice::create(&task);
        // TODO: 得到sub_compact线程数
        let num_sub_compact = 4;
        let mean = rws.total_size / num_sub_compact;
        let ranges = rws.split(mean);

        let (tx, rx) = unbounded();
        for (lower, upper) in ranges.iter() {
            let this = self.clone();
            let task = task.clone();
            let tx = tx.clone();
            let lower = lower.clone();
            let upper = upper.clone();
            std::thread::spawn(move || tx.send(this.sub_compact(&task, lower, upper)));
        }
        let mut new_tables = vec![];
        for mut table in rx.iter().take(ranges.len()).flatten() {
            new_tables.append(&mut table);
        }
        new_tables.sort_by(|a, b| a.smallest_key.partial_cmp(&b.smallest_key).unwrap());

        let change_set = build_change_set(&task, &new_tables);
        self.manifest.apply_change_set(&change_set)?;
        self.update_with_tables(&task, &new_tables)?;

        info!("compactor #{} on level {} success", idx, task.this_level_id);

        Ok(())
    }

    fn sub_compact(
        self: &Arc<Self>,
        task: &Task,
        lower: Bound<Bytes>,
        upper: Bound<Bytes>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut tables = Vec::with_capacity(task.this_tables.len() + task.next_tables.len());
        tables.extend_from_slice(&task.this_tables);
        tables.extend_from_slice(&task.next_tables);

        let mut iters = Vec::with_capacity(tables.len());
        for table in tables {
            let iter = match lower {
                Bound::Included(ref key) => SsTableIterator::create_and_seek_to_key(table, key)?,
                _ => panic!("invalid lower"),
            };
            iters.push(Box::new(iter));
        }

        let mut iter = MergeIterator::create(iters);
        let mut new_tables = vec![];
        fn key_vaild(iter: &MergeIterator<SsTableIterator>, upper: &Bound<Bytes>) -> bool {
            match upper {
                Bound::Unbounded => panic!("invalid upper"),
                Bound::Included(key) => iter.key() <= key,
                Bound::Excluded(key) => iter.key() < key,
            }
        }
        while iter.is_valid() && key_vaild(&iter, &upper) {
            let mut build = SsTableBuilder::new(4096, self.opt.compress_option);

            while iter.is_valid() && !build.reach_capacity() && key_vaild(&iter, &upper) {
                build.add(iter.key(), iter.value())?;
                iter.next()?;
            }

            let id = self.next_sst_id.fetch_add(1, Ordering::Relaxed);
            new_tables.push(Arc::new(build.build(
                id,
                None,
                sstable_file_path(&self.opt.dir, id),
            )?));
        }
        Ok(new_tables)
    }

    fn fill_table_l0(&self) -> Option<Task> {
        let this_tables = self.levels[0].read().clone();
        let next_tables = self.levels[1].read().clone();
        let mut task = Task {
            this_level_id: 0,
            next_level_id: 1,
            ..Default::default()
        };

        let mut this_compact_job = self.compact_job[0].lock();
        let mut next_compact_job = self.compact_job[1].lock();
        let mut job = HashSet::new();
        // l0 tables all must be not in compact job
        for table in this_tables.iter().rev() {
            if this_compact_job.contains(&table.id) {
                return None;
            }
            let mut family = vec![];
            for next_table in &next_tables {
                if next_table.smallest_key > table.biggest_key
                    || next_table.biggest_key < table.smallest_key
                {
                    continue;
                }
                if next_compact_job.contains(&next_table.id) {
                    return None;
                }
                family.push(next_table.id);
            }
            task.this_tables.push(table.clone());
            for id in family {
                job.insert(id);
            }
        }
        if task.this_tables.is_empty() {
            return None;
        }

        for table in next_tables {
            if job.contains(&table.id) {
                task.next_tables.push(table);
            }
        }

        for table in &task.this_tables {
            this_compact_job.insert(table.id);
        }
        for table in &task.next_tables {
            next_compact_job.insert(table.id);
        }

        Some(task)
    }

    fn create_task(&self, level: usize) -> Option<Task> {
        if level == 0 {
            return self.fill_table_l0();
        }
        self.fill_table(level)
    }

    fn fill_table(&self, level: usize) -> Option<Task> {
        assert_ne!(level, 0);

        let mut this_tables = self.levels[level].read().clone();
        this_tables.sort_by(|a, b| b.size.partial_cmp(&a.size).unwrap());
        let next_tables = self.levels[level + 1].read().clone();

        let mut task = Task {
            this_level_id: level,
            next_level_id: level + 1,
            ..Default::default()
        };

        let mut this_compact_job = self.compact_job[level].lock();
        let mut next_compact_job = self.compact_job[level + 1].lock();
        let mut job = HashSet::new();

        for table in &this_tables {
            if this_compact_job.contains(&table.id) {
                continue;
            }
            let mut choose = true;
            let mut family = vec![];
            for next_table in &next_tables {
                if next_table.smallest_key > table.biggest_key
                    || next_table.biggest_key < table.smallest_key
                {
                    continue;
                }
                if next_compact_job.contains(&next_table.id) {
                    choose = false;
                    break;
                }
                family.push(next_table.id);
            }
            if choose {
                task.this_tables.push(table.clone());
                for id in family {
                    job.insert(id);
                }
            }
        }
        if task.this_tables.is_empty() {
            return None;
        }
        for table in next_tables {
            if job.contains(&table.id) {
                task.next_tables.push(table);
            }
        }

        for table in &task.this_tables {
            this_compact_job.insert(table.id);
        }
        for table in &task.next_tables {
            next_compact_job.insert(table.id);
        }

        Some(task)
    }

    fn update_with_tables(&self, task: &Task, new_tables: &[Arc<SsTable>]) -> Result<()> {
        // l0 tired compaction
        if task.this_level_id == task.next_level_id {
            assert!(task.this_level_id == 0 && new_tables.len() == 1);
            let mut guard = self.levels[task.this_level_id].write();
            let mut new_level = new_tables.to_vec();
            // should check that this_table equals tables[..new_level.len()]?
            new_level.extend_from_slice(&guard[new_level.len()..]);
            *guard = new_level;
            return Ok(());
        }

        let delete = task
            .this_tables
            .iter()
            .chain(task.next_tables.iter())
            .map(|table| table.id)
            .collect::<HashSet<_>>();

        // TODO: WAL
        {
            let mut level = self.levels[task.next_level_id].write();
            let mut new_level = level
                .iter()
                .filter(|table| !delete.contains(&table.id))
                .cloned()
                .collect::<Vec<_>>();
            new_level.extend_from_slice(new_tables);
            new_level.sort_by(|a, b| a.smallest_key.partial_cmp(&b.smallest_key).unwrap());
            *level = new_level;
        }
        {
            let mut level = self.levels[task.this_level_id].write();
            let new_level = level
                .iter()
                .filter(|table| !delete.contains(&table.id))
                .cloned()
                .collect::<Vec<_>>();
            *level = new_level;
        }

        Ok(())
    }
}

fn build_change_set(task: &Task, new_tables: &[Arc<SsTable>]) -> ManifestChangeSet {
    let mut changes = vec![];

    for table in new_tables {
        changes.push(Change::create(table.id, task.next_level_id));
    }
    for table in &task.this_tables {
        changes.push(Change::delete(table.id));
    }
    for table in &task.next_tables {
        changes.push(Change::delete(table.id));
    }

    ManifestChangeSet { changes }
}

pub struct LevelController {
    inner: Arc<LevelsControllerInner>,
    block_cache: Arc<BlockCache>,
    opt: LsmOptions,
}

impl LevelController {
    pub fn open(opt: LsmOptions) -> Result<Self> {
        let block_cache = Arc::new(BlockCache::new(opt.block_cache_size));
        let inner = Arc::new(LevelsControllerInner::new(
            opt.clone(),
            block_cache.clone(),
        )?);
        Ok(Self {
            inner,
            block_cache,
            opt,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let tables = self.inner.levels[0].read().clone();
        if !tables.is_empty() {
            let mut iters = Vec::with_capacity(tables.len());
            for table in tables.iter().rev() {
                iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table.clone(),
                    key,
                )?));
            }

            let iter = MergeIterator::create(iters);
            if iter.is_valid() && iter.key() == key {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        for i in 1..self.opt.num_levels {
            let tables = self.inner.levels[i].read().clone();
            if tables.is_empty() {
                continue;
            }
            let idx = tables
                .partition_point(|table| table.smallest_key <= key)
                .saturating_sub(1);
            let iter = SsTableIterator::create_and_seek_to_key(tables[idx].clone(), key)?;
            if iter.is_valid() && iter.key() == key {
                if iter.value().is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }
        Ok(None)
    }

    pub fn start_compact(&self) {
        for i in 0..self.opt.compactor_num {
            self.run_compactor(i);
        }
    }

    fn run_compactor(&self, idx: usize) {
        let inner = self.inner.clone();
        let move_l0_to_front =
            |prios: Vec<TaskPriority>| match prios.iter().position(|x| x.level == 0) {
                Some(pos) => {
                    let mut result = vec![prios[pos].clone()];
                    result.extend_from_slice(&prios[..pos]);
                    result.extend_from_slice(&prios[pos + 1..]);
                    result
                }
                _ => prios,
            };
        spawn(move || {
            let run_once = || {
                let mut prios = inner.pick_compact_levels();
                if idx == 0 {
                    prios = move_l0_to_front(prios);
                }

                for p in prios {
                    if p.score < 1.0 {
                        break;
                    }

                    if let Err(err) = inner.do_compact(idx, p) {
                        error!("compactor {idx} error: {err}")
                        // TODO: Handle error.
                    }
                }
            };

            let ticker = tick(Duration::from_millis(50));

            info!("compactor {idx} start");

            loop {
                ticker.recv().unwrap();
                run_once();
            }
        });
    }

    pub fn l0_push_sstable(&self, builder: SsTableBuilder) -> Result<()> {
        let id = self.inner.next_sst_id.fetch_add(1, Ordering::Relaxed);
        let table = Arc::new(builder.build(
            id,
            Some(self.block_cache.clone()),
            sstable_file_path(&self.opt.dir, id),
        )?);
        self.inner.manifest.apply_change(&Change::create(id, 0))?;
        self.inner.levels[0].write().push(table);
        Ok(())
    }

    pub fn level_tables_sorted(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Vec<Arc<SsTable>> {
        let smallest_key = match lower {
            Bound::Included(key) => Some(key),
            Bound::Excluded(key) => Some(key),
            Bound::Unbounded => None,
        };
        let biggest_key = match upper {
            Bound::Included(key) => Some(key),
            Bound::Excluded(key) => Some(key),
            Bound::Unbounded => None,
        };
        let mut res = vec![];
        let mut filter_push = |tables: Vec<Arc<SsTable>>| {
            for table in tables {
                if let Some(key) = smallest_key {
                    if table.biggest_key < key {
                        continue;
                    }
                }
                if let Some(key) = biggest_key {
                    if table.smallest_key > key {
                        continue;
                    }
                }
                res.push(table);
            }
        };

        let mut l0_tables = self.inner.levels[0].read().clone();
        l0_tables.reverse();
        filter_push(l0_tables);

        for lock in &self.inner.levels[1..] {
            let tables = lock.read().clone();
            filter_push(tables);
        }
        res
    }
}

#[cfg(test)]
mod test;
