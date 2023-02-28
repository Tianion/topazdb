use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::{block::CompressOptions, lsm_storage::LsmStorage};

#[derive(Clone)]
pub struct LsmOptions {
    pub dir: PathBuf,
    pub flush_num: usize,             //  it must be 1 now. TODO: use lock
    pub compactor_num: usize,         // default 4
    pub subcompactor_num: usize,      // default 4
    pub block_cache_size: u64,        // default 2GB
    pub block_size: usize,            // default 32KB
    pub memtable_size: usize,         // default 256MB
    pub max_memtable_num: usize,      // default 5
    pub min_memtable_to_merge: usize, // default 2
    pub level0_file_num_compaction_trigger: usize, // default 5
    // l1 single table size
    pub target_file_size_base: usize,
    // l1 total size
    // advice: memtable_size*min_memtable_to_merge*level0_file_num_compaction_trigger
    pub max_bytes_for_level_base: usize, // default 256MB * 2 * 5
    pub max_bytes_for_level_multiplier: usize, // default 10
    pub num_levels: usize,               // default 6
    pub compress_option: CompressOptions,
}

impl Default for LsmOptions {
    fn default() -> Self {
        LsmOptions {
            dir: PathBuf::new(),
            flush_num: 1,
            compactor_num: 4,
            subcompactor_num: 4,
            block_cache_size: 2 * 1024 * 1024 * 1024,
            block_size: 4 * 1024,
            memtable_size: 256 * 1024 * 1024,
            max_memtable_num: 5,
            min_memtable_to_merge: 2,
            level0_file_num_compaction_trigger: 5,
            max_bytes_for_level_base: 256 * 1024 * 1024 * 2 * 5,
            target_file_size_base: 256 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10,
            num_levels: 6,
            compress_option: CompressOptions::Snappy,
        }
    }
}

impl LsmOptions {
    pub fn set_path(mut self, path: impl AsRef<Path>) -> Self {
        self.dir = path.as_ref().to_path_buf();
        self
    }

    pub fn open(self) -> Result<LsmStorage> {
        LsmStorage::open(self)
    }
}
