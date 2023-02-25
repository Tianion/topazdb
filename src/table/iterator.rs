use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator};

/// An iterator over the contents of an SSTable.
#[derive(Debug)]
pub struct SsTableIterator {
    block_iter: BlockIterator,
    table: Arc<SsTable>,
    idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_iter = Self::seek_to_first_inner(table.clone(), 0)?;
        Ok(Self {
            block_iter,
            table,
            idx: 0,
        })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        // if self.idx == 0 {
        //     self.block_iter.seek_to_first();
        //     return Ok(());
        // }

        self.idx = 0;
        self.block_iter = Self::seek_to_first_inner(self.table.clone(), 0)?;
        Ok(())
    }

    fn seek_to_first_inner(table: Arc<SsTable>, idx: usize) -> Result<BlockIterator> {
        let block = table.read_block_cached(idx)?;
        Ok(BlockIterator::create_and_seek_to_first(block))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (idx, block_iter) = Self::seek_to_key_inner(table.clone(), key)?;
        Ok(Self {
            block_iter,
            table,
            idx,
        })
    }

    fn seek_to_key_inner(table: Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
        let mut idx = table.find_block_idx(key);
        let block = table.read_block_cached(idx)?;
        let mut block_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !block_iter.is_valid() && idx + 1 < table.num_of_blocks() {
            idx += 1;
            block_iter = Self::seek_to_first_inner(table, idx)?;
        }

        Ok((idx, block_iter))
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (idx, block_iter) = Self::seek_to_key_inner(self.table.clone(), key)?;
        self.idx = idx;
        self.block_iter = block_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() && self.idx < self.table.num_of_blocks() - 1 {
            self.idx += 1;
            self.block_iter = Self::seek_to_first_inner(self.table.clone(), self.idx)?;
        }
        Ok(())
    }
}
