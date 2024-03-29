#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod file_object;
mod iterator;

use anyhow::{anyhow, Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use file_object::FileObject;
pub use iterator::SsTableIterator;
use std::sync::Arc;

use crate::block::{Block, BlockIterator, SIZEOF_U16};
use crate::bloom::Bloom;
use crate::level::BlockCache;

const SIZEOF_U32: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    ///
    /// It is u32 in encoding
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // |offset first_key_len first_key|
        let size = block_meta
            .iter()
            .map(|meta| SIZEOF_U32 + SIZEOF_U16 + meta.first_key.len())
            .sum::<usize>();
        buf.reserve(size);

        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.clone());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas = vec![];
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let klen = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(klen);
            // buf.advance(klen);
            metas.push(BlockMeta { offset, first_key });
        }
        metas
    }
}

#[derive(Debug)]
pub struct SsTable {
    pub id: u64,
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
    block_cache: Option<Arc<BlockCache>>,
    pub smallest_key: Bytes,
    pub biggest_key: Bytes,
    pub size: usize,
    bloom: Option<Bloom>,
}

fn read_bloom(file: &FileObject) -> Result<(usize, Option<Bloom>)> {
    let size = file.size();
    let offset = file
        .read(size - SIZEOF_U32, SIZEOF_U32)?
        .as_slice()
        .get_u32() as usize;
    if size == offset + SIZEOF_U32 {
        return Ok((offset, None));
    }
    let bloom_buf = file.read(offset, size - SIZEOF_U32 - offset)?;
    let bloom = Bloom::decode(&bloom_buf);
    Ok((offset, Some(bloom)))
}

impl SsTable {
    /// Open SSTable from a file.
    pub fn open(id: u64, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let (offset, bloom) = read_bloom(&file)?;
        let meta_offset = file
            .read(offset - SIZEOF_U32, SIZEOF_U32)?
            .as_slice()
            .get_u32() as usize;
        let meta_buf = file.read(meta_offset, offset - SIZEOF_U32 - meta_offset)?;

        let mut table = Self {
            id,
            size: file.size(),
            file,
            block_metas: BlockMeta::decode_block_meta(meta_buf.as_slice()),
            block_meta_offset: meta_offset,
            block_cache,
            smallest_key: Bytes::new(),
            biggest_key: Bytes::new(),
            bloom,
        };
        table.init_samllest_biggest_key()?;
        Ok(table)
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if let Some(bloom) = self.bloom.as_ref() {
            return bloom.may_contain(xxhash_rust::xxh3::xxh3_64(key));
        }
        true
    }

    /// Save file when it drop
    pub(crate) fn mark_save(&self) {
        self.file.save()
    }

    // calculating accurate size is expensive
    pub fn overlap_size(&self, lower: &[u8], upper: &[u8]) -> usize {
        let l = self.find_block_idx(lower);
        let r = self.find_block_idx(upper);
        let loffset = self
            .block_metas
            .get(l)
            .map(|x| x.offset)
            .unwrap_or(self.block_meta_offset);
        let roffset = self
            .block_metas
            .get(r)
            .map(|x| x.offset)
            .unwrap_or(self.block_meta_offset);
        roffset - loffset
    }

    pub fn init_samllest_biggest_key(&mut self) -> Result<()> {
        self.smallest_key = self.block_metas[0].first_key.clone();
        let last_block = self.read_block(self.num_of_blocks() - 1)?;
        let mut iter = BlockIterator::create_and_seek_to_first(last_block);
        iter.seek_to_last();
        assert!(iter.is_valid());
        self.biggest_key = Bytes::copy_from_slice(iter.key());
        Ok(())
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas[block_idx].offset;
        let end = self
            .block_metas
            .get(block_idx + 1)
            .map(|x| x.offset)
            .unwrap_or(self.block_meta_offset);
        let buf = self.file.read(offset, end - offset)?;
        let block = Block::decode(&buf)?;
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache.
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!(e))
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.block_metas
            .partition_point(|x| x.first_key <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
