use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes, BytesMut};

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::block::SIZEOF_U16;
use crate::level::BlockCache;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    data: BytesMut,
    // current block builder
    block_builder: BlockBuilder,
    base_key: Bytes,
    block_size: usize,
}

const TABLE_CAPACITY: usize = 64 * 1024 * 1024;

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: vec![],
            data: BytesMut::new(),
            block_builder: BlockBuilder::new(block_size),
            base_key: Bytes::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.base_key.is_empty() {
            self.base_key = Bytes::copy_from_slice(key);
        }

        if self.block_builder.add(key, value) {
            return;
        }

        self.block_build();
        self.add(key, value)
    }

    fn block_build(&mut self) {
        let mut builder = BlockBuilder::new(self.block_size);
        std::mem::swap(&mut self.block_builder, &mut builder);

        let byte = builder.build().encode();
        let mut key = Bytes::new();
        std::mem::swap(&mut key, &mut self.base_key);

        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: key,
        };
        self.meta.push(meta);
        self.data.put(byte);
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.meta.len() * SIZEOF_U16
    }

    pub fn reach_capacity(&self) -> bool {
        self.estimated_size() >= TABLE_CAPACITY
    }

    /// Builds the SSTable and writes it to the given path. 
    pub fn build(
        mut self,
        id: u64,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.block_build();
        let offset = self.data.len();
        let mut buf = vec![];
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        self.data.put(buf.as_slice());
        self.data.put_u32(offset as u32);
        let file = FileObject::create(path.as_ref(), self.data.to_vec())?;
        let mut sst = SsTable {
            id,
            size: file.size(),
            file,
            block_metas: self.meta,
            block_meta_offset: offset,
            block_cache,
            smallest_key: Bytes::new(),
            biggest_key: Bytes::new(),
        };
        sst.init_samllest_biggest_key()?;
        Ok(sst)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
