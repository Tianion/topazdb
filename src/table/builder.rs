use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes, BytesMut};

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;

use crate::block::SIZEOF_U16;
use crate::bloom::Bloom;
use crate::level::BlockCache;
use crate::opt::LsmOptions;

/// Builds an SSTable from key-value pairs.
#[derive(Debug)]
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    data: BytesMut,
    // current block builder
    block_builder: BlockBuilder,
    base_key: Bytes,
    pub opt: LsmOptions,
    key_hashs: Option<Vec<u64>>,
}

const TABLE_CAPACITY: usize = 64 * 1024 * 1024;

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(opt: LsmOptions) -> Self {
        let key_hashs = if opt.false_positive_rate.is_sign_positive() {
            Some(Vec::new())
        } else {
            None
        };

        Self {
            meta: vec![],
            data: BytesMut::new(),
            block_builder: BlockBuilder::new(opt.block_size),
            base_key: Bytes::new(),
            opt,
            key_hashs,
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.base_key.is_empty() {
            self.base_key = Bytes::copy_from_slice(key);
        }

        if !self.block_builder.add(key, value) {
            self.block_build()?;
            return self.add(key, value);
        }

        if let Some(hs) = self.key_hashs.as_mut() {
            hs.push(xxhash_rust::xxh3::xxh3_64(key));
        }

        Ok(())
    }

    fn block_build(&mut self) -> Result<()> {
        let mut builder = BlockBuilder::new(self.opt.block_size);
        std::mem::swap(&mut self.block_builder, &mut builder);

        let byte = builder.build().encode(self.opt.compress_option)?;
        let mut key = Bytes::new();
        std::mem::swap(&mut key, &mut self.base_key);

        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: key,
        };
        self.meta.push(meta);
        self.data.put(byte);
        Ok(())
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
        self.block_build()?;
        let offset = self.data.len();
        let mut buf = vec![];
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        self.data.put(buf.as_slice());
        self.data.put_u32(offset as u32);

        let mut bloom = None;
        if self.opt.false_positive_rate.is_sign_positive() {
            bloom = Some(self.build_bloom());
        }

        let file = FileObject::create(path.as_ref(), &self.data, self.opt.o_direct)?;
        let mut sst = SsTable {
            id,
            size: file.size(),
            file,
            block_metas: self.meta,
            block_meta_offset: offset,
            block_cache,
            smallest_key: Bytes::new(),
            biggest_key: Bytes::new(),
            bloom,
        };

        sst.init_samllest_biggest_key()?;
        Ok(sst)
    }

    fn build_bloom(&mut self) -> Bloom {
        let offset = self.data.len();
        let bloom = Bloom::from_keys(
            self.key_hashs.as_ref().expect("expect key hashs"),
            self.opt.false_positive_rate,
        );
        self.data.put(bloom.encode());
        self.data.put_u32(offset as u32);
        bloom
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
