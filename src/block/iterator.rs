use std::sync::Arc;

use bytes::Buf;

use super::Block;

/// Iterates on a block.
#[derive(Debug)]
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }
    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    pub fn seek_to_last(&mut self) {
        self.seek_to(self.block.offsets.len() - 1);
    }

    fn seek_to(&mut self, idx: usize) {
        self.key.clear();
        self.value.clear();

        if idx >= self.block.offsets.len() {
            self.idx = self.block.offsets.len();
            return;
        }

        self.idx = idx;

        let offset = self.block.offsets[idx] as usize;
        let mut buf = &self.block.data[offset..];

        let klen = buf.get_u16() as usize;
        self.key = buf[..klen].to_vec();
        buf.advance(klen);

        let vlen = buf.get_u16() as usize;
        self.value = buf[..vlen].to_vec();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut left = 0;
        let mut right = self.block.offsets.len();

        while left < right {
            let mid = (right - left) / 2 + left;
            let offset = self.block.offsets[mid] as usize;
            let mut buf = &self.block.data[offset..];
            let klen = buf.get_u16() as usize;
            let mid_key = &buf[..klen];
            match mid_key.cmp(key) {
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Equal => return self.seek_to(mid),
            }
        }

        self.seek_to(left)
    }
}
