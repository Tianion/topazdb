use super::{Block, SIZEOF_U16};
use bytes::{BufMut, Bytes, BytesMut};

/// Builds a block.
#[derive(Debug)]
pub struct BlockBuilder {
    target_size: usize,
    data: BytesMut,
    offsets: Vec<u16>,
    size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(target_size: usize) -> Self {
        Self {
            target_size,
            data: BytesMut::new(),
            offsets: Vec::new(),
            size: 0,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        let entry = Entry::new(key, value);
        let encode_len = entry.encode_len();

        if encode_len + self.size + SIZEOF_U16 > self.target_size {
            return false;
        }

        self.data.put(entry.encode());
        self.offsets.push(self.size as u16);
        self.size += encode_len;

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        assert!(!self.is_empty(), "block must be not empty");

        Block {
            data: self.data.freeze(),
            offsets: self.offsets,
        }
    }
}

pub struct Entry {
    key: Bytes,
    value: Bytes,
}

impl Entry {
    pub fn new(key: &[u8], value: &[u8]) -> Self {
        Entry {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        }
    }

    pub fn encode(self) -> Bytes {
        let klen = self.key.len();
        let vlen = self.value.len();
        let mut buf = BytesMut::with_capacity(SIZEOF_U16 + klen + SIZEOF_U16 + vlen);
        buf.put_u16(klen as u16);
        buf.put(self.key);
        buf.put_u16(vlen as u16);
        buf.put(self.value);
        buf.freeze()
    }

    pub fn encode_len(&self) -> usize {
        SIZEOF_U16 + self.key.len() + SIZEOF_U16 + self.value.len()
    }
}
