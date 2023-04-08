mod builder;
mod compress;
mod iterator;

use anyhow::Ok;
use anyhow::Result;
pub use builder::BlockBuilder;
pub use builder::Entry;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

use crate::checksum;

pub use self::compress::CompressOptions;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
#[derive(Debug)]
pub struct Block {
    data: Bytes,
    offsets: Vec<u16>,
}

impl Block {
    pub fn uncompress_size(&self) -> usize {
        SIZEOF_U16 + SIZEOF_U16 * self.offsets.len() + self.data.len()
    }

    pub fn encode(&self, compress_option: CompressOptions) -> Result<Bytes> {
        let num_element = self.offsets.len();
        let mut buf = BytesMut::with_capacity(self.uncompress_size());
        // |num_element|offsets|data| is easier to decode than |data|offsets|num_element|
        buf.put_u16(num_element as u16);
        for &offset in &self.offsets {
            buf.put_u16(offset);
        }
        buf.put(self.data.clone());

        let checksum = checksum::calculate_checksum(&buf);
        buf.put_u32(checksum);
        compress::encode(&buf, compress_option)
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut data = compress::decode(data)?;

        let mut buf = data.split_to(data.len() - 4);

        let checksum = data.get_u32();
        checksum::verify_checksum(&buf, checksum)?;

        let num_element = buf.get_u16() as usize;

        let mut offsets = Vec::with_capacity(num_element);
        for _ in 0..num_element {
            offsets.push(buf.get_u16());
        }

        Ok(Self {
            data: buf.freeze(),
            offsets,
        })
    }
}

#[cfg(test)]
mod tests;
