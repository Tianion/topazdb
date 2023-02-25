mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use builder::Entry;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
#[derive(Debug)]
pub struct Block {
    data: Bytes,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let num_element = self.offsets.len();
        let mut buf =
            BytesMut::with_capacity(SIZEOF_U16 + SIZEOF_U16 * num_element + self.data.len());
        // |num_element|offsets|data| is easier to decode than |data|offsets|num_element|
        buf.put_u16(num_element as u16);
        for &offset in &self.offsets {
            buf.put_u16(offset);
        }
        buf.put(self.data.clone());
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        let mut buf = data;
        let num_element = buf.get_u16() as usize;

        let mut offsets = Vec::with_capacity(num_element);
        for _ in 0..num_element {
            offsets.push(buf.get_u16());
        }

        let data = Bytes::copy_from_slice(buf);
        Self { data, offsets }
    }
}

#[cfg(test)]
mod tests;
