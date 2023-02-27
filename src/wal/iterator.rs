use bytes::{Buf, Bytes};

pub struct WalIterator {
    data: Bytes,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl WalIterator {
    pub fn create(buf: &[u8]) -> Self {
        let mut iter = WalIterator {
            data: Bytes::copy_from_slice(buf),
            key: vec![],
            value: vec![],
        };
        iter.next();
        iter
    }

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

    pub fn next(&mut self) {
        if self.data.is_empty() {
            self.key.clear();
            return;
        }
        let klen = self.data.get_u16() as usize;
        self.key = self.data[..klen].to_vec();
        self.data.advance(klen);
        let vlen = self.data.get_u16() as usize;
        self.value = self.data[..vlen].to_vec();
        self.data.advance(vlen);
    }
}
