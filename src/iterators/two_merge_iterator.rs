use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    choose_a: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = Self {
            a,
            b,
            choose_a: false,
        };
        if iter.a.is_valid() {
            while iter.b.is_valid() && iter.b.key() == iter.a.key() {
                iter.b.next()?;
            }
        }
        iter.choose_a = iter.choose_a();
        Ok(iter)
    }

    fn choose_a(&self) -> bool {
        if !self.b.is_valid() {
            return true;
        }

        self.a.is_valid() && self.a.key() <= self.b.key()
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.choose_a {
            return self.a.key();
        }
        self.b.key()
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            return self.a.value();
        }
        self.b.value()
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.choose_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        if self.a.is_valid() {
            while self.b.is_valid() && self.b.key() == self.a.key() {
                self.b.next()?;
            }
        }
        self.choose_a = self.choose_a();
        Ok(())
    }
}
