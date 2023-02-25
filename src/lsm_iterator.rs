use std::ops::Bound;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;
pub struct LsmIterator {
    inner: LsmIteratorInner,
    end: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub fn new(inner: LsmIteratorInner, end: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: inner.is_valid(),
            inner,
            end,
        };

        while iter.is_valid && iter.value().is_empty() {
            iter.next_inner()?;
        }
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        if !self.is_valid {
            return Ok(());
        }

        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }

        match &self.end {
            Bound::Included(key) if self.inner.key() > key => self.is_valid = false,
            Bound::Excluded(key) if self.inner.key() >= key => self.is_valid = false,
            _ => {}
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        while self.is_valid && self.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        self.iter.next()
    }
}
