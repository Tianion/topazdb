use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use super::StorageIterator;

// note: use '>' to compare priority, not fields
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

#[cfg(not(tarpaulin_include))]
impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

#[cfg(not(tarpaulin_include))]
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters = iters
            .into_iter()
            .filter(|x| x.is_valid())
            .enumerate()
            .map(|(id, b)| HeapWrapper(id, b))
            .collect::<BinaryHeap<_>>();
        // assert!(!iters.is_empty(), "iters is invalid");
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let key = self.key().to_vec();

        while let Some(mut inner) = self.iters.peek_mut() {
            if key != inner.1.key() {
                break;
            }
            if let e @ Err(_) = inner.1.next() {
                PeekMut::pop(inner);
                return e;
            }

            if !inner.1.is_valid() {
                PeekMut::pop(inner);
            }
        }

        let current = self.current.as_mut().unwrap();
        current.1.next()?;

        if !current.1.is_valid() {
            self.current = self.iters.pop();
            return Ok(());
        }

        if let Some(mut iter) = self.iters.peek_mut() {
            if *iter > *current {
                std::mem::swap(current, &mut iter);
            }
        }

        Ok(())
    }
}
