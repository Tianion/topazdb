use std::{collections::BTreeSet, ops::Bound};

use bytes::Bytes;

use super::task::Task;

#[derive(Debug)]
pub struct RwsSlice {
    pub ranges: Vec<RangeWithSize>,
    pub total_size: usize,
}

impl RwsSlice {
    pub fn split(&self, mean: usize) -> Vec<(Bound<Bytes>, Bound<Bytes>)> {
        if self.total_size == 0 {
            return vec![];
        }
        let mut res = vec![];
        let mut acc_size = 0;
        let mut first_key = Bytes::new();
        for rws in &self.ranges {
            if acc_size == 0 {
                first_key = rws.smallest_key.clone();
            }
            acc_size += rws.size;
            if acc_size >= mean {
                res.push((
                    Bound::Included(first_key.clone()),
                    Bound::Excluded(rws.biggest_key.clone()),
                ));
                acc_size = 0;
            }
        }
        if acc_size != 0 {
            res.push((
                Bound::Included(first_key),
                Bound::Included(self.ranges.last().unwrap().biggest_key.clone()),
            ));
        } else if let Bound::Excluded(key) = res.last().unwrap().1.clone() {
            res.last_mut().unwrap().1 = Bound::Included(key);
        }
        res
    }

    pub fn create(task: &Task) -> RwsSlice {
        let mut set = BTreeSet::new();
        if task.this_level_id == 0 {
            for table in &task.this_tables {
                set.insert(table.smallest_key.to_vec());
                set.insert(table.biggest_key.to_vec());
            }
        } else {
            for table in &task.this_tables {
                set.insert(table.smallest_key.to_vec());
            }
        }
        for table in &task.next_tables {
            set.insert(table.smallest_key.to_vec());
        }
        if !task.next_tables.is_empty() {
            set.insert(task.next_tables.last().unwrap().biggest_key.to_vec());
        }

        let mut ranges = Vec::with_capacity(set.len());
        let mut total_size = 0;
        let mut iter = set.iter();
        iter.next();
        for (lower, upper) in set.iter().zip(iter) {
            let lower = Bytes::copy_from_slice(lower);
            let upper = Bytes::copy_from_slice(upper);
            let mut size = 0;
            for table in &task.this_tables {
                if table.smallest_key <= upper || table.biggest_key >= lower {
                    size += table.overlap_size(&lower, &upper);
                }
            }
            for table in &task.next_tables {
                if table.smallest_key <= upper || table.biggest_key >= lower {
                    size += table.overlap_size(&lower, &upper);
                }
            }
            total_size += size;
            ranges.push(RangeWithSize {
                smallest_key: lower,
                biggest_key: upper,
                size,
            });
        }
        RwsSlice { ranges, total_size }
    }
}

#[derive(Debug)]
pub struct RangeWithSize {
    pub smallest_key: Bytes,
    pub biggest_key: Bytes,
    pub size: usize,
}
