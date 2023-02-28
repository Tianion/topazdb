use anyhow::Result;
use std::sync::Arc;

use crate::table::SsTable;

#[derive(Clone, Debug)]
pub struct TaskPriority {
    pub level: usize,
    pub score: f64,
}

impl TaskPriority {
    pub fn new(level: usize, score: f64) -> Self {
        Self { level, score }
    }
}

#[derive(Default, Debug)]
pub struct Task {
    pub this_level_id: usize,
    pub next_level_id: usize,
    pub this_tables: Vec<Arc<SsTable>>,
    pub next_tables: Vec<Arc<SsTable>>,
}
