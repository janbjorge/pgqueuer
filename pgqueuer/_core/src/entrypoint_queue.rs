use crate::types::{HeapEntry, JobRow, JobStatus};
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Per-entrypoint queue storage with priority heap and job tracking.
pub struct EntrypointQueue {
    /// All jobs for this entrypoint: job_id -> JobRow
    pub jobs: HashMap<i64, JobRow>,

    /// Job IDs currently in queued state
    pub queued_ids: HashSet<i64>,

    /// Binary heap of queued jobs: HeapEntry(priority, job_id)
    /// Uses max-heap to pop highest priority first
    pub queued_heap: BinaryHeap<HeapEntry>,

    /// Job IDs currently in picked state
    pub picked: HashSet<i64>,
}

impl EntrypointQueue {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            queued_ids: HashSet::new(),
            queued_heap: BinaryHeap::new(),
            picked: HashSet::new(),
        }
    }

    /// Pop up to `limit` ready candidates from the heap in priority order.
    /// Returns list of (priority, job_id). Stale entries (removed jobs) are skipped.
    /// Not-yet-ready jobs are pushed back.
    pub fn pop_queued_candidates(&mut self, now_us: i64, limit: usize) -> Vec<(i32, i64)> {
        let mut result = Vec::new();
        let mut deferred = Vec::new();

        while let Some(HeapEntry(priority, jid)) = self.queued_heap.pop() {
            if !self.queued_ids.contains(&jid) {
                continue;
            }

            if let Some(job) = self.jobs.get(&jid) {
                if job.execute_after_us > now_us {
                    deferred.push(HeapEntry(priority, jid));
                    continue;
                }
            }

            result.push((priority, jid));
            if result.len() >= limit {
                break;
            }
        }

        for entry in deferred {
            self.queued_heap.push(entry);
        }

        result
    }

    pub fn clear(&mut self) {
        self.jobs.clear();
        self.queued_ids.clear();
        self.queued_heap.clear();
        self.picked.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}
