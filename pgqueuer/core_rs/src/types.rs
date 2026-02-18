use std::cmp::Ordering;

/// Job status enum with discriminant values matching Python constants.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobStatus {
    Queued = 0,
    Picked = 1,
    Successful = 2,
    Canceled = 3,
    Deleted = 4,
    Exception = 5,
}

impl JobStatus {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(JobStatus::Queued),
            1 => Some(JobStatus::Picked),
            2 => Some(JobStatus::Successful),
            3 => Some(JobStatus::Canceled),
            4 => Some(JobStatus::Deleted),
            5 => Some(JobStatus::Exception),
            _ => None,
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Picked => "picked",
            JobStatus::Successful => "successful",
            JobStatus::Canceled => "canceled",
            JobStatus::Deleted => "deleted",
            JobStatus::Exception => "exception",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(JobStatus::Queued),
            "picked" => Some(JobStatus::Picked),
            "successful" => Some(JobStatus::Successful),
            "canceled" => Some(JobStatus::Canceled),
            "deleted" => Some(JobStatus::Deleted),
            "exception" => Some(JobStatus::Exception),
            _ => None,
        }
    }
}

/// Compact row representation of a job in memory.
#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: i64,
    pub priority: i32,
    pub created_us: i64,
    pub updated_us: i64,
    pub heartbeat_us: i64,
    pub execute_after_us: i64,
    pub status: JobStatus,
    pub entrypoint: String,
    pub payload: Option<Vec<u8>>,
    pub queue_manager_id: Option<[u8; 16]>,
    pub headers: Option<String>,
    pub dedupe_key: Option<String>,
}

impl JobRow {
    pub fn to_dequeue_tuple(
        &self,
    ) -> (
        i64,
        i32,
        i64,
        i64,
        i64,
        i64,
        String,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<String>,
    ) {
        let queue_manager_id_bytes = self.queue_manager_id.as_ref().map(|b| b.to_vec());
        (
            self.id,
            self.priority,
            self.created_us,
            self.updated_us,
            self.heartbeat_us,
            self.execute_after_us,
            self.entrypoint.clone(),
            self.payload.clone(),
            queue_manager_id_bytes,
            self.headers.clone(),
        )
    }
}

/// Compact log row record.
#[derive(Debug, Clone)]
pub struct LogRow {
    pub created_us: i64,
    pub job_id: i64,
    pub status: JobStatus,
    pub priority: i32,
    pub entrypoint: String,
    pub traceback: Option<String>,
    pub aggregated: bool,
}

/// Max-heap entry for BinaryHeap: higher priority pops first.
/// Since Rust's BinaryHeap is a max-heap, we store (-priority, job_id).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct HeapEntry(pub i32, pub i64);

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse priority comparison (higher priority comes first)
        match other.0.cmp(&self.0) {
            Ordering::Equal => other.1.cmp(&self.1), // for determinism
            ord => ord,
        }
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
