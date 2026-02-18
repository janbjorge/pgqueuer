use pyo3::prelude::*;
use std::collections::{HashMap, HashSet};

use crate::entrypoint_queue::EntrypointQueue;
use crate::types::{HeapEntry, JobRow, JobStatus, LogRow};

/// In-memory core implementing hot-path job queue logic.
#[pyclass]
pub struct InMemoryCore {
    /// Map of entrypoint names to their queues
    queues: HashMap<String, EntrypointQueue>,

    /// Sequence counter for job IDs
    job_seq: i64,

    /// All log entries
    log: Vec<LogRow>,

    /// Deduplication: dedupe_key -> job_id
    dedupe: HashMap<String, i64>,

    /// Reverse deduplication: job_id -> dedupe_key
    dedupe_reverse: HashMap<i64, String>,

    /// Picked job counts per entrypoint
    picked_count: HashMap<String, i32>,

    /// Total picked jobs
    total_picked: i32,

    /// Log status index: job_id -> last log entry index for O(1) status lookup
    log_status_idx: HashMap<i64, usize>,
}

#[pymethods]
impl InMemoryCore {
    #[new]
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
            job_seq: 1,
            log: Vec::new(),
            dedupe: HashMap::new(),
            dedupe_reverse: HashMap::new(),
            picked_count: HashMap::new(),
            total_picked: 0,
            log_status_idx: HashMap::new(),
        }
    }

    fn get_queue(&mut self, entrypoint: &str) -> &mut EntrypointQueue {
        self.queues
            .entry(entrypoint.to_string())
            .or_insert_with(EntrypointQueue::new)
    }

    /// Enqueue a batch of jobs.
    /// Returns list of job IDs.
    #[pyo3(signature = (entrypoints, payloads, priorities, execute_after_us, dedupe_keys, headers, now_us))]
    pub fn enqueue_batch(
        &mut self,
        entrypoints: Vec<String>,
        payloads: Vec<Option<Vec<u8>>>,
        priorities: Vec<i32>,
        execute_after_us: Vec<i64>,
        dedupe_keys: Vec<Option<String>>,
        headers: Vec<Option<String>>,
        now_us: i64,
    ) -> PyResult<Vec<i64>> {
        let n = entrypoints.len();

        // Check deduplication constraints
        for dk in &dedupe_keys {
            if let Some(key) = dk {
                if self.dedupe.contains_key(key) {
                    return Err(pyo3::exceptions::ValueError::new_err(format!(
                        "Duplicate job error: {:?}",
                        dedupe_keys
                    )));
                }
            }
        }

        let mut ids = Vec::with_capacity(n);
        let mut queue_cache: HashMap<String, usize> = HashMap::new();

        for i in 0..n {
            let jid = self.job_seq;
            self.job_seq += 1;

            let ep = entrypoints[i].clone();
            let priority = priorities[i];
            let payload = payloads[i].clone();
            let execute_after = now_us + execute_after_us[i];
            let headers_val = headers[i].clone();

            let job = JobRow {
                id: jid,
                priority,
                created_us: now_us,
                updated_us: now_us,
                heartbeat_us: now_us,
                execute_after_us: execute_after,
                status: JobStatus::Queued,
                entrypoint: ep.clone(),
                payload,
                queue_manager_id: None,
                headers: headers_val,
                dedupe_key: dedupe_keys[i].clone(),
            };

            let q = self.get_queue(&ep);
            q.jobs.insert(jid, job);
            q.queued_ids.insert(jid);
            q.queued_heap.push(HeapEntry(priority, jid));

            if let Some(dk) = &dedupe_keys[i] {
                self.dedupe.insert(dk.clone(), jid);
                self.dedupe_reverse.insert(jid, dk.clone());
            }

            // Log entry
            self.log.push(LogRow {
                created_us: now_us,
                job_id: jid,
                status: JobStatus::Queued,
                priority,
                entrypoint: ep,
                traceback: None,
                aggregated: false,
            });
            self.log_status_idx.insert(jid, self.log.len() - 1);

            ids.push(jid);
        }

        Ok(ids)
    }

    /// Dequeue a batch of jobs according to concurrency constraints.
    /// Returns list of tuples: (id, priority, created_us, updated_us, heartbeat_us, execute_after_us, entrypoint, payload, queue_manager_id, headers)
    #[pyo3(signature = (batch_size, ep_names, ep_retry_after_us, ep_serialized, ep_concurrency_limits, queue_manager_id_bytes, global_concurrency_limit, now_us))]
    pub fn dequeue_batch(
        &mut self,
        batch_size: usize,
        ep_names: Vec<String>,
        ep_retry_after_us: Vec<i64>,
        ep_serialized: Vec<bool>,
        ep_concurrency_limits: Vec<i32>,
        queue_manager_id_bytes: Vec<u8>,
        global_concurrency_limit: Option<i32>,
        now_us: i64,
    ) -> PyResult<
        Vec<(
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
        )>,
    > {
        if ep_names.len() != ep_retry_after_us.len()
            || ep_names.len() != ep_serialized.len()
            || ep_names.len() != ep_concurrency_limits.len()
        {
            return Err(pyo3::exceptions::ValueError::new_err(
                "Entrypoint parameter arrays must have equal length",
            ));
        }

        let mut picked_by_ep = self.picked_count.clone();
        let mut total_picked = self.total_picked;

        // Collect candidates
        let mut candidates: Vec<(i32, i64, String)> = Vec::new();

        for (ep_idx, ep_name) in ep_names.iter().enumerate() {
            if let Some(q) = self.queues.get_mut(ep_name) {
                let retry_after_us = ep_retry_after_us[ep_idx];
                let serialized = ep_serialized[ep_idx];
                let concurrency_limit = ep_concurrency_limits[ep_idx];

                // Check queued candidates
                if !(serialized && picked_by_ep.get(ep_name).unwrap_or(&0) > &0)
                    && !(concurrency_limit > 0
                        && picked_by_ep.get(ep_name).unwrap_or(&0) >= &concurrency_limit)
                    && !(global_concurrency_limit.is_some()
                        && total_picked >= global_concurrency_limit.unwrap())
                {
                    for (priority, jid) in q.pop_queued_candidates(now_us, batch_size) {
                        candidates.push((priority, jid, ep_name.clone()));
                    }
                }

                // Check picked jobs for retries
                if retry_after_us > 0 {
                    let retry_cutoff = now_us - retry_after_us;
                    let picked_ids: Vec<i64> = q.picked.iter().cloned().collect();
                    for jid in picked_ids {
                        if let Some(job) = q.jobs.get(&jid) {
                            if job.heartbeat_us < retry_cutoff && job.execute_after_us <= now_us {
                                candidates.push((job.priority, jid, ep_name.clone()));
                            }
                        }
                    }
                }
            }
        }

        // Sort by priority (higher priority first)
        candidates.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));

        // Parse queue_manager_id UUID from bytes
        let queue_manager_id_arr = if queue_manager_id_bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&queue_manager_id_bytes);
            Some(arr)
        } else {
            None
        };

        let mut result = Vec::new();

        for (_, jid, ep_name) in candidates.iter().take(batch_size) {
            if let Some(q) = self.queues.get_mut(ep_name) {
                if let Some(job) = q.jobs.get_mut(jid) {
                    job.status = JobStatus::Picked;
                    job.updated_us = now_us;
                    job.heartbeat_us = now_us;
                    job.queue_manager_id = queue_manager_id_arr;

                    let tuple = job.to_dequeue_tuple();
                    result.push(tuple);

                    if q.queued_ids.remove(jid) {
                        q.picked.insert(*jid);
                        *picked_by_ep.entry(ep_name.clone()).or_insert(0) += 1;
                        total_picked += 1;
                    }

                    // Log entry
                    self.log.push(LogRow {
                        created_us: now_us,
                        job_id: *jid,
                        status: JobStatus::Picked,
                        priority: job.priority,
                        entrypoint: ep_name.clone(),
                        traceback: None,
                        aggregated: false,
                    });
                    self.log_status_idx.insert(*jid, self.log.len() - 1);
                }
            }
        }

        self.picked_count = picked_by_ep;
        self.total_picked = total_picked;

        Ok(result)
    }

    /// Log jobs with their final statuses and tracebacks.
    #[pyo3(signature = (job_ids, statuses, tracebacks, now_us))]
    pub fn log_jobs(
        &mut self,
        job_ids: Vec<i64>,
        statuses: Vec<String>,
        tracebacks: Vec<Option<String>>,
        now_us: i64,
    ) -> PyResult<()> {
        for i in 0..job_ids.len() {
            let jid = job_ids[i];
            let status_str = &statuses[i];
            let traceback = tracebacks[i].clone();

            let status = JobStatus::from_str(status_str).ok_or_else(|| {
                pyo3::exceptions::ValueError::new_err(format!("Invalid status: {}", status_str))
            })?;

            // Get priority and entrypoint before removing the job
            let (priority, entrypoint) = {
                let mut found = (0i32, String::new());
                for q in self.queues.values() {
                    if let Some(job) = q.jobs.get(&jid) {
                        found = (job.priority, job.entrypoint.clone());
                        break;
                    }
                }
                found
            };

            self.remove_job(jid);

            self.log.push(LogRow {
                created_us: now_us,
                job_id: jid,
                status,
                priority,
                entrypoint,
                traceback,
                aggregated: false,
            });
            self.log_status_idx.insert(jid, self.log.len() - 1);
        }

        Ok(())
    }

    /// Update heartbeat for a list of job IDs.
    #[pyo3(signature = (job_ids, now_us))]
    pub fn update_heartbeat(&mut self, job_ids: Vec<i64>, now_us: i64) -> PyResult<()> {
        let unique_ids: HashSet<i64> = job_ids.into_iter().collect();

        for jid in unique_ids {
            for q in self.queues.values_mut() {
                if let Some(job) = q.jobs.get_mut(&jid) {
                    job.heartbeat_us = now_us;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Mark jobs as cancelled.
    #[pyo3(signature = (job_ids, now_us))]
    pub fn mark_cancelled(&mut self, job_ids: Vec<i64>, now_us: i64) -> PyResult<()> {
        for jid in job_ids {
            // Get priority and entrypoint from last log entry for this job
            let (priority, entrypoint) = {
                if let Some(idx) = self.log_status_idx.get(&jid) {
                    self.log
                        .get(*idx)
                        .map(|lr| (lr.priority, lr.entrypoint.clone()))
                        .unwrap_or((0, String::new()))
                } else {
                    (0, String::new())
                }
            };

            self.remove_job(jid);

            self.log.push(LogRow {
                created_us: now_us,
                job_id: jid,
                status: JobStatus::Canceled,
                priority,
                entrypoint,
                traceback: None,
                aggregated: false,
            });
            self.log_status_idx.insert(jid, self.log.len() - 1);
        }

        Ok(())
    }

    /// Clear queue(s).
    #[pyo3(signature = (entrypoints, now_us))]
    pub fn clear_queue(&mut self, entrypoints: Option<Vec<String>>, now_us: i64) -> PyResult<()> {
        if let Some(eps) = entrypoints {
            for ep in eps {
                if let Some(q) = self.queues.get_mut(&ep) {
                    for (jid, job) in q.jobs.iter() {
                        let dk = self.dedupe_reverse.remove(jid);
                        if let Some(key) = dk {
                            self.dedupe.remove(&key);
                        }

                        self.log.push(LogRow {
                            created_us: now_us,
                            job_id: *jid,
                            status: JobStatus::Deleted,
                            priority: job.priority,
                            entrypoint: ep.clone(),
                            traceback: None,
                            aggregated: false,
                        });
                        self.log_status_idx.insert(*jid, self.log.len() - 1);
                    }

                    let picked_count = q.picked.len() as i32;
                    if picked_count > 0 {
                        self.picked_count.insert(ep.clone(), 0);
                        self.total_picked -= picked_count;
                    }

                    q.clear();
                }
            }
        } else {
            self.queues.clear();
            self.dedupe.clear();
            self.dedupe_reverse.clear();
            self.picked_count.clear();
            self.total_picked = 0;
        }

        Ok(())
    }

    /// Get queue size statistics: (entrypoint, priority, status, count)
    #[pyo3(signature = ())]
    pub fn queue_size(&self) -> PyResult<Vec<(String, i32, String, i32)>> {
        let mut counts: HashMap<(String, i32, String), i32> = HashMap::new();

        for (ep_name, q) in &self.queues {
            for (_, job) in &q.jobs {
                let key = (
                    ep_name.clone(),
                    job.priority,
                    job.status.to_str().to_string(),
                );
                *counts.entry(key).or_insert(0) += 1;
            }
        }

        let result: Vec<_> = counts
            .into_iter()
            .map(|((ep, pr, st), c)| (ep, pr, st, c))
            .collect();

        Ok(result)
    }

    /// Count total queued work for given entrypoints.
    #[pyo3(signature = (entrypoints))]
    pub fn queued_work(&self, entrypoints: Vec<String>) -> PyResult<i32> {
        let ep_set: HashSet<_> = entrypoints.into_iter().collect();
        let mut total = 0;

        for ep in ep_set {
            if let Some(q) = self.queues.get(&ep) {
                total += q.queued_ids.len() as i32;
            }
        }

        Ok(total)
    }

    /// Get full queue log: (created_us, job_id, status_str, priority, entrypoint, traceback, aggregated)
    #[pyo3(signature = ())]
    pub fn queue_log(
        &self,
    ) -> PyResult<Vec<(i64, i64, String, i32, String, Option<String>, bool)>> {
        let result: Vec<_> = self
            .log
            .iter()
            .map(|lr| {
                (
                    lr.created_us,
                    lr.job_id,
                    lr.status.to_str().to_string(),
                    lr.priority,
                    lr.entrypoint.clone(),
                    lr.traceback.clone(),
                    lr.aggregated,
                )
            })
            .collect();

        Ok(result)
    }

    /// Get job status by ID (O(1) lookup): [(job_id, status_str)]
    #[pyo3(signature = (job_ids))]
    pub fn job_status(&self, job_ids: Vec<i64>) -> PyResult<Vec<(i64, String)>> {
        let mut result = Vec::new();

        for jid in job_ids {
            if let Some(idx) = self.log_status_idx.get(&jid) {
                if let Some(lr) = self.log.get(*idx) {
                    result.push((jid, lr.status.to_str().to_string()));
                }
            }
        }

        Ok(result)
    }

    /// Get log statistics: (entrypoint, priority, status_str, count, created_us_rounded)
    /// Rounds created_us to remove microseconds (to match Python behavior).
    #[pyo3(signature = (tail, since_us))]
    pub fn log_statistics(
        &self,
        tail: Option<usize>,
        since_us: Option<i64>,
    ) -> PyResult<Vec<(String, i32, String, i32, i64)>> {
        let mut counts: HashMap<(String, i32, String, i64), i32> = HashMap::new();

        for lr in &self.log {
            let created_rounded = (lr.created_us / 1_000_000) * 1_000_000;

            if let Some(cutoff) = since_us {
                if lr.created_us < cutoff {
                    continue;
                }
            }

            let key = (
                lr.entrypoint.clone(),
                lr.priority,
                lr.status.to_str().to_string(),
                created_rounded,
            );
            *counts.entry(key).or_insert(0) += 1;
        }

        let mut stats: Vec<_> = counts
            .into_iter()
            .map(|((ep, pr, st, ts), c)| (ep, pr, st, c, ts))
            .collect();

        // Sort by timestamp for consistent ordering
        stats.sort_by_key(|s| s.4);

        if let Some(t) = tail {
            stats = stats.into_iter().rev().take(t).collect();
            stats.reverse();
        }

        Ok(stats)
    }

    // Helper: remove a job from all structures
    fn remove_job(&mut self, jid: i64) {
        let mut removed_ep = None;

        for (ep_name, q) in self.queues.iter_mut() {
            if let Some(job) = q.jobs.remove(&jid) {
                let dk = self.dedupe_reverse.remove(&jid);
                if let Some(key) = dk {
                    self.dedupe.remove(&key);
                }

                if q.picked.remove(&jid) {
                    if let Some(count) = self.picked_count.get_mut(ep_name) {
                        *count -= 1;
                        if *count == 0 {
                            self.picked_count.remove(ep_name);
                        }
                    }
                    self.total_picked -= 1;
                } else {
                    q.queued_ids.remove(&jid);
                }

                removed_ep = Some(ep_name.clone());
                break;
            }
        }

        if removed_ep.is_none() {
            // Job not found in queues, but still update log status index
            self.log_status_idx.remove(&jid);
        }
    }
}
