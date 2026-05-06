use crate::shared::{DedupKey, Job, JobStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct DedupEntry {
    pub job_id: Uuid,
}

#[derive(Default)]
pub struct RegistryState {
    pub dedup: HashMap<DedupKey, DedupEntry>,
    pub jobs: HashMap<Uuid, Job>,
}

impl RegistryState {
    pub fn new() -> Self {
        RegistryState {
            dedup: HashMap::new(),
            jobs: HashMap::new(),
        }
    }

    /// Atomic get-or-create operation under lock.
    /// Returns `(job_id, was_deduplicated, current_status)`. On a dedup hit
    /// the status is the existing job's snapshot at this moment (not always
    /// `Queued`). On a miss it is `JobStatus::Queued` (the freshly inserted
    /// job).
    pub fn get_or_create(&mut self, key: DedupKey, profile: String) -> (Uuid, bool, JobStatus) {
        if let Some(entry) = self.dedup.get(&key) {
            let existing_id = entry.job_id;
            let status = self
                .jobs
                .get(&existing_id)
                .map(|j| j.status)
                .unwrap_or(JobStatus::Queued);
            return (existing_id, true, status);
        }

        let job_id = Uuid::new_v4();
        let job = Job::new(job_id, profile, key.content_hash.clone());
        self.jobs.insert(job_id, job);
        self.dedup.insert(key, DedupEntry { job_id });

        (job_id, false, JobStatus::Queued)
    }

    /// Roll back a freshly created (key, job_id) when staging or enqueue fails
    /// before the job becomes effective. Idempotent. The dedup entry is only
    /// removed if it still points at the same job_id (another canonical job
    /// may have replaced it under contention; do not clobber that).
    pub fn remove(&mut self, key: &DedupKey, job_id: &Uuid) {
        if matches!(self.dedup.get(key), Some(entry) if entry.job_id == *job_id) {
            self.dedup.remove(key);
        }
        self.jobs.remove(job_id);
    }

    pub fn get_job(&self, job_id: &Uuid) -> Option<&Job> {
        self.jobs.get(job_id)
    }

    pub fn get_jobs(&self) -> Vec<&Job> {
        self.jobs.values().collect()
    }
}

pub type RegistryArc = Arc<Mutex<RegistryState>>;

pub fn create_registry() -> RegistryArc {
    Arc::new(Mutex::new(RegistryState::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// On a dedup hit, the returned status must reflect the canonical job's
    /// *current* state (not always `Queued`). Cover all four reachable
    /// states with a single test.
    #[tokio::test]
    #[allow(clippy::type_complexity)]
    async fn dedup_hit_returns_current_status_for_each_state() {
        let cases: &[(fn(&mut Job), JobStatus)] = &[
            (|_| {}, JobStatus::Queued),
            (|j| j.start().unwrap(), JobStatus::Running),
            (
                |j| {
                    j.start().unwrap();
                    j.succeed(std::path::PathBuf::from("/tmp/out.mp4")).unwrap();
                },
                JobStatus::Succeeded,
            ),
            (
                |j| {
                    j.start().unwrap();
                    j.fail("ffmpeg crashed".to_string()).unwrap();
                },
                JobStatus::Failed,
            ),
        ];
        for (transition, want) in cases {
            let mut state = RegistryState::new();
            let key = DedupKey::new("h".to_string(), "p".to_string());
            let (job_id, fresh, fresh_status) = state.get_or_create(key.clone(), "p".to_string());
            assert!(!fresh);
            assert_eq!(fresh_status, JobStatus::Queued);

            transition(state.jobs.get_mut(&job_id).unwrap());

            let (id2, dedup, status) = state.get_or_create(key, "p".to_string());
            assert_eq!(id2, job_id, "want={want:?}");
            assert!(dedup, "want={want:?}");
            assert_eq!(status, *want);
        }
    }
}
