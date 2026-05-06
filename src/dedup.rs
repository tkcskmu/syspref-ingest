use crate::shared::{DedupKey, Job, JobStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct DedupEntry {
    pub job_id: Uuid,
}

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

    /// Atomic get-or-create operation. Caller holds the registry lock.
    /// Sync (no `.await`) so it can be called under `tokio::sync::Mutex` guard
    /// without violating the "no .await under registry lock" rule.
    /// Returns `(job_id, was_deduplicated, current_status)`. On a dedup hit the
    /// status is the existing job's snapshot at this moment. On a miss it is
    /// `JobStatus::Queued` (the freshly inserted job).
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
    use tokio::task::JoinHandle;

    #[tokio::test]
    async fn dedup_registry_concurrent_access() {
        // Create shared registry and key
        let registry = create_registry();
        let key = DedupKey::new("same_hash".to_string(), "profile1".to_string());

        // Spawn 100 tasks that all try to get_or_create the same key
        let mut handles: Vec<JoinHandle<(Uuid, bool, JobStatus)>> = vec![];
        for _ in 0..100 {
            let reg = registry.clone();
            let key = key.clone();
            let handle = tokio::spawn(async move {
                let mut r = reg.lock().await;
                r.get_or_create(key, "profile1".to_string())
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete and collect results
        let results: Vec<(Uuid, bool, JobStatus)> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Extract the job IDs and dedup flags
        let job_ids: Vec<Uuid> = results.iter().map(|r| r.0).collect();
        let dedup_flags: Vec<bool> = results.iter().map(|r| r.1).collect();

        // All job IDs should be the same (one canonical job)
        let first_id = job_ids[0];
        for &id in &job_ids {
            assert_eq!(id, first_id, "All jobs should have the same ID");
        }

        // Exactly one should have dedup=false, rest should be true
        let false_count = dedup_flags.iter().filter(|&&x| !x).count();
        assert_eq!(
            false_count, 1,
            "Exactly one job should be newly created (dedup=false)"
        );

        // The canonical job is freshly inserted, so status should be Queued for everyone
        for (_, _, status) in &results {
            assert_eq!(*status, JobStatus::Queued);
        }

        // Verify registry state
        let r = registry.lock().await;
        assert_eq!(r.dedup.len(), 1, "Dedup map should have exactly one entry");
        assert_eq!(
            r.jobs.len(),
            1,
            "Jobs map should have exactly one canonical job"
        );
    }

    fn seed_existing_job(state: &mut RegistryState) -> (DedupKey, Uuid) {
        let key = DedupKey::new("hash_x".to_string(), "p_web".to_string());
        let (job_id, dedup, status) = state.get_or_create(key.clone(), "p_web".to_string());
        assert!(!dedup);
        assert_eq!(status, JobStatus::Queued);
        (key, job_id)
    }

    #[tokio::test]
    async fn dedup_hit_returns_queued_status_for_fresh_job() {
        let registry = create_registry();
        let mut state = registry.lock().await;
        let (key, job_id) = seed_existing_job(&mut state);

        let (id2, dedup, status) = state.get_or_create(key, "p_web".to_string());
        assert_eq!(id2, job_id);
        assert!(dedup);
        assert_eq!(status, JobStatus::Queued);
    }

    #[tokio::test]
    async fn dedup_hit_returns_running_status() {
        let registry = create_registry();
        let mut state = registry.lock().await;
        let (key, job_id) = seed_existing_job(&mut state);
        // Manually transition the canonical job to Running.
        state.jobs.get_mut(&job_id).unwrap().start().unwrap();

        let (_, dedup, status) = state.get_or_create(key, "p_web".to_string());
        assert!(dedup);
        assert_eq!(status, JobStatus::Running);
    }

    #[tokio::test]
    async fn dedup_hit_returns_succeeded_status() {
        let registry = create_registry();
        let mut state = registry.lock().await;
        let (key, job_id) = seed_existing_job(&mut state);
        let job = state.jobs.get_mut(&job_id).unwrap();
        job.start().unwrap();
        job.succeed(std::path::PathBuf::from("/tmp/out.mp4"))
            .unwrap();

        let (_, dedup, status) = state.get_or_create(key, "p_web".to_string());
        assert!(dedup);
        assert_eq!(status, JobStatus::Succeeded);
    }

    #[tokio::test]
    async fn dedup_hit_returns_failed_status() {
        let registry = create_registry();
        let mut state = registry.lock().await;
        let (key, job_id) = seed_existing_job(&mut state);
        let job = state.jobs.get_mut(&job_id).unwrap();
        job.start().unwrap();
        job.fail("ffmpeg crashed".to_string()).unwrap();

        let (_, dedup, status) = state.get_or_create(key, "p_web".to_string());
        assert!(dedup);
        assert_eq!(status, JobStatus::Failed);
    }
}
