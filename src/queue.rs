use crate::shared::Job;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct JobQueue {
    queue: Vec<Uuid>,
    jobs: HashMap<Uuid, Job>,
}

impl JobQueue {
    pub fn new() -> Self {
        JobQueue {
            queue: Vec::new(),
            jobs: HashMap::new(),
        }
    }

    pub async fn enqueue(&mut self, job_id: Uuid, job: Job) {
        self.queue.push(job_id);
        self.jobs.insert(job_id, job);
    }

    pub async fn dequeue(&mut self) -> Option<(Uuid, Job)> {
        let job_id = self.queue.pop()?;
        let job = self.jobs.remove(&job_id)?;
        Some((job_id, job))
    }

    pub async fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub async fn get_job(&self, job_id: &Uuid) -> Option<&Job> {
        self.jobs.get(job_id)
    }
}

pub type QueueArc = Arc<Mutex<JobQueue>>;

pub fn create_queue() -> QueueArc {
    Arc::new(Mutex::new(JobQueue::new()))
}
