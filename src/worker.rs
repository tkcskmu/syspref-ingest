use crate::dedup::RegistryArc;
use crate::ffmpeg::Transcoder;
use crate::queue::JobReceiver;
use crate::shared::AppResult;
use crate::storage::Storage;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn build_proxy_output_path(output_dir: &Path, ext: &str) -> PathBuf {
    output_dir.join(format!("proxy.{}", ext))
}

#[derive(Clone)]
pub struct Worker {
    id: usize,
    registry: RegistryArc,
    queue_rx: JobReceiver,
    transcoder: Arc<dyn Transcoder>,
    storage: Arc<Storage>,
}

impl Worker {
    pub fn new(
        id: usize,
        registry: RegistryArc,
        queue_rx: JobReceiver,
        transcoder: Arc<dyn Transcoder>,
        storage: Arc<Storage>,
    ) -> Self {
        Worker {
            id,
            registry,
            queue_rx,
            transcoder,
            storage,
        }
    }

    pub async fn run(&self) -> AppResult<()> {
        loop {
            // Receiver is shared across workers via Arc<Mutex<_>>; only one
            // worker holds the guard at a time, so each enqueued JobId is
            // delivered to at most one worker (docs/ARCHITECTURE.md Job Queue
            // Contract). Holding this mutex across `recv().await` is
            // intentional and distinct from the no-await-under-registry-lock
            // rule.
            let job_id = {
                let mut rx = self.queue_rx.lock().await;
                match rx.recv().await {
                    Some(id) => id,
                    None => break, // all senders dropped
                }
            };

            // Snapshot profile + flip to running under a narrow registry
            // guard. No `.await` inside this block.
            let profile_name = {
                let mut registry = self.registry.lock().await;
                let Some(job) = registry.jobs.get_mut(&job_id) else {
                    continue;
                };
                if job.start().is_err() {
                    eprintln!("Worker {}: failed to start job {}", self.id, job_id);
                    continue;
                }
                job.profile.clone()
            };

            // Resolve output extension from profile. The profile was validated
            // at startup, so a missing entry here is an invariant violation:
            // fail the job rather than silently fall back.
            let ext = match self.transcoder.get_profile(&profile_name) {
                Some(p) => p.output_extension.clone(),
                None => {
                    let mut registry = self.registry.lock().await;
                    if let Some(j) = registry.jobs.get_mut(&job_id) {
                        let _ = j.fail(format!("profile '{}' missing at runtime", profile_name));
                    }
                    continue;
                }
            };

            // FFmpeg runs without any lock held. Input path must be the
            // canonical file (data/jobs/{id}/input/input.bin), not the
            // directory.
            let input_path: PathBuf = self.storage.job_input_path(&job_id).join("input.bin");
            let output_dir = self.storage.job_output_path(&job_id);
            let output_path: PathBuf = build_proxy_output_path(&output_dir, &ext);
            let result = self
                .transcoder
                .run(&input_path, &output_path, &profile_name)
                .await;

            // Apply terminal status under a narrow registry guard.
            let mut registry = self.registry.lock().await;
            if let Some(job) = registry.jobs.get_mut(&job_id) {
                match result {
                    Ok(()) => {
                        if job.succeed(output_path).is_err() {
                            eprintln!(
                                "Worker {}: failed to mark job {} succeeded",
                                self.id, job_id
                            );
                        }
                    }
                    Err(e) => {
                        if job.fail(e.to_string()).is_err() {
                            eprintln!("Worker {}: failed to mark job {} failed", self.id, job_id);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub struct WorkerPool {
    workers: Vec<Worker>,
}

impl WorkerPool {
    pub fn new(
        size: usize,
        registry: RegistryArc,
        queue_rx: JobReceiver,
        transcoder: Arc<dyn Transcoder>,
        storage: Arc<Storage>,
    ) -> Self {
        let workers = (0..size)
            .map(|id| {
                Worker::new(
                    id,
                    registry.clone(),
                    queue_rx.clone(),
                    transcoder.clone(),
                    storage.clone(),
                )
            })
            .collect();

        WorkerPool { workers }
    }

    pub async fn start(&self) {
        let workers: Vec<_> = self.workers.to_vec();
        for worker in workers {
            tokio::spawn(async move {
                if let Err(e) = worker.run().await {
                    eprintln!("Worker {} error: {}", worker.id, e);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_proxy_output_path_uses_extension() {
        let dir = Path::new("/data/jobs/abc/output");
        assert_eq!(
            build_proxy_output_path(dir, "mp4"),
            PathBuf::from("/data/jobs/abc/output/proxy.mp4")
        );
        assert_eq!(
            build_proxy_output_path(dir, "webm"),
            PathBuf::from("/data/jobs/abc/output/proxy.webm")
        );
        assert_eq!(
            build_proxy_output_path(dir, "mp3"),
            PathBuf::from("/data/jobs/abc/output/proxy.mp3")
        );
    }
}
