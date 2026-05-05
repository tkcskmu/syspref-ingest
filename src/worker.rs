use crate::dedup::RegistryArc;
use crate::ffmpeg::{FfmpegRunner};
use crate::queue::QueueArc;
use crate::shared::AppResult;
use crate::storage::Storage;
use std::sync::Arc;

#[derive(Clone)]
pub struct Worker {
    id: usize,
    registry: RegistryArc,
    queue: QueueArc,
    ffmpeg_runner: FfmpegRunner,
    storage: Arc<Storage>,
}

impl Worker {
    pub fn new(
        id: usize,
        registry: RegistryArc,
        queue: QueueArc,
        ffmpeg_runner: FfmpegRunner,
        storage: Arc<Storage>,
    ) -> Self {
        Worker {
            id,
            registry,
            queue,
            ffmpeg_runner,
            storage,
        }
    }

    pub async fn run(&self) -> AppResult<()> {
        loop {
            let (job_id, mut job) = match self.queue.lock().await.dequeue().await {
                Some((jid, j)) => (jid, j),
                None => continue,
            };

            // Update status to running
            {
                let mut registry = self.registry.lock().await;
                if let Some(j) = registry.jobs.get_mut(&job_id) {
                    match j.start() {
                        Ok(()) => {}
                        Err(_) => {
                            eprintln!("Failed to set job {} to running", job_id);
                            continue;
                        }
                    }
                }
            }

            // Run FFmpeg
            let input_path = self.storage.job_input_path(&job_id);
            let output_path = self.storage.job_output_path(&job_id).join("proxy.mp4");

            match self
                .ffmpeg_runner
                .run(&input_path, &output_path, &job.profile)
                .await
            {
                Ok(()) => {
                    let mut registry = self.registry.lock().await;
                    if let Some(j) = registry.jobs.get_mut(&job_id) {
                        match j.succeed(output_path) {
                            Ok(()) => {}
                            Err(_) => {
                                eprintln!("Failed to set job {} to succeeded", job_id);
                            }
                        }
                    }
                }
                Err(e) => {
                    let mut registry = self.registry.lock().await;
                    if let Some(j) = registry.jobs.get_mut(&job_id) {
                        match j.fail(e.to_string()) {
                            Ok(()) => {}
                            Err(_) => {
                                eprintln!("Failed to set job {} to failed", job_id);
                            }
                        }
                    }
                }
            }
        }
    }
}

pub struct WorkerPool {
    workers: Vec<Worker>,
}

impl WorkerPool {
    pub fn new(
        size: usize,
        registry: RegistryArc,
        queue: QueueArc,
        ffmpeg_runner: FfmpegRunner,
        storage: Arc<Storage>,
    ) -> Self {
        let workers = (0..size)
            .map(|id| {
                Worker::new(id, registry.clone(), queue.clone(), ffmpeg_runner.clone(), storage.clone())
            })
            .collect();

        WorkerPool { workers }
    }

    pub async fn start(&self) {
        let workers: Vec<_> = self.workers.iter().cloned().collect();
        for worker in workers {
            tokio::spawn(async move {
                if let Err(e) = worker.run().await {
                    eprintln!("Worker {} error: {}", worker.id, e);
                }
            });
        }
    }
}
