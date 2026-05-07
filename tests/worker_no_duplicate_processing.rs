//! TEST_PLAN.md ## 3 — `worker_no_duplicate_processing`
//!
//! Multiple queued jobs + multiple workers; assert each job_id is processed
//! exactly once and reaches a terminal state. The per-job invocation tracker
//! is keyed by job_id, recovered from the canonical output path supplied to
//! `MockBehavior::Custom` (`data/jobs/{job_id}/output/proxy.{ext}`).

mod support;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::{sleep, timeout};
use uuid::Uuid;

use support::mock_transcoder::{CustomFn, MockBehavior, MockTranscoder};
use syspref_ingest::dedup::create_registry;
use syspref_ingest::ffmpeg::{FfmpegProfile, Transcoder};
use syspref_ingest::queue::create_queue;
use syspref_ingest::shared::{DedupKey, JobStatus};
use syspref_ingest::storage::Storage;
use syspref_ingest::worker::WorkerPool;

const NUM_JOBS: usize = 8;
const NUM_WORKERS: usize = 4;
const PROFILE: &str = "p";
const POLL_TIMEOUT: Duration = Duration::from_secs(5);

fn extract_job_id_from_output(output: &Path) -> Option<Uuid> {
    output
        .parent() // .../output
        .and_then(|p| p.parent()) // .../{job_id}
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .and_then(|s| Uuid::parse_str(s).ok())
}

#[tokio::test]
async fn each_queued_job_is_processed_exactly_once() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let storage = Arc::new(Storage::new(tmp.path()));
    storage.ensure_exists().expect("storage");
    let registry = create_registry();
    let (queue_tx, queue_rx) = create_queue();

    // Per-job counter, keyed by job_id, populated by the closure each time
    // the mock transcoder is invoked. `std::sync::Mutex` because the
    // `MockBehavior::Custom` closure runs in sync context.
    let counts: Arc<Mutex<HashMap<Uuid, usize>>> = Arc::new(Mutex::new(HashMap::new()));
    let counts_cl = counts.clone();
    let custom: CustomFn = Arc::new(move |_input, output, _profile| {
        let job_id = extract_job_id_from_output(output)
            .expect("output path must end in `.../{job_id}/output/proxy.{ext}`");
        let mut guard = counts_cl.lock().expect("counts mutex");
        *guard.entry(job_id).or_insert(0) += 1;
        // AlwaysSucceed-equivalent: create the output file so worker.run
        // can mark the job `succeeded`.
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent).expect("output dir");
        }
        std::fs::write(output, b"").expect("write output");
        Ok(())
    });
    let profiles = vec![FfmpegProfile {
        name: PROFILE.to_string(),
        args: vec![],
        output_extension: "mp4".to_string(),
    }];
    let mock = Arc::new(MockTranscoder::new(MockBehavior::Custom(custom), profiles));
    let transcoder: Arc<dyn Transcoder> = mock.clone();

    // Insert NUM_JOBS distinct canonical jobs and enqueue each. We bypass
    // the HTTP path because TEST_PLAN #3 is purely about the worker-pool
    // delivery contract, not multipart parsing.
    let mut job_ids = Vec::with_capacity(NUM_JOBS);
    {
        let mut reg = registry.lock().await;
        for i in 0..NUM_JOBS {
            let key = DedupKey::new(format!("hash-{i}"), PROFILE.to_string());
            let (job_id, deduped, _status) = reg.get_or_create(key, PROFILE.to_string());
            assert!(!deduped, "first insert for hash-{i} must be canonical");
            storage.create_job_dirs(&job_id).expect("create_job_dirs");
            job_ids.push(job_id);
        }
    }
    for job_id in &job_ids {
        queue_tx.send(*job_id).expect("enqueue");
    }

    // Spawn the worker pool. WorkerPool::start() spawns each worker as a
    // detached tokio task; they exit when all senders drop, which we do
    // below to force the loop to break.
    let pool = WorkerPool::new(
        NUM_WORKERS,
        registry.clone(),
        queue_rx.clone(),
        transcoder.clone(),
        storage.clone(),
    );
    pool.start().await;

    // Bounded polling: wait until every queued job reaches a terminal
    // state. The 10 ms `sleep` is the polling interval, not the test
    // timeout — the timeout is enforced by `tokio::time::timeout`.
    let registry_for_poll = registry.clone();
    let job_ids_for_poll = job_ids.clone();
    let polled = timeout(POLL_TIMEOUT, async move {
        loop {
            let all_terminal = {
                let reg = registry_for_poll.lock().await;
                job_ids_for_poll.iter().all(|id| {
                    reg.get_job(id)
                        .map(|j| matches!(j.status, JobStatus::Succeeded | JobStatus::Failed))
                        .unwrap_or(false)
                })
            };
            if all_terminal {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        polled.is_ok(),
        "not all jobs reached terminal state in {POLL_TIMEOUT:?}",
    );

    // Assertion 1: each job_id was delivered to exactly one worker.
    let final_counts = counts.lock().expect("counts mutex").clone();
    assert_eq!(
        final_counts.len(),
        NUM_JOBS,
        "expected {NUM_JOBS} distinct job_ids, got {}: {:?}",
        final_counts.len(),
        final_counts,
    );
    for job_id in &job_ids {
        let count = final_counts.get(job_id).copied().unwrap_or(0);
        assert_eq!(count, 1, "job {job_id} processed {count} times, want 1");
    }

    // Assertion 2: every job is in `succeeded` (Custom returns Ok).
    {
        let reg = registry.lock().await;
        for job_id in &job_ids {
            let job = reg.get_job(job_id).expect("job present");
            assert_eq!(
                job.status,
                JobStatus::Succeeded,
                "job {job_id} not succeeded: {:?}",
                job.status,
            );
        }
    }

    // Assertion 3: total spawn count matches NUM_JOBS (no extra invocations).
    assert_eq!(mock.spawn_count.load(Ordering::SeqCst), NUM_JOBS);

    // Drop the queue sender so workers exit cleanly when the test ends.
    drop(queue_tx);
}
