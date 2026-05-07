//! TEST_PLAN.md ## 6 — `same_file_concurrent_upload`
//!
//! N concurrent HTTP clients upload the same `(file bytes, profile)` to the
//! in-process router. Exactly one canonical job must be created (201,
//! `deduplicated=false`); the other N-1 must dedup (200, `deduplicated=true`).
//! All responses must share the same `job_id`. The mock transcoder must be
//! invoked exactly once and the registry must end up with a single
//! canonical Job.

mod support;

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tokio::time::{sleep, timeout};
use tower::ServiceExt;

use support::helpers::{make_test_setup, multipart_body, post_jobs_request, read_json};
use support::mock_transcoder::MockBehavior;
use syspref_ingest::ffmpeg::Transcoder;
use syspref_ingest::worker::WorkerPool;

const N: usize = 16;
const PROFILE: &str = "p";
const BOUNDARY: &str = "BOUNDARY";
const POLL_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn same_file_concurrent_upload_dedupes_at_http_layer() {
    let setup = make_test_setup(MockBehavior::AlwaysSucceed, PROFILE, "mp4", 1024 * 1024);

    // Spawn one worker so the queued canonical job is actually picked up
    // and the mock transcoder gets invoked. WorkerPool::start() detaches
    // each worker via tokio::spawn; they exit when all senders drop.
    let transcoder_dyn: Arc<dyn Transcoder> = setup.transcoder.clone();
    let pool = WorkerPool::new(
        1,
        setup.registry.clone(),
        setup.queue_rx.clone(),
        transcoder_dyn,
        setup.storage.clone(),
    );
    pool.start().await;

    // Fire N identical requests concurrently. The router is `Clone`, so we
    // build per-request clones and drive them via `oneshot`.
    let file_bytes: &'static [u8] = b"shared-content-bytes-for-dedup";
    let futures = (0..N).map(|_| {
        let router = setup.router.clone();
        async move {
            let body = multipart_body(BOUNDARY, file_bytes, PROFILE);
            let resp = router
                .oneshot(post_jobs_request(BOUNDARY, body))
                .await
                .expect("oneshot");
            read_json(resp).await
        }
    });
    let results = join_all(futures).await;

    // Tally response-level invariants.
    let mut new_count = 0usize;
    let mut dedup_count = 0usize;
    let mut job_ids: HashSet<String> = HashSet::new();
    for (status, body) in &results {
        let job_id = body["job_id"]
            .as_str()
            .unwrap_or_else(|| panic!("missing job_id in body: {body}"))
            .to_string();
        job_ids.insert(job_id);

        let deduplicated = body["deduplicated"]
            .as_bool()
            .unwrap_or_else(|| panic!("missing deduplicated in body: {body}"));
        if deduplicated {
            assert_eq!(
                status.as_u16(),
                200,
                "dedup response must be 200, got {status}"
            );
            dedup_count += 1;
        } else {
            assert_eq!(
                status.as_u16(),
                201,
                "new canonical response must be 201, got {status}",
            );
            new_count += 1;
        }
    }
    assert_eq!(new_count, 1, "exactly one canonical creation expected");
    assert_eq!(
        dedup_count,
        N - 1,
        "exactly N-1 dedup responses expected for N={N}",
    );
    assert_eq!(
        job_ids.len(),
        1,
        "all responses must share the same job_id, got {job_ids:?}",
    );

    // Worker-level invariant: the mock transcoder is invoked exactly once
    // for the single canonical job. Bounded polling — `tokio::time::timeout`
    // fences the wait; the inner sleep is the polling interval, not the
    // overall test timeout.
    let mock = setup.transcoder.clone();
    let polled = timeout(POLL_TIMEOUT, async move {
        loop {
            if mock.spawn_count.load(Ordering::SeqCst) >= 1 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        polled.is_ok(),
        "mock transcoder spawn_count never reached 1 within {POLL_TIMEOUT:?}",
    );
    assert_eq!(
        setup.transcoder.spawn_count.load(Ordering::SeqCst),
        1,
        "transcoder must be invoked exactly once for the canonical job",
    );

    // Registry: exactly one canonical Job exists.
    {
        let reg = setup.registry.lock().await;
        let jobs = reg.get_jobs();
        assert_eq!(
            jobs.len(),
            1,
            "registry must hold exactly one canonical job, got {}: {:?}",
            jobs.len(),
            jobs.iter().map(|j| j.job_id).collect::<Vec<_>>(),
        );
    }
}
