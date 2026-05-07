//! TEST_PLAN.md ## 4 — `incomplete_upload_no_job`
//!
//! Drive the in-process router with a multipart body that begins a `file`
//! part and then errors mid-stream. The handler must reject with
//! `400 invalid_multipart`, leave the registry empty, leave the queue
//! empty, and leave no `upload_*.tmp` staging file behind.

mod support;

use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::Request;
use futures::stream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::timeout;
use tower::ServiceExt;

use support::helpers::{count_upload_tmp_files, make_test_setup, read_json};
use support::mock_transcoder::MockBehavior;

const PROFILE: &str = "p";
const BOUNDARY: &str = "BOUNDARY";

/// Build a partial multipart body that includes a valid `file` part header
/// plus some payload bytes, but never closes the stream — the next chunk
/// surfaces an `io::Error`. `file_bytes_prefix` is the prefix delivered
/// before the abort.
fn build_partial_stream(file_bytes_prefix: &[u8]) -> Body {
    let mut header = Vec::new();
    header.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    header.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"sample.mp4\"\r\n\r\n",
    );

    let header_chunk: Result<Bytes, std::io::Error> = Ok(Bytes::from(header));
    let body_chunk: Result<Bytes, std::io::Error> = Ok(Bytes::copy_from_slice(file_bytes_prefix));
    let abort: Result<Bytes, std::io::Error> = Err(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "client disconnected mid-upload",
    ));
    Body::from_stream(stream::iter(vec![header_chunk, body_chunk, abort]))
}

#[tokio::test]
async fn incomplete_upload_does_not_register_job() {
    let setup = make_test_setup(MockBehavior::AlwaysSucceed, PROFILE, "mp4", 1024 * 1024);

    let body = build_partial_stream(b"some-partial-bytes-that-staging-would-write");
    let request = Request::builder()
        .method("POST")
        .uri("/api/jobs")
        .header(
            "content-type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .body(body)
        .expect("request build");

    let response = setup
        .router
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot");

    let (status, body) = read_json(response).await;
    // axum 0.7 multipart errors during field iteration map to
    // `AppError::InvalidMultipart` → 400 + the structured envelope.
    assert_eq!(status.as_u16(), 400, "want 400, got {status}");
    assert_eq!(body["error"]["code"], "invalid_multipart");
    assert!(
        body["error"]["message"].is_string(),
        "envelope must include `message`: {body}",
    );

    // Registry: no Job was inserted.
    {
        let reg = setup.registry.lock().await;
        assert!(
            reg.get_jobs().is_empty(),
            "registry must be empty, has: {:?}",
            reg.get_jobs().iter().map(|j| j.job_id).collect::<Vec<_>>(),
        );
    }

    // Queue: no JobId was sent. `try_recv` is nonblocking; under contention
    // a small grace period helps avoid spuriously racing the receive vs the
    // handler returning. Use `tokio::time::timeout` to bound that grace.
    let recv_attempt = timeout(Duration::from_millis(50), async {
        let mut rx = setup.queue_rx.lock().await;
        rx.try_recv()
    })
    .await
    .expect("queue try_recv timed out");
    assert!(
        matches!(recv_attempt, Err(TryRecvError::Empty)),
        "queue must be empty, got {recv_attempt:?}",
    );

    // Tmp directory: no `upload_*.tmp` was left behind. `cleanup_tmp` runs
    // on every error path that owns a staged file, so the directory must
    // contain zero `upload_` entries.
    let leftover = count_upload_tmp_files(setup._tmp.path());
    assert_eq!(leftover, 0, "expected 0 upload_*.tmp, got {leftover}");
}
