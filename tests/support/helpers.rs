//! Integration-test helpers shared across HTTP-level tests.
//!
//! `src/api.rs` defines similar helpers under `#[cfg(test)] mod tests`, but
//! those are private to that module's unit-test scope. Integration tests
//! under `tests/*.rs` cannot reach them, so this module reproduces the
//! minimum set needed by the placeholder unwirings (#3 / #4 / #5 / #6 in
//! `docs/TEST_PLAN.md`).
//!
//! `#![allow(dead_code)]` is applied at module scope because each test file
//! that imports `mod support;` only references a subset of these helpers,
//! and clippy `-D warnings` would otherwise fail per-target builds.

#![allow(dead_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use tempfile::TempDir;

use syspref_ingest::api::create_router;
use syspref_ingest::dedup::{create_registry, RegistryArc};
use syspref_ingest::ffmpeg::Transcoder;
use syspref_ingest::queue::{create_queue, JobReceiver, JobSender};
use syspref_ingest::storage::Storage;

use super::mock_transcoder::{MockBehavior, MockTranscoder};
use syspref_ingest::ffmpeg::FfmpegProfile;

/// Bundle returned by `make_test_setup`. Holds every owner the integration
/// tests need to drive a router + worker pool. `_tmp` is kept here so its
/// `Drop` (and therefore the on-disk cleanup) only runs after the test
/// finishes; never let the test bind it to `_` directly.
pub struct TestSetup {
    pub router: Router,
    pub registry: RegistryArc,
    pub storage: Arc<Storage>,
    pub queue_tx: JobSender,
    pub queue_rx: JobReceiver,
    pub transcoder: Arc<MockTranscoder>,
    pub _tmp: TempDir,
}

/// Build a router + worker-pool fixture wired to a `MockTranscoder`. The
/// returned `TestSetup` keeps every dependency alive for the test scope.
///
/// `profile` is registered on the mock so `unknown_profile` is not raised on
/// the happy path; tests that care about validation error paths can pass
/// arbitrary profile names through `multipart_body`.
pub fn make_test_setup(
    behavior: MockBehavior,
    profile: &str,
    output_extension: &str,
    max_upload_bytes: usize,
) -> TestSetup {
    let tmp = TempDir::new().expect("tempdir");
    let storage = Arc::new(Storage::new(tmp.path()));
    storage.ensure_exists().expect("storage ensure_exists");
    let registry = create_registry();
    let (queue_tx, queue_rx) = create_queue();

    let profiles = vec![FfmpegProfile {
        name: profile.to_string(),
        args: vec![],
        output_extension: output_extension.to_string(),
    }];
    let mock = Arc::new(MockTranscoder::new(behavior, profiles));
    // `Arc<dyn Transcoder>` is what `create_router` and `Worker::new` consume.
    let transcoder_dyn: Arc<dyn Transcoder> = mock.clone();

    let router = create_router(
        registry.clone(),
        storage.clone(),
        queue_tx.clone(),
        transcoder_dyn,
        max_upload_bytes,
    );

    TestSetup {
        router,
        registry,
        storage,
        queue_tx,
        queue_rx,
        transcoder: mock,
        _tmp: tmp,
    }
}

/// Build a complete `multipart/form-data` body with one `file` part and one
/// `profile` part. Mirrors the helper in `src/api.rs::tests::multipart_body`.
pub fn multipart_body(boundary: &str, file_bytes: &[u8], profile: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"sample.mp4\"\r\n\r\n",
    );
    body.extend_from_slice(file_bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n\r\n");
    body.extend_from_slice(profile.as_bytes());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

/// Build a `POST /api/jobs` request with the given multipart body.
pub fn post_jobs_request(boundary: &str, body: Vec<u8>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/jobs")
        .header(
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .expect("request build")
}

/// Read the response body as JSON. Mirrors `src/api.rs::tests::read_json`.
pub async fn read_json(resp: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "expected JSON body but got status {} body {:?}: {}",
            status,
            String::from_utf8_lossy(&bytes),
            e
        )
    });
    (status, v)
}

/// Count files under `<storage_root>/tmp/` whose name starts with `upload_`.
/// Uses the real on-disk path because `Storage` exposes no public
/// `tmp_dir()` accessor.
pub fn count_upload_tmp_files(storage_root: &std::path::Path) -> usize {
    let tmp_dir = storage_root.join("tmp");
    let read_dir = match std::fs::read_dir(&tmp_dir) {
        Ok(rd) => rd,
        Err(_) => return 0,
    };
    read_dir
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("upload_"))
        .count()
}
