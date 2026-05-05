mod support;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use support::mock_transcoder::{CustomFn, MockBehavior, MockTranscoder};
use syspref_ingest::ffmpeg::{FfmpegRunner, Transcoder};
use uuid::Uuid;

// Compile-only: production `FfmpegRunner` implements `Transcoder` with the
// `Send + Sync + 'static` bounds required by `Arc<dyn Transcoder>` in
// `WorkerPool`. ffmpeg binary is NOT executed.
#[allow(dead_code)]
fn _assert_transcoder<T: Transcoder + Send + Sync + 'static>(_: &T) {}

#[allow(dead_code)]
fn _check_ffmpeg_runner_impls_transcoder() {
    _assert_transcoder(&FfmpegRunner::new(vec![]));
}

// Compile-only: same `Arc<dyn Transcoder>` coercion main.rs uses for the
// production wiring also works for the mock.
#[allow(dead_code)]
fn _check_arc_dyn_coercion() {
    let _: Arc<dyn Transcoder> = Arc::new(MockTranscoder::new(MockBehavior::AlwaysSucceed, vec![]));
}

fn unique_paths() -> (PathBuf, PathBuf) {
    let id = Uuid::new_v4();
    let base = std::env::temp_dir().join(format!("syspref-ingest-mock-{}", id));
    let input = base.join("input.bin");
    let output = base.join("output").join("proxy.mp4");
    (input, output)
}

#[tokio::test]
async fn always_succeed_increments_spawn_count_and_writes_output() {
    let mock = MockTranscoder::new(MockBehavior::AlwaysSucceed, vec![]);
    assert_eq!(mock.spawn_count(), 0);

    let (input, output) = unique_paths();
    mock.run(&input, &output, "any").await.expect("mock run");

    assert_eq!(mock.spawn_count(), 1);
    assert!(output.exists(), "AlwaysSucceed should create dummy output");

    // Cleanup
    if let Some(parent) = output.parent() {
        let _ = std::fs::remove_dir_all(parent);
    }
}

#[tokio::test]
async fn always_fail_returns_error_and_skips_output() {
    let mock = MockTranscoder::new(MockBehavior::AlwaysFail, vec![]);

    let (input, output) = unique_paths();
    let err = mock
        .run(&input, &output, "any")
        .await
        .expect_err("AlwaysFail should error");
    assert!(err.to_string().contains("mock failure"));
    assert_eq!(mock.spawn_count(), 1);
    assert!(!output.exists(), "AlwaysFail must not create output");
}

#[tokio::test]
async fn custom_closure_branches_on_profile_and_captures_state() {
    let captured = Arc::new(AtomicUsize::new(0));
    let captured_cl = captured.clone();
    let custom: CustomFn = Arc::new(move |_input, _output, profile| {
        captured_cl.fetch_add(1, Ordering::SeqCst);
        if profile == "fail" {
            Err(anyhow::anyhow!("custom-fail"))
        } else {
            Ok(())
        }
    });
    let mock = MockTranscoder::new(MockBehavior::Custom(custom), vec![]);

    let (input, output) = unique_paths();
    mock.run(&input, &output, "ok").await.expect("ok branch");
    let err = mock
        .run(&input, &output, "fail")
        .await
        .expect_err("fail branch");
    assert!(err.to_string().contains("custom-fail"));

    assert_eq!(mock.spawn_count(), 2);
    assert_eq!(captured.load(Ordering::SeqCst), 2);
}
