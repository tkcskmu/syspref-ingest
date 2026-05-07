//! TEST_PLAN.md ## 5 — `same_filename_different_content`
//!
//! Two uploads with the same filename `sample.mp4` but different bytes
//! must produce two distinct canonical jobs. Dedup keys by content hash,
//! not by filename.

mod support;

use tower::ServiceExt;

use support::helpers::{make_test_setup, multipart_body, post_jobs_request, read_json};
use support::mock_transcoder::MockBehavior;

const PROFILE: &str = "p";
const BOUNDARY: &str = "BOUNDARY";

#[tokio::test]
async fn same_filename_different_content_creates_distinct_jobs() {
    let setup = make_test_setup(MockBehavior::AlwaysSucceed, PROFILE, "mp4", 1024 * 1024);

    let body_a = multipart_body(BOUNDARY, b"AAAAAAAAAAAA-content-A", PROFILE);
    let body_b = multipart_body(BOUNDARY, b"BBBBBBBBBBBB-content-B", PROFILE);

    let resp_a = setup
        .router
        .clone()
        .oneshot(post_jobs_request(BOUNDARY, body_a))
        .await
        .expect("oneshot a");
    let resp_b = setup
        .router
        .clone()
        .oneshot(post_jobs_request(BOUNDARY, body_b))
        .await
        .expect("oneshot b");

    let (status_a, json_a) = read_json(resp_a).await;
    let (status_b, json_b) = read_json(resp_b).await;

    assert_eq!(status_a.as_u16(), 201, "first upload must be 201 (new)");
    assert_eq!(status_b.as_u16(), 201, "second upload must be 201 (new)");

    assert_eq!(json_a["deduplicated"], serde_json::json!(false));
    assert_eq!(json_b["deduplicated"], serde_json::json!(false));

    assert_ne!(
        json_a["job_id"], json_b["job_id"],
        "distinct content must yield distinct job_ids",
    );
    assert_ne!(
        json_a["content_hash"], json_b["content_hash"],
        "distinct content must yield distinct hashes",
    );
}
