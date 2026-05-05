use crate::dedup::RegistryArc;
use crate::shared::{AppError, DedupKey, Job, JobStatus};
use crate::storage::Storage;
use axum::{
    body::Bytes,
    extract::multipart::Multipart,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::fs;

pub struct AppState {
    pub registry: RegistryArc,
    pub storage: Arc<Storage>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        AppState {
            registry: Arc::clone(&self.registry),
            storage: self.storage.clone(),
        }
    }
}

fn job_status_str(s: JobStatus) -> &'static str {
    match s {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Succeeded => "succeeded",
        JobStatus::Failed => "failed",
    }
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn create_job(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    let mut file_data: Option<(String, String)> = None;
    let mut profile_str: Option<String> = None;

    // Multipart parse failures map best-effort to MissingFile/MissingProfile;
    // #6 (input validation) will tighten the wire-protocol error mapping.
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| AppError::MissingFile)?
    {
        let name = field.name().unwrap_or("").to_string();
        let bytes: Bytes = field.bytes().await.map_err(|_| AppError::MissingFile)?;

        if name == "file" {
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let hash = hex::encode(hasher.finalize());

            let filename = format!("upload_{}.tmp", uuid::Uuid::new_v4());
            let tmp_path = state.storage.tmp_path(&filename);
            fs::write(&tmp_path, &bytes).await?; // io::Error -> AppError::IoError

            file_data = Some((hash, filename));
        } else if name == "profile" {
            profile_str =
                Some(String::from_utf8(bytes.to_vec()).map_err(|_| AppError::MissingProfile)?);
        }
    }

    let (content_hash, filename) = file_data.ok_or(AppError::MissingFile)?;
    let profile = profile_str.ok_or(AppError::MissingProfile)?;

    let key = DedupKey::new(content_hash.clone(), profile.clone());
    let profile_for_response = profile.clone();

    // get_or_create is sync; the lock guard never spans an `.await`.
    let (job_id, deduplicated, dedup_status) = {
        let mut registry = state.registry.lock().await;
        registry.get_or_create(key, profile)
    };

    if let Err(e) = state.storage.cleanup_tmp(&filename) {
        eprintln!("Failed to cleanup temp file: {}", e);
    }

    let (status_code, status_for_body) = if deduplicated {
        (StatusCode::OK, dedup_status)
    } else {
        (StatusCode::CREATED, JobStatus::Queued)
    };

    let body = serde_json::json!({
        "job_id": job_id.to_string(),
        "status": job_status_str(status_for_body),
        "deduplicated": deduplicated,
        "profile": profile_for_response,
        "content_hash": content_hash,
    });

    Ok((status_code, Json(body)))
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, AppError> {
    // Snapshot under lock, drop guard, then serialize.
    let snapshot = {
        let registry = state.registry.lock().await;
        registry.get_job(&job_id).cloned()
    };
    let job = snapshot.ok_or(AppError::JobNotFound(job_id))?;

    let response = serde_json::json!({
        "job_id": job.job_id.to_string(),
        "status": job_status_str(job.status),
        "profile": job.profile,
        "content_hash": job.content_hash,
        "created_at": job.created_at.to_rfc3339(),
        "started_at": job.started_at.map(|t| t.to_rfc3339()),
        "finished_at": job.finished_at.map(|t| t.to_rfc3339()),
        "artifact": job.artifact.as_ref().map(|a| serde_json::json!({
            "path": a.path.display().to_string()
        })),
        "error": job.error,
    });

    Ok(Json(response))
}

pub async fn list_jobs(State(state): State<AppState>) -> Json<serde_json::Value> {
    let registry = state.registry.lock().await;
    let jobs: Vec<&Job> = registry.get_jobs();

    let job_list: Vec<serde_json::Value> = jobs
        .iter()
        .map(|j| {
            serde_json::json!({
                "job_id": j.job_id.to_string(),
                "status": job_status_str(j.status),
                "profile": j.profile,
                "content_hash": j.content_hash
            })
        })
        .collect();

    Json(serde_json::json!({ "jobs": job_list }))
}

pub async fn get_artifact(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<Response, AppError> {
    // Snapshot under lock; do file I/O after dropping the guard.
    let snapshot = {
        let registry = state.registry.lock().await;
        registry.get_job(&job_id).cloned()
    };
    let job = snapshot.ok_or(AppError::JobNotFound(job_id))?;

    match job.status {
        JobStatus::Succeeded => {
            let artifact = job.artifact.ok_or(AppError::ArtifactNotReady)?;
            let bytes = fs::read(&artifact.path).await?;
            // Header customization (Content-Disposition, output-extension based MIME)
            // is deferred to issues #4 and #9.
            Ok(([("Content-Type", "video/mp4")], bytes).into_response())
        }
        JobStatus::Queued | JobStatus::Running => Err(AppError::ArtifactNotReady),
        JobStatus::Failed => Err(AppError::JobFailed),
    }
}

pub fn create_router(registry: RegistryArc, storage: Arc<Storage>) -> Router {
    let state = AppState { registry, storage };

    Router::new()
        .route("/api/health", get(health))
        .route("/api/jobs", post(create_job))
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/:job_id", get(get_job))
        .route("/api/jobs/:job_id/artifact", get(get_artifact))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dedup::create_registry;
    use axum::body::to_bytes;
    use axum::http::Request;
    use axum::Router;
    use tower::ServiceExt;

    fn router() -> (Router, RegistryArc, Arc<Storage>) {
        let registry = create_registry();
        let tmp = std::env::temp_dir().join(format!("syspref_test_{}", uuid::Uuid::new_v4()));
        let storage = Arc::new(Storage::new(&tmp));
        storage.ensure_exists().unwrap();
        let app = create_router(registry.clone(), storage.clone());
        (app, registry, storage)
    }

    fn multipart_body(boundary: &str, file_bytes: &[u8], profile: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n",
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

    async fn read_json(resp: axum::response::Response) -> (StatusCode, serde_json::Value) {
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
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

    #[tokio::test]
    async fn post_new_job_returns_201_and_queued() {
        let (app, _registry, _storage) = router();
        let boundary = "BNDRY";
        let body = multipart_body(boundary, b"hello world", "web_720p");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["deduplicated"], serde_json::json!(false));
        assert_eq!(body["status"], "queued");
        assert_eq!(body["profile"], "web_720p");
        assert!(body["job_id"].as_str().is_some());
        assert!(body["content_hash"].as_str().is_some());
    }

    #[tokio::test]
    async fn post_dedup_returns_200_and_current_status() {
        let (app, registry, _storage) = router();
        let boundary = "BNDRY";
        let body = multipart_body(boundary, b"same content", "web_720p");

        let make_req = || {
            Request::builder()
                .method("POST")
                .uri("/api/jobs")
                .header(
                    "Content-Type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(axum::body::Body::from(body.clone()))
                .unwrap()
        };

        let resp1 = app.clone().oneshot(make_req()).await.unwrap();
        let (status1, body1) = read_json(resp1).await;
        assert_eq!(status1, StatusCode::CREATED);
        let job_id = body1["job_id"].as_str().unwrap().to_string();

        // Manually transition the canonical job to Running so we can prove
        // the dedup response carries the *current* status (not "queued").
        {
            let id = uuid::Uuid::parse_str(&job_id).unwrap();
            let mut state = registry.lock().await;
            state.jobs.get_mut(&id).unwrap().start().unwrap();
        }

        let resp2 = app.oneshot(make_req()).await.unwrap();
        let (status2, body2) = read_json(resp2).await;
        assert_eq!(status2, StatusCode::OK);
        assert_eq!(body2["deduplicated"], serde_json::json!(true));
        assert_eq!(body2["status"], "running");
        assert_eq!(body2["job_id"], serde_json::json!(job_id));
    }

    #[tokio::test]
    async fn get_unknown_job_returns_envelope() {
        let (app, _registry, _storage) = router();
        let id = uuid::Uuid::new_v4();
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{id}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "unknown_job_id");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&id.to_string()));
    }

    #[tokio::test]
    async fn get_job_artifact_shape_object_when_succeeded() {
        let (app, registry, _storage) = router();
        // Seed a succeeded job directly.
        let job_id = {
            let mut state = registry.lock().await;
            let key = DedupKey::new("h".into(), "p".into());
            let (id, _, _) = state.get_or_create(key, "p".into());
            let job = state.jobs.get_mut(&id).unwrap();
            job.start().unwrap();
            job.succeed(std::path::PathBuf::from("/tmp/out.mp4"))
                .unwrap();
            id
        };

        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{job_id}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "succeeded");
        assert!(body["artifact"].is_object());
        assert_eq!(body["artifact"]["path"], "/tmp/out.mp4");
    }

    #[tokio::test]
    async fn get_job_artifact_null_when_not_succeeded() {
        let (app, registry, _storage) = router();
        let job_id = {
            let mut state = registry.lock().await;
            let key = DedupKey::new("h2".into(), "p".into());
            let (id, _, _) = state.get_or_create(key, "p".into());
            id
        };

        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{job_id}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["artifact"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn get_artifact_envelopes_for_not_ready_and_failed() {
        let (app, registry, _storage) = router();

        // queued -> 409 artifact_not_ready
        let queued_id = {
            let mut state = registry.lock().await;
            let key = DedupKey::new("hq".into(), "p".into());
            let (id, _, _) = state.get_or_create(key, "p".into());
            id
        };
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{queued_id}/artifact"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "artifact_not_ready");

        // failed -> 409 job_failed
        let failed_id = {
            let mut state = registry.lock().await;
            let key = DedupKey::new("hf".into(), "p".into());
            let (id, _, _) = state.get_or_create(key, "p".into());
            let job = state.jobs.get_mut(&id).unwrap();
            job.start().unwrap();
            job.fail("ffmpeg crashed".into()).unwrap();
            id
        };
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{failed_id}/artifact"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "job_failed");
    }

    fn multipart_only_profile(boundary: &str, profile: &str) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(b"Content-Disposition: form-data; name=\"profile\"\r\n\r\n");
        body.extend_from_slice(profile.as_bytes());
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    fn multipart_only_file(boundary: &str, file_bytes: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            b"Content-Disposition: form-data; name=\"file\"; filename=\"v.mp4\"\r\n\r\n",
        );
        body.extend_from_slice(file_bytes);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
        body
    }

    #[tokio::test]
    async fn post_missing_file_returns_400_envelope() {
        let (app, _registry, _storage) = router();
        let boundary = "BNDRY";
        let body = multipart_only_profile(boundary, "web_720p");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "missing_file");
        assert!(body["error"]["message"].as_str().is_some());
    }

    #[tokio::test]
    async fn post_missing_profile_returns_400_envelope() {
        let (app, _registry, _storage) = router();
        let boundary = "BNDRY";
        let body = multipart_only_file(boundary, b"hello");
        let req = Request::builder()
            .method("POST")
            .uri("/api/jobs")
            .header(
                "Content-Type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "missing_profile");
        assert!(body["error"]["message"].as_str().is_some());
    }

    #[tokio::test]
    async fn get_artifact_unknown_returns_404_envelope() {
        let (app, _registry, _storage) = router();
        let id = uuid::Uuid::new_v4();
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{id}/artifact"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "unknown_job_id");
    }

    #[tokio::test]
    async fn get_artifact_running_returns_409_envelope() {
        let (app, registry, _storage) = router();
        let running_id = {
            let mut state = registry.lock().await;
            let key = DedupKey::new("hr".into(), "p".into());
            let (id, _, _) = state.get_or_create(key, "p".into());
            state.jobs.get_mut(&id).unwrap().start().unwrap();
            id
        };
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/jobs/{running_id}/artifact"))
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let (status, body) = read_json(resp).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"]["code"], "artifact_not_ready");
    }
}
