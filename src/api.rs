use crate::dedup::RegistryArc;
use crate::ffmpeg::Transcoder;
use crate::queue::JobSender;
use crate::shared::{DedupKey, Job, JobStatus};
use crate::storage::Storage;
use axum::{
    body::Body,
    extract::multipart::Multipart,
    extract::{Path, State},
    http::{header, StatusCode},
    response::Json,
    routing::{get, post},
    Router,
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::fs;
use tokio_util::io::ReaderStream;

#[derive(Clone)]
pub struct AppState {
    pub registry: RegistryArc,
    pub storage: Arc<Storage>,
    pub queue_tx: JobSender,
    pub transcoder: Arc<dyn Transcoder>,
}

fn ext_to_mime(ext: &str) -> &'static str {
    match ext {
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        _ => "application/octet-stream",
    }
}

pub async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

pub async fn create_job(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut file_data = None;
    let mut profile_str: Option<String> = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("Failed to read field: {}", e),
        )
    })? {
        let name = field.name().unwrap_or("").to_string();

        // Read the bytes using axum's Bytes
        use axum::body::Bytes;
        let bytes: Bytes = field.bytes().await.map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to read bytes: {}", e),
            )
        })?;

        if name == "file" {
            // Compute SHA-256 while reading
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let hash = hex::encode(hasher.finalize());

            // Write to temp file
            let filename = format!("upload_{}.tmp", uuid::Uuid::new_v4());
            let tmp_path = state.storage.tmp_path(&filename);

            fs::write(&tmp_path, &bytes).await.map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to write file: {}", e),
                )
            })?;

            file_data = Some((hash, filename));
        } else if name == "profile" {
            profile_str = Some(String::from_utf8(bytes.to_vec()).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("Invalid UTF-8 in profile: {}", e),
                )
            })?);
        }
    }

    let (content_hash, filename) = file_data.ok_or((
        StatusCode::BAD_REQUEST,
        "Missing required field: file".to_string(),
    ))?;
    let profile = profile_str.ok_or((
        StatusCode::BAD_REQUEST,
        "Missing required field: profile".to_string(),
    ))?;

    // Create DedupKey
    let key = DedupKey::new(content_hash.clone(), profile.clone());

    // Atomic dedup lookup/insert. Synchronous body — no `.await` while holding
    // the registry guard (docs/ARCHITECTURE.md, docs/TEST_PLAN.md).
    let profile_for_response = profile.clone();
    let (job_id, deduplicated) = {
        let mut registry = state.registry.lock().await;
        registry.get_or_create(key.clone(), profile)
    };

    if deduplicated {
        // Existing canonical job: discard tmp, do not enqueue.
        state.storage.cleanup_tmp(&filename).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to cleanup tmp: {}", e),
            )
        })?;
    } else {
        // New canonical job: stage tmp -> canonical input, then enqueue.
        // Order: create_job_dirs -> rename -> send. If any step fails after
        // the registry insert, roll back the dedup/jobs entries so future
        // requests for the same key can produce a fresh canonical job
        // (docs/API_SPEC.md: "creates one canonical job and enqueues it").
        let canonical_path = state.storage.job_input_path(&job_id).join("input.bin");
        let tmp_path = state.storage.tmp_path(&filename);

        let staging: Result<(), String> = async {
            state
                .storage
                .create_job_dirs(&job_id)
                .map_err(|e| format!("create_job_dirs: {}", e))?;
            fs::rename(&tmp_path, &canonical_path)
                .await
                .map_err(|e| format!("rename: {}", e))?;
            state
                .queue_tx
                .send(job_id)
                .map_err(|e| format!("queue send: {}", e))?;
            Ok(())
        }
        .await;

        if let Err(e) = staging {
            {
                let mut registry = state.registry.lock().await;
                registry.remove(&key, &job_id);
            }
            // Best-effort tmp cleanup. Ignore errors (tmp may already be
            // gone if rename succeeded before a later step failed).
            let _ = state.storage.cleanup_tmp(&filename);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("staging failed: {}", e),
            ));
        }
    }

    let response = serde_json::json!({
        "job_id": job_id.to_string(),
        "status": "queued",
        "deduplicated": deduplicated,
        "profile": profile_for_response,
        "content_hash": content_hash
    });

    Ok(Json(response))
}

fn status_str(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Succeeded => "succeeded",
        JobStatus::Failed => "failed",
    }
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    // docs/ARCHITECTURE.md: snapshot under lock, then serialize after lock drops.
    let job = {
        let registry = state.registry.lock().await;
        registry.get_job(&job_id).cloned()
    }
    .ok_or((StatusCode::NOT_FOUND, format!("unknown_job_id: {}", job_id)))?;

    Ok(Json(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "status": status_str(job.status),
        "profile": job.profile,
        "content_hash": job.content_hash,
        "created_at": job.created_at.to_rfc3339(),
        "started_at": job.started_at.map(|t| t.to_rfc3339()),
        "finished_at": job.finished_at.map(|t| t.to_rfc3339()),
        "artifact": job.artifact.as_ref().map(|a| a.path.display().to_string()),
        "error": job.error,
    })))
}

pub async fn list_jobs(State(state): State<AppState>) -> Json<serde_json::Value> {
    // docs/ARCHITECTURE.md: snapshot under lock, then serialize after lock drops.
    let jobs: Vec<Job> = {
        let registry = state.registry.lock().await;
        registry.get_jobs().into_iter().cloned().collect()
    };

    let job_list: Vec<serde_json::Value> = jobs
        .iter()
        .map(|j| {
            serde_json::json!({
                "job_id": j.job_id.to_string(),
                "status": status_str(j.status),
                "profile": j.profile,
                "content_hash": j.content_hash,
            })
        })
        .collect();

    Json(serde_json::json!({ "jobs": job_list }))
}

pub async fn get_artifact(
    State(state): State<AppState>,
    Path(job_id): Path<uuid::Uuid>,
) -> Result<axum::response::Response, (StatusCode, String)> {
    // docs/ARCHITECTURE.md: snapshot under lock, drop guard before any .await.
    let job = {
        let registry = state.registry.lock().await;
        registry.get_job(&job_id).cloned()
    }
    .ok_or((StatusCode::NOT_FOUND, format!("unknown_job_id: {}", job_id)))?;

    let internal = |msg: String| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal_error: {}", msg),
        )
    };

    match job.status {
        JobStatus::Queued | JobStatus::Running => {
            Err((StatusCode::CONFLICT, "artifact_not_ready".to_string()))
        }
        JobStatus::Failed => Err((
            StatusCode::CONFLICT,
            format!("job_failed: {}", job.error.as_deref().unwrap_or("")),
        )),
        JobStatus::Succeeded => {
            let path = job
                .artifact
                .ok_or_else(|| internal(format!("profile '{}' artifact missing", job.profile)))?
                .path;
            let profile = state
                .transcoder
                .get_profile(&job.profile)
                .ok_or_else(|| internal(format!("profile '{}' missing at runtime", job.profile)))?;
            let ext = profile.output_extension.clone();
            let mime = ext_to_mime(&ext);

            // Stream the artifact to avoid loading large files into memory.
            // Body::from_stream does not set Content-Length; do it explicitly.
            let len = fs::metadata(&path)
                .await
                .map_err(|e| internal(e.to_string()))?
                .len();
            let file = fs::File::open(&path)
                .await
                .map_err(|e| internal(e.to_string()))?;
            let body = Body::from_stream(ReaderStream::new(file));

            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .header(
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"{}.{}\"", job_id, ext),
                )
                .header(header::CONTENT_LENGTH, len.to_string())
                .body(body)
                .map_err(|e| internal(e.to_string()))
        }
    }
}

pub fn create_router(
    registry: RegistryArc,
    storage: Arc<Storage>,
    queue_tx: JobSender,
    transcoder: Arc<dyn Transcoder>,
) -> Router {
    let state = AppState {
        registry,
        storage,
        queue_tx,
        transcoder,
    };

    // axum 0.7 + matchit 0.7 use ":name" path-param syntax; "{name}" is matchit 0.8 / axum 0.8.
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
    use crate::ffmpeg::{FfmpegProfile, FfmpegRunner};
    use crate::queue::create_queue;
    use crate::shared::Job;

    #[test]
    fn ext_to_mime_covers_known_and_unmapped_branches() {
        assert_eq!(ext_to_mime("mp4"), "video/mp4");
        assert_eq!(ext_to_mime("webm"), "video/webm");
        assert_eq!(ext_to_mime("mp3"), "audio/mpeg");
        // Anything that passes `validate_output_extension` but is not in the
        // known table must still fall back to a usable response type.
        assert_eq!(ext_to_mime("mov"), "application/octet-stream");
    }

    fn make_state_with_succeeded_job(
        ext: &str,
        profile_name: &str,
        artifact_bytes: &[u8],
    ) -> (AppState, uuid::Uuid, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let artifact_path = tmp.path().join(format!("proxy.{}", ext));
        std::fs::write(&artifact_path, artifact_bytes).expect("write artifact");

        let registry = create_registry();
        let job_id = uuid::Uuid::new_v4();
        {
            let mut reg = registry.try_lock().expect("uncontended");
            let mut job = Job::new(job_id, profile_name.to_string(), "deadbeef".to_string());
            job.start().expect("start");
            job.succeed(artifact_path.clone()).expect("succeed");
            reg.jobs.insert(job_id, job);
        }

        let storage = Arc::new(Storage::new(tmp.path()));
        let transcoder: Arc<dyn Transcoder> = Arc::new(FfmpegRunner::new(vec![FfmpegProfile {
            name: profile_name.to_string(),
            args: vec![],
            output_extension: ext.to_string(),
        }]));

        let (queue_tx, _queue_rx) = create_queue();
        let state = AppState {
            registry,
            storage,
            queue_tx,
            transcoder,
        };
        (state, job_id, tmp)
    }

    fn header_str<'a>(resp: &'a axum::response::Response, name: &str) -> &'a str {
        resp.headers()
            .get(name)
            .expect("header present")
            .to_str()
            .expect("ascii")
    }

    #[tokio::test]
    async fn artifact_headers_derive_from_output_extension() {
        let cases: &[(&str, &str, &str, &[u8])] = &[
            ("mp4", "web_720p", "video/mp4", b"\0\0\0 ftypmp42"),
            ("webm", "web_720p_webm", "video/webm", b"\x1aE\xdf\xa3"),
        ];
        for (ext, profile_name, mime, bytes) in cases {
            let (state, job_id, _tmp) = make_state_with_succeeded_job(ext, profile_name, bytes);
            let resp = get_artifact(State(state), Path(job_id)).await.expect("ok");
            assert_eq!(header_str(&resp, "Content-Type"), *mime);
            assert_eq!(
                header_str(&resp, "Content-Disposition"),
                format!("attachment; filename=\"{}.{}\"", job_id, ext)
            );
        }
    }

    #[tokio::test]
    async fn artifact_handler_returns_500_when_profile_lookup_fails() {
        // Build state with a profile that does NOT match the job's profile name.
        let tmp = tempfile::tempdir().expect("tempdir");
        let artifact_path = tmp.path().join("proxy.mp4");
        std::fs::write(&artifact_path, b"x").expect("write");

        let registry = create_registry();
        let job_id = uuid::Uuid::new_v4();
        {
            let mut reg = registry.try_lock().expect("uncontended");
            let mut job = Job::new(job_id, "vanished_profile".to_string(), "h".to_string());
            job.start().expect("start");
            job.succeed(artifact_path).expect("succeed");
            reg.jobs.insert(job_id, job);
        }

        let storage = Arc::new(Storage::new(tmp.path()));
        let transcoder: Arc<dyn Transcoder> = Arc::new(FfmpegRunner::new(vec![FfmpegProfile {
            name: "different_profile".to_string(),
            args: vec![],
            output_extension: "mp4".to_string(),
        }]));
        let (queue_tx, _queue_rx) = create_queue();
        let state = AppState {
            registry,
            storage,
            queue_tx,
            transcoder,
        };

        let err = get_artifact(State(state), Path(job_id))
            .await
            .expect_err("err");
        assert_eq!(err.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(err.1.contains("profile 'vanished_profile'"), "{}", err.1);
    }
}
