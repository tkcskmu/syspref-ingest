# API Specification

This document describes the API contract; the current implementation matches.

## Base URL

```text
http://localhost:8080
```

## Profile Rule

Clients send only a profile name, for example `web_720p`.

Raw FFmpeg arguments from clients are not accepted. FFmpeg arguments are loaded from server-side YAML profile configuration.

### Profile Configuration

Profiles are loaded at startup from `profiles.yaml` at the repository root (overridable via `--profiles-path`). When the file is absent, the server falls back to built-in profiles (`web_720p`, `web_480p`, both `mp4`).

Each profile entry has the following shape:

```yaml
- name: web_720p
  args: ["-vf", "scale=1280:720"]
  output_extension: mp4
```

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `name` | string | yes | — | Profile identifier sent by clients. |
| `args` | array of strings | yes | — | FFmpeg arguments inserted between input and output paths. |
| `output_extension` | string | no | `"mp4"` | Used both as the worker output filename `proxy.{output_extension}` and the artifact download filename `{job_id}.{output_extension}`. Must match `^[a-z0-9]{1,8}$` — no leading dot, no uppercase, no slashes, no whitespace, non-empty. The server fails to start if any profile violates this constraint. |

## Error Shape

Error responses use a stable JSON shape:

```json
{
  "error": {
    "code": "unknown_profile",
    "message": "profile does not exist"
  }
}
```

## `GET /api/health`

Response:

```json
{
  "status": "ok"
}
```

## `POST /api/jobs`

Create or deduplicate a transcoding job.

Request:

```text
Content-Type: multipart/form-data
```

| Field | Type | Required | Description |
|---|---|---|---|
| `file` | file | yes | Uploaded video file |
| `profile` | string | yes | Server-side YAML profile name |

Behavior:

- The server streams the file to a temporary path.
- The server computes SHA-256 while reading the stream.
- The dedup key is `(content_sha256, profile_name)`.
- If the key is new, the server creates one canonical job and enqueues it.
- If the key already exists, the server returns the existing job ID and current job status.

New job response:

```json
{
  "job_id": "uuid",
  "status": "queued",
  "deduplicated": false,
  "profile": "web_720p",
  "content_hash": "sha256..."
}
```

Deduplicated response:

```json
{
  "job_id": "uuid",
  "status": "running",
  "deduplicated": true,
  "profile": "web_720p",
  "content_hash": "sha256..."
}
```

The `status` in a deduplicated response is the existing job's current status. It is not always `queued`.

Success status codes:

| Status | Condition |
|---:|---|
| 201 | New canonical job created |
| 200 | Existing job returned (`deduplicated=true`) |

Deduplication property: for a fixed `(content_sha256, profile_name)`, repeated `POST /api/jobs` calls return the same `job_id` until that job is purged from in-memory state. The first request creates the canonical job with status `queued`; subsequent requests return the same `job_id` along with the canonical job's current status.

Errors:

| Status | Code | Condition |
|---:|---|---|
| 400 | `missing_file` | multipart request has no `file` field |
| 400 | `missing_profile` | multipart request has no `profile` field |
| 400 | `unknown_profile` | profile name is not configured |
| 400 | `invalid_multipart` | multipart wire-protocol parse failure: missing or invalid `boundary`, non-multipart `Content-Type`, truncated body, multipart header parse error, or invalid UTF-8 in the `profile` field |
| 413 | `payload_too_large` | encoded multipart body exceeds the configured maximum size |

The `payload_too_large` limit is applied to the entire encoded multipart body — boundary delimiters, part headers, and file bytes summed — not to the raw file contents alone. The limit is configured per-request via the `--max-upload-bytes` CLI flag and is not a concurrency cap.

If the client disconnects before upload completion, the server may not be able to send a response. The server-side behavior is still defined: no content hash is finalized, no job metadata is inserted, and no job is enqueued.

## `GET /api/jobs/{job_id}`

Response:

```json
{
  "job_id": "uuid",
  "status": "succeeded",
  "profile": "web_720p",
  "content_hash": "sha256...",
  "created_at": "2026-04-27T00:00:00Z",
  "started_at": "2026-04-27T00:00:01Z",
  "finished_at": "2026-04-27T00:00:03Z",
  "artifact": {
    "path": "data/jobs/{job_id}/output/proxy.{output_extension}"
  },
  "error": null
}
```

Errors:

| Status | Code | Condition |
|---:|---|---|
| 404 | `unknown_job_id` | no job exists for `job_id` |

## `GET /api/jobs`

Returns recent jobs.

Response:

```json
{
  "jobs": [
    {
      "job_id": "uuid",
      "status": "succeeded",
      "profile": "web_720p",
      "content_hash": "sha256..."
    }
  ]
}
```

## `GET /api/jobs/{job_id}/artifact`

Downloads the output artifact.

Rules:

- `succeeded`: return artifact file
- `queued` or `running`: return artifact-not-ready error
- `failed`: return job-failed error
- unknown `job_id`: return not-found error

Successful response headers:

| Header | Value |
|---|---|
| `Content-Type` | derived from the profile's `output_extension` (see MIME mapping below) |
| `Content-Disposition` | `attachment; filename="{job_id}.{output_extension}"` |
| `Content-Length` | artifact byte size |

MIME mapping from `output_extension` to `Content-Type`:

| `output_extension` | `Content-Type` |
|---|---|
| `mp4` | `video/mp4` |
| `webm` | `video/webm` |
| `mp3` | `audio/mpeg` |
| (other) | `application/octet-stream` |

Errors:

| Status | Code | Condition |
|---:|---|---|
| 404 | `unknown_job_id` | no job exists for `job_id` |
| 409 | `artifact_not_ready` | job is still `queued` or `running` |
| 409 | `job_failed` | job reached `failed` |
