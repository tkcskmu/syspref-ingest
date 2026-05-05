# Test Plan

This document describes the planned tests. Tests will be implemented along with the server in later PRs.

## Primary Safety Property

```text
For the same (content_sha256, profile_name), only one canonical job is created.
```

## Core Test Order

| Order | Test | Level | Purpose |
|---:|---|---|---|
| 1 | `tests/dedup_registry_concurrent.rs` | registry integration | Prove atomic check-and-insert under concurrent tasks |
| 2 | `invalid_status_transition` | unit | Prove the job state machine rejects invalid transitions |
| 3 | `worker_no_duplicate_processing` | integration | Prove one queued job is processed at most once |
| 4 | `incomplete_upload_no_job` | integration | Prove interrupted uploads do not register jobs |
| 5 | `same_filename_different_content` | integration | Prove filenames are not identity |
| 6 | `same_file_concurrent_upload` | HTTP integration | Prove end-to-end upload dedup after the API exists |

The first implementation milestone should prioritize `tests/dedup_registry_concurrent.rs`. HTTP-level same-file concurrent upload testing can come later, after the upload API exists.

## 1. `tests/dedup_registry_concurrent.rs`

Setup:

- create one shared `RegistryState`
- wrap it in `Arc<tokio::sync::Mutex<_>>`
- create one `DedupKey`
- spawn many Tokio tasks
- each task calls the registry `get_or_create` operation with the same key

Expected:

- exactly one canonical job is created
- all tasks receive the same `job_id`
- dedup map size is `1`
- job map contains one canonical job for the key
- no task observes a partially inserted registry state

To force a tight race window, gate every spawned task on a `tokio::sync::Barrier` so that all `get_or_create` calls fire only after every task has reached the barrier. Without that synchronization the spawn order tends to serialize the calls and the race is rarely exercised, leaving a buggy implementation green.

## 2. `invalid_status_transition`

Setup:

- create jobs in each supported state
- attempt valid and invalid transitions

Expected:

- `queued -> running` is accepted
- `running -> succeeded` is accepted
- `running -> failed` is accepted
- terminal states cannot transition back to active states
- `running -> queued` is rejected

## 3. `worker_no_duplicate_processing`

Setup:

- create multiple queued jobs
- run multiple workers
- track processing count by `job_id`

Expected:

- each `job_id` is delivered to at most one worker
- each processed job reaches `succeeded` or `failed`
- invalid status transitions do not occur

## 4. `incomplete_upload_no_job`

Setup:

- open a request connection
- begin multipart upload
- drop the connection before upload completion

Expected:

- no content hash is finalized
- no dedup entry is inserted
- no job metadata is inserted
- no job is enqueued
- temporary upload data is removed or ignored

## 5. `same_filename_different_content`

Setup:

- prepare two files with different bytes
- upload both using the original filename `sample.mp4`

Expected:

- content hashes differ
- job IDs differ
- dedup does not use original filename as identity

## 6. `same_file_concurrent_upload`

This is a later HTTP integration test after `POST /api/jobs` exists.

Setup:

- start the test server
- prepare one small sample file
- spawn N HTTP clients
- every client uploads the same file and same profile

Expected:

- all successful responses contain the same `job_id`
- `deduplicated=false` appears once
- `deduplicated=true` appears N-1 times
- FFmpeg or mock transcoder is invoked once

## Mock Transcoder

Tests that exercise the worker pool (`worker_no_duplicate_processing`, `same_file_concurrent_upload`) should run against a mock transcoder rather than spawning real FFmpeg. The mock implements the same trait/interface as the production runner, lets tests assert spawn count, and keeps CI free of FFmpeg installation requirements.

## Coding Rules

- no `unwrap()` or `expect()` outside tests
- no `.await` while holding the registry mutex guard
- no FFmpeg execution while holding the registry lock
- no upload streaming while holding the registry lock
- enqueue new jobs after dropping the registry lock
- serialize list-job responses after copying snapshots and dropping the registry lock

## CI Quality Gates

Once the Rust scaffold exists, CI should run:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all
```
