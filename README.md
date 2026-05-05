# Video Digest Server

Video Digest Server is a planned Rust/Tokio service for concurrent video uploads and FFmpeg transcoding.

When multiple clients upload the same video with the same profile, the target behavior is to create one canonical transcoding job and return that job ID to duplicate requests.

> Current status: design documentation only. Implementation will be added in later PRs.

## Scope

This project is intentionally scoped as a focused Video Digest Server implementation. The goal is not to build a full video platform, but to implement the backend path needed to demonstrate concurrent access to a shared resource, explain the race condition, fix it, and prove the fix with tests.

## Core Idea

The shared resource is the in-memory job registry:

```text
RegistryState {
  dedup: HashMap<DedupKey, DedupEntry>,
  jobs: HashMap<JobId, Job>,
}
```

The deduplication key is:

```text
DedupKey = (content_sha256, profile_name)
```

The main concurrency issue is a logical check-then-insert race in `dedup`. This is an application-level race condition, not a Rust memory data race.

## System Overview

```mermaid
flowchart LR
    C["Concurrent Clients"] -->|"POST /api/jobs"| API["Axum HTTP API"]
    API --> U["Upload Stream"]
    U --> H["Temp File + SHA-256"]
    H --> R["RegistryState<br/>Arc&lt;Mutex&gt;"]
    R -->|"new canonical job_id"| Q["Job Queue"]
    Q --> W["Worker Pool"]
    W --> F["FFmpeg"]
    F --> A["Local Artifact"]
```

Runtime state is kept in memory and is lost on restart. Uploaded files and output artifacts are stored on the local filesystem.

## Concurrency Goals

| Goal | Project behavior |
|---|---|
| Concurrent requests to the same resource | Multiple clients can upload the same bytes with the same profile at the same time |
| Race condition to analyze | Naive dedup lookup can create duplicate jobs for one `(content_sha256, profile_name)` |
| Fix | Protect `RegistryState` with `Arc<tokio::sync::Mutex<_>>` and keep lookup plus insert in one critical section |
| Evidence | Registry concurrency tests, upload integration tests, worker processing tests, and CI quality gates |

## Race Condition

Naive check-then-insert can create duplicate jobs:

```mermaid
sequenceDiagram
    autonumber
    participant A as Request A
    participant B as Request B
    participant R as Dedup HashMap

    A->>R: check key
    R-->>A: missing
    B->>R: check same key
    R-->>B: missing
    A->>A: create job J1
    B->>B: create job J2
    A->>R: insert J1
    B->>R: insert J2
```

The planned fix is to perform lookup and registry mutation inside one mutex-protected critical section:

```mermaid
flowchart TD
    L["lock registry"] --> E{"dedup.entry(key)"}
    E -->|"occupied"| O["copy existing job snapshot"]
    E -->|"vacant"| N["create job_id"]
    N --> D["insert dedup entry"]
    D --> J["insert job metadata"]
    O --> U["unlock registry"]
    J --> U
    U --> Q{"new job?"}
    Q -->|"yes"| EN["enqueue job_id outside lock"]
    Q -->|"no"| R["return existing job"]
```

The mutex protects registry mutation only. Upload streaming, hashing, FFmpeg execution, artifact download, queue enqueue, and response serialization happen outside the lock.

## Request Flow

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as API
    participant R as Registry
    participant Q as Queue
    participant W as Worker

    C->>API: POST /api/jobs
    API->>API: stream file + compute SHA-256
    API->>R: lock + get_or_create(DedupKey)
    alt new key
        R-->>API: new job_id
        API->>Q: enqueue job_id outside lock
        API-->>C: deduplicated=false, status=queued
    else existing key
        R-->>API: existing job snapshot
        API-->>C: deduplicated=true, current status
    end
    Q->>W: deliver job_id to one worker
    W->>W: run FFmpeg profile
```

If an upload is interrupted, the hash is not finalized and no job is registered.

## Failure Handling

| Failure | Effect |
|---|---|
| Upload connection dropped mid-stream | Hash is not finalized, no dedup entry, no job registered, temp file discarded |
| Client A drops mid-upload, then client B uploads the same bytes | A leaves no trace; B's upload computes its own hash and becomes the canonical job |
| FFmpeg exits non-zero | Job transitions `running -> failed`, `error` field stores a summary |
| Worker crash mid-FFmpeg | Job may remain `running`; automatic recovery is out of scope |
| Server restart | All in-memory job state is lost; uploaded inputs and finished artifacts on disk are not garbage-collected automatically |

## Job States

```mermaid
stateDiagram-v2
    [*] --> queued
    queued --> running
    running --> succeeded
    running --> failed
    succeeded --> [*]
    failed --> [*]
```

Invalid transitions are rejected.

## API Surface

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/health` | Health check |
| `POST` | `/api/jobs` | Upload video and create or reuse a job |
| `GET` | `/api/jobs` | List recent jobs |
| `GET` | `/api/jobs/{job_id}` | Read job status |
| `GET` | `/api/jobs/{job_id}/artifact` | Download succeeded artifact |

Clients send only a profile name. Raw FFmpeg arguments from clients are not accepted.

## In Scope

- Rust/Tokio + Axum API
- multipart uploads
- SHA-256 content hashing
- in-memory dedup registry
- worker queue
- YAML FFmpeg profiles
- local filesystem artifacts
- focused concurrency and state-machine tests

## Out of Scope

- database persistence, including SQLite and PostgreSQL
- authentication and session management
- web dashboard or required UI
- distributed workers
- resumable uploads
- cloud object storage
- GPU acceleration or advanced codec tuning
- runtime profile reload
- production-grade observability and deployment automation

## Quality Gates

Once the Rust scaffold exists, CI should run:

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all
```

Implementation code should not use `unwrap()` or `expect()` outside tests.

## Benchmark Results

Benchmark numbers are not target KPIs and should not be filled in before implementation. After implementation, measured results can be recorded with this template:

| Scenario | Clients | Profile | Canonical jobs | Dedup hits | FFmpeg spawns | Duplicate jobs | Elapsed |
|---|---:|---|---:|---:|---:|---:|---:|
| same file concurrent upload | TBD | TBD | TBD | TBD | TBD | TBD | TBD |

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [API specification](docs/API_SPEC.md)
- [Test plan](docs/TEST_PLAN.md)
