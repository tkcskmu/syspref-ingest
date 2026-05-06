---
name: Project Build Fixes
description: Fixed axum 0.7 API changes and type issues in syspref-ingest project
type: project
---

The syspref-ingest project required several fixes to build with axum 0.7:
1. **Server API change**: `axum::Server::bind()` was replaced with `tokio::net::TcpListener::bind()` + `axum::serve(listener, app)`
2. **Storage ownership**: Wrapped Storage in Arc to share between worker pool and router
3. **Module imports**: Fixed module paths from `crate::{DedupKey, Job}` to `crate::shared::{DedupKey, Job}`
4. **Type signatures**: Updated WorkerPool and AppState to use `Arc<Storage>` instead of `Storage`

**Why:** axum 0.7 changed the server API and the project had type mismatches from using Storage directly instead of Arc.

**How to apply:** When modifying server startup code or worker/pool types, ensure Storage is wrapped in Arc for shared ownership.
