//! Test helper for the worker pool. Implements `Transcoder` (the
//! production trait declared in `src/ffmpeg.rs`) without invoking the
//! real `ffmpeg` binary, so worker-pool tests can run in CI without an
//! ffmpeg installation (`docs/TEST_PLAN.md` Mock Transcoder section).
//!
//! Field/variant names match the issue body exactly: `spawn_count`,
//! `profiles`, `behavior`, with `MockBehavior::{AlwaysSucceed, AlwaysFail,
//! Custom}`. `Custom` takes a `Send + Sync` closure so callers can capture
//! external state (e.g., path log) for assertions.
//!
//! `#![allow(dead_code)]` is applied at module scope because each test
//! file that imports `mod support;` only references a subset of the
//! variants/methods, and clippy `-D warnings` would otherwise fail
//! per-target builds.

#![allow(dead_code)]

use async_trait::async_trait;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use syspref_ingest::ffmpeg::{FfmpegProfile, Transcoder};

pub type CustomFn = Arc<dyn Fn(&Path, &Path, &str) -> anyhow::Result<()> + Send + Sync>;

pub enum MockBehavior {
    AlwaysSucceed,
    AlwaysFail,
    Custom(CustomFn),
}

pub struct MockTranscoder {
    pub behavior: MockBehavior,
    pub spawn_count: Arc<AtomicUsize>,
    pub profiles: Vec<FfmpegProfile>,
}

impl MockTranscoder {
    pub fn new(behavior: MockBehavior, profiles: Vec<FfmpegProfile>) -> Self {
        Self {
            behavior,
            spawn_count: Arc::new(AtomicUsize::new(0)),
            profiles,
        }
    }

    pub fn spawn_count(&self) -> usize {
        self.spawn_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Transcoder for MockTranscoder {
    async fn run(&self, input: &Path, output: &Path, profile: &str) -> anyhow::Result<()> {
        self.spawn_count.fetch_add(1, Ordering::SeqCst);
        match &self.behavior {
            MockBehavior::AlwaysSucceed => {
                if let Some(parent) = output.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(output, b"")?;
                Ok(())
            }
            MockBehavior::AlwaysFail => Err(anyhow::anyhow!("mock failure")),
            MockBehavior::Custom(f) => f(input, output, profile),
        }
    }

    fn get_profile(&self, name: &str) -> Option<&FfmpegProfile> {
        self.profiles.iter().find(|p| p.name == name)
    }
}
