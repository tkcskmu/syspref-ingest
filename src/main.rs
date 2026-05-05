mod api;
mod config;
mod dedup;
mod ffmpeg;
mod queue;
mod shared;
mod storage;
mod worker;

use anyhow::Result;
use config::Config;
use dedup::{create_registry, RegistryArc};
use ffmpeg::{FfmpegProfile, FfmpegRunner, Transcoder};
use queue::create_queue;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use storage::Storage;
use tower_http::trace::TraceLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .init();

    let config = Config::load()?;

    // Initialize storage
    let storage = Storage::new(&config.data_dir);
    storage.ensure_exists()?;

    // Load FFmpeg profiles from YAML
    let profiles_path = config
        .profiles_path
        .as_deref()
        .unwrap_or(Path::new("profiles.yaml"));
    let profiles_yaml = if profiles_path.exists() {
        fs::read_to_string(profiles_path)?
    } else {
        eprintln!(
            "Profiles file not found at {:?}, using defaults",
            profiles_path
        );
        String::new()
    };

    // Parse profiles (default if empty). Fallback profiles still need to satisfy
    // validate_output_extension; YAML-loaded profiles are validated inside
    // load_profiles_from_yaml so we only re-validate on the fallback path.
    let profiles = if profiles_yaml.is_empty() {
        let fallback = vec![
            FfmpegProfile {
                name: "web_720p".to_string(),
                args: vec!["-vf".to_string(), "scale=1280:720".to_string()],
                output_extension: "mp4".to_string(),
            },
            FfmpegProfile {
                name: "web_480p".to_string(),
                args: vec!["-vf".to_string(), "scale=854:480".to_string()],
                output_extension: "mp4".to_string(),
            },
        ];
        ffmpeg::validate_profiles(&fallback)?;
        fallback
    } else {
        ffmpeg::load_profiles_from_yaml(&profiles_yaml)?
    };

    // Wire production runner behind the Transcoder port. Both the worker pool
    // and the artifact handler depend on `Arc<dyn Transcoder>`, not the
    // concrete `FfmpegRunner`, so tests can substitute a mock with the same
    // trait surface (docs/TEST_PLAN.md mock transcoder section). The trait
    // exposes `get_profile`, which the artifact handler uses for the
    // output_extension lookup added in [07/10].
    let transcoder: Arc<dyn Transcoder> = Arc::new(FfmpegRunner::new(profiles));

    // Initialize shared state
    let registry: RegistryArc = create_registry();
    let (queue_tx, queue_rx) = create_queue();
    let storage = Arc::new(storage);

    // Start worker pool (consumers share the receiver via Arc<Mutex<_>>)
    let worker_pool = worker::WorkerPool::new(
        4,
        registry.clone(),
        queue_rx.clone(),
        transcoder.clone(),
        storage.clone(),
    );
    worker_pool.start().await;

    // Create router (producer side holds the sender + profile lookup via trait)
    let app = api::create_router(registry, storage.clone(), queue_tx, transcoder)
        .layer(TraceLayer::new_for_http());

    // Start server
    let addr = config.bind.parse::<std::net::SocketAddr>()?;
    println!("Starting server on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
