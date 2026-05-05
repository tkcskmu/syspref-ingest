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
use ffmpeg::{FfmpegProfile, FfmpegRunner};
use queue::create_queue;
use std::fs;
use std::path::Path;
use storage::Storage;
use tower_http::trace::{TraceLayer};
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
    let profiles_path = config.profiles_path.as_deref().unwrap_or(Path::new("profiles.yaml"));
    let profiles_yaml = if profiles_path.exists() {
        fs::read_to_string(profiles_path)?
    } else {
        eprintln!("Profiles file not found at {:?}, using defaults", profiles_path);
        String::new()
    };

    // Parse profiles (default if empty)
    let profiles = if profiles_yaml.is_empty() {
        vec![
            FfmpegProfile {
                name: "web_720p".to_string(),
                args: vec!["-vf".to_string(), "scale=1280:720".to_string()],
            },
            FfmpegProfile {
                name: "web_480p".to_string(),
                args: vec!["-vf".to_string(), "scale=854:480".to_string()],
            },
        ]
    } else {
        ffmpeg::load_profiles_from_yaml(&profiles_yaml)?
    };

    let ffmpeg_runner = FfmpegRunner::new(profiles);

    // Initialize shared state
    let registry: RegistryArc = create_registry();
    let queue = create_queue();
    let storage = std::sync::Arc::new(storage);

    // Start worker pool
    let worker_pool = worker::WorkerPool::new(4, registry.clone(), queue.clone(), ffmpeg_runner, storage.clone());
    worker_pool.start().await;

    // Create router
    let app = api::create_router(registry, storage.clone())
        .layer(TraceLayer::new_for_http());

    // Start server
    let addr = config.bind.parse::<std::net::SocketAddr>()?;
    println!("Starting server on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
