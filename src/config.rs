use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "syspref-ingest")]
#[command(about = "Video Digest Server - concurrent video upload and transcoding")]
pub struct Config {
    /// Path to FFmpeg profiles YAML file
    #[arg(short, long)]
    pub profiles_path: Option<PathBuf>,

    /// Base directory for storing files
    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// HTTP server address
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    pub bind: String,
}

impl Config {
    pub fn load() -> Result<Self> {
        Ok(Config::parse())
    }
}
