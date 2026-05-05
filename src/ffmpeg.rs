use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FfmpegProfile {
    pub name: String,
    pub args: Vec<String>,
}

#[derive(Clone)]
pub struct FfmpegRunner {
    profiles: Vec<FfmpegProfile>,
}

impl FfmpegRunner {
    pub fn new(profiles: Vec<FfmpegProfile>) -> Self {
        FfmpegRunner { profiles }
    }

    pub fn get_profile(&self, name: &str) -> Option<&FfmpegProfile> {
        self.profiles.iter().find(|p| p.name == name)
    }

    pub async fn run(
        &self,
        input_path: impl AsRef<std::path::Path>,
        output_path: impl AsRef<std::path::Path>,
        profile_name: &str,
    ) -> Result<()> {
        let profile = self
            .get_profile(profile_name)
            .ok_or_else(|| anyhow::anyhow!("Profile not found: {}", profile_name))?;

        // Convert paths to OsStr for Command
        let input_os = input_path.as_ref().as_os_str();
        let output_os = output_path.as_ref().as_os_str();

        let mut cmd = Command::new("ffmpeg");
        cmd.arg("-i")
            .arg(input_os)
            .args(&profile.args)
            .arg(output_os);

        // Use spawn + wait for async execution
        let child = cmd.spawn()?;
        let status = tokio::task::spawn_blocking(move || {
            let mut c = child;
            c.wait()
        }).await??;
        
        if status.success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("FFmpeg failed with status: {}", status))
        }
    }
}

pub fn load_profiles_from_yaml(yaml_content: &str) -> Result<Vec<FfmpegProfile>> {
    let profiles: Vec<FfmpegProfile> = serde_yaml::from_str(yaml_content)?;
    Ok(profiles)
}
