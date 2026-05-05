use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FfmpegProfile {
    pub name: String,
    pub args: Vec<String>,
    #[serde(default = "default_output_extension")]
    pub output_extension: String,
}

fn default_output_extension() -> String {
    "mp4".to_string()
}

pub(crate) fn validate_output_extension(ext: &str) -> Result<()> {
    if ext.is_empty() || ext.len() > 8 {
        return Err(anyhow!(
            "invalid output_extension '{}': length 1-8 required",
            ext
        ));
    }
    if !ext
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        return Err(anyhow!(
            "invalid output_extension '{}': only [a-z0-9] allowed (no dot, no uppercase, no slash)",
            ext
        ));
    }
    Ok(())
}

pub(crate) fn validate_profiles(profiles: &[FfmpegProfile]) -> Result<()> {
    for p in profiles {
        validate_output_extension(&p.output_extension)
            .map_err(|e| anyhow!("profile '{}': {}", p.name, e))?;
    }
    Ok(())
}

/// Worker-facing port. Production wires `FfmpegRunner` into `Arc<dyn Transcoder>`;
/// tests substitute `MockTranscoder`. See `docs/TEST_PLAN.md` Mock Transcoder section.
#[async_trait]
pub trait Transcoder: Send + Sync {
    async fn run(&self, input: &Path, output: &Path, profile: &str) -> Result<()>;
    fn get_profile(&self, name: &str) -> Option<&FfmpegProfile>;
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
            .ok_or_else(|| anyhow!("Profile not found: {}", profile_name))?;

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
        })
        .await??;

        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("FFmpeg failed with status: {}", status))
        }
    }
}

#[async_trait]
impl Transcoder for FfmpegRunner {
    async fn run(&self, input: &Path, output: &Path, profile: &str) -> Result<()> {
        FfmpegRunner::run(self, input, output, profile).await
    }

    fn get_profile(&self, name: &str) -> Option<&FfmpegProfile> {
        FfmpegRunner::get_profile(self, name)
    }
}

pub fn load_profiles_from_yaml(yaml_content: &str) -> Result<Vec<FfmpegProfile>> {
    let profiles: Vec<FfmpegProfile> = serde_yaml::from_str(yaml_content)?;
    validate_profiles(&profiles)?;
    Ok(profiles)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_profile_with_explicit_mp4() {
        let yaml = r#"
- name: web_720p
  args: ["-vf", "scale=1280:720"]
  output_extension: mp4
"#;
        let profiles = load_profiles_from_yaml(yaml).expect("should load");
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].output_extension, "mp4");
    }

    #[test]
    fn deserialize_profile_with_webm() {
        let yaml = r#"
- name: web_webm
  args: ["-c:v", "libvpx-vp9"]
  output_extension: webm
"#;
        let profiles = load_profiles_from_yaml(yaml).expect("should load");
        assert_eq!(profiles[0].output_extension, "webm");
    }

    #[test]
    fn deserialize_profile_with_mp3() {
        let yaml = r#"
- name: audio_only
  args: ["-vn"]
  output_extension: mp3
"#;
        let profiles = load_profiles_from_yaml(yaml).expect("should load");
        assert_eq!(profiles[0].output_extension, "mp3");
    }

    #[test]
    fn deserialize_profile_missing_extension_defaults_to_mp4() {
        let yaml = r#"
- name: legacy
  args: ["-vf", "scale=1280:720"]
"#;
        let profiles = load_profiles_from_yaml(yaml).expect("should load");
        assert_eq!(profiles[0].output_extension, "mp4");
    }

    #[test]
    fn validate_accepts_valid_extensions() {
        for ext in ["mp4", "webm", "mp3", "mov", "m4a", "a", "abcdefgh", "mp4a"] {
            assert!(
                validate_output_extension(ext).is_ok(),
                "expected {ext} to be valid"
            );
        }
    }

    #[test]
    fn validate_rejects_invalid_extensions() {
        for ext in [
            "",
            "abcdefghi",
            ".webm",
            "WEBM",
            "Mp4",
            "mp 4",
            "mp4/foo",
            "mp4\u{4}",
            "..",
        ] {
            assert!(
                validate_output_extension(ext).is_err(),
                "expected {ext:?} to be rejected"
            );
        }
    }

    #[test]
    fn load_profiles_rejects_invalid_extension_with_profile_name() {
        let yaml = r#"
- name: bad_profile
  args: []
  output_extension: WEBM
"#;
        let err = load_profiles_from_yaml(yaml).expect_err("should fail");
        let msg = format!("{err}");
        assert!(msg.contains("bad_profile"), "missing profile name: {msg}");
        assert!(
            msg.contains("output_extension"),
            "missing field name in error: {msg}"
        );
    }

    #[test]
    fn root_profiles_yaml_is_valid() {
        let yaml = include_str!("../profiles.yaml");
        load_profiles_from_yaml(yaml).expect("root profiles.yaml should be valid");
    }
}
