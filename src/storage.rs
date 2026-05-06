use anyhow::Result;
use std::path::{Path, PathBuf};
use uuid::Uuid;

#[derive(Clone)]
pub struct Storage {
    base_path: PathBuf,
}

impl Storage {
    pub fn new(base_path: impl AsRef<Path>) -> Self {
        Storage {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    pub fn ensure_exists(&self) -> Result<()> {
        std::fs::create_dir_all(self.base_path.join("tmp"))?;
        Ok(())
    }

    pub fn tmp_path(&self, filename: &str) -> PathBuf {
        self.base_path.join("tmp").join(filename)
    }

    pub fn job_input_path(&self, job_id: &Uuid) -> PathBuf {
        self.base_path.join("jobs").join(job_id.to_string()).join("input")
    }

    pub fn job_output_path(&self, job_id: &Uuid) -> PathBuf {
        self.base_path.join("jobs").join(job_id.to_string()).join("output")
    }

    pub fn create_job_dirs(&self, job_id: &Uuid) -> Result<()> {
        let input_path = self.job_input_path(job_id);
        let output_path = self.job_output_path(job_id);
        std::fs::create_dir_all(&input_path)?;
        std::fs::create_dir_all(&output_path)?;
        Ok(())
    }

    pub fn cleanup_tmp(&self, filename: &str) -> Result<()> {
        let path = self.tmp_path(filename);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }
}
