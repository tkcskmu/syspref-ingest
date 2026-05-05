"""HTTP API wrappers for Video Digest Server."""

import aiohttp
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, List
import json


@dataclass
class JobResponse:
    """Response from job creation/upload."""
    job_id: str
    status: str
    deduplicated: bool
    profile: str
    content_hash: str

    @classmethod
    def from_dict(cls, data: dict) -> "JobResponse":
        return cls(
            job_id=data["job_id"],
            status=data["status"],
            deduplicated=data["deduplicated"],
            profile=data["profile"],
            content_hash=data.get("content_hash", ""),
        )


@dataclass
class JobStatusResponse:
    """Response from job status check."""
    job_id: str
    status: str
    profile: str
    content_hash: str
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    artifact_path: Optional[str]
    error: Optional[str]

    @classmethod
    def from_dict(cls, data: dict) -> "JobStatusResponse":
        def parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
            if dt_str is None:
                return None
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

        artifact = data.get("artifact")
        artifact_path = artifact["path"] if artifact else None

        return cls(
            job_id=data["job_id"],
            status=data["status"],
            profile=data["profile"],
            content_hash=data["content_hash"],
            created_at=parse_datetime(data["created_at"]),
            started_at=parse_datetime(data.get("started_at")),
            finished_at=parse_datetime(data.get("finished_at")),
            artifact_path=artifact_path,
            error=data.get("error"),
        )


@dataclass
class JobSummary:
    """Summary of a job for listing."""
    job_id: str
    status: str
    profile: str
    content_hash: str

    @classmethod
    def from_dict(cls, data: dict) -> "JobSummary":
        return cls(
            job_id=data["job_id"],
            status=data["status"],
            profile=data["profile"],
            content_hash=data["content_hash"],
        )


@dataclass
class JobListResponse:
    """Response from jobs list."""
    jobs: List[JobSummary]

    @classmethod
    def from_dict(cls, data: dict) -> "JobListResponse":
        return cls(
            jobs=[JobSummary.from_dict(j) for j in data.get("jobs", [])]
        )


class VideoDigestClient:
    """Async HTTP client for the Video Digest Server API."""

    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url.rstrip("/")

    async def upload_file(self, file_path: str, profile: str) -> JobResponse:
        """
        Upload a video file to create or deduplicate a transcoding job.

        Args:
            file_path: Path to the video file
            profile: Profile name defined in YAML config

        Returns:
            JobResponse with job_id and deduplication status
        """
        url = f"{self.base_url}/api/jobs"
        file_path_obj = Path(file_path)

        if not file_path_obj.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Read file for upload
        with open(file_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("file", f, filename=file_path_obj.name)
            data.add_field("profile", profile)

            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=data) as response:
                    response.raise_for_status()
                    result = await response.json()

        return JobResponse.from_dict(result)

    async def get_job(self, job_id: str) -> JobStatusResponse:
        """
        Get the status of a job.

        Args:
            job_id: The UUID of the job

        Returns:
            JobStatusResponse with full job details
        """
        url = f"{self.base_url}/api/jobs/{job_id}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 404:
                    raise ValueError(f"Job not found: {job_id}")
                response.raise_for_status()
                result = await response.json()

        return JobStatusResponse.from_dict(result)

    async def download_artifact(self, job_id: str, output_path: str) -> None:
        """
        Download the transcoding artifact.

        Args:
            job_id: The UUID of the completed job
            output_path: Path to save the downloaded file

        Raises:
            ValueError: If job not found or artifact not ready/failed
        """
        url = f"{self.base_url}/api/jobs/{job_id}/artifact"
        output_path_obj = Path(output_path)

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 404:
                    raise ValueError(f"Job not found: {job_id}")
                elif response.status == 409:
                    raise ValueError("Artifact not ready or job failed")
                response.raise_for_status()

                # Write file in chunks
                output_path_obj.parent.mkdir(parents=True, exist_ok=True)
                with open(output_path, "wb") as f:
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)

    async def list_jobs(self, limit: int = 10) -> JobListResponse:
        """
        List recent jobs.

        Args:
            limit: Maximum number of jobs to return

        Returns:
            JobListResponse with list of job summaries
        """
        url = f"{self.base_url}/api/jobs"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                result = await response.json()

        return JobListResponse.from_dict(result)

    async def wait_for_completion(
        self, job_id: str, poll_interval: float = 2.0
    ) -> JobStatusResponse:
        """
        Poll a job until it reaches terminal state.

        Args:
            job_id: The UUID of the job
            poll_interval: Seconds between status checks

        Returns:
            Final JobStatusResponse (succeeded or failed)
        """
        import asyncio

        while True:
            status = await self.get_job(job_id)
            if status.status in ("succeeded", "failed"):
                return status
            await asyncio.sleep(poll_interval)
