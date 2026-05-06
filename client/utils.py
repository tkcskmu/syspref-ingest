"""Helper utilities for the Video Digest client."""

import hashlib
from pathlib import Path
from typing import Optional


def compute_sha256(file_path: str, chunk_size: int = 8192) -> str:
    """
    Compute SHA-256 hash of a file.

    Args:
        file_path: Path to the file
        chunk_size: Size of chunks to read at once

    Returns:
        Hex string of the SHA-256 hash
    """
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_size(size_bytes: int) -> str:
    """Format bytes into human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def format_timestamp(dt_str: Optional[str]) -> str:
    """Format ISO timestamp for display."""
    if dt_str is None:
        return "-"
    # Remove timezone info for cleaner display
    return dt_str.replace("Z", "").replace("+00:00", "")


class ProgressBar:
    """Simple progress bar for file operations."""

    def __init__(self, total: int, description: str = ""):
        self.total = total
        self.description = description
        self.current = 0
        self.last_update = 0

    def update(self, chunk_size: int) -> None:
        """Update progress with a chunk."""
        import sys
        import time

        self.current += chunk_size
        now = time.time()

        # Update every 10% or at least every 0.5 seconds
        if (self.total > 0 and
            (self.current % max(1, self.total // 10) == 0 or now - self.last_update >= 0.5)):
            percent = (self.current / self.total) * 100 if self.total > 0 else 0
            bar_len = 40
            filled = int(bar_len * self.current / self.total) if self.total > 0 else 0
            bar = "█" * filled + "-" * (bar_len - filled)

            size_str = format_size(self.current)
            total_str = format_size(self.total)
            speed_str = ""

            print(f"\r{self.description} [{bar}] {percent:6.1f}% ({size_str}/{total_str}) {speed_str}", end="")
            self.last_update = now

    def finish(self) -> None:
        """Finish the progress bar."""
        import sys
        print()  # New line after progress bar


def get_file_size(file_path: str) -> int:
    """Get file size in bytes."""
    return Path(file_path).stat().st_size
