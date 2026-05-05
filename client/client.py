#!/usr/bin/env python3
"""Video Digest Client - Command-line interface for the Video Digest Server."""

import argparse
import asyncio
import sys

from api import VideoDigestClient, JobResponse, JobStatusResponse, JobListResponse
from utils import format_duration, format_size


def print_job_response(job: JobResponse) -> None:
    """Print job creation response."""
    dedup_str = " (deduplicated)" if job.deduplicated else ""
    print(f"Job created: {job.job_id}{dedup_str}")
    print(f"  Status: {job.status}")
    print(f"  Profile: {job.profile}")
    print(f"  Content hash: {job.content_hash}")


def print_job_status(status: JobStatusResponse) -> None:
    """Print job status information."""
    print(f"Job ID: {status.job_id}")
    print(f"  Status: {status.status.upper()}")
    print(f"  Profile: {status.profile}")
    print(f"  Content hash: {status.content_hash}")
    print(f"  Created: {format_timestamp(status.created_at)}")
    if status.started_at:
        print(f"  Started: {format_timestamp(status.started_at)}")
    if status.finished_at:
        print(f"  Finished: {format_timestamp(status.finished_at)}")

    if status.artifact_path:
        print(f"  Artifact: {status.artifact_path}")
    if status.error:
        print(f"  Error: {status.error}")


def format_timestamp(value) -> str:
    """Format an ISO timestamp for display.

    Accepts either a `datetime` (the shape produced by ``JobStatusResponse``)
    or a raw ISO string. ``datetime`` objects also have a ``replace`` method
    but its signature collides with ``str.replace``; routing through
    ``isoformat()`` first avoids that footgun.
    """
    if value is None:
        return "-"
    s = value.isoformat() if hasattr(value, "isoformat") else str(value)
    return s.replace("Z", "").replace("+00:00", "")


async def cmd_upload(client: VideoDigestClient, args: argparse.Namespace) -> int:
    """Handle the upload command."""
    try:
        job = await client.upload_file(args.file, args.profile)
        print_job_response(job)

        if not args.no_wait and not job.deduplicated:
            # Wait for completion only if it's a new job
            print("\nWaiting for transcoding to complete...")
            try:
                final_status = await client.wait_for_completion(job.job_id)
                print_job_status(final_status)
            except KeyboardInterrupt:
                print("\nCancelled")
                return 130
        elif job.deduplicated:
            print("\nNote: Job was deduplicated - no new transcoding will occur")

        return 0

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Upload failed: {e}", file=sys.stderr)
        return 1


async def cmd_status(client: VideoDigestClient, args: argparse.Namespace) -> int:
    """Handle the status command."""
    try:
        status = await client.get_job(args.job_id)

        if args.poll:
            print(f"Polling for job {args.job_id} (Ctrl+C to cancel)...")
            while True:
                status = await client.get_job(args.job_id)
                print(f"\rStatus: {status.status}", end="", flush=True)
                if status.status in ("succeeded", "failed"):
                    break
                import asyncio
                await asyncio.sleep(2)

        print()
        print_job_status(status)
        return 0

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nCancelled")
        return 130
    except Exception as e:
        print(f"Failed to get status: {e}", file=sys.stderr)
        return 1


async def cmd_download(client: VideoDigestClient, args: argparse.Namespace) -> int:
    """Handle the download command."""
    try:
        await client.download_artifact(args.job_id, args.output)

        import os
        size = os.path.getsize(args.output)
        print(f"Artifact downloaded to: {args.output}")
        print(f"Size: {format_size(size)}")
        return 0

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Download failed: {e}", file=sys.stderr)
        return 1


async def cmd_list(client: VideoDigestClient, args: argparse.Namespace) -> int:
    """Handle the list command."""
    try:
        result = await client.list_jobs(limit=args.limit)

        if not result.jobs:
            print("No jobs found.")
            return 0

        # Print table header
        print(f"{'Job ID':<36} {'Status':<12} {'Profile'}")
        print("-" * 70)

        for job in result.jobs:
            print(f"{job.job_id[:36]:<36} {job.status:<12} {job.profile}")

        return 0

    except Exception as e:
        print(f"Failed to list jobs: {e}", file=sys.stderr)
        return 1


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="client.py",
        description="Video Digest Client - Upload and manage transcoding jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s upload --file video.mp4 --profile web_720p
  %(prog)s status abc-123-def --poll
  %(prog)s download abc-123-def --output output.mp4
  %(prog)s list --limit 5
        """,
    )

    parser.add_argument(
        "--base-url",
        default="http://localhost:8080",
        help="Server base URL (default: http://localhost:8080)",
    )

    subparsers = parser.add_subparsers(dest="command", title="commands", required=True)

    # Upload command
    upload_parser = subparsers.add_parser(
        "upload",
        help="Upload a video file for transcoding",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  %(prog)s --file video.mp4 --profile web_720p
        """,
    )
    upload_parser.add_argument(
        "--file", "-f", required=True, help="Path to the video file to upload"
    )
    upload_parser.add_argument(
        "--profile", "-p", required=True, help="Profile name (e.g., web_720p)"
    )
    upload_parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Do not wait for transcoding to complete",
    )

    # Status command
    status_parser = subparsers.add_parser(
        "status",
        help="Check job status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  %(prog)s abc-123-def --poll
        """,
    )
    status_parser.add_argument("job_id", help="The UUID of the job")
    status_parser.add_argument(
        "--poll",
        action="store_true",
        help="Poll until job reaches terminal state",
    )

    # Download command
    download_parser = subparsers.add_parser(
        "download",
        help="Download artifact from completed job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  %(prog)s abc-123-def --output output.mp4
        """,
    )
    download_parser.add_argument("job_id", help="The UUID of the job")
    download_parser.add_argument(
        "--output", "-o", required=True, help="Output file path"
    )

    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List recent jobs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  %(prog)s --limit 10
        """,
    )
    list_parser.add_argument(
        "--limit", "-l", type=int, default=10, help="Maximum number of jobs to show (default: 10)"
    )

    return parser


async def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    client = VideoDigestClient(args.base_url)

    if args.command == "upload":
        return await cmd_upload(client, args)
    elif args.command == "status":
        return await cmd_status(client, args)
    elif args.command == "download":
        return await cmd_download(client, args)
    elif args.command == "list":
        return await cmd_list(client, args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
