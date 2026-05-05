# Video Digest Client

A Python command-line client for the Video Digest Server.

## Requirements

- Python 3.8 or higher
- pip (Python package manager)

## Installation

```bash
cd client
pip install -r requirements.txt
```

## Usage

### Upload a File

```bash
python client.py upload --file video.mp4 --profile web_720p
```

This will:
1. Upload the file to the server
2. Wait for transcoding to complete (unless `--no-wait` is specified)
3. Display job status and artifact path

### Check Job Status

```bash
python client.py status <job_id>
```

To poll continuously until completion:

```bash
python client.py status <job_id> --poll
```

### Download Artifact

```bash
python client.py download <job_id> --output output.mp4
```

### List Recent Jobs

```bash
python client.py list --limit 10
```

## Options

- `--base-url URL`: Set the server base URL (default: `http://localhost:8080`)

## Examples

```bash
# Upload with default profile
python client.py upload --file sample.mp4 --profile web_720p

# Upload without waiting for completion
python client.py upload --file sample.mp4 --profile web_720p --no-wait

# Poll status every 2 seconds
python client.py status abc-123-def --poll

# Download to specific path
python client.py download abc-123-def --output /tmp/output.mp4
```

## Error Handling

The client will exit with non-zero status on errors:
- File not found: exit 1
- Job not found: exit 1
- Server error: exit 1
- Network error: exit 1
