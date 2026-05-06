# Example Client Plan

## Purpose

Create a simple command-line example client to demonstrate the server API. This client will:

- Upload files with multipart form data
- Handle deduplication responses
- Poll for job status
- Download artifacts on success

The client is for **demonstration and testing**, not production use.

---

## Implementation Language

**Python 3.x** (recommended: 3.8+)

Rationale:
- No compilation required, easy to run
- Rich ecosystem for HTTP and file operations
- Readable example code for non-Rust developers
- Cross-platform (Windows/macOS/Linux)

Alternative: Rust CLI client if the project wants to stay within Rust ecosystem.

---

## Client Features

### 1. Upload a File

```bash
python client.py upload --file video.mp4 --profile web_720p
```

**Behavior:**
- Reads file in chunks
- Builds multipart form-data with `file` and `profile` fields
- POSTs to `/api/jobs`
- Parses response for `job_id` and `deduplicated` flag
- Outputs job information

### 2. Poll Job Status

```bash
python client.py status <job_id> [--poll]
```

**Behavior:**
- GET `/api/jobs/{job_id}`
- Displays current status
- If `--poll` flag, polls every 5 seconds until terminal state

### 3. Download Artifact

```bash
python client.py download <job_id> --output output.mp4
```

**Behavior:**
- GET `/api/jobs/{job_id}/artifact`
- Saves to specified path
- Handles errors (not-ready, failed, not-found)

### 4. List Recent Jobs

```bash
python client.py list [--limit 10]
```

**Behavior:**
- GET `/api/jobs`
- Displays table of jobs with status and timestamps

---

## Command Structure

```
usage: client.py [-h] [--base-url URL] <command> [<args>]

options:
  -h, --help           Show help message
  --base-url URL       Server base URL (default: http://localhost:8080)

commands:
  upload    Upload a video file
  status    Check job status
  download  Download artifact
  list      List recent jobs
```

---

## Response Handling

### Deduplication Response

Server may return `deduplicated=true`:

```json
{
  "job_id": "uuid",
  "status": "queued",
  "deduplicated": true,
  "profile": "web_720p",
  "content_hash": "sha256..."
}
```

Client behavior:
- Note that job was deduplicated (no new FFmpeg processing)
- Still return the existing `job_id`
- User can check if they need to wait for processing

### Status Values

| Status | Meaning |
|--------|---------|
| `queued` | Job waiting in queue |
| `running` | Worker processing with FFmpeg |
| `succeeded` | Transcoding complete, artifact ready |
| `failed` | Transcoding failed |

---

## Error Handling

| HTTP Status | Client Action |
|-------------|---------------|
| 400 Bad Request | Print error message, exit 1 |
| 404 Not Found | Print "job not found", exit 1 |
| 5xx Server Error | Print error, suggest retry |
| Network Error | Print connection error, exit 1 |

---

## File Structure

```
client/
  client.py           # Main CLI entry point
  api.py              # HTTP API wrappers
  utils.py            # Helper functions (hashing, progress)
  requirements.txt    # Python dependencies
  README.md           # Client-specific documentation
```

### `api.py` - HTTP Wrappers

```python
class VideoDigestClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    async def upload_file(self, file_path: str, profile: str) -> JobResponse:
        # Multipart POST to /api/jobs
        pass
    
    async def get_job(self, job_id: str) -> JobStatusResponse:
        # GET /api/jobs/{job_id}
        pass
    
    async def download_artifact(self, job_id: str, output_path: str):
        # GET /api/jobs/{job_id}/artifact
        pass
    
    async def list_jobs(self, limit: int = 10) -> List[JobSummary]:
        # GET /api/jobs
        pass
```

---

## Example Usage

```bash
# Upload a file
python client.py upload --file sample.mp4 --profile web_720p
# Output:
# Job created: abc-123-def
# Deduplicated: false

# Poll until complete
python client.py status abc-123-def --poll
# Output every 5s:
# Status: running (ETA: ~2s remaining)
# Status: succeeded

# Download artifact
python client.py download abc-123-def --output output.mp4
```

---

## Testing Strategy

1. **Manual testing with server**
   - Start server: `cargo run`
   - Run client commands
   - Verify file upload, dedup, status polling, download

2. **Deduplication test**
   - Upload same file twice
   - Second upload should show `deduplicated=true`

3. **Concurrent upload test** (advanced)
   - Spawn multiple clients uploading same file
   - Verify only one canonical job created

---

## Dependencies (`requirements.txt`)

```
aiohttp>=3.9.0      # Async HTTP client
tqdm>=4.65.0        # Progress bars (optional)
```

No external crypto libraries needed - Python has built-in SHA-256.

---

## Future Enhancements

- [ ] Progress bar during upload
- [ ] Config file for default profile/base URL
- [ ] Batch upload multiple files
- [ ] Web UI client (React/Vue) as alternative
- [ ] Library mode (importable Python module)
