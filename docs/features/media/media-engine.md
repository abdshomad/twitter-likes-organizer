# Decoupled Media Downloader & Queue Daemon

## Overview
The media system uses a decoupled 2-stage architecture:
1. **Stage 1 (Immediate Enqueue)**: During timeline sync, media URLs are enqueued in `data/media_queue.json` in microseconds without blocking text scraping or embedding.
2. **Stage 2 (Async Background Queue Worker)**: `MediaQueueWorker` processes image and video downloads concurrently (`concurrency=3`) with exponential backoff retry.

---

## Capabilities
- **Direct Image Downloader**: Downloads full-resolution JPG/PNG files directly to `data/media/`.
- **Video Extraction**: Uses `yt-dlp` with local Cobalt fallback for native MP4 video archiving.
- **Persistence & Resume**: Queue state is preserved across application restarts.
- **LanceDB Auto-Update**: Updates tweet records with localized asset file paths upon download completion.

---

## Code Architecture
- **Queue Manager & Worker**: `src/media/media_queue.py`
- **Downloader Core**: `src/media/downloader.py`
- **Cordis Plugin**: `packages/plugin-media/`
