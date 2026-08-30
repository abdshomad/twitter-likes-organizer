# Ingestion & Scraping Pipeline

## Overview
The ingestion pipeline provides two distinct methods to populate your tweet likes into the pure LanceDB embedded vector storage:

---

### 1. Live In-App Sync (Automatic & Progressive)
- **Zero-Download Workflow**: Directly communicates with Twitter/X via Playwright headless session runner.
- **Location**: Navigates to `https://x.com/i/history/likes` with fallback to `https://x.com/{username}/likes`.
- **Deduplication**: Automatically checks indexed tweet IDs in LanceDB to prevent re-processing existing tweets.
- **Background Daemon**: Runs every 10 minutes automatically in the background (configurable via header toggle).
- **Web UI**: Open `http://0.0.0.0:4024` -> Click **Connect Twitter** -> Click **Sync Now**.

---

### 2. Full Official X Data Archive (`like.js` Bulk Ingest)
For complete historical archives spanning the lifetime of your account:

1. **Request Archive on Twitter/X**:
   - Go to [**X Account Settings**](https://x.com/settings/your_twitter_data) -> **Download an archive of your data**.
   - Confirm your password and submit the export request.
2. **Download & Locate File**:
   - Download the `.zip` archive from Twitter.
   - Extract the `.zip` and locate `data/like.js`.
3. **Ingest into Application**:
   - **Via Web Dashboard**: Click the **"Import like.js"** button in the header at `http://0.0.0.0:4024` and pick your `like.js` file.
   - **Via REST API (curl)**:
     ```bash
     curl -F "file=@/path/to/extracted/data/like.js" http://0.0.0.0:4024/api/ingest/archive
     ```

---

## Code Architecture
- **Archive Stream Parser**: `src/ingestion/archive_parser.py`
- **Playwright Scraper**: `src/ingestion/playwright_scraper.py`
- **SSE Sync Stream Pipeline**: `src/ingestion/sync_pipeline.py`
- **10m Progressive Daemon**: `src/ingestion/background_sync.py`
- **Cordis Plugin**: `packages/plugin-ingestion/`
