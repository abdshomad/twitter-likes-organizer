# Ingestion & Scraping Pipeline

## Overview
The ingestion pipeline provides multiple high-speed methods to populate and continuously synchronize your Twitter/X likes into pure LanceDB embedded vector storage:

---

### 1. ⚡ Direct Internal GraphQL API Interceptor (Ultra-Fast)
- **Engine**: `src/ingestion/graphql_client.py` using `curl-cffi` TLS fingerprinting (`chrome120`).
- **Speed**: **50x–100x faster** than browser scraping (~100ms per 100 raw likes batch).
- **Pagination**: Uses internal GraphQL cursor pagination (`cursor-bottom-...`).
- **Resilience**: Zero browser memory overhead; automatically falls back to headless Playwright if Twitter queries change or require interactive intervention.

---

### 2. 🤖 Playwright Headless Infinite Scraper (Fallback & Progressive)
- **Engine**: `src/ingestion/playwright_scraper.py`
- **Location**: Navigates to `https://x.com/i/history/likes` with fallback to `https://x.com/{username}/likes`.
- **Deduplication**: Automatically checks indexed tweet IDs in LanceDB to prevent re-processing existing tweets.
- **Background Daemon**: `src/ingestion/background_sync.py` runs every 10 minutes automatically in the background.
- **Web UI**: Open `http://0.0.0.0:4024` -> Click **Connect Twitter** -> Click **Sync Now**.

---

### 3. 🗄️ Full Official X Data Archive (`like.js` Bulk Ingest)
For complete historical archives spanning the lifetime of your account:

1. **Request Archive on Twitter/X**:
   - Go to [**X Account Settings**](https://x.com/settings/your_twitter_data) -> **Download an archive of your data**.
2. **Download & Locate File**:
   - Download the `.zip` archive from Twitter, extract it, and locate `data/like.js`.
3. **Ingest into Application**:
   - **Via Web Dashboard**: Click the **"Import like.js"** button in the header at `http://0.0.0.0:4024` and pick your `like.js` file.
   - **Via REST API (curl)**:
     ```bash
     curl -F "file=@/path/to/extracted/data/like.js" http://0.0.0.0:4024/api/ingest/archive
     ```

---

## Code Architecture
- **GraphQL Interceptor**: `src/ingestion/graphql_client.py`
- **Playwright Scraper**: `src/ingestion/playwright_scraper.py`
- **Archive Stream Parser**: `src/ingestion/archive_parser.py`
- **SSE Sync Stream Pipeline**: `src/ingestion/sync_pipeline.py`
- **10m Progressive Daemon**: `src/ingestion/background_sync.py`
- **Cordis Plugin**: `packages/plugin-ingestion/`
