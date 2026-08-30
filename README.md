# 𝕏 Likes Organizer

High-performance, autonomous Twitter/X liked tweets organizer with pure LanceDB embedded vector storage, Cordis microkernel plugin architecture, AI auto-tagging, media hoarder archiving, and Obsidian/Notion Markdown export.

---

## 🚀 Quickstart

1. **Install dependencies**:
   ```bash
   pnpm install
   uv sync
   uv run playwright install chromium
   ```

2. **Start Service (PM2 on Port 4024)**:
   ```bash
   pm2 start ecosystem.config.cjs
   ```
   Open dashboard at [`http://0.0.0.0:4024`](http://0.0.0.0:4024).

---

## 📥 Ingesting Your Likes

### Option 1: Live In-App Sync (Automatic)
- Connect Twitter directly from the Web Dashboard (via credentials or `auth_token` cookie).
- Click **Sync Now** to start progressive ingestion with real-time SSE progress.
- Keep **Auto-Sync: ON** to automatically fetch new likes every 10 minutes in the background.

### Option 2: Full Official X Data Archive (`like.js`)
- Request archive from [X Account Data](https://x.com/settings/your_twitter_data).
- Download & extract the archive ZIP to find `data/like.js`.
- Click **"Import like.js"** in the Web Dashboard or upload via CLI:
  ```bash
  curl -F "file=@/path/to/like.js" http://0.0.0.0:4024/api/ingest/archive
  ```

---

## 📖 Documentation

- [Features Index](docs/features/README.md)
- [Ingestion & Scraper Guide](docs/features/ingestion/parsers-and-scraper.md)
- [LanceDB Storage](docs/features/storage/lancedb.md)
- [Media Downloader](docs/features/media/media-engine.md)
- [AI Tagging & Embeddings](docs/features/ai/tagger-and-embeddings.md)
- [Web Dashboard & Exporter](docs/features/dashboard/web-and-exporter.md)

---

## 🤖 Agent Guidelines
Refer to [AGENTS.md](AGENTS.md) and [autonomous-coding-agents](autonomous-coding-agents/AGENTS.md).