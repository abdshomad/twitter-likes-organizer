import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from fastapi import FastAPI, UploadFile, File, Query, BackgroundTasks, Body
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from src.server.config import HOST, PORT, DATA_DIR, MEDIA_DIR
from src.storage.lancedb_client import LanceDBStore
from src.storage.history_manager import HistoryManager
from src.media.media_queue import MediaQueue
from src.ai.tagger import AITagger
from src.ai.embedder import VectorEmbedder
from src.exporter.markdown_exporter import export_tweets_to_directory
from src.ingestion.archive_parser import parse_like_js_content
from src.ingestion.playwright_scraper import PlaywrightXScraper
from src.ingestion.graphql_client import TwitterGraphQLClient
from src.ingestion.unliker import TwitterUnliker
from src.ingestion.sync_pipeline import stream_likes_sync
from src.ingestion.background_sync import BackgroundSyncScheduler

MEDIA_DIR.mkdir(parents=True, exist_ok=True)
store = LanceDBStore()
history = HistoryManager()
tagger = AITagger()
embedder = VectorEmbedder()
media_queue = MediaQueue(store=store)
scraper = PlaywrightXScraper()
unliker = TwitterUnliker(scraper.session_path)
scheduler = BackgroundSyncScheduler(scraper, store, tagger, embedder, media_queue, interval_sec=600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sched_task = asyncio.create_task(scheduler.start_loop())
    queue_task = asyncio.create_task(media_queue.worker_loop())
    yield
    sched_task.cancel()
    queue_task.cancel()


app = FastAPI(title="𝕏 Likes Organizer HUD", version="0.2.0", lifespan=lifespan)
app.mount("/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")


@app.get("/health")
async def health_check():
    return {"status": "healthy", "host": HOST, "port": PORT, "storage": str(DATA_DIR)}


@app.get("/api/stats")
async def get_stats():
    base = store.get_stats()
    base["media_queue"] = media_queue.get_status()
    return base


@app.get("/api/tags")
async def get_tags():
    return {"tags": store.get_all_tags()}


@app.get("/api/auth/status")
async def auth_status():
    return scraper.get_session_status()


@app.get("/api/scheduler/status")
async def scheduler_status():
    return scheduler.get_status()


@app.post("/api/scheduler/toggle")
async def scheduler_toggle():
    state = scheduler.toggle()
    return {"enabled": state, "status": scheduler.get_status()}


@app.post("/api/scheduler/interval")
async def scheduler_interval(payload: dict[str, Any] = Body(...)):
    sec = int(payload.get("interval_sec", 600))
    scheduler.set_interval(sec)
    return {"status": "success", "scheduler": scheduler.get_status()}


@app.post("/api/settings/auto-unlike/toggle")
async def toggle_auto_unlike():
    state = scheduler.toggle_auto_unlike()
    return {"auto_unlike": state, "status": scheduler.get_status()}


@app.get("/api/media-queue/status")
async def get_media_queue_status():
    return media_queue.get_status()


@app.post("/api/maintenance/unlike-synced")
async def maintenance_unlike_synced(bg: BackgroundTasks):
    tweets = store.get_all_tweets(limit=10000)
    bg.add_task(unliker.bulk_unlike, tweets)
    return {"status": "started", "target_count": len(tweets), "message": f"Started unliking {len(tweets)} tweets on X."}


@app.get("/api/history/logs")
async def get_history_logs(limit: int = 50):
    return {"logs": history.get_sync_logs(limit=limit)}


@app.get("/api/history/notifications")
async def get_notifications(limit: int = 50):
    return history.get_notifications(limit=limit)


@app.post("/api/history/notifications/read-all")
async def mark_notifications_read():
    count = history.mark_all_read()
    return {"status": "success", "marked_read": count}


@app.post("/api/auth/login")
async def auth_login(payload: dict[str, Any] = Body(...)):
    res = await scraper.login_with_credentials(
        payload.get("username", ""), payload.get("password", ""), payload.get("email_or_phone", "")
    )
    return res if res.get("status") == "success" else JSONResponse(res, status_code=401)


@app.post("/api/auth/cookies")
async def auth_cookies(payload: dict[str, Any] = Body(...)):
    token = payload.get("auth_token", "")
    if not token:
        return JSONResponse({"status": "error", "message": "auth_token is required"}, status_code=400)
    return {"status": "success", "session": scraper.save_cookies(token, payload.get("ct0", ""), payload.get("username", ""))}


@app.post("/api/auth/disconnect")
async def auth_disconnect():
    scraper.disconnect()
    return {"status": "success", "connected": False}


@app.get("/api/sync/stream")
async def sync_stream(max_tweets: int = 0, username: str = ""):
    sched_status = scheduler.get_status()
    auto_unlike = sched_status.get("auto_unlike", True)
    return StreamingResponse(
        stream_likes_sync(scraper, store, tagger, embedder, media_queue, username, max_tweets, auto_unlike),
        media_type="text/event-stream",
    )


@app.get("/api/search")
async def search_likes(
    q: str = Query("", description="Search query"),
    tag: str | None = Query(None, description="Tag filter"),
    sort_by: str = Query("newest", description="Sort option"),
    semantic: bool = Query(False, description="Enable vector semantic search"),
    offset: int = Query(0, ge=0),
    limit: int = Query(24, ge=1, le=100),
):
    vector = embedder.embed_text(q.strip()) if semantic and q.strip() else None
    results = store.search_hybrid(query=q.strip(), query_vector=vector, tag=tag, sort_by=sort_by, offset=offset, limit=limit)
    return {"count": len(results), "offset": offset, "limit": limit, "results": results}


@app.post("/api/ingest/archive")
async def ingest_archive(file: UploadFile = File(...)):
    content = await file.read()
    tweets = parse_like_js_content(content.decode("utf-8", errors="ignore"))
    for t in tweets:
        if not t.get("tags"):
            t["tags"] = tagger.generate_tags(t["text"])
    inserted = store.upsert_tweets(tweets)
    total_db = store.get_stats().get("total_likes", 0)
    history.add_sync_log("archive-upload", "parser", "success", len(tweets), total_db, "Imported like.js archive.")
    return {"status": "success", "parsed": len(tweets), "inserted": inserted}


@app.post("/api/export/markdown")
async def export_markdown():
    export_dir = DATA_DIR / "exports"
    files = export_tweets_to_directory(store.get_all_tweets(limit=5000), export_dir)
    history.add_notification("info", "Markdown Export Completed", f"Exported {len(files)} files to {export_dir}.")
    return {"status": "success", "exported_count": len(files), "export_dir": str(export_dir)}


@app.get("/", response_class=HTMLResponse)
async def index():
    stats = store.get_stats()
    q_stat = media_queue.get_status()
    tags = store.get_all_tags()[:30]
    tags_html = "".join([f"<span class='hud-tag' id='tag-{t['tag']}' onclick='filterTag(\"{t['tag']}\")'>{t['tag']} <span class='tag-count'>{t['count']}</span></span>" for t in tags])
    auth = scraper.get_session_status()
    sched = scheduler.get_status()
    notifs = history.get_notifications(limit=10)
    unread = notifs["unread_count"]
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>𝕏 Likes Organizer HUD</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet" />
  <style>
    :root {{
      --bg: #07090e;
      --card-bg: rgba(15, 20, 32, 0.75);
      --card-border: rgba(255, 255, 255, 0.08);
      --primary: #1d9bf0;
      --accent: #6366f1;
      --success: #10b981;
      --warn: #f59e0b;
      --danger: #ef4444;
      --text: #f1f5f9;
      --muted: #94a3b8;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); padding-top: 80px; min-height: 100vh; overflow-x: hidden; }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 0 1.5rem 3rem; }}
    
    /* HUD Floating Topbar */
    .hud-topbar {{ position: fixed; top: 12px; left: 16px; right: 16px; height: 56px; background: rgba(11, 15, 25, 0.85); backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); border: 1px solid rgba(255, 255, 255, 0.12); border-radius: 12px; display: flex; align-items: center; justify-content: space-between; padding: 0 16px; z-index: 100; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.6); }}
    .hud-brand {{ display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 1rem; color: #fff; text-decoration: none; }}
    .hud-brand span {{ background: linear-gradient(135deg, #1d9bf0, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
    .hud-badge {{ font-size: 0.75rem; padding: 3px 8px; border-radius: 6px; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; font-family: 'JetBrains Mono', monospace; }}
    .hud-badge.connected {{ background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.35); }}
    .hud-badge.disconnected {{ background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.35); }}
    .dot {{ width: 7px; height: 7px; border-radius: 50%; display: inline-block; }}
    .dot.connected {{ background: #10b981; box-shadow: 0 0 8px #10b981; }}
    .dot.disconnected {{ background: #ef4444; }}
    
    /* HUD Stats Ticker */
    .hud-ticker {{ display: flex; align-items: center; gap: 12px; font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; background: rgba(0, 0, 0, 0.35); border: 1px solid var(--card-border); padding: 4px 12px; border-radius: 8px; }}
    .ticker-item {{ color: var(--muted); }}
    .ticker-item strong {{ color: var(--text); }}
    
    /* HUD Buttons & Controls */
    .hud-btn {{ background: rgba(255, 255, 255, 0.06); color: var(--text); border: 1px solid var(--card-border); padding: 0.4rem 0.8rem; border-radius: 6px; cursor: pointer; font-size: 0.8rem; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; transition: all 0.2s ease; }}
    .hud-btn:hover {{ background: rgba(255, 255, 255, 0.12); border-color: rgba(255, 255, 255, 0.2); }}
    .hud-btn.accent {{ background: linear-gradient(135deg, rgba(29, 155, 240, 0.3), rgba(99, 102, 241, 0.3)); border-color: rgba(29, 155, 240, 0.5); color: #fff; box-shadow: 0 0 12px rgba(29, 155, 240, 0.2); }}
    .hud-btn.accent:hover {{ background: linear-gradient(135deg, rgba(29, 155, 240, 0.5), rgba(99, 102, 241, 0.5)); }}
    .sync-pill {{ display: flex; align-items: center; gap: 6px; background: rgba(0, 0, 0, 0.4); border: 1px solid var(--card-border); padding: 3px 8px; border-radius: 8px; font-size: 0.75rem; font-family: 'JetBrains Mono', monospace; }}
    .sync-select, .sort-select {{ background: #131926; color: var(--text); border: 1px solid var(--card-border); padding: 0.3rem 0.6rem; border-radius: 6px; font-size: 0.8rem; outline: none; }}
    
    /* HUD Filter Capsule */
    .hud-filter-dock {{ background: var(--card-bg); backdrop-filter: blur(12px); border: 1px solid var(--card-border); border-radius: 12px; padding: 1rem 1.25rem; margin-bottom: 1.5rem; display: flex; flex-direction: column; gap: 0.85rem; box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4); }}
    .search-row {{ display: flex; gap: 0.75rem; align-items: center; }}
    .hud-input {{ flex: 1; padding: 0.75rem 1rem; background: rgba(8, 11, 18, 0.8); border: 1px solid var(--card-border); border-radius: 8px; color: var(--text); font-size: 0.95rem; outline: none; }}
    .hud-input:focus {{ border-color: var(--primary); box-shadow: 0 0 10px rgba(29, 155, 240, 0.3); }}
    
    /* Toolbar Group & SVG Switchers */
    .controls-row {{ display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 0.75rem; }}
    .btn-group {{ display: flex; gap: 3px; background: rgba(0, 0, 0, 0.35); border: 1px solid var(--card-border); padding: 3px; border-radius: 8px; }}
    .icon-btn {{ background: transparent; color: var(--muted); border: none; padding: 0.35rem 0.6rem; border-radius: 6px; cursor: pointer; display: flex; align-items: center; gap: 5px; font-size: 0.75rem; font-weight: 600; transition: all 0.15s ease; }}
    .icon-btn svg {{ width: 14px; height: 14px; fill: currentColor; stroke: currentColor; }}
    .icon-btn.active {{ background: rgba(29, 155, 240, 0.25); color: #60a5fa; border: 1px solid rgba(29, 155, 240, 0.4); }}
    
    /* Tag Cloud */
    .hud-tag-cloud {{ display: flex; flex-wrap: wrap; gap: 0.45rem; }}
    .hud-tag {{ background: rgba(29, 155, 240, 0.1); color: #7dd3fc; border: 1px solid rgba(29, 155, 240, 0.25); padding: 0.25rem 0.6rem; border-radius: 999px; font-size: 0.75rem; cursor: pointer; transition: all 0.15s ease; }}
    .hud-tag:hover {{ background: rgba(29, 155, 240, 0.25); border-color: rgba(29, 155, 240, 0.5); }}
    .hud-tag.active {{ background: #0284c7; color: #fff; font-weight: bold; border-color: #38bdf8; box-shadow: 0 0 10px rgba(56, 189, 248, 0.4); }}
    .tag-count {{ font-size: 0.7rem; opacity: 0.7; font-family: 'JetBrains Mono', monospace; }}

    /* Multi-Column Layout Grid */
    #results.cols-1 {{ display: grid; grid-template-columns: 1fr; max-width: 820px; margin: 0 auto; gap: 1rem; }}
    #results.cols-2 {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
    #results.cols-3 {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }}
    #results.cols-4 {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.85rem; }}
    @media (max-width: 1100px) {{ #results.cols-4 {{ grid-template-columns: repeat(3, 1fr); }} }}
    @media (max-width: 850px) {{ #results.cols-3, #results.cols-4 {{ grid-template-columns: repeat(2, 1fr); }} }}
    @media (max-width: 600px) {{ #results.cols-2, #results.cols-3, #results.cols-4 {{ grid-template-columns: 1fr; }} }}

    /* HUD Tweet Cards */
    .hud-tweet-card {{ background: var(--card-bg); backdrop-filter: blur(8px); border: 1px solid var(--card-border); padding: 1.25rem; border-radius: 12px; display: flex; flex-direction: column; justify-content: space-between; transition: all 0.2s ease; }}
    .hud-tweet-card:hover {{ border-color: rgba(29, 155, 240, 0.4); transform: translateY(-2px); box-shadow: 0 8px 20px rgba(0,0,0,0.5); }}
    .tweet-header {{ display: flex; justify-content: space-between; color: var(--muted); font-size: 0.85rem; margin-bottom: 0.6rem; }}
    .media-grid {{ display: flex; gap: 0.5rem; margin-top: 0.75rem; overflow-x: auto; }}
    .media-thumb {{ max-height: 180px; border-radius: 8px; object-fit: cover; border: 1px solid var(--card-border); }}
    
    /* Compact Row & Gallery Styles */
    .compact-row {{ display: flex; justify-content: space-between; align-items: center; background: var(--card-bg); border: 1px solid var(--card-border); padding: 0.75rem 1rem; border-radius: 8px; gap: 1rem; font-size: 0.85rem; }}
    .compact-text {{ flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .gallery-card {{ background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 10px; overflow: hidden; display: flex; flex-direction: column; }}
    .gallery-img {{ width: 100%; height: 220px; object-fit: cover; border-bottom: 1px solid var(--card-border); }}
    .gallery-body {{ padding: 0.85rem; }}

    /* Shimmer Skeleton */
    .skeleton {{ background: linear-gradient(90deg, #101524 25%, #192238 50%, #101524 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 6px; }}
    @keyframes shimmer {{ 0% {{ background-position: 200% 0; }} 100% {{ background-position: -200% 0; }} }}
    .skeleton-card {{ background: var(--card-bg); border: 1px solid var(--card-border); padding: 1.25rem; border-radius: 12px; }}

    /* Sliding HUD Right Sidesheet */
    .hud-sidesheet {{ position: fixed; top: 76px; bottom: 16px; right: 16px; width: 420px; background: rgba(11, 15, 25, 0.92); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); border: 1px solid rgba(255, 255, 255, 0.12); border-radius: 16px; z-index: 120; display: flex; flex-direction: column; box-shadow: -10px 0 40px rgba(0, 0, 0, 0.7); transform: translateX(460px); transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1); }}
    .hud-sidesheet.open {{ transform: translateX(0); }}
    .hud-sidesheet-header {{ padding: 16px 20px; border-bottom: 1px solid var(--card-border); display: flex; align-items: center; justify-content: space-between; }}
    .hud-tabs {{ display: flex; gap: 8px; padding: 12px 16px 0; border-bottom: 1px solid var(--card-border); }}
    .hud-tab {{ padding: 8px 12px; cursor: pointer; color: var(--muted); border-bottom: 2px solid transparent; font-size: 0.85rem; font-weight: 600; }}
    .hud-tab.active {{ color: var(--primary); border-bottom-color: var(--primary); }}
    .hud-sidesheet-content {{ flex: 1; overflow-y: auto; padding: 16px; font-size: 0.85rem; }}
    
    /* Non-blocking Floating Sync Toast */
    .hud-floating-toast {{ position: fixed; bottom: 1.5rem; right: 1.5rem; background: rgba(11, 15, 25, 0.95); backdrop-filter: blur(16px); border: 1px solid rgba(29, 155, 240, 0.4); box-shadow: 0 10px 30px rgba(0,0,0,0.8); border-radius: 12px; width: 340px; z-index: 95; display: none; overflow: hidden; font-size: 0.85rem; }}
    .toast-header {{ padding: 0.65rem 0.9rem; display: flex; justify-content: space-between; align-items: center; background: rgba(29, 155, 240, 0.1); border-bottom: 1px solid var(--card-border); }}
    .toast-body {{ padding: 0.75rem 0.9rem; }}
    .toast-progress {{ background: #131926; height: 6px; border-radius: 3px; overflow: hidden; margin-top: 0.5rem; }}
    .toast-progress-fill {{ background: linear-gradient(90deg, var(--primary), var(--success)); height: 100%; width: 0%; transition: width 0.3s ease; }}
    .toast-log {{ font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: var(--muted); max-height: 90px; overflow-y: auto; margin-top: 0.5rem; }}
    .badge {{ background: #ef4444; color: white; border-radius: 999px; padding: 0.15rem 0.45rem; font-size: 0.75rem; font-weight: bold; }}
  </style>
</head>
<body>
  <!-- Top HUD Bar -->
  <header class="hud-topbar">
    <div style="display:flex; align-items:center; gap:12px;">
      <a href="#" class="hud-brand">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
          <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
        </svg>
        <span>𝕏 LIKES HUD</span>
      </a>
      <span class="hud-badge {'connected' if auth['connected'] else 'disconnected'}">
        <span class="dot {'connected' if auth['connected'] else 'disconnected'}"></span>
        {f'@{auth["username"]}' if auth['connected'] and auth['username'] else ('ONLINE' if auth['connected'] else 'DISCONNECTED')}
      </span>
    </div>

    <!-- Center HUD Telemetry Ticker -->
    <div class="hud-ticker">
      <span class="ticker-item">TOTAL: <strong id="stat-total">{stats['total_likes']}</strong></span>
      <span class="ticker-item">MEDIA: <strong id="stat-media">{stats['archived_media_files']}</strong></span>
      <span class="ticker-item">TAGS: <strong id="stat-tags">{stats['tags_count']}</strong></span>
    </div>

    <!-- Right HUD Control Deck -->
    <div style="display:flex; align-items:center; gap:8px;">
      <div class="sync-pill">
        <button id="btn-auto-sync" class="hud-btn" style="padding:0.2rem 0.5rem; font-size:0.75rem;" onclick="toggleAutoSync()">
          {'Sync: ON' if sched['enabled'] else 'Sync: OFF'}
        </button>
        <select id="select-interval" class="sync-select" onchange="changeSyncInterval(this.value)">
          <option value="300" {'selected' if sched['interval_sec'] == 300 else ''}>5m</option>
          <option value="600" {'selected' if sched['interval_sec'] == 600 else ''}>10m</option>
          <option value="1800" {'selected' if sched['interval_sec'] == 1800 else ''}>30m</option>
          <option value="3600" {'selected' if sched['interval_sec'] == 3600 else ''}>1h</option>
          <option value="0" {'selected' if sched['interval_sec'] == 0 else ''}>Manual</option>
        </select>
        <span id="sync-countdown" style="color:var(--muted); min-width:65px;">Next: --:--</span>
      </div>

      <button id="btn-auto-unlike" class="hud-btn" onclick="toggleAutoUnlike()">
        {'Unlike: ON' if sched.get('auto_unlike') else 'Unlike: OFF'}
      </button>

      <button id="btn-sync" class="hud-btn accent" onclick="startSyncStream()">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M21.5 2v6h-6M2.5 22v-6h6"/><path d="M2 11.5a10 10 0 0 1 18.8-4.3L21.5 8M2.5 16l1.2 1.2A10 10 0 0 0 22 12.5"/></svg>
        Sync Now
      </button>

      <button class="hud-btn" onclick="toggleSidesheet()">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
        Logs & Alerts {f'<span class="badge" id="unread-badge">{unread}</span>' if unread > 0 else '<span id="unread-badge"></span>'}
      </button>

      <button class="hud-btn" onclick="document.getElementById('file-upload').click()">Import</button>
      <input type="file" id="file-upload" style="display:none" onchange="uploadArchive(this)">
      <button class="hud-btn" onclick="exportMarkdown()">Export</button>
    </div>
  </header>

  <!-- Main Viewport -->
  <div class="container">
    <!-- Filter Dock -->
    <div class="hud-filter-dock">
      <div class="search-row">
        <input id="query" type="text" class="hud-input" placeholder="Search likes by keywords, concepts, or semantic context..." onkeyup="if(event.key==='Enter') triggerNewSearch()">
        <button class="hud-btn accent" onclick="triggerNewSearch()" style="padding:0.75rem 1.25rem;">Search</button>
        <button class="hud-btn" onclick="clearFilters()" id="btn-clear-filter" style="display:none;">Clear</button>
      </div>

      <div class="controls-row">
        <div class="hud-tag-cloud" id="tag-cloud-container">{tags_html}</div>

        <div style="display:flex; gap:8px; align-items:center;">
          <!-- Sort Selector -->
          <select id="select-sort" class="sort-select" onchange="changeSort(this.value)">
            <option value="newest">Newest First</option>
            <option value="oldest">Oldest First</option>
            <option value="media_only">Media Only</option>
            <option value="author">Author A-Z</option>
          </select>

          <!-- Columns Selector (1, 2, 3, 4) -->
          <div class="btn-group" title="Column Layout">
            <button class="icon-btn" id="btn-cols-1" onclick="setCols('1')">
              <svg viewBox="0 0 24 24"><rect x="5" y="3" width="14" height="18" rx="2"/></svg> 1
            </button>
            <button class="icon-btn" id="btn-cols-2" onclick="setCols('2')">
              <svg viewBox="0 0 24 24"><rect x="3" y="3" width="8" height="18" rx="1.5"/><rect x="13" y="3" width="8" height="18" rx="1.5"/></svg> 2
            </button>
            <button class="icon-btn" id="btn-cols-3" onclick="setCols('3')">
              <svg viewBox="0 0 24 24"><rect x="2" y="3" width="5.3" height="18" rx="1"/><rect x="9.3" y="3" width="5.3" height="18" rx="1"/><rect x="16.6" y="3" width="5.3" height="18" rx="1"/></svg> 3
            </button>
            <button class="icon-btn" id="btn-cols-4" onclick="setCols('4')">
              <svg viewBox="0 0 24 24"><rect x="2" y="3" width="3.5" height="18" rx="0.8"/><rect x="7.5" y="3" width="3.5" height="18" rx="0.8"/><rect x="13" y="3" width="3.5" height="18" rx="0.8"/><rect x="18.5" y="3" width="3.5" height="18" rx="0.8"/></svg> 4
            </button>
          </div>

          <!-- Display Mode Selector (Cards, List, Gallery) -->
          <div class="btn-group" title="Display Style">
            <button class="icon-btn" id="btn-mode-card" onclick="setDisplayMode('card')">
              <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="7" y1="8" x2="17" y2="8"/><line x1="7" y1="12" x2="14" y2="12"/></svg> Cards
            </button>
            <button class="icon-btn" id="btn-mode-list" onclick="setDisplayMode('list')">
              <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/></svg> List
            </button>
            <button class="icon-btn" id="btn-mode-gallery" onclick="setDisplayMode('gallery')">
              <svg viewBox="0 0 24 24" fill="none" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5" fill="currentColor"/><path d="M21 15l-5-5L5 21"/></svg> Gallery
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Results Multi-Column Container -->
    <div id="results" class="cols-2 mode-card"></div>
    <div id="scroll-sentinel" style="height:20px; margin-top:1rem;"></div>
  </div>

  <!-- HUD Right Sidesheet -->
  <aside class="hud-sidesheet" id="hud-sidesheet">
    <div class="hud-sidesheet-header">
      <h3 style="font-size:0.95rem; font-weight:700;">HUD Control & Telemetry</h3>
      <button class="hud-btn" onclick="toggleSidesheet()" style="padding:0.2rem 0.5rem;">✕</button>
    </div>
    <div class="hud-tabs">
      <div class="hud-tab active" id="tab-btn-logs" onclick="switchHistTab('logs')">Sync Logs</div>
      <div class="hud-tab" id="tab-btn-notifs" onclick="switchHistTab('notifs')">Alerts</div>
      <div class="hud-tab" id="tab-btn-auth" onclick="switchHistTab('auth')">Account</div>
    </div>
    <div class="hud-sidesheet-content">
      <div id="tab-logs">
        <div id="logs-container">Loading telemetry...</div>
      </div>
      <div id="tab-notifs" style="display:none;">
        <div style="display:flex; justify-content:flex-end; margin-bottom:0.75rem;">
          <button class="hud-btn" onclick="markAllNotifsRead()" style="font-size:0.75rem;">Mark All Read</button>
        </div>
        <div id="notifs-container">Loading notifications...</div>
      </div>
      <div id="tab-auth" style="display:none;">
        <p style="color:var(--muted); font-size:0.8rem; margin-bottom:0.75rem;">Session cookies & bulk clean operations.</p>
        <input type="text" id="auth-username" class="hud-input" placeholder="@handle" style="width:100%; margin-bottom:0.5rem;">
        <input type="text" id="auth-token" class="hud-input" placeholder="auth_token (required)" style="width:100%; margin-bottom:0.5rem;">
        <input type="text" id="auth-ct0" class="hud-input" placeholder="ct0 (optional)" style="width:100%; margin-bottom:0.75rem;">
        <button class="hud-btn accent" onclick="saveCookiesAuth()" style="width:100%; margin-bottom:0.75rem;">Save Session</button>
        <button class="hud-btn" onclick="startBulkUnlike()" style="background:rgba(239,68,68,0.2); border-color:#ef4444; color:#f87171; width:100%; margin-bottom:0.75rem;">Clean & Unlike All on X</button>
        <button class="hud-btn" onclick="disconnectTwitter()" style="width:100%;">Disconnect Account</button>
      </div>
    </div>
  </aside>

  <!-- Non-blocking Floating Sync Toast -->
  <div id="floating-sync-toast" class="hud-floating-toast">
    <div class="toast-header">
      <span style="font-weight:600; display:flex; align-items:center; gap:6px;">
        <span class="dot connected"></span> <span id="toast-title">Syncing Likes...</span>
      </span>
      <button class="hud-btn" onclick="document.getElementById('floating-sync-toast').style.display='none'" style="padding:0.1rem 0.35rem; font-size:0.7rem;">✕</button>
    </div>
    <div class="toast-body">
      <div style="display:flex; justify-content:space-between; font-size:0.8rem;">
        <span id="toast-status-detail">Extracting timeline...</span>
        <span id="toast-percent" style="color:var(--primary); font-weight:bold;">0%</span>
      </div>
      <div class="toast-progress">
        <div id="toast-progress-fill" class="toast-progress-fill"></div>
      </div>
      <div id="toast-log" class="toast-log">Streaming active...</div>
    </div>
  </div>

  <script>
    let currentQuery = '';
    let currentTag = null;
    let currentSort = 'newest';
    let currentOffset = 0;
    let isLoading = false;
    let hasMore = true;
    let colCount = localStorage.getItem('likes_cols') || '2';
    let displayMode = localStorage.getItem('likes_mode') || 'card';
    let nextSyncSeconds = {sched.get('next_sync_in_sec', 0)};
    let isSyncEnabled = {str(sched.get('enabled', True)).lower()};
    let syncInterval = {sched.get('interval_sec', 600)};
    const PAGE_LIMIT = 24;

    function applyLayout() {{
      const results = document.getElementById('results');
      results.className = (displayMode === 'list' ? 'cols-1' : 'cols-' + colCount) + ' mode-' + displayMode;
      
      document.querySelectorAll('[id^=\"btn-cols-\"]').forEach(b => b.classList.remove('active'));
      const colBtn = document.getElementById('btn-cols-' + colCount);
      if (colBtn) colBtn.classList.add('active');

      document.querySelectorAll('[id^=\"btn-mode-\"]').forEach(b => b.classList.remove('active'));
      const modeBtn = document.getElementById('btn-mode-' + displayMode);
      if (modeBtn) modeBtn.classList.add('active');
    }}

    function setCols(c) {{
      colCount = c;
      localStorage.setItem('likes_cols', c);
      applyLayout();
      loadLikes(false);
    }}

    function setDisplayMode(m) {{
      displayMode = m;
      localStorage.setItem('likes_mode', m);
      applyLayout();
      loadLikes(false);
    }}

    function changeSort(val) {{
      currentSort = val;
      loadLikes(false);
    }}

    function renderSkeleton() {{
      const container = document.getElementById('results');
      container.innerHTML = `
        <div class="skeleton-card"><div class="skeleton" style="height:16px; width:30%; margin-bottom:10px;"></div><div class="skeleton" style="height:14px; width:90%; margin-bottom:8px;"></div><div class="skeleton" style="height:14px; width:70%;"></div></div>
        <div class="skeleton-card"><div class="skeleton" style="height:16px; width:25%; margin-bottom:10px;"></div><div class="skeleton" style="height:14px; width:95%; margin-bottom:8px;"></div><div class="skeleton" style="height:14px; width:60%;"></div></div>
        <div class="skeleton-card"><div class="skeleton" style="height:16px; width:35%; margin-bottom:10px;"></div><div class="skeleton" style="height:14px; width:80%; margin-bottom:8px;"></div></div>
        <div class="skeleton-card"><div class="skeleton" style="height:16px; width:28%; margin-bottom:10px;"></div><div class="skeleton" style="height:14px; width:88%; margin-bottom:8px;"></div></div>
      `;
    }}

    async function loadLikes(append = false) {{
      if (isLoading) return;
      isLoading = true;
      const container = document.getElementById('results');
      if (!append) {{
        renderSkeleton();
        currentOffset = 0;
        hasMore = true;
      }}

      const url = `/api/search?q=${{encodeURIComponent(currentQuery)}}&sort_by=${{currentSort}}&semantic=true&offset=${{currentOffset}}&limit=${{PAGE_LIMIT}}` + (currentTag ? `&tag=${{encodeURIComponent(currentTag)}}` : '');
      try {{
        const res = await fetch(url);
        const data = await res.json();
        const results = data.results || [];
        
        if (!append) container.innerHTML = '';
        if (results.length < PAGE_LIMIT) hasMore = false;

        if (results.length === 0 && !append) {{
          container.innerHTML = '<div class="hud-tweet-card" style="text-align:center; color: var(--muted); grid-column: 1 / -1; padding:2rem;">No matching likes found.</div>';
          return;
        }}

        const html = results.map(r => {{
          const mediaList = (r.local_media_paths && r.local_media_paths.length)
            ? r.local_media_paths.map(p => `/media/${{p.split('/').pop()}}`)
            : (r.media_urls || []);
          const fallbackSrc = (r.media_urls && r.media_urls.length) ? r.media_urls[0] : '';

          if (displayMode === 'list') {{
            return `
              <div class="compact-row">
                <span style="font-weight:600; color:var(--primary); min-width:120px; font-family:'JetBrains Mono',monospace;">@${{r.author_handle || 'user'}}</span>
                <span class="compact-text">${{r.text}}</span>
                <div style="display:flex; gap:0.25rem;">${{(r.tags || []).slice(0, 2).map(t => `<span class="hud-tag" style="font-size:0.7rem; padding:0.1rem 0.4rem;" onclick="filterTag('${{t}}')">${{t}}</span>`).join('')}}</div>
                <a href="${{r.url}}" target="_blank" style="color:var(--muted); font-size:0.75rem;">Link</a>
              </div>
            `;
          }}
          if (displayMode === 'gallery') {{
            const mediaSrc = mediaList.length ? mediaList[0] : '';
            return `
              <div class="gallery-card">
                ${{mediaSrc ? `<img class="gallery-img" src="${{mediaSrc}}" onerror="this.src='${{fallbackSrc}}'" loading="lazy">` : '<div style=\"height:120px; background:#131926; display:flex; align-items:center; justify-content:center; color:var(--muted); font-size:0.8rem;\">Text Post</div>'}}
                <div class="gallery-body">
                  <div class="tweet-header"><span><strong>${{r.author_name || 'User'}}</strong></span><a href="${{r.url}}" target="_blank" style="color:var(--primary)">View</a></div>
                  <p style="font-size:0.82rem; line-height:1.3; margin-bottom:0.5rem; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden;">${{r.text}}</p>
                  <div>${{(r.tags || []).slice(0, 3).map(t => `<span class="hud-tag" style="font-size:0.7rem; padding:0.1rem 0.4rem;" onclick="filterTag('${{t}}')">${{t}}</span>`).join(' ')}}</div>
                </div>
              </div>
            `;
          }}
          return `
            <div class="hud-tweet-card">
              <div>
                <div class="tweet-header"><span><strong>${{r.author_name || 'User'}}</strong> <span style="color:var(--muted); font-family:'JetBrains Mono',monospace;">@${{r.author_handle || 'user'}}</span></span><a href="${{r.url}}" target="_blank" style="color:var(--primary)">View</a></div>
                <p style="white-space:pre-wrap; line-height:1.4; font-size:0.9rem;">${{r.text}}</p>
                ${{mediaList.length ? `<div class="media-grid">${{mediaList.map((m, idx) => `<img class="media-thumb" src="${{m}}" onerror="this.src='${{fallbackSrc}}'" loading="lazy">`).join('')}}</div>` : ''}}
              </div>
              <div style="margin-top:0.75rem">${{(r.tags || []).map(t => `<span class="hud-tag" onclick="filterTag('${{t}}')">${{t}}</span>`).join(' ')}}</div>
            </div>
          `;
        }}).join('');

        container.insertAdjacentHTML('beforeend', html);
        currentOffset += results.length;
      }} finally {{
        isLoading = false;
      }}
    }}

    function filterTag(tag) {{
      document.querySelectorAll('.hud-tag').forEach(el => el.classList.remove('active'));
      const activeEl = document.getElementById('tag-' + tag);
      if (activeEl) activeEl.classList.add('active');
      document.getElementById('btn-clear-filter').style.display = 'inline-block';
      currentTag = tag;
      document.getElementById('query').value = '';
      currentQuery = '';
      loadLikes(false);
    }}

    function clearFilters() {{
      document.querySelectorAll('.hud-tag').forEach(el => el.classList.remove('active'));
      document.getElementById('btn-clear-filter').style.display = 'none';
      currentTag = null;
      document.getElementById('query').value = '';
      currentQuery = '';
      loadLikes(false);
    }}

    function triggerNewSearch() {{
      currentQuery = document.getElementById('query').value;
      loadLikes(false);
    }}

    const observer = new IntersectionObserver((entries) => {{
      if (entries[0].isIntersecting && hasMore && !isLoading) {{
        loadLikes(true);
      }}
    }}, {{ rootMargin: '300px' }});
    observer.observe(document.getElementById('scroll-sentinel'));

    setInterval(() => {{
      const cd = document.getElementById('sync-countdown');
      if (!isSyncEnabled || syncInterval === 0) {{
        cd.innerText = isSyncEnabled ? 'Manual' : 'Paused';
        return;
      }}
      if (nextSyncSeconds > 0) nextSyncSeconds--;
      const m = Math.floor(nextSyncSeconds / 60);
      const s = nextSyncSeconds % 60;
      cd.innerText = `Next: ${{m.toString().padStart(2, '0')}}:${{s.toString().padStart(2, '0')}}`;
      if (nextSyncSeconds === 0) nextSyncSeconds = syncInterval;
    }}, 1000);

    async function toggleAutoSync() {{
      const res = await fetch('/api/scheduler/toggle', {{ method: 'POST' }});
      const data = await res.json();
      isSyncEnabled = data.enabled;
      nextSyncSeconds = data.status.next_sync_in_sec || syncInterval;
      const btn = document.getElementById('btn-auto-sync');
      btn.innerText = isSyncEnabled ? 'Sync: ON' : 'Sync: OFF';
    }}

    async function changeSyncInterval(val) {{
      const sec = parseInt(val);
      syncInterval = sec;
      const res = await fetch('/api/scheduler/interval', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ interval_sec: sec }})
      }});
      const data = await res.json();
      nextSyncSeconds = data.scheduler.next_sync_in_sec || sec;
    }}

    async function refreshStats() {{
      try {{
        const res = await fetch('/api/stats');
        const st = await res.json();
        document.getElementById('stat-total').innerText = st.total_likes;
        document.getElementById('stat-media').innerText = st.archived_media_files;
        document.getElementById('stat-tags').innerText = st.tags_count;
        
        const tagsRes = await fetch('/api/tags');
        const tagsData = await tagsRes.json();
        const tHtml = (tagsData.tags || []).slice(0, 30).map(t => `<span class='hud-tag ${{t.tag === currentTag ? 'active' : ''}}' id='tag-${{t.tag}}' onclick='filterTag("${{t.tag}}")'>${{t.tag}} <span class='tag-count'>${{t.count}}</span></span>`).join('');
        document.getElementById('tag-cloud-container').innerHTML = tHtml;
      }} catch (e) {{}}
    }}

    function startSyncStream() {{
      const toast = document.getElementById('floating-sync-toast');
      const toastTitle = document.getElementById('toast-title');
      const toastDetail = document.getElementById('toast-status-detail');
      const toastFill = document.getElementById('toast-progress-fill');
      const toastPercent = document.getElementById('toast-percent');
      const toastLog = document.getElementById('toast-log');
      const btn = document.getElementById('btn-sync');
      
      toast.style.display = 'block';
      toastLog.innerHTML = '<div>[Connected] Initializing sync...</div>';
      toastFill.style.width = '10%';
      toastPercent.innerText = '10%';
      btn.innerText = 'Syncing...';

      const es = new EventSource('/api/sync/stream?max_tweets=0');
      es.onmessage = function(e) {{
        const data = JSON.parse(e.data);
        if (data.error) {{
          toastTitle.innerText = 'Sync Error';
          toastDetail.innerText = data.error;
          toastLog.innerHTML += `<div style="color:#ef4444;">[ERROR] ${{data.error}}</div>`;
          es.close();
          btn.innerText = 'Sync Now';
          return;
        }}
        if (data.stage === 'scrolling') {{
          toastTitle.innerText = `Found ${{data.tweets_found}} likes...`;
          toastDetail.innerText = `Scroll attempt #${{data.scroll_attempt}}`;
          toastLog.innerHTML += `<div>Scraped ${{data.tweets_found}} likes...</div>`;
          toastLog.scrollTop = toastLog.scrollHeight;
        }} else if (data.stage === 'item_done') {{
          toastTitle.innerText = `Ingesting (#${{data.current}})...`;
          toastDetail.innerText = `@${{data.author_handle || 'user'}}: "${{data.text.slice(0, 30)}}..."`;
          toastFill.style.width = `${{Math.min(90, 20 + data.current * 3)}}%`;
          toastPercent.innerText = `${{Math.min(90, 20 + data.current * 3)}}%`;
          toastLog.innerHTML += `<div>[Saved] @${{data.author_handle}} ${{data.unliked ? '(Unliked on X)' : ''}}</div>`;
          toastLog.scrollTop = toastLog.scrollHeight;
        }} else if (data.stage === 'complete') {{
          toastFill.style.width = '100%';
          toastPercent.innerText = '100%';
          toastTitle.innerText = 'Sync Complete!';
          toastDetail.innerText = data.message;
          toastLog.innerHTML += `<div style="color:#10b981; font-weight:bold;">[DONE] ${{data.message}}</div>`;
          es.close();
          btn.innerText = 'Sync Now';
          refreshStats();
          if (!currentQuery && !currentTag) loadLikes(false);
          setTimeout(() => {{ toast.style.display = 'none'; }}, 4000);
        }}
      }};
      es.onerror = function() {{
        toastTitle.innerText = 'Sync Ended';
        es.close();
        btn.innerText = 'Sync Now';
      }};
    }}

    function toggleSidesheet() {{
      const sheet = document.getElementById('hud-sidesheet');
      sheet.classList.toggle('open');
      if (sheet.classList.contains('open')) {{
        loadHistoryLogs();
        loadNotifications();
      }}
    }}

    function switchHistTab(t) {{
      ['logs', 'notifs', 'auth'].forEach(tab => {{
        document.getElementById('tab-' + tab).style.display = tab === t ? 'block' : 'none';
        document.getElementById('tab-btn-' + tab).className = 'hud-tab ' + (tab === t ? 'active' : '');
      }});
    }}

    async function loadHistoryLogs() {{
      const res = await fetch('/api/history/logs?limit=30');
      const data = await res.json();
      const container = document.getElementById('logs-container');
      if (!data.logs || data.logs.length === 0) {{
        container.innerHTML = '<p style="color:var(--muted); text-align:center; padding:1rem;">No sync logs recorded yet.</p>';
        return;
      }}
      container.innerHTML = data.logs.map(l => `
        <div style="background:rgba(0,0,0,0.3); border:1px solid var(--card-border); padding:0.65rem; border-radius:6px; margin-bottom:0.5rem;">
          <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:var(--muted); margin-bottom:4px;">
            <span><strong>${{l.trigger}}</strong> (${{l.engine}})</span>
            <span style="font-family:'JetBrains Mono',monospace;">${{l.timestamp.split(' ')[1]}}</span>
          </div>
          <p style="font-size:0.8rem; line-height:1.3;">${{l.message}}</p>
          <div style="display:flex; justify-content:space-between; font-size:0.7rem; color:var(--muted); margin-top:4px;">
            <span>+${{l.new_likes}} likes (Total: ${{l.total_db_likes}})</span>
            <span>${{l.duration_sec}}s</span>
          </div>
        </div>
      `).join('');
    }}

    async function loadNotifications() {{
      const res = await fetch('/api/history/notifications?limit=30');
      const data = await res.json();
      const container = document.getElementById('notifs-container');
      if (!data.notifications || data.notifications.length === 0) {{
        container.innerHTML = '<p style="color:var(--muted); text-align:center; padding:1rem;">No notifications.</p>';
        return;
      }}
      container.innerHTML = data.notifications.map(n => `
        <div style="background:rgba(0,0,0,0.3); border-left:3px solid ${{n.type === 'error' ? '#ef4444' : (n.type === 'success' ? '#10b981' : '#1d9bf0')}}; border:1px solid var(--card-border); padding:0.65rem; border-radius:6px; margin-bottom:0.5rem;">
          <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:var(--muted); margin-bottom:4px;">
            <strong>${{n.title}}</strong><span style="font-family:'JetBrains Mono',monospace;">${{n.timestamp}}</span>
          </div>
          <p style="font-size:0.8rem;">${{n.message}}</p>
        </div>
      `).join('');
    }}

    async function markAllNotifsRead() {{
      await fetch('/api/history/notifications/read-all', {{ method: 'POST' }});
      document.getElementById('unread-badge').innerText = '';
      loadNotifications();
    }}

    async function toggleAutoUnlike() {{
      const res = await fetch('/api/settings/auto-unlike/toggle', {{ method: 'POST' }});
      const data = await res.json();
      document.getElementById('btn-auto-unlike').innerText = data.auto_unlike ? 'Unlike: ON' : 'Unlike: OFF';
    }}

    async function startBulkUnlike() {{
      if (!confirm('Clean and unlike all synced likes on Twitter/X? Local database will remain safe.')) return;
      const res = await fetch('/api/maintenance/unlike-synced', {{ method: 'POST' }});
      const data = await res.json();
      alert(data.message);
    }}

    async function saveCookiesAuth() {{
      const auth_token = document.getElementById('auth-token').value;
      const ct0 = document.getElementById('auth-ct0').value;
      const username = document.getElementById('auth-username').value;
      const res = await fetch('/api/auth/cookies', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ auth_token, ct0, username }})
      }});
      if (res.ok) {{ alert('Connected successfully!'); location.reload(); }}
      else {{ alert('Failed to save cookies.'); }}
    }}

    async function disconnectTwitter() {{
      await fetch('/api/auth/disconnect', {{ method: 'POST' }});
      alert('Disconnected.'); location.reload();
    }}

    async function uploadArchive(input) {{
      if (!input.files || !input.files[0]) return;
      const file = input.files[0];
      const formData = new FormData();
      formData.append('file', file);
      const res = await fetch('/api/ingest/archive', {{ method: 'POST', body: formData }});
      const data = await res.json();
      if (res.ok) {{ alert(`Imported ${{data.parsed}} likes from archive!`); refreshStats(); loadLikes(false); }}
      else {{ alert('Import failed.'); }}
    }}

    async function exportMarkdown() {{
      const res = await fetch('/api/export/markdown', {{ method: 'POST' }});
      const data = await res.json();
      alert(`Exported ${{data.exported_count}} tweets to ${{data.export_dir}}!`);
    }}

    window.addEventListener('DOMContentLoaded', () => {{
      applyLayout();
      loadLikes(false);
    }});
  </script>
</body>
</html>"""
