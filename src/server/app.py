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


app = FastAPI(title="X-Likes Organizer", version="0.1.0", lifespan=lifespan)
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
    semantic: bool = Query(False, description="Enable vector semantic search"),
    offset: int = Query(0, ge=0),
    limit: int = Query(15, ge=1, le=100),
):
    vector = embedder.embed_text(q.strip()) if semantic and q.strip() else None
    results = store.search_hybrid(query=q.strip(), query_vector=vector, tag=tag, offset=offset, limit=limit)
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
    tags = store.get_all_tags()[:25]
    tags_html = "".join([f"<span class='tag' id='tag-{t['tag']}' onclick='filterTag(\"{t['tag']}\")'>{t['tag']} ({t['count']})</span>" for t in tags])
    auth = scraper.get_session_status()
    sched = scheduler.get_status()
    notifs = history.get_notifications(limit=10)
    unread = notifs["unread_count"]
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>𝕏 Likes Organizer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    :root {{ --bg: #090a0f; --card: #12151f; --border: #232936; --primary: #1d9bf0; --text: #e2e8f0; --muted: #94a3b8; --success: #10b981; --warn: #f59e0b; }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); padding: 2rem; }}
    .container {{ max-width: 1200px; margin: 0 auto; }}
    header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 1rem; margin-bottom: 1.5rem; }}
    .auth-banner {{ display: flex; align-items: center; gap: 0.75rem; background: var(--card); border: 1px solid var(--border); padding: 0.5rem 0.9rem; border-radius: 8px; font-size: 0.85rem; }}
    .dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
    .dot.connected {{ background: var(--success); box-shadow: 0 0 8px var(--success); }}
    .dot.disconnected {{ background: #ef4444; }}
    .sync-pill {{ display: flex; align-items: center; gap: 0.5rem; background: var(--card); border: 1px solid var(--border); padding: 0.4rem 0.75rem; border-radius: 8px; font-size: 0.85rem; }}
    .sync-select {{ background: #1a202c; color: var(--text); border: 1px solid var(--border); padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.8rem; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-bottom: 1.5rem; }}
    .card {{ background: var(--card); border: 1px solid var(--border); padding: 1.25rem; border-radius: 8px; }}
    .card h4 {{ color: var(--muted); font-size: 0.8rem; text-transform: uppercase; }}
    .card p {{ font-size: 1.6rem; font-weight: bold; }}
    .search-bar {{ display: flex; gap: 0.5rem; margin-bottom: 1rem; }}
    input[type="text"], input[type="password"] {{ flex: 1; padding: 0.8rem 1rem; background: var(--card); border: 1px solid var(--border); border-radius: 6px; color: var(--text); font-size: 1rem; }}
    button {{ background: var(--primary); color: white; border: none; padding: 0.55rem 1.1rem; border-radius: 6px; cursor: pointer; font-weight: 500; font-size: 0.85rem; }}
    button.secondary {{ background: #27272a; color: var(--text); }}
    button:hover {{ opacity: 0.9; }}
    .controls-row {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; }}
    .view-switcher {{ display: flex; gap: 0.35rem; background: var(--card); border: 1px solid var(--border); padding: 0.3rem; border-radius: 8px; }}
    .view-btn {{ background: transparent; color: var(--muted); border: none; padding: 0.35rem 0.75rem; border-radius: 5px; cursor: pointer; font-size: 0.8rem; }}
    .view-btn.active {{ background: rgba(29,155,240,0.2); color: var(--primary); font-weight: 600; }}
    .tag-cloud {{ display: flex; flex-wrap: wrap; gap: 0.5rem; margin-bottom: 1.5rem; }}
    .tag {{ background: rgba(29,155,240,0.15); color: var(--primary); padding: 0.25rem 0.6rem; border-radius: 999px; font-size: 0.8rem; cursor: pointer; transition: all 0.2s ease; }}
    .tag:hover {{ background: rgba(29,155,240,0.3); }}
    .tag.active {{ background: var(--primary); color: #fff; font-weight: bold; }}
    #results.view-1col {{ display: flex; flex-direction: column; gap: 1rem; max-width: 800px; margin: 0 auto; }}
    #results.view-2col {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
    #results.view-3col {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }}
    #results.view-compact {{ display: flex; flex-direction: column; gap: 0.4rem; }}
    @media (max-width: 900px) {{ #results.view-3col {{ grid-template-columns: repeat(2, 1fr); }} }}
    @media (max-width: 650px) {{ #results.view-2col, #results.view-3col {{ grid-template-columns: 1fr; }} }}
    .tweet-card {{ background: var(--card); border: 1px solid var(--border); padding: 1.25rem; border-radius: 8px; transition: transform 0.15s ease; }}
    .tweet-card:hover {{ border-color: rgba(29,155,240,0.4); }}
    .tweet-header {{ display: flex; justify-content: space-between; color: var(--muted); font-size: 0.85rem; margin-bottom: 0.5rem; }}
    .compact-row {{ display: flex; justify-content: space-between; align-items: center; background: var(--card); border: 1px solid var(--border); padding: 0.75rem 1rem; border-radius: 6px; gap: 1rem; font-size: 0.85rem; }}
    .compact-text {{ flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .sync-drawer {{ display: none; background: #0e111a; border: 1px solid var(--border); border-radius: 8px; padding: 1.25rem; margin-bottom: 1.5rem; }}
    .progress-bar-bg {{ background: #1e2330; border-radius: 6px; height: 10px; overflow: hidden; margin: 0.75rem 0; }}
    .progress-bar-fill {{ background: linear-gradient(90deg, #1d9bf0, #10b981); height: 100%; width: 0%; transition: width 0.3s ease; }}
    .feed-terminal {{ background: #06070a; border: 1px solid #1a202c; border-radius: 6px; padding: 0.75rem; max-height: 160px; overflow-y: auto; font-family: monospace; font-size: 0.8rem; color: #a0aec0; }}
    .modal {{ display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); align-items: center; justify-content: center; z-index: 100; }}
    .modal-content {{ background: var(--card); border: 1px solid var(--border); padding: 2rem; border-radius: 12px; max-width: 650px; width: 90%; max-height: 85vh; overflow-y: auto; }}
    .tabs {{ display: flex; gap: 1rem; border-bottom: 1px solid var(--border); margin-bottom: 1.5rem; }}
    .tab {{ padding: 0.5rem 1rem; cursor: pointer; color: var(--muted); border-bottom: 2px solid transparent; }}
    .tab.active {{ color: var(--primary); border-bottom-color: var(--primary); font-weight: 600; }}
    .badge {{ background: #ef4444; color: white; border-radius: 999px; padding: 0.15rem 0.45rem; font-size: 0.75rem; font-weight: bold; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
    th, td {{ padding: 0.6rem; text-align: left; border-bottom: 1px solid var(--border); }}
    th {{ color: var(--muted); }}
    .status-tag {{ padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem; }}
    .status-tag.success {{ background: rgba(16,185,129,0.2); color: var(--success); }}
    .status-tag.error {{ background: rgba(239,68,68,0.2); color: #ef4444; }}
    .notif-card {{ background: #090a0f; border: 1px solid var(--border); padding: 0.85rem; border-radius: 6px; margin-bottom: 0.75rem; }}
    .skeleton {{ background: linear-gradient(90deg, #151926 25%, #1e2436 50%, #151926 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 4px; }}
    @keyframes shimmer {{ 0% {{ background-position: 200% 0; }} 100% {{ background-position: -200% 0; }} }}
    .skeleton-card {{ background: var(--card); border: 1px solid var(--border); padding: 1.25rem; border-radius: 8px; }}
    .media-grid {{ display: flex; gap: 0.5rem; margin-top: 0.75rem; overflow-x: auto; }}
    .media-thumb {{ max-height: 180px; border-radius: 6px; object-fit: cover; border: 1px solid var(--border); }}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>𝕏 Likes Organizer</h1>
      <div style="display:flex; gap:0.5rem; align-items:center;">
        <div class="auth-banner">
          <span class="dot {'connected' if auth['connected'] else 'disconnected'}"></span>
          <span>{f'Connected @{auth["username"]}' if auth['connected'] and auth['username'] else ('Connected' if auth['connected'] else 'Not Connected')}</span>
          <button class="secondary" onclick="openAuthModal()" style="padding:0.25rem 0.5rem; font-size:0.75rem;">{ 'Manage' if auth['connected'] else 'Connect' }</button>
        </div>

        <div class="sync-pill">
          <button id="btn-auto-sync" class="{'secondary' if not sched['enabled'] else ''}" style="padding:0.25rem 0.6rem; font-size:0.75rem;" onclick="toggleAutoSync()">
            {'Sync: ON' if sched['enabled'] else 'Sync: OFF'}
          </button>
          <select id="select-interval" class="sync-select" onchange="changeSyncInterval(this.value)">
            <option value="300" {'selected' if sched['interval_sec'] == 300 else ''}>5m</option>
            <option value="600" {'selected' if sched['interval_sec'] == 600 else ''}>10m</option>
            <option value="1800" {'selected' if sched['interval_sec'] == 1800 else ''}>30m</option>
            <option value="3600" {'selected' if sched['interval_sec'] == 3600 else ''}>1h</option>
            <option value="0" {'selected' if sched['interval_sec'] == 0 else ''}>Manual</option>
          </select>
          <span id="sync-countdown" style="font-size:0.75rem; color:var(--muted); min-width:65px;">Next: --:--</span>
        </div>

        <button id="btn-auto-unlike" class="secondary" onclick="toggleAutoUnlike()">{'Auto-Unlike: ON' if sched.get('auto_unlike') else 'Auto-Unlike: OFF'}</button>
        <button id="btn-sync" onclick="startSyncStream()">Sync Now</button>
        <button class="secondary" onclick="openHistoryModal()">
          Logs & Alerts {f'<span class="badge" id="unread-badge">{unread}</span>' if unread > 0 else '<span id="unread-badge"></span>'}
        </button>
        <button class="secondary" onclick="document.getElementById('file-upload').click()">Import like.js</button>
        <input type="file" id="file-upload" style="display:none" onchange="uploadArchive(this)">
        <button class="secondary" onclick="exportMarkdown()">Export</button>
      </div>
    </header>

    <div id="sync-drawer" class="sync-drawer">
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <strong id="sync-status-title" style="font-size:0.9rem;">Syncing Likes...</strong>
        <span id="sync-percent-text" style="font-size:0.85rem; color:var(--primary); font-weight:bold;">0%</span>
      </div>
      <div class="progress-bar-bg">
        <div id="progress-fill" class="progress-bar-fill"></div>
      </div>
      <div id="feed-terminal" class="feed-terminal">
        <div>[Ready] Decoupled 2-stage pipeline with unified Sync ON/OFF controls.</div>
      </div>
    </div>

    <div class="grid">
      <div class="card"><h4>Total Likes</h4><p id="stat-total">{stats['total_likes']}</p></div>
      <div class="card"><h4>Vectors</h4><p id="stat-vectors">{stats['indexed_vectors']}</p></div>
      <div class="card">
        <h4>Media Files</h4>
        <p id="stat-media">{stats['archived_media_files']} <span style="font-size:0.75rem; color:var(--muted); font-weight:normal;">({q_stat['pending_count']} queued)</span></p>
      </div>
      <div class="card"><h4>Tags</h4><p id="stat-tags">{stats['tags_count']}</p></div>
    </div>

    <div class="search-bar">
      <input id="query" type="text" placeholder="Search likes (FTS + Vector Semantic)..." onkeyup="if(event.key==='Enter') triggerNewSearch()">
      <button onclick="triggerNewSearch()">Search</button>
      <button class="secondary" onclick="clearFilters()" id="btn-clear-filter" style="display:none;">Clear Filter</button>
    </div>

    <div class="controls-row">
      <div class="tag-cloud" style="margin-bottom:0;">{tags_html}</div>
      <div class="view-switcher">
        <button class="view-btn" id="btn-view-1col" onclick="setViewMode('1col')">1 Col</button>
        <button class="view-btn active" id="btn-view-2col" onclick="setViewMode('2col')">2 Col</button>
        <button class="view-btn" id="btn-view-3col" onclick="setViewMode('3col')">3 Col</button>
        <button class="view-btn" id="btn-view-compact" onclick="setViewMode('compact')">List</button>
      </div>
    </div>

    <div id="results" class="view-2col"></div>
    <div id="scroll-sentinel" style="height:20px; margin-top:1rem;"></div>
  </div>

  <div class="modal" id="history-modal">
    <div class="modal-content">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem;">
        <h3>Activity & Notification History</h3>
        <button class="secondary" onclick="closeHistoryModal()">Close</button>
      </div>
      <div class="tabs">
        <div class="tab active" id="tab-btn-logs" onclick="switchHistTab('logs')">Sync Logs</div>
        <div class="tab" id="tab-btn-notifs" onclick="switchHistTab('notifs')">Notifications</div>
      </div>
      <div id="tab-logs">
        <div id="logs-container" style="overflow-x:auto;">Loading logs...</div>
      </div>
      <div id="tab-notifs" style="display:none;">
        <div style="display:flex; justify-content:flex-end; margin-bottom:0.75rem;">
          <button class="secondary" onclick="markAllNotifsRead()" style="padding:0.25rem 0.6rem; font-size:0.75rem;">Mark All Read</button>
        </div>
        <div id="notifs-container">Loading notifications...</div>
      </div>
    </div>
  </div>

  <div class="modal" id="auth-modal">
    <div class="modal-content">
      <h3 style="margin-bottom:1rem;">Twitter Account & Maintenance</h3>
      <div class="tabs">
        <div class="tab active" id="tab-btn-login" onclick="switchTab('login')">Account Login</div>
        <div class="tab" id="tab-btn-cookies" onclick="switchTab('cookies')">Paste Cookies</div>
        <div class="tab" id="tab-btn-clean" onclick="switchTab('clean')">Clean X Likes</div>
      </div>
      <div id="tab-login">
        <p style="color:var(--muted); margin-bottom:1rem; font-size:0.9rem;">Sign in directly with your Twitter credentials.</p>
        <input type="text" id="login-username" placeholder="Username, email, or phone" style="width:100%; margin-bottom:0.75rem;">
        <input type="password" id="login-password" placeholder="Password" style="width:100%; margin-bottom:0.75rem;">
        <input type="text" id="login-phone" placeholder="Phone or handle (if challenged)" style="width:100%; margin-bottom:1rem;">
        <button onclick="submitDirectLogin()" id="btn-submit-login" style="width:100%;">Sign In to Twitter</button>
      </div>
      <div id="tab-cookies" style="display:none;">
        <p style="color:var(--muted); margin-bottom:1rem; font-size:0.9rem;">Paste session cookies from your browser (F12 > Application > Cookies).</p>
        <input type="text" id="auth-username" placeholder="@handle (e.g. karpathy)" style="width:100%; margin-bottom:0.75rem;">
        <input type="text" id="auth-token" placeholder="auth_token (required)" style="width:100%; margin-bottom:0.75rem;">
        <input type="text" id="auth-ct0" placeholder="ct0 (optional)" style="width:100%; margin-bottom:1rem;">
        <button onclick="saveCookiesAuth()" style="width:100%;">Save & Connect</button>
      </div>
      <div id="tab-clean" style="display:none;">
        <p style="color:var(--muted); margin-bottom:1rem; font-size:0.9rem;">Unlikes all indexed tweets on X to clean up your public timeline. Your local LanceDB database will NOT be touched.</p>
        <button onclick="startBulkUnlike()" style="background:#ef4444; width:100%;">Clean & Unlike All on X</button>
      </div>
      <div style="display:flex; justify-content:space-between; margin-top:1.5rem;">
        <button class="secondary" onclick="disconnectTwitter()">Disconnect</button>
        <button class="secondary" onclick="closeAuthModal()">Close</button>
      </div>
    </div>
  </div>

  <script>
    let currentQuery = '';
    let currentTag = null;
    let currentOffset = 0;
    let isLoading = false;
    let hasMore = true;
    let viewMode = localStorage.getItem('likes_view_mode') || '2col';
    let nextSyncSeconds = {sched.get('next_sync_in_sec', 0)};
    let isSyncEnabled = {str(sched.get('enabled', True)).lower()};
    let syncInterval = {sched.get('interval_sec', 600)};
    const PAGE_LIMIT = 18;

    function setViewMode(mode) {{
      viewMode = mode;
      localStorage.setItem('likes_view_mode', mode);
      document.querySelectorAll('.view-btn').forEach(b => b.classList.remove('active'));
      const activeBtn = document.getElementById('btn-view-' + mode);
      if (activeBtn) activeBtn.classList.add('active');
      const results = document.getElementById('results');
      results.className = 'view-' + mode;
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

      const url = `/api/search?q=${{encodeURIComponent(currentQuery)}}&semantic=true&offset=${{currentOffset}}&limit=${{PAGE_LIMIT}}` + (currentTag ? `&tag=${{encodeURIComponent(currentTag)}}` : '');
      try {{
        const res = await fetch(url);
        const data = await res.json();
        const results = data.results || [];
        
        if (!append) container.innerHTML = '';
        if (results.length < PAGE_LIMIT) hasMore = false;

        if (results.length === 0 && !append) {{
          container.innerHTML = '<div class="card" style="text-align:center; color: var(--muted); grid-column: 1 / -1;">No matching likes found.</div>';
          return;
        }}

        const html = results.map(r => {{
          if (viewMode === 'compact') {{
            return `
              <div class="compact-row">
                <span style="font-weight:600; color:var(--primary); min-width:120px;">@${{r.author_handle || 'user'}}</span>
                <span class="compact-text">${{r.text}}</span>
                <div style="display:flex; gap:0.25rem;">${{(r.tags || []).slice(0, 2).map(t => `<span class="tag" style="font-size:0.7rem; padding:0.1rem 0.4rem;" onclick="filterTag('${{t}}')">${{t}}</span>`).join('')}}</div>
                <a href="${{r.url}}" target="_blank" style="color:var(--muted); font-size:0.75rem;">Link</a>
              </div>
            `;
          }}
          return `
            <div class="tweet-card">
              <div class="tweet-header"><span><strong>${{r.author_name || 'User'}}</strong> <span style="color:var(--muted)">@${{r.author_handle || 'user'}}</span></span><a href="${{r.url}}" target="_blank" style="color:var(--primary)">View on X</a></div>
              <p style="white-space:pre-wrap; line-height:1.4;">${{r.text}}</p>
              ${{r.media_urls && r.media_urls.length ? `<div class="media-grid">${{r.media_urls.map(m => `<img class="media-thumb" src="${{m}}" loading="lazy">`).join('')}}</div>` : ''}}
              <div style="margin-top:0.75rem">${{(r.tags || []).map(t => `<span class="tag" onclick="filterTag('${{t}}')">${{t}}</span>`).join(' ')}}</div>
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
      document.querySelectorAll('.tag').forEach(el => el.classList.remove('active'));
      const activeEl = document.getElementById('tag-' + tag);
      if (activeEl) activeEl.classList.add('active');
      document.getElementById('btn-clear-filter').style.display = 'inline-block';
      currentTag = tag;
      document.getElementById('query').value = '';
      currentQuery = '';
      loadLikes(false);
    }}

    function clearFilters() {{
      document.querySelectorAll('.tag').forEach(el => el.classList.remove('active'));
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

    // Countdown timer ticker
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
      if (nextSyncSeconds === 0) {{
        nextSyncSeconds = syncInterval;
      }}
    }}, 1000);

    async function toggleAutoSync() {{
      const res = await fetch('/api/scheduler/toggle', {{ method: 'POST' }});
      const data = await res.json();
      isSyncEnabled = data.enabled;
      nextSyncSeconds = data.status.next_sync_in_sec || syncInterval;
      const btn = document.getElementById('btn-auto-sync');
      btn.innerText = isSyncEnabled ? 'Sync: ON' : 'Sync: OFF';
      btn.className = isSyncEnabled ? '' : 'secondary';
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

    window.addEventListener('DOMContentLoaded', () => {{
      setViewMode(viewMode);
    }});

    function openAuthModal() {{ document.getElementById('auth-modal').style.display = 'flex'; }}
    function closeAuthModal() {{ document.getElementById('auth-modal').style.display = 'none'; }}
    function openHistoryModal() {{ document.getElementById('history-modal').style.display = 'flex'; loadHistoryLogs(); loadNotifications(); }}
    function closeHistoryModal() {{ document.getElementById('history-modal').style.display = 'none'; }}
    
    function switchTab(t) {{
      ['login', 'cookies', 'clean'].forEach(tab => {{
        document.getElementById('tab-' + tab).style.display = tab === t ? 'block' : 'none';
        document.getElementById('tab-btn-' + tab).className = 'tab ' + (tab === t ? 'active' : '');
      }});
    }}
    function switchHistTab(t) {{
      document.getElementById('tab-logs').style.display = t === 'logs' ? 'block' : 'none';
      document.getElementById('tab-notifs').style.display = t === 'notifs' ? 'block' : 'none';
      document.getElementById('tab-btn-logs').className = 'tab ' + (t === 'logs' ? 'active' : '');
      document.getElementById('tab-btn-notifs').className = 'tab ' + (t === 'notifs' ? 'active' : '');
    }}

    async function loadHistoryLogs() {{
      const res = await fetch('/api/history/logs?limit=30');
      const data = await res.json();
      const container = document.getElementById('logs-container');
      if (!data.logs || data.logs.length === 0) {{
        container.innerHTML = '<p style="color:var(--muted); text-align:center; padding:1rem;">No sync logs recorded yet.</p>';
        return;
      }}
      container.innerHTML = `
        <table>
          <thead>
            <tr><th>Time</th><th>Trigger</th><th>Engine</th><th>Added</th><th>Total</th><th>Status</th><th>Duration</th></tr>
          </thead>
          <tbody>
            ${{data.logs.map(l => `
              <tr>
                <td>${{l.timestamp.split(' ')[1]}}</td>
                <td><strong>${{l.trigger}}</strong></td>
                <td>${{l.engine}}</td>
                <td>+${{l.new_likes}}</td>
                <td>${{l.total_db_likes}}</td>
                <td><span class="status-tag ${{l.status}}">${{l.status}}</span></td>
                <td>${{l.duration_sec}}s</td>
              </tr>
            `).join('')}}
          </tbody>
        </table>
      `;
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
        <div class="notif-card" style="border-left: 3px solid ${{n.type === 'error' ? '#ef4444' : (n.type === 'success' ? '#10b981' : '#1d9bf0')}}">
          <div style="display:flex; justify-content:space-between; font-size:0.8rem; color:var(--muted); margin-bottom:0.25rem;">
            <strong>${{n.title}}</strong><span>${{n.timestamp}}</span>
          </div>
          <p style="font-size:0.85rem;">${{n.message}}</p>
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
      document.getElementById('btn-auto-unlike').innerText = data.auto_unlike ? 'Auto-Unlike: ON' : 'Auto-Unlike: OFF';
    }}

    async function startBulkUnlike() {{
      if (!confirm('Clean and unlike all synced likes on Twitter/X? Local database will remain safe.')) return;
      const res = await fetch('/api/maintenance/unlike-synced', {{ method: 'POST' }});
      const data = await res.json();
      alert(data.message);
      closeAuthModal();
    }}

    async function submitDirectLogin() {{
      const username = document.getElementById('login-username').value;
      const password = document.getElementById('login-password').value;
      const email_or_phone = document.getElementById('login-phone').value;
      const btn = document.getElementById('btn-submit-login');
      btn.innerText = 'Signing in...'; btn.disabled = true;
      try {{
        const res = await fetch('/api/auth/login', {{
          method: 'POST',
          headers: {{ 'Content-Type': 'application/json' }},
          body: JSON.stringify({{ username, password, email_or_phone }})
        }});
        const data = await res.json();
        if (res.ok) {{ alert('Logged in successfully!'); location.reload(); }}
        else {{ alert(data.message || 'Login failed.'); }}
      }} finally {{
        btn.innerText = 'Sign In to Twitter'; btn.disabled = false;
      }}
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
    function startSyncStream() {{
      const drawer = document.getElementById('sync-drawer');
      const progressFill = document.getElementById('progress-fill');
      const percentText = document.getElementById('sync-percent-text');
      const statusTitle = document.getElementById('sync-status-title');
      const feed = document.getElementById('feed-terminal');
      const btn = document.getElementById('btn-sync');
      
      drawer.style.display = 'block';
      feed.innerHTML = '';
      btn.disabled = true; btn.innerText = 'Syncing...';

      const es = new EventSource('/api/sync/stream?max_tweets=0');
      es.onmessage = function(e) {{
        const data = JSON.parse(e.data);
        if (data.error) {{
          statusTitle.innerText = 'Sync Error';
          feed.innerHTML += `<div style="color:#ef4444;">[ERROR] ${{data.error}}</div>`;
          es.close();
          btn.disabled = false; btn.innerText = 'Sync Now';
          if (data.error.includes('connect')) openAuthModal();
          return;
        }}
        if (data.stage === 'scrolling') {{
          statusTitle.innerText = `[Scroll #${{data.scroll_attempt}}] Scraped ${{data.tweets_found}} likes...`;
          feed.innerHTML += `<div>[Scroll #${{data.scroll_attempt}}] ${{data.page_url}} | Found: <strong style="color:#10b981;">${{data.tweets_found}}</strong> likes (Height: ${{data.height}}px)</div>`;
          feed.scrollTop = feed.scrollHeight;
        }} else if (data.stage === 'item_done') {{
          statusTitle.innerText = `Ingesting (#${{data.current}})...`;
          const unlikeTag = data.unliked ? ' <span style="color:#ef4444;">[Unliked on X]</span>' : '';
          feed.innerHTML += `<div>[Stage 1 Ingested] <strong style="color:var(--primary)">@${{data.author_handle || 'user'}}</strong>: "${{data.text}}" <span style="color:#10b981;">[${{data.tags.join(', ')}}]</span>${{unlikeTag}}</div>`;
          feed.scrollTop = feed.scrollHeight;
        }} else if (data.stage === 'complete') {{
          progressFill.style.width = '100%';
          percentText.innerText = '100%';
          statusTitle.innerText = 'Sync Complete!';
          feed.innerHTML += `<div style="color:#10b981; font-weight:bold;">[DONE] ${{data.message}}</div>`;
          es.close();
          btn.disabled = false; btn.innerText = 'Sync Now';
          setTimeout(() => location.reload(), 2000);
        }}
      }};
      es.onerror = function() {{
        statusTitle.innerText = 'Connection Closed';
        es.close();
        btn.disabled = false; btn.innerText = 'Sync Now';
      }};
    }}
    async function uploadArchive(input) {{
      if (!input.files || !input.files[0]) return;
      const file = input.files[0];
      const formData = new FormData();
      formData.append('file', file);
      const res = await fetch('/api/ingest/archive', {{ method: 'POST', body: formData }});
      const data = await res.json();
      if (res.ok) {{ alert(`Imported ${{data.parsed}} likes from archive!`); location.reload(); }}
      else {{ alert('Import failed.'); }}
    }}
    async function exportMarkdown() {{
      const res = await fetch('/api/export/markdown', {{ method: 'POST' }});
      const data = await res.json();
      alert(`Exported ${{data.exported_count}} tweets to ${{data.export_dir}}!`);
    }}
  </script>
</body>
</html>"""
