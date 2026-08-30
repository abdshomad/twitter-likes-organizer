import asyncio
import time
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
from src.ai.rag_chat import RAGChatEngine
from src.exporter.markdown_exporter import export_tweets_to_directory
from src.ingestion.archive_parser import parse_like_js_content
from src.ingestion.playwright_scraper import PlaywrightXScraper
from src.ingestion.graphql_client import TwitterGraphQLClient
from src.ingestion.unliker import TwitterUnliker
from src.ingestion.sync_pipeline import stream_likes_sync
from src.ingestion.author_hydrator import AuthorHydrator
from src.ingestion.background_sync import BackgroundSyncScheduler

MEDIA_DIR.mkdir(parents=True, exist_ok=True)
store = LanceDBStore()
history = HistoryManager()
tagger = AITagger()
embedder = VectorEmbedder()
rag_chat = RAGChatEngine(store=store, embedder=embedder)
media_queue = MediaQueue(store=store)
scraper = PlaywrightXScraper()
unliker = TwitterUnliker(scraper.session_path)
scheduler = BackgroundSyncScheduler(scraper, store, tagger, embedder, media_queue, interval_sec=600)
author_hydrator = AuthorHydrator(store=store)


async def author_hydrator_loop(hydrator: AuthorHydrator):
    while True:
        try:
            count = await hydrator.hydrate_missing_authors(batch_size=30, concurrency=5)
            if count == 0:
                await asyncio.sleep(60)
            else:
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sched_task = asyncio.create_task(scheduler.start_loop())
    queue_task = asyncio.create_task(media_queue.worker_loop())
    author_task = asyncio.create_task(author_hydrator_loop(author_hydrator))
    yield
    sched_task.cancel()
    queue_task.cancel()
    author_task.cancel()
    await author_hydrator.close()
    await rag_chat.close()


app = FastAPI(title="𝕏 Likes Organizer HUD", version="0.2.0", lifespan=lifespan)
app.mount("/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return HTMLResponse('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24"><text y="20" font-size="20">𝕏</text></svg>', media_type="image/svg+xml")


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


@app.post("/api/maintenance/unlike-single")
async def maintenance_unlike_single(payload: dict[str, Any] = Body(...)):
    tweet_id = payload.get("tweet_id", "")
    if not tweet_id:
        return JSONResponse({"status": "error", "message": "tweet_id is required"}, status_code=400)
    success = await unliker.unlike_tweet(tweet_id)
    return {"status": "success" if success else "failed", "tweet_id": tweet_id, "unliked": success}


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


@app.get("/api/chat/stream")
async def chat_stream(q: str = Query(..., description="User chat query to LanceDB")):
    return StreamingResponse(
        rag_chat.stream_chat_response(query=q),
        media_type="text/event-stream",
    )


@app.get("/api/search")
async def search_likes(
    q: str = Query("", description="Search query"),
    tag: str | None = Query(None, description="Tag filter"),
    author: str | None = Query(None, description="Author filter"),
    sort_by: str = Query("newest", description="Sort option"),
    semantic: bool = Query(False, description="Enable vector semantic search"),
    offset: int = Query(0, ge=0),
    limit: int = Query(24, ge=1, le=100),
):
    t0 = time.perf_counter()
    vector = embedder.embed_text(q.strip()) if (semantic and q.strip()) else None
    
    # Pushdown author filter if present
    query_text = q.strip()
    if author:
        author_clean = author.lstrip("@").strip()
        if not query_text:
            query_text = author_clean

    results = store.search_hybrid(query=query_text, query_vector=vector, tag=tag, sort_by=sort_by, offset=offset, limit=limit)
    if author:
        author_clean = author.lstrip("@").lower()
        results = [r for r in results if (r.get("author_handle") or "").lstrip("@").lower() == author_clean or author_clean in (r.get("author_name") or "").lower()]

    latency_ms = (time.perf_counter() - t0) * 1000.0
    return {
        "count": len(results),
        "latency_ms": round(latency_ms, 2),
        "semantic": semantic,
        "offset": offset,
        "limit": limit,
        "results": results,
    }


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
    
    /* Pure HUD Floating Topbar */
    .hud-topbar {{ position: fixed; top: 12px; left: 16px; right: 16px; height: 56px; background: rgba(11, 15, 25, 0.85); backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); border: 1px solid rgba(255, 255, 255, 0.12); border-radius: 12px; display: flex; align-items: center; justify-content: space-between; padding: 0 16px; z-index: 100; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.6); }}
    .hud-brand {{ display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 1rem; color: #fff; text-decoration: none; }}
    .hud-brand span {{ background: linear-gradient(135deg, #1d9bf0, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
    
    .hud-badge {{ font-size: 0.75rem; padding: 3px 8px; border-radius: 6px; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; font-family: 'JetBrains Mono', monospace; cursor: pointer; }}
    .hud-badge.connected {{ background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.35); }}
    .hud-badge.disconnected {{ background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.35); }}
    .dot {{ width: 7px; height: 7px; border-radius: 50%; display: inline-block; }}
    .dot.connected {{ background: #10b981; box-shadow: 0 0 8px #10b981; }}
    .dot.disconnected {{ background: #ef4444; }}
    
    /* HUD Stats Ticker */
    .hud-ticker {{ display: flex; align-items: center; gap: 12px; font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; background: rgba(0, 0, 0, 0.35); border: 1px solid var(--card-border); padding: 4px 12px; border-radius: 8px; }}
    .ticker-item {{ color: var(--muted); }}
    .ticker-item strong {{ color: var(--text); }}
    
    /* Standardized Pure HUD Icon Deck */
    .hud-deck {{ display: flex; align-items: center; gap: 6px; }}
    .hud-icon-btn {{ position: relative; width: 36px; height: 36px; background: rgba(255, 255, 255, 0.05); color: var(--muted); border: 1px solid var(--card-border); border-radius: 8px; display: flex; align-items: center; justify-content: center; cursor: pointer; transition: all 0.2s ease; outline: none; }}
    .hud-icon-btn svg {{ width: 17px; height: 17px; fill: none; stroke: currentColor; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }}
    .hud-icon-btn:hover {{ background: rgba(255, 255, 255, 0.12); color: #fff; border-color: rgba(255, 255, 255, 0.25); transform: translateY(-1px); }}
    .hud-icon-btn.active {{ background: rgba(29, 155, 240, 0.2); color: var(--primary); border-color: rgba(29, 155, 240, 0.5); box-shadow: 0 0 10px rgba(29, 155, 240, 0.3); }}
    .hud-icon-btn.accent {{ background: linear-gradient(135deg, rgba(29, 155, 240, 0.3), rgba(99, 102, 241, 0.3)); border-color: rgba(29, 155, 240, 0.6); color: #fff; box-shadow: 0 0 12px rgba(29, 155, 240, 0.3); }}
    .hud-icon-btn.accent:hover {{ background: linear-gradient(135deg, rgba(29, 155, 240, 0.5), rgba(99, 102, 241, 0.5)); }}
    .badge-corner {{ position: absolute; top: -3px; right: -3px; background: #ef4444; color: white; border-radius: 999px; padding: 0.1rem 0.35rem; font-size: 0.65rem; font-weight: bold; font-family: 'JetBrains Mono', monospace; }}
    
    .sync-select, .sort-select {{ background: #131926; color: var(--text); border: 1px solid var(--card-border); padding: 0.35rem 0.55rem; border-radius: 6px; font-size: 0.75rem; font-family: 'JetBrains Mono', monospace; outline: none; }}
    
    /* Blazingly Fast HUD Filter Capsule */
    .hud-filter-dock {{ background: var(--card-bg); backdrop-filter: blur(12px); border: 1px solid var(--card-border); border-radius: 12px; padding: 1rem 1.25rem; margin-bottom: 1.5rem; display: flex; flex-direction: column; gap: 0.85rem; box-shadow: 0 8px 24px rgba(0, 0, 0, 0.4); }}
    .search-row {{ display: flex; gap: 0.6rem; align-items: center; }}
    .search-capsule {{ flex: 1; position: relative; display: flex; align-items: center; background: rgba(8, 11, 18, 0.85); border: 1px solid var(--card-border); border-radius: 8px; padding: 0 10px; transition: border-color 0.2s; }}
    .search-capsule:focus-within {{ border-color: var(--primary); box-shadow: 0 0 10px rgba(29, 155, 240, 0.3); }}
    .search-capsule svg {{ color: var(--muted); width: 16px; height: 16px; flex-shrink: 0; }}
    .hud-input {{ flex: 1; padding: 0.75rem 0.6rem; background: transparent; border: none; color: var(--text); font-size: 0.95rem; outline: none; }}
    .clear-search-btn {{ background: transparent; border: none; color: var(--muted); cursor: pointer; padding: 4px; display: none; }}
    .clear-search-btn:hover {{ color: #fff; }}
    .semantic-toggle-btn {{ background: rgba(255, 255, 255, 0.05); color: var(--muted); border: 1px solid var(--card-border); padding: 0.5rem 0.8rem; border-radius: 6px; cursor: pointer; font-size: 0.75rem; font-weight: 600; font-family: 'JetBrains Mono', monospace; display: inline-flex; align-items: center; gap: 6px; transition: all 0.2s ease; }}
    .semantic-toggle-btn.active {{ background: rgba(99, 102, 241, 0.25); color: #a5b4fc; border-color: #6366f1; box-shadow: 0 0 10px rgba(99, 102, 241, 0.3); }}
    .latency-badge {{ font-size: 0.7rem; font-family: 'JetBrains Mono', monospace; color: #10b981; padding: 0 4px; }}
    
    /* Toolbar Group & Switchers */
    .controls-row {{ display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 0.75rem; }}
    .btn-group {{ display: flex; gap: 3px; background: rgba(0, 0, 0, 0.35); border: 1px solid var(--card-border); padding: 3px; border-radius: 8px; }}
    .icon-btn {{ background: transparent; color: var(--muted); border: none; padding: 0.35rem 0.6rem; border-radius: 6px; cursor: pointer; display: flex; align-items: center; gap: 5px; font-size: 0.75rem; font-weight: 600; transition: all 0.15s ease; }}
    .icon-btn svg {{ width: 14px; height: 14px; fill: currentColor; stroke: currentColor; }}
    .icon-btn.active {{ background: rgba(29, 155, 240, 0.25); color: #60a5fa; border: 1px solid rgba(29, 155, 240, 0.4); }}
    
    /* Tag Cloud & Author Filter Active Pills */
    .hud-tag-cloud {{ display: flex; flex-wrap: wrap; gap: 0.45rem; }}
    .hud-tag {{ background: rgba(29, 155, 240, 0.1); color: #7dd3fc; border: 1px solid rgba(29, 155, 240, 0.25); padding: 0.25rem 0.6rem; border-radius: 999px; font-size: 0.75rem; cursor: pointer; transition: all 0.15s ease; }}
    .hud-tag:hover {{ background: rgba(29, 155, 240, 0.25); border-color: rgba(29, 155, 240, 0.5); }}
    .hud-tag.active {{ background: #0284c7; color: #fff; font-weight: bold; border-color: #38bdf8; box-shadow: 0 0 10px rgba(56, 189, 248, 0.4); }}
    .tag-count {{ font-size: 0.7rem; opacity: 0.7; font-family: 'JetBrains Mono', monospace; }}
    .active-author-pill {{ background: rgba(99, 102, 241, 0.25); color: #c7d2fe; border: 1px solid #6366f1; padding: 0.25rem 0.65rem; border-radius: 999px; font-size: 0.75rem; font-family: 'JetBrains Mono', monospace; cursor: pointer; display: inline-flex; align-items: center; gap: 5px; }}

    /* Multi-Column Layout Grid */
    #results.cols-1 {{ display: grid; grid-template-columns: 1fr; max-width: 820px; margin: 0 auto; gap: 1rem; }}
    #results.cols-2 {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; }}
    #results.cols-3 {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }}
    #results.cols-4 {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.85rem; }}
    @media (max-width: 1100px) {{ #results.cols-4 {{ grid-template-columns: repeat(3, 1fr); }} }}
    @media (max-width: 850px) {{ #results.cols-3, #results.cols-4 {{ grid-template-columns: repeat(2, 1fr); }} }}
    @media (max-width: 600px) {{ #results.cols-2, #results.cols-3, #results.cols-4 {{ grid-template-columns: 1fr; }} }}

    /* HUD Tweet Cards - Fully Clickable Surface */
    .hud-tweet-card {{ background: var(--card-bg); backdrop-filter: blur(8px); border: 1px solid var(--card-border); padding: 1.25rem; border-radius: 12px; display: flex; flex-direction: column; justify-content: space-between; transition: all 0.2s ease; cursor: pointer; position: relative; }}
    .hud-tweet-card:hover {{ border-color: rgba(29, 155, 240, 0.5); transform: translateY(-2px); box-shadow: 0 8px 25px rgba(0,0,0,0.6); }}
    .tweet-header {{ display: flex; justify-content: space-between; align-items: center; color: var(--muted); font-size: 0.85rem; margin-bottom: 0.6rem; }}
    .author-interactive {{ color: var(--text); font-weight: 700; transition: color 0.15s; display: inline-flex; align-items: center; gap: 4px; }}
    .author-interactive:hover {{ color: var(--primary); text-decoration: underline; }}
    .handle-badge {{ font-family: 'JetBrains Mono', monospace; color: var(--muted); font-size: 0.8rem; margin-left: 4px; }}
    .handle-badge:hover {{ color: #38bdf8; text-decoration: underline; }}
    .ext-link-icon {{ color: var(--muted); opacity: 0.6; padding: 2px 4px; border-radius: 4px; display: inline-flex; align-items: center; }}
    .ext-link-icon:hover {{ opacity: 1; color: var(--primary); background: rgba(29, 155, 240, 0.15); }}
    
    /* In-Text Clickable Elements */
    .tweet-text {{ white-space: pre-wrap; line-height: 1.45; font-size: 0.9rem; word-break: break-word; }}
    .tweet-text a {{ color: var(--primary); text-decoration: none; word-break: break-all; }}
    .tweet-text a:hover {{ text-decoration: underline; }}
    .tweet-mention {{ color: #38bdf8; cursor: pointer; font-weight: 500; font-family: 'JetBrains Mono', monospace; }}
    .tweet-mention:hover {{ text-decoration: underline; }}
    .tweet-hashtag {{ color: #a5b4fc; cursor: pointer; font-weight: 500; }}
    .tweet-hashtag:hover {{ text-decoration: underline; }}

    /* Media Grid & Lightbox Trigger */
    .media-grid {{ display: flex; gap: 0.5rem; margin-top: 0.75rem; overflow-x: auto; }}
    .media-thumb {{ max-height: 180px; border-radius: 8px; object-fit: cover; border: 1px solid var(--card-border); cursor: zoom-in; transition: transform 0.2s; }}
    .media-thumb:hover {{ transform: scale(1.02); border-color: rgba(29, 155, 240, 0.5); }}
    
    /* Compact Row & Gallery Styles */
    .compact-row {{ display: flex; justify-content: space-between; align-items: center; background: var(--card-bg); border: 1px solid var(--card-border); padding: 0.75rem 1rem; border-radius: 8px; gap: 1rem; font-size: 0.85rem; cursor: pointer; transition: border-color 0.2s; }}
    .compact-row:hover {{ border-color: var(--primary); }}
    .compact-text {{ flex: 1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .gallery-card {{ background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 10px; overflow: hidden; display: flex; flex-direction: column; cursor: pointer; transition: all 0.2s; }}
    .gallery-card:hover {{ border-color: var(--primary); transform: translateY(-2px); }}
    .gallery-img {{ width: 100%; height: 220px; object-fit: cover; border-bottom: 1px solid var(--card-border); }}
    .gallery-body {{ padding: 0.85rem; }}

    /* Fullscreen Glassmorphic Tweet Detail Lightbox Modal */
    .hud-modal-backdrop {{ position: fixed; inset: 0; background: rgba(3, 5, 10, 0.85); backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px); z-index: 200; display: none; align-items: center; justify-content: center; padding: 1.5rem; }}
    .hud-modal-backdrop.open {{ display: flex; }}
    .hud-modal-box {{ background: rgba(13, 17, 28, 0.95); border: 1px solid rgba(255, 255, 255, 0.15); border-radius: 16px; width: 100%; max-width: 760px; max-height: 90vh; overflow-y: auto; display: flex; flex-direction: column; box-shadow: 0 25px 60px rgba(0, 0, 0, 0.9); animation: modalIn 0.2s cubic-bezier(0.16, 1, 0.3, 1); }}
    @keyframes modalIn {{ from {{ opacity: 0; transform: scale(0.96); }} to {{ opacity: 1; transform: scale(1); }} }}
    .hud-modal-header {{ padding: 1.25rem 1.5rem; border-bottom: 1px solid var(--card-border); display: flex; justify-content: space-between; align-items: center; }}
    .hud-modal-body {{ padding: 1.5rem; display: flex; flex-direction: column; gap: 1.25rem; }}
    .hud-modal-media {{ display: flex; flex-direction: column; gap: 0.75rem; }}
    .hud-modal-img {{ width: 100%; max-height: 480px; object-fit: contain; background: #000; border-radius: 10px; border: 1px solid var(--card-border); }}
    .hud-modal-actions {{ display: flex; flex-wrap: wrap; gap: 8px; padding-top: 1rem; border-top: 1px solid var(--card-border); }}

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

    /* Sliding HUD RAG Chat Drawer */
    .hud-chat-drawer {{ position: fixed; top: 76px; bottom: 16px; left: 16px; width: 460px; background: rgba(11, 15, 25, 0.94); backdrop-filter: blur(22px); -webkit-backdrop-filter: blur(22px); border: 1px solid rgba(99, 102, 241, 0.35); border-radius: 16px; z-index: 120; display: flex; flex-direction: column; box-shadow: 10px 0 40px rgba(0, 0, 0, 0.75); transform: translateX(-500px); transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1); }}
    .hud-chat-drawer.open {{ transform: translateX(0); }}
    .chat-messages {{ flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 12px; font-size: 0.85rem; }}
    .chat-msg {{ padding: 10px 14px; border-radius: 10px; line-height: 1.4; max-width: 90%; }}
    .chat-msg.user {{ align-self: flex-end; background: linear-gradient(135deg, #1d9bf0, #4f46e5); color: #fff; border-bottom-right-radius: 2px; }}
    .chat-msg.assistant {{ align-self: flex-start; background: rgba(255, 255, 255, 0.05); border: 1px solid var(--card-border); color: var(--text); border-bottom-left-radius: 2px; }}
    .chat-source-card {{ font-size: 0.75rem; background: rgba(0, 0, 0, 0.4); border: 1px solid var(--card-border); padding: 6px 10px; border-radius: 6px; margin-top: 6px; cursor: pointer; }}
    .chat-source-card:hover {{ border-color: var(--primary); }}
    .chat-chip {{ font-size: 0.72rem; background: rgba(99, 102, 241, 0.15); color: #a5b4fc; border: 1px solid rgba(99, 102, 241, 0.3); padding: 4px 8px; border-radius: 6px; cursor: pointer; display: inline-block; transition: all 0.15s; }}
    .chat-chip:hover {{ background: rgba(99, 102, 241, 0.3); border-color: rgba(99, 102, 241, 0.6); }}
    .chat-input-row {{ padding: 12px 16px; border-top: 1px solid var(--card-border); display: flex; gap: 8px; background: rgba(8, 11, 18, 0.8); border-radius: 0 0 16px 16px; }}
    
    /* Non-blocking Floating Sync Toast */
    .hud-floating-toast {{ position: fixed; bottom: 1.5rem; right: 1.5rem; background: rgba(11, 15, 25, 0.95); backdrop-filter: blur(16px); border: 1px solid rgba(29, 155, 240, 0.4); box-shadow: 0 10px 30px rgba(0,0,0,0.8); border-radius: 12px; width: 340px; z-index: 95; display: none; overflow: hidden; font-size: 0.85rem; }}
    .toast-header {{ padding: 0.65rem 0.9rem; display: flex; justify-content: space-between; align-items: center; background: rgba(29, 155, 240, 0.1); border-bottom: 1px solid var(--card-border); }}
    .toast-body {{ padding: 0.75rem 0.9rem; }}
    .toast-progress {{ background: #131926; height: 6px; border-radius: 3px; overflow: hidden; margin-top: 0.5rem; }}
    .toast-progress-fill {{ background: linear-gradient(90deg, var(--primary), var(--success)); height: 100%; width: 0%; transition: width 0.3s ease; }}
    .toast-log {{ font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: var(--muted); max-height: 90px; overflow-y: auto; margin-top: 0.5rem; }}
    .hud-btn {{ background: rgba(255, 255, 255, 0.06); color: var(--text); border: 1px solid var(--card-border); padding: 0.4rem 0.8rem; border-radius: 6px; cursor: pointer; font-size: 0.8rem; font-weight: 600; display: inline-flex; align-items: center; gap: 6px; transition: all 0.2s ease; min-height: 38px; }}
    .hud-btn:hover {{ background: rgba(255, 255, 255, 0.12); border-color: rgba(255, 255, 255, 0.25); }}
    .mobile-drag-handle {{ width: 40px; height: 5px; background: rgba(255, 255, 255, 0.25); border-radius: 3px; margin: 8px auto 4px; display: none; }}

    /* Responsive Mobile Overrides */
    @media (max-width: 768px) {{
      body {{ padding-top: 70px; }}
      .container {{ padding: 0 0.5rem 2rem; }}
      .hud-topbar {{ left: 8px; right: 8px; top: 8px; height: 52px; padding: 0 8px; }}
      .hud-brand span {{ display: none; }}
      .hud-ticker {{ display: none; }}
      .desktop-only {{ display: none !important; }}
      .mobile-drag-handle {{ display: block; }}
      
      .hud-deck {{ gap: 4px; }}
      .hud-icon-btn {{ width: 38px; height: 38px; }}
      
      .hud-tag-cloud {{ display: flex; flex-wrap: nowrap; overflow-x: auto; -webkit-overflow-scrolling: touch; padding-bottom: 6px; scrollbar-width: none; }}
      .hud-tag-cloud::-webkit-scrollbar {{ display: none; }}
      .hud-tag {{ white-space: nowrap; flex-shrink: 0; padding: 0.4rem 0.75rem; font-size: 0.8rem; }}
      .active-author-pill {{ flex-shrink: 0; white-space: nowrap; padding: 0.4rem 0.75rem; font-size: 0.8rem; }}

      /* Mobile Full-Width Bottom Sheet Drawers */
      .hud-chat-drawer, .hud-sidesheet {{
        top: auto !important;
        left: 0 !important;
        right: 0 !important;
        bottom: 0 !important;
        width: 100% !important;
        height: 85vh !important;
        max-height: 85vh !important;
        border-radius: 20px 20px 0 0 !important;
        border-bottom: none !important;
        transform: translateY(105%) !important;
        box-shadow: 0 -10px 40px rgba(0, 0, 0, 0.85) !important;
      }}
      .hud-chat-drawer.open, .hud-sidesheet.open {{
        transform: translateY(0) !important;
      }}

      /* Mobile Modal */
      .hud-modal-backdrop {{ padding: 0; align-items: flex-end; }}
      .hud-modal-box {{ max-height: 90vh; border-radius: 20px 20px 0 0; width: 100%; border-bottom: none; }}
      .hud-modal-actions {{ flex-direction: column; }}
      .hud-modal-actions .hud-btn {{ width: 100%; justify-content: center; }}
      
      .hud-floating-toast {{ left: 1rem; right: 1rem; width: auto; bottom: 1rem; }}
    }}
  </style>
</head>
<body>
  <!-- Pure HUD Floating Topbar -->
  <header class="hud-topbar">
    <div style="display:flex; align-items:center; gap:10px;">
      <a href="#" class="hud-brand" onclick="clearFilters(); return false;">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
          <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
        </svg>
        <span>𝕏 LIKES HUD</span>
      </a>
      <span class="hud-badge {'connected' if auth['connected'] else 'disconnected'}" onclick="openAccountTab()" title="Manage Twitter Session">
        <span class="dot {'connected' if auth['connected'] else 'disconnected'}"></span>
        {f'@{auth["username"]}' if auth['connected'] and auth['username'] else ('ONLINE' if auth['connected'] else 'OFFLINE')}
      </span>
    </div>

    <!-- Center HUD Telemetry Ticker -->
    <div class="hud-ticker">
      <span class="ticker-item">TOTAL: <strong id="stat-total">{stats['total_likes']}</strong></span>
      <span class="ticker-item">MEDIA: <strong id="stat-media">{stats['archived_media_files']}</strong></span>
      <span class="ticker-item">TAGS: <strong id="stat-tags">{stats['tags_count']}</strong></span>
      <span id="sync-countdown" style="color:#38bdf8; font-weight:600; border-left:1px solid rgba(255,255,255,0.1); padding-left:8px;">Next: --:--</span>
    </div>

    <!-- Pure HUD SVG Icon Deck -->
    <div class="hud-deck">
      <!-- Chat with LanceDB RAG Button -->
      <button id="btn-chat-icon" class="hud-icon-btn accent" onclick="toggleChatDrawer()" title="Chat with LanceDB (AI RAG)">
        <svg viewBox="0 0 24 24"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>
      </button>

      <!-- Auto-Sync Toggle Icon Button -->
      <button id="btn-auto-sync-icon" class="hud-icon-btn {'active' if sched['enabled'] else ''}" onclick="toggleAutoSync()" title="Toggle Auto-Sync ({'ON' if sched['enabled'] else 'OFF'})">
        <svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
      </button>

      <!-- Sync Interval Selector -->
      <select id="select-interval" class="sync-select desktop-only" onchange="changeSyncInterval(this.value)" title="Sync Cycle Interval">
        <option value="300" {'selected' if sched['interval_sec'] == 300 else ''}>5m</option>
        <option value="600" {'selected' if sched['interval_sec'] == 600 else ''}>10m</option>
        <option value="1800" {'selected' if sched['interval_sec'] == 1800 else ''}>30m</option>
        <option value="3600" {'selected' if sched['interval_sec'] == 3600 else ''}>1h</option>
        <option value="0" {'selected' if sched['interval_sec'] == 0 else ''}>Manual</option>
      </select>

      <!-- Auto-Unlike Toggle Icon Button -->
      <button id="btn-auto-unlike-icon" class="hud-icon-btn desktop-only {'active' if sched.get('auto_unlike') else ''}" onclick="toggleAutoUnlike()" title="Toggle Auto-Unlike on X ({'ON' if sched.get('auto_unlike') else 'OFF'})">
        <svg viewBox="0 0 24 24"><path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/><path d="m12 5-1 4 2 3-2 4"/></svg>
      </button>

      <!-- Sync Now Trigger Button -->
      <button id="btn-sync-icon" class="hud-icon-btn accent" onclick="startSyncStream()" title="Trigger Immediate Sync [Stream]">
        <svg viewBox="0 0 24 24"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>
      </button>

      <!-- Logs & Notification Sidesheet Button -->
      <button class="hud-icon-btn" onclick="toggleSidesheet()" title="Telemetry Logs & Notifications">
        <svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
        {f'<span class="badge-corner" id="unread-badge">{unread}</span>' if unread > 0 else '<span id="unread-badge"></span>'}
      </button>

      <!-- Import Archive Icon Button -->
      <button class="hud-icon-btn desktop-only" onclick="document.getElementById('file-upload').click()" title="Import Twitter like.js Archive">
        <svg viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
      </button>
      <input type="file" id="file-upload" style="display:none" onchange="uploadArchive(this)">

      <!-- Export Markdown Icon Button -->
      <button class="hud-icon-btn desktop-only" onclick="exportMarkdown()" title="Export All Tweets to Markdown Files">
        <svg viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
      </button>

      <!-- Account & Maintenance Button -->
      <button class="hud-icon-btn" onclick="openAccountTab()" title="Twitter Account & Maintenance Tools">
        <svg viewBox="0 0 24 24"><path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
      </button>
    </div>
  </header>

  <!-- Main Viewport -->
  <div class="container">
    <!-- Blazingly Fast Filter Dock -->
    <div class="hud-filter-dock">
      <div class="search-row">
        <div class="search-capsule">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input id="query" type="text" class="hud-input" placeholder="Instant Search likes (FTS / Semantic)..." oninput="onSearchInput(this.value)">
          <button id="clear-search-btn" class="clear-search-btn" onclick="clearSearch()" title="Clear search (Esc)">✕</button>
        </div>

        <button id="btn-semantic-toggle" class="semantic-toggle-btn" onclick="toggleSemanticMode()" title="Toggle Deep Vector AI Semantic Search">
          <span>🧠 AI Semantic</span>
        </button>

        <span id="latency-indicator" class="latency-badge">⚡ &lt;2ms</span>
      </div>

      <div class="controls-row">
        <div class="hud-tag-cloud" id="tag-cloud-container">
          <span id="active-author-filter-slot"></span>
          {tags_html}
        </div>

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

  <!-- Fullscreen Glassmorphic Tweet Detail Lightbox Modal -->
  <div id="hud-tweet-modal" class="hud-modal-backdrop" onclick="if(event.target===this) closeTweetModal()">
    <div class="hud-modal-box">
      <div class="mobile-drag-handle"></div>
      <div class="hud-modal-header">
        <div style="display:flex; align-items:center; gap:8px;">
          <span style="font-size:1.1rem;">𝕏</span>
          <h3 id="modal-author-name" style="font-size:0.95rem; font-weight:700;">Tweet Detail</h3>
          <span id="modal-author-handle" style="color:var(--muted); font-family:'JetBrains Mono',monospace; font-size:0.85rem;"></span>
        </div>
        <button class="hud-icon-btn" onclick="closeTweetModal()" style="width:28px; height:28px;">✕</button>
      </div>
      <div class="hud-modal-body">
        <div id="modal-tweet-text" class="tweet-text" style="font-size:1rem; line-height:1.5;"></div>
        <div id="modal-media-container" class="hud-modal-media"></div>
        <div id="modal-tags-container" style="display:flex; flex-wrap:wrap; gap:6px;"></div>
        <div class="hud-modal-actions">
          <button class="hud-btn" onclick="copyTweetText()"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg> Copy Text</button>
          <button class="hud-btn" onclick="copyTweetLink()"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/></svg> Copy Link</button>
          <a id="modal-open-x-btn" href="#" target="_blank" class="hud-btn" style="text-decoration:none;"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg> Open on 𝕏</a>
          <button id="modal-filter-author-btn" class="hud-btn"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg> Filter by Author</button>
          <button id="modal-unlike-btn" class="hud-btn" style="background:rgba(239,68,68,0.15); border-color:#ef4444; color:#f87171;" onclick="unlikeActiveModalTweet()"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/><path d="m12 5-1 4 2 3-2 4"/></svg> Unlike on 𝕏</button>
        </div>
      </div>
    </div>
  </div>

  <!-- HUD RAG Chat Drawer -->
  <aside class="hud-chat-drawer" id="hud-chat-drawer">
    <div class="mobile-drag-handle"></div>
    <div class="hud-sidesheet-header">
      <div style="display:flex; align-items:center; gap:8px;">
        <span style="font-size:1.1rem;">💬</span>
        <h3 style="font-size:0.95rem; font-weight:700;">Chat with LanceDB</h3>
        <span class="hud-badge connected" style="font-size:0.65rem; padding:2px 6px;">RAG ACTIVE</span>
      </div>
      <button class="hud-icon-btn" onclick="toggleChatDrawer()" style="width:28px; height:28px;">✕</button>
    </div>

    <!-- Chat Messages Feed -->
    <div class="chat-messages" id="chat-messages-container">
      <div class="chat-msg assistant">
        <strong>👋 Assistant</strong><br>
        Ask anything across your 3,300+ likes! I search your local LanceDB vectors and synthesize answers with citations.
        <div style="display:flex; flex-wrap:wrap; gap:6px; margin-top:10px;">
          <span class="chat-chip" onclick="askPreset('What did Karpathy or DHH tweet about AI agents and code?')">🤖 Karpathy on AI Agents</span>
          <span class="chat-chip" onclick="askPreset('Summarize the top frontend and UI libraries I liked')">🎨 Top UI Libraries</span>
          <span class="chat-chip" onclick="askPreset('Find interesting Python developer tools and tips')">🐍 Python Tools</span>
        </div>
      </div>
    </div>

    <!-- Chat Input Form -->
    <div class="chat-input-row">
      <input type="text" id="chat-input" class="hud-input" placeholder="Ask a question about your likes..." onkeyup="if(event.key==='Enter') sendChatMessage()" style="background:#080b12;">
      <button class="hud-btn accent" onclick="sendChatMessage()" id="btn-chat-send" style="padding:0.5rem 1rem;">Send</button>
    </div>
  </aside>

  <!-- HUD Right Sidesheet -->
  <aside class="hud-sidesheet" id="hud-sidesheet">
    <div class="mobile-drag-handle"></div>
    <div class="hud-sidesheet-header">
      <h3 style="font-size:0.95rem; font-weight:700;">HUD Telemetry & Controls</h3>
      <button class="hud-icon-btn" onclick="toggleSidesheet()" style="width:28px; height:28px;">✕</button>
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
        <p style="color:var(--muted); font-size:0.8rem; margin-bottom:0.75rem;">Session authentication & bulk maintenance.</p>
        <input type="text" id="auth-username" class="hud-input" placeholder="@handle" style="width:100%; margin-bottom:0.5rem; background:#080b12;">
        <input type="text" id="auth-token" class="hud-input" placeholder="auth_token (required)" style="width:100%; margin-bottom:0.5rem; background:#080b12;">
        <input type="text" id="auth-ct0" class="hud-input" placeholder="ct0 (optional)" style="width:100%; margin-bottom:0.75rem; background:#080b12;">
        <button class="hud-btn" onclick="saveCookiesAuth()" style="width:100%; margin-bottom:0.75rem; background:rgba(29,155,240,0.2); border-color:#1d9bf0; color:#38bdf8;">Save Session</button>
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
      <button class="hud-icon-btn" onclick="document.getElementById('floating-sync-toast').style.display='none'" style="width:24px; height:24px; font-size:0.7rem;">✕</button>
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
    let currentAuthor = null;
    let currentSort = 'newest';
    let isSemantic = false;
    let currentOffset = 0;
    let isLoading = false;
    let hasMore = true;
    let colCount = localStorage.getItem('likes_cols') || '2';
    let displayMode = localStorage.getItem('likes_mode') || 'card';
    let nextSyncSeconds = {sched.get('next_sync_in_sec', 0)};
    let isSyncEnabled = {str(sched.get('enabled', True)).lower()};
    let syncInterval = {sched.get('interval_sec', 600)};
    let searchDebounceTimer = null;
    const searchCache = new Map();
    const loadedTweetsMap = new Map();
    let activeModalTweet = null;
    const PAGE_LIMIT = 24;

    function formatTweetText(text) {{
      if (!text) return '';
      let escaped = text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      
      // Auto-hyperlink URLs
      escaped = escaped.replace(/(https?:\\/\\/[^\\s]+)/g, '<a href="$1" target="_blank" onclick="event.stopPropagation()">$1</a>');
      
      // Auto-hyperlink @mentions
      escaped = escaped.replace(/@([a-zA-Z0-9_]+)/g, '<span class="tweet-mention" data-author="$1" onclick="event.stopPropagation(); filterAuthor(this.dataset.author)">@$1</span>');
      
      // Auto-hyperlink #hashtags
      escaped = escaped.replace(/#([a-zA-Z0-9_]+)/g, '<span class="tweet-hashtag" data-tag="$1" onclick="event.stopPropagation(); filterTag(this.dataset.tag)">#$1</span>');
      
      return escaped;
    }}

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

    function toggleSemanticMode() {{
      isSemantic = !isSemantic;
      const btn = document.getElementById('btn-semantic-toggle');
      if (isSemantic) btn.classList.add('active');
      else btn.classList.remove('active');
      loadLikes(false);
    }}

    function onSearchInput(val) {{
      const clearBtn = document.getElementById('clear-search-btn');
      clearBtn.style.display = val.length ? 'block' : 'none';
      clearTimeout(searchDebounceTimer);
      searchDebounceTimer = setTimeout(() => {{
        currentQuery = val.trim();
        loadLikes(false);
      }}, 180);
    }}

    function clearSearch() {{
      const input = document.getElementById('query');
      input.value = '';
      document.getElementById('clear-search-btn').style.display = 'none';
      currentQuery = '';
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
      const cacheKey = `${{currentQuery}}_${{currentTag}}_${{currentAuthor}}_${{currentSort}}_${{isSemantic}}_${{currentOffset}}`;
      if (searchCache.has(cacheKey)) {{
        const cachedData = searchCache.get(cacheKey);
        renderResults(cachedData, append);
        return;
      }}

      isLoading = true;
      const container = document.getElementById('results');
      if (!append) {{
        renderSkeleton();
        currentOffset = 0;
        hasMore = true;
      }}

      let url = `/api/search?q=${{encodeURIComponent(currentQuery)}}&sort_by=${{currentSort}}&semantic=${{isSemantic}}&offset=${{currentOffset}}&limit=${{PAGE_LIMIT}}`;
      if (currentTag) url += `&tag=${{encodeURIComponent(currentTag)}}`;
      if (currentAuthor) url += `&author=${{encodeURIComponent(currentAuthor)}}`;

      try {{
        const res = await fetch(url);
        const data = await res.json();
        searchCache.set(cacheKey, data);
        renderResults(data, append);
      }} finally {{
        isLoading = false;
      }}
    }}

    function resolveMediaUrl(p) {{
      if (!p) return '';
      if (p.startsWith('http://') || p.startsWith('https://') || p.startsWith('data:')) return p;
      let clean = p.replace(/^data\//, '').replace(/^\/+/, '');
      if (!clean.startsWith('media/')) clean = 'media/' + clean;
      return '/' + clean;
    }}

    function isVideoMedia(url) {{
      if (!url) return false;
      const lower = url.toLowerCase();
      return lower.endsWith('.mp4') || lower.endsWith('.webm') || lower.endsWith('.mov') || lower.includes('/video/');
    }}

    function renderMediaElement(src, fallback, tweetId, isModal = false) {{
      if (isVideoMedia(src)) {{
        return isModal
          ? `<video class="hud-modal-img" src="${{src}}" controls preload="metadata"></video>`
          : `<video class="media-thumb" src="${{src}}" preload="metadata" muted playsinline onclick="event.stopPropagation(); openTweetModal('${{tweetId}}')"></video>`;
      }}
      return isModal
        ? `<a href="${{src}}" target="_blank" title="View high-res in new tab"><img class="hud-modal-img" src="${{src}}" onerror="this.src='${{fallback}}'"></a>`
        : `<img class="media-thumb" src="${{src}}" onerror="this.src='${{fallback}}'" onclick="event.stopPropagation(); openTweetModal('${{tweetId}}')" loading="lazy">`;
    }}

    function renderResults(data, append) {{
      const container = document.getElementById('results');
      const results = data.results || [];
      const latencyIndicator = document.getElementById('latency-indicator');
      if (data.latency_ms !== undefined) {{
        latencyIndicator.innerText = data.semantic ? `🧠 ${{data.latency_ms}}ms (AI)` : `⚡ ${{data.latency_ms}}ms (FTS)`;
      }}

      if (!append) container.innerHTML = '';
      if (results.length < PAGE_LIMIT) hasMore = false;

      if (results.length === 0 && !append) {{
        container.innerHTML = '<div class="hud-tweet-card" style="text-align:center; color: var(--muted); grid-column: 1 / -1; padding:2rem;">No matching likes found.</div>';
        return;
      }}

      results.forEach(r => loadedTweetsMap.set(r.tweet_id, r));

      const html = results.map(r => {{
        const mediaList = (r.local_media_paths && r.local_media_paths.length)
          ? r.local_media_paths.map(resolveMediaUrl)
          : (r.media_urls || []).map(resolveMediaUrl);
        const fallbackSrc = (r.media_urls && r.media_urls.length) ? r.media_urls[0] : '';
        const cleanHandle = (r.author_handle || '').replace(/^@+/, '');
        const authorDisplay = cleanHandle ? '@' + cleanHandle : 'Post #' + r.tweet_id;
        const formattedText = formatTweetText(r.text);

        if (displayMode === 'list') {{
          return `
            <div class="compact-row" onclick="openTweetModal('${{r.tweet_id}}')">
              <span class="author-interactive" onclick="event.stopPropagation(); filterAuthor('${{cleanHandle}}')">${{authorDisplay}}</span>
              <span class="compact-text">${{r.text}}</span>
              <div style="display:flex; gap:0.25rem;">${{(r.tags || []).slice(0, 2).map(t => `<span class="hud-tag" style="font-size:0.7rem; padding:0.1rem 0.4rem;" onclick="event.stopPropagation(); filterTag('${{t}}')">${{t}}</span>`).join('')}}</div>
              <a href="${{r.url}}" target="_blank" class="ext-link-icon" onclick="event.stopPropagation()" title="Open on X">↗</a>
            </div>
          `;
        }}
        if (displayMode === 'gallery') {{
          const mediaSrc = mediaList.length ? mediaList[0] : '';
          return `
            <div class="gallery-card" onclick="openTweetModal('${{r.tweet_id}}')">
              ${{mediaSrc ? (isVideoMedia(mediaSrc) ? `<video class="gallery-img" src="${{mediaSrc}}" preload="metadata" muted playsinline></video>` : `<img class="gallery-img" src="${{mediaSrc}}" onerror="this.src='${{fallbackSrc}}'" loading="lazy">`) : '<div style=\"height:120px; background:#131926; display:flex; align-items:center; justify-content:center; color:var(--muted); font-size:0.8rem;\">Text Post</div>'}}
              <div class="gallery-body">
                <div class="tweet-header">
                  <span class="author-interactive" onclick="event.stopPropagation(); filterAuthor('${{cleanHandle}}')"><strong>${{r.author_name || authorDisplay}}</strong></span>
                  <a href="${{r.url}}" target="_blank" class="ext-link-icon" onclick="event.stopPropagation()" title="Open on X">↗</a>
                </div>
                <p style="font-size:0.82rem; line-height:1.3; margin-bottom:0.5rem; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden;">${{r.text}}</p>
                <div>${{(r.tags || []).slice(0, 3).map(t => `<span class="hud-tag" style="font-size:0.7rem; padding:0.1rem 0.4rem;" onclick="event.stopPropagation(); filterTag('${{t}}')">${{t}}</span>`).join(' ')}}</div>
              </div>
            </div>
          `;
        }}
        return `
          <div class="hud-tweet-card" onclick="openTweetModal('${{r.tweet_id}}')">
            <div>
              <div class="tweet-header">
                <div style="display:flex; align-items:center; gap:4px;">
                  <span class="author-interactive" onclick="event.stopPropagation(); filterAuthor('${{cleanHandle}}')"><strong>${{r.author_name || cleanHandle}}</strong></span>
                  <span class="handle-badge" onclick="event.stopPropagation(); filterAuthor('${{cleanHandle}}')">${{authorDisplay}}</span>
                </div>
                <a href="${{r.url}}" target="_blank" class="ext-link-icon" onclick="event.stopPropagation()" title="Open on X">
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
                </a>
              </div>
              <div class="tweet-text">${{formattedText}}</div>
              ${{mediaList.length ? `<div class="media-grid">${{mediaList.map(m => renderMediaElement(m, fallbackSrc, r.tweet_id, false)).join('')}}</div>` : ''}}
            </div>
            <div style="margin-top:0.75rem">${{(r.tags || []).map(t => `<span class="hud-tag" onclick="event.stopPropagation(); filterTag('${{t}}')">${{t}}</span>`).join(' ')}}</div>
          </div>
        `;
      }}).join('');

      container.insertAdjacentHTML('beforeend', html);
      currentOffset += results.length;
    }}

    function filterAuthor(handle) {{
      if (!handle) return;
      currentAuthor = handle.replace(/^@+/, '');
      document.getElementById('active-author-filter-slot').innerHTML = `
        <span class="active-author-pill" onclick="clearAuthorFilter()">
          @${{currentAuthor}} <strong style="margin-left:4px;">✕</strong>
        </span>
      `;
      closeTweetModal();
      loadLikes(false);
    }}

    function clearAuthorFilter() {{
      currentAuthor = null;
      document.getElementById('active-author-filter-slot').innerHTML = '';
      loadLikes(false);
    }}

    function filterTag(tag) {{
      document.querySelectorAll('.hud-tag').forEach(el => el.classList.remove('active'));
      const activeEl = document.getElementById('tag-' + tag);
      if (activeEl) activeEl.classList.add('active');
      currentTag = tag;
      document.getElementById('query').value = '';
      currentQuery = '';
      closeTweetModal();
      loadLikes(false);
    }}

    function clearFilters() {{
      document.querySelectorAll('.hud-tag').forEach(el => el.classList.remove('active'));
      currentTag = null;
      clearAuthorFilter();
      document.getElementById('query').value = '';
      currentQuery = '';
      loadLikes(false);
    }}

    /* Lightbox Modal Logic */
    function openTweetModal(tweetId) {{
      const tweet = loadedTweetsMap.get(tweetId);
      if (!tweet) return;
      activeModalTweet = tweet;

      const cleanHandle = (tweet.author_handle || '').replace(/^@+/, '');
      document.getElementById('modal-author-name').innerText = tweet.author_name || cleanHandle || 'Tweet';
      document.getElementById('modal-author-handle').innerText = cleanHandle ? '@' + cleanHandle : '';
      document.getElementById('modal-tweet-text').innerHTML = formatTweetText(tweet.text);
      document.getElementById('modal-open-x-btn').href = tweet.url || `https://x.com/${{cleanHandle}}/status/${{tweet.tweet_id}}`;

      const filterAuthorBtn = document.getElementById('modal-filter-author-btn');
      if (cleanHandle) {{
        filterAuthorBtn.style.display = 'inline-flex';
        filterAuthorBtn.onclick = () => filterAuthor(cleanHandle);
      }} else {{
        filterAuthorBtn.style.display = 'none';
      }}

      const mediaContainer = document.getElementById('modal-media-container');
      const mediaList = (tweet.local_media_paths && tweet.local_media_paths.length)
        ? tweet.local_media_paths.map(resolveMediaUrl)
        : (tweet.media_urls || []).map(resolveMediaUrl);
      const fallbackSrc = (tweet.media_urls && tweet.media_urls.length) ? tweet.media_urls[0] : '';
      
      if (mediaList.length > 0) {{
        mediaContainer.innerHTML = mediaList.map(m => renderMediaElement(m, fallbackSrc, tweet.tweet_id, true)).join('');
      }} else {{
        mediaContainer.innerHTML = '';
      }}

      const tagsContainer = document.getElementById('modal-tags-container');
      tagsContainer.innerHTML = (tweet.tags || []).map(t => `<span class="hud-tag" onclick="filterTag('${{t}}')">${{t}}</span>`).join(' ');

      document.getElementById('hud-tweet-modal').classList.add('open');
    }}

    function closeTweetModal() {{
      document.getElementById('hud-tweet-modal').classList.remove('open');
      activeModalTweet = null;
    }}

    function copyTweetText() {{
      if (!activeModalTweet) return;
      navigator.clipboard.writeText(activeModalTweet.text);
      alert('Tweet text copied to clipboard!');
    }}

    function copyTweetLink() {{
      if (!activeModalTweet) return;
      const url = activeModalTweet.url || `https://x.com/i/web/status/${{activeModalTweet.tweet_id}}`;
      navigator.clipboard.writeText(url);
      alert('Tweet URL copied to clipboard!');
    }}

    async function unlikeActiveModalTweet() {{
      if (!activeModalTweet) return;
      if (!confirm('Unlike this tweet on X?')) return;
      const res = await fetch('/api/maintenance/unlike-single', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ tweet_id: activeModalTweet.tweet_id }})
      }});
      const data = await res.json();
      if (data.unliked) {{
        alert('Tweet unliked successfully on X!');
        closeTweetModal();
      }} else {{
        alert('Failed to unlike on X.');
      }}
    }}

    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape') {{
        closeTweetModal();
        const drawer = document.getElementById('hud-chat-drawer');
        if (drawer.classList.contains('open')) drawer.classList.remove('open');
        const sheet = document.getElementById('hud-sidesheet');
        if (sheet.classList.contains('open')) sheet.classList.remove('open');
      }}
    }});

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
      const btn = document.getElementById('btn-auto-sync-icon');
      if (isSyncEnabled) btn.classList.add('active');
      else btn.classList.remove('active');
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

    /* RAG Chat Drawer Logic */
    function toggleChatDrawer() {{
      const drawer = document.getElementById('hud-chat-drawer');
      drawer.classList.toggle('open');
      if (drawer.classList.contains('open')) {{
        document.getElementById('chat-input').focus();
      }}
    }}

    function askPreset(promptText) {{
      document.getElementById('chat-input').value = promptText;
      sendChatMessage();
    }}

    function sendChatMessage() {{
      const input = document.getElementById('chat-input');
      const q = input.value.trim();
      if (!q) return;
      input.value = '';

      const container = document.getElementById('chat-messages-container');
      container.innerHTML += `<div class="chat-msg user">${{q}}</div>`;
      
      const responseId = 'ai-resp-' + Date.now();
      container.innerHTML += `
        <div class="chat-msg assistant" id="${{responseId}}">
          <div style="display:flex; align-items:center; gap:6px; color:var(--primary); font-weight:600; margin-bottom:4px;">
            <span>🧠 Searching LanceDB...</span>
          </div>
          <div class="ai-text" style="white-space:pre-wrap;"></div>
          <div class="ai-sources" style="margin-top:8px;"></div>
        </div>
      `;
      container.scrollTop = container.scrollHeight;

      const aiMsgEl = document.getElementById(responseId);
      const textEl = aiMsgEl.querySelector('.ai-text');
      const sourcesEl = aiMsgEl.querySelector('.ai-sources');

      const es = new EventSource(`/api/chat/stream?q=${{encodeURIComponent(q)}}`);
      es.onmessage = function(e) {{
        const data = JSON.parse(e.data);
        if (data.type === 'sources') {{
          const sources = data.sources || [];
          if (sources.length > 0) {{
            sourcesEl.innerHTML = '<strong style="color:var(--muted); font-size:0.75rem;">Cited Likes:</strong>' + sources.map(s => `
              <div class="chat-source-card" onclick="openTweetModal('${{s.tweet_id}}')">
                <strong>[${{s.index}}] @${{(s.author_handle || '').replace(/^@+/, '')}}</strong>: ${{s.text.slice(0, 70)}}...
                <span style="color:var(--primary); margin-left:4px;">[View Detail]</span>
              </div>
            `).join('');
          }}
        }} else if (data.type === 'token') {{
          textEl.textContent += data.token;
          container.scrollTop = container.scrollHeight;
        }} else if (data.type === 'done') {{
          es.close();
        }}
      }};
      es.onerror = function() {{
        es.close();
      }};
    }}

    function startSyncStream() {{
      const toast = document.getElementById('floating-sync-toast');
      const toastTitle = document.getElementById('toast-title');
      const toastDetail = document.getElementById('toast-status-detail');
      const toastFill = document.getElementById('toast-progress-fill');
      const toastPercent = document.getElementById('toast-percent');
      const toastLog = document.getElementById('toast-log');
      
      toast.style.display = 'block';
      toastLog.innerHTML = '<div>[Connected] Initializing sync...</div>';
      toastFill.style.width = '10%';
      toastPercent.innerText = '10%';

      const es = new EventSource('/api/sync/stream?max_tweets=0');
      es.onmessage = function(e) {{
        const data = JSON.parse(e.data);
        if (data.error) {{
          toastTitle.innerText = 'Sync Error';
          toastDetail.innerText = data.error;
          toastLog.innerHTML += `<div style="color:#ef4444;">[ERROR] ${{data.error}}</div>`;
          es.close();
          return;
        }}
        if (data.stage === 'scrolling') {{
          toastTitle.innerText = `Found ${{data.tweets_found}} likes...`;
          toastDetail.innerText = `Scroll attempt #${{data.scroll_attempt}}`;
          toastLog.innerHTML += `<div>Scraped ${{data.tweets_found}} likes...</div>`;
          toastLog.scrollTop = toastLog.scrollHeight;
        }} else if (data.stage === 'item_done') {{
          const cleanHandle = (data.author_handle || 'user').replace(/^@+/, '');
          toastTitle.innerText = `Ingesting (#${{data.current}})...`;
          toastDetail.innerText = `@${{cleanHandle}}: "${{data.text.slice(0, 30)}}..."`;
          toastFill.style.width = `${{Math.min(90, 20 + data.current * 3)}}%`;
          toastPercent.innerText = `${{Math.min(90, 20 + data.current * 3)}}%`;
          toastLog.innerHTML += `<div>[Saved] @${{cleanHandle}} ${{data.unliked ? '(Unliked on X)' : ''}}</div>`;
          toastLog.scrollTop = toastLog.scrollHeight;
        }} else if (data.stage === 'complete') {{
          toastFill.style.width = '100%';
          toastPercent.innerText = '100%';
          toastTitle.innerText = 'Sync Complete!';
          toastDetail.innerText = data.message;
          toastLog.innerHTML += `<div style="color:#10b981; font-weight:bold;">[DONE] ${{data.message}}</div>`;
          es.close();
          searchCache.clear();
          refreshStats();
          if (!currentQuery && !currentTag) loadLikes(false);
          setTimeout(() => {{ toast.style.display = 'none'; }}, 4000);
        }}
      }};
      es.onerror = function() {{
        toastTitle.innerText = 'Sync Ended';
        es.close();
      }};
    }}

    let cachedLogs = null;
    let cachedNotifs = null;

    function renderSidesheetSkeleton() {{
      return `
        <div style="display:flex; flex-direction:column; gap:8px;">
          <div class="skeleton" style="height:50px; width:100%;"></div>
          <div class="skeleton" style="height:50px; width:100%;"></div>
          <div class="skeleton" style="height:50px; width:100%;"></div>
        </div>
      `;
    }}

    function toggleSidesheet() {{
      const sheet = document.getElementById('hud-sidesheet');
      sheet.classList.toggle('open');
      if (sheet.classList.contains('open')) {{
        const activeTab = document.querySelector('.hud-tab.active')?.id.replace('tab-btn-', '') || 'logs';
        switchHistTab(activeTab);
      }}
    }}

    function openAccountTab() {{
      const sheet = document.getElementById('hud-sidesheet');
      if (!sheet.classList.contains('open')) sheet.classList.add('open');
      switchHistTab('auth');
    }}

    function switchHistTab(t) {{
      ['logs', 'notifs', 'auth'].forEach(tab => {{
        document.getElementById('tab-' + tab).style.display = tab === t ? 'block' : 'none';
        document.getElementById('tab-btn-' + tab).className = 'hud-tab ' + (tab === t ? 'active' : '');
      }});
      if (t === 'logs' && !cachedLogs) loadHistoryLogs();
      if (t === 'notifs' && !cachedNotifs) loadNotifications();
    }}

    async function loadHistoryLogs() {{
      const container = document.getElementById('logs-container');
      if (!cachedLogs) container.innerHTML = renderSidesheetSkeleton();
      try {{
        const res = await fetch('/api/history/logs?limit=30');
        const data = await res.json();
        cachedLogs = data.logs || [];
        if (cachedLogs.length === 0) {{
          container.innerHTML = '<p style="color:var(--muted); text-align:center; padding:1rem;">No sync logs recorded yet.</p>';
          return;
        }}
        container.innerHTML = cachedLogs.map(l => `
          <div style="background:rgba(0,0,0,0.3); border:1px solid var(--card-border); padding:0.65rem; border-radius:6px; margin-bottom:0.5rem;">
            <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:var(--muted); margin-bottom:4px;">
              <span><strong>${{l.trigger}}</strong> (${{l.engine}})</span>
              <span style="font-family:'JetBrains Mono',monospace;">${{(l.timestamp||'').split(' ')[1] || l.timestamp}}</span>
            </div>
            <p style="font-size:0.8rem; line-height:1.3;">${{l.message}}</p>
            <div style="display:flex; justify-content:space-between; font-size:0.7rem; color:var(--muted); margin-top:4px;">
              <span>+${{l.new_likes}} likes (Total: ${{l.total_db_likes}})</span>
              <span>${{l.duration_sec}}s</span>
            </div>
          </div>
        `).join('');
      }} catch (e) {{
        container.innerHTML = '<p style="color:#ef4444; font-size:0.75rem;">Failed to load logs.</p>';
      }}
    }}

    async function loadNotifications() {{
      const container = document.getElementById('notifs-container');
      if (!cachedNotifs) container.innerHTML = renderSidesheetSkeleton();
      try {{
        const res = await fetch('/api/history/notifications?limit=30');
        const data = await res.json();
        cachedNotifs = data.notifications || [];
        if (cachedNotifs.length === 0) {{
          container.innerHTML = '<p style="color:var(--muted); text-align:center; padding:1rem;">No notifications.</p>';
          return;
        }}
        container.innerHTML = cachedNotifs.map(n => `
          <div style="background:rgba(0,0,0,0.3); border-left:3px solid ${{n.type === 'error' ? '#ef4444' : (n.type === 'success' ? '#10b981' : '#1d9bf0')}}; border:1px solid var(--card-border); padding:0.65rem; border-radius:6px; margin-bottom:0.5rem;">
            <div style="display:flex; justify-content:space-between; font-size:0.75rem; color:var(--muted); margin-bottom:4px;">
              <strong>${{n.title}}</strong><span style="font-family:'JetBrains Mono',monospace;">${{n.timestamp}}</span>
            </div>
            <p style="font-size:0.8rem;">${{n.message}}</p>
          </div>
        `).join('');
      }} catch (e) {{
        container.innerHTML = '<p style="color:#ef4444; font-size:0.75rem;">Failed to load notifications.</p>';
      }}
    }}

    async function markAllNotifsRead() {{
      await fetch('/api/history/notifications/read-all', {{ method: 'POST' }});
      document.getElementById('unread-badge').innerText = '';
      cachedNotifs = null;
      loadNotifications();
    }}

    async function toggleAutoUnlike() {{
      const res = await fetch('/api/settings/auto-unlike/toggle', {{ method: 'POST' }});
      const data = await res.json();
      const btn = document.getElementById('btn-auto-unlike-icon');
      if (data.auto_unlike) btn.classList.add('active');
      else btn.classList.remove('active');
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
      if (res.ok) {{ alert(`Imported ${{data.parsed}} likes from archive!`); searchCache.clear(); refreshStats(); loadLikes(false); }}
      else {{ alert('Import failed.'); }}
    }}

    async function exportMarkdown() {{
      const res = await fetch('/api/export/markdown', {{ method: 'POST' }});
      const data = await res.json();
      alert(`Exported ${{data.exported_count}} tweets to ${{data.export_dir}}!`);
    }}

    function prefetchTopTags() {{
      const topTags = Array.from(document.querySelectorAll('.hud-tag')).slice(0, 6).map(el => el.id.replace('tag-', ''));
      topTags.forEach(tag => {{
        const url = `/api/search?q=&sort_by=newest&semantic=false&offset=0&limit=${{PAGE_LIMIT}}&tag=${{encodeURIComponent(tag)}}`;
        fetch(url).then(res => res.json()).then(data => {{
          searchCache.set(`_${{tag}}__newest_false_0`, data);
        }}).catch(() => {{}});
      }});
    }}

    window.addEventListener('DOMContentLoaded', () => {{
      applyLayout();
      loadLikes(false);
      setTimeout(prefetchTopTags, 500);
    }});
  </script>
</body>
</html>"""
