import os
import json
import logging
import urllib.request
from pathlib import Path
from typing import Any, AsyncGenerator
import httpx
from src.ingestion.youtube_parser import parse_youtube_api_item, get_youtube_thumbnails

logger = logging.getLogger(__name__)

YOUTUBE_MEDIA_DIR = Path("data/media/youtube")


def download_youtube_thumbnail(video_id: str, thumbnail_url: str | None = None) -> str | None:
    """Download thumbnail image only (zero video downloads)."""
    if not video_id:
        return None

    YOUTUBE_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    local_path = YOUTUBE_MEDIA_DIR / f"{video_id}.jpg"
    if local_path.exists() and local_path.stat().st_size > 0:
        return str(local_path)

    candidate_urls = [thumbnail_url] if thumbnail_url else []
    candidate_urls.extend(get_youtube_thumbnails(video_id))

    headers = {"User-Agent": "Mozilla/5.0"}
    for u in candidate_urls:
        if not u:
            continue
        try:
            req = urllib.request.Request(u, headers=headers)
            with urllib.request.urlopen(req, timeout=8) as resp:
                if resp.status == 200:
                    data = resp.read()
                    if len(data) > 1000:  # Valid image data
                        with open(local_path, "wb") as f:
                            f.write(data)
                        return str(local_path)
        except Exception:
            continue

    return None


class YouTubeSyncClient:
    def __init__(self, api_key: str | None = None, oauth_token: str | None = None):
        self.api_key = api_key or os.getenv("YOUTUBE_API_KEY")
        self.oauth_token = oauth_token or os.getenv("YOUTUBE_OAUTH_TOKEN")
        self.base_url = "https://www.googleapis.com/youtube/v3"

    def has_credentials(self) -> bool:
        return bool(self.api_key or self.oauth_token)

    async def fetch_liked_videos(self, playlist_id: str = "LL", max_pages: int = 5) -> list[dict[str, Any]]:
        """Fetch liked videos from YouTube Data API v3."""
        if not self.has_credentials():
            logger.warning("[YouTube] No API Key or OAuth token configured.")
            return []

        headers = {}
        if self.oauth_token:
            headers["Authorization"] = f"Bearer {self.oauth_token}"

        params: dict[str, Any] = {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": 50,
        }
        if self.api_key:
            params["key"] = self.api_key

        videos = []
        page_token = None
        pages_fetched = 0

        async with httpx.AsyncClient(timeout=15.0) as client:
            while pages_fetched < max_pages:
                if page_token:
                    params["pageToken"] = page_token

                try:
                    res = await client.get(f"{self.base_url}/playlistItems", params=params, headers=headers)
                    if res.status_code != 200:
                        logger.error(f"[YouTube API Error] {res.status_code}: {res.text}")
                        break

                    data = res.json()
                    items = data.get("items", [])
                    for it in items:
                        parsed = parse_youtube_api_item(it)
                        if parsed:
                            videos.append(parsed)

                    page_token = data.get("nextPageToken")
                    pages_fetched += 1
                    if not page_token:
                        break
                except Exception as e:
                    logger.error(f"[YouTube Sync Error] {e}")
                    break

        return videos


async def stream_youtube_sync(api_key: str | None = None, oauth_token: str | None = None) -> AsyncGenerator[str, None]:
    """Stream live status for syncing YouTube likes."""
    client = YouTubeSyncClient(api_key=api_key, oauth_token=oauth_token)
    yield json.dumps({"stage": "init", "message": "Connecting to YouTube API..."}) + "\n"

    if not client.has_credentials():
        yield json.dumps({
            "stage": "error",
            "error": "No YouTube API Key or OAuth token found. Provide credentials or import via Google Takeout."
        }) + "\n"
        return

    yield json.dumps({"stage": "fetching", "message": "Querying Liked Videos (LL)..."}) + "\n"
    videos = await client.fetch_liked_videos(playlist_id="LL")
    
    if not videos:
        yield json.dumps({"stage": "complete", "count": 0, "message": "No liked videos found or access denied."}) + "\n"
        return

    from src.storage.lancedb_client import LanceDBStore
    store = LanceDBStore()

    count = store.upsert_tweets(videos, default_source="youtube")
    yield json.dumps({
        "stage": "complete",
        "count": count,
        "message": f"Successfully synced {count} YouTube liked videos!"
    }) + "\n"
