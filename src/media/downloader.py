import hashlib
import os
from pathlib import Path
from typing import Any
import httpx
import yt_dlp

DEFAULT_MEDIA_DIR = Path(os.getenv("DATA_DIR", "data")) / "media"
DEFAULT_COBALT_URL = os.getenv("COBALT_URL", "http://127.0.0.1:9000")


def compute_file_sha256(file_path: Path) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


class MediaDownloader:
    def __init__(
        self,
        media_dir: Path | str | None = None,
        cobalt_url: str | None = None,
    ):
        self.media_dir = Path(media_dir or DEFAULT_MEDIA_DIR)
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self.cobalt_url = cobalt_url or DEFAULT_COBALT_URL

    def download_tweet_media(self, tweet: dict[str, Any]) -> list[str]:
        tweet_id = str(tweet.get("tweet_id") or tweet.get("id"))
        tweet_url = str(tweet.get("url") or f"https://x.com/i/web/status/{tweet_id}")
        target_dir = self.media_dir / tweet_id
        target_dir.mkdir(parents=True, exist_ok=True)

        downloaded_paths: list[str] = []

        # 1. Direct image downloads if direct media_urls are available
        media_urls = tweet.get("media_urls") or []
        for idx, url in enumerate(media_urls):
            try:
                ext = url.split("?")[0].split(".")[-1] or "jpg"
                filename = f"media_{idx}.{ext}"
                out_path = target_dir / filename
                if not out_path.exists():
                    with httpx.Client(timeout=30.0) as client:
                        resp = client.get(url)
                        if resp.status_code == 200:
                            out_path.write_bytes(resp.content)
                if out_path.exists():
                    downloaded_paths.append(str(out_path.relative_to(self.media_dir.parent)))
            except Exception:
                continue

        # 2. Try yt-dlp native extraction (especially for video/multi-media)
        if not downloaded_paths or any("video" in u for u in media_urls):
            try:
                ydl_opts = {
                    "outtmpl": str(target_dir / "%(id)s_%(title).50s.%(ext)s"),
                    "quiet": True,
                    "no_warnings": True,
                    "noplaylist": True,
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([tweet_url])
                
                # Collect newly downloaded files
                for f in target_dir.iterdir():
                    if f.is_file():
                        rel = str(f.relative_to(self.media_dir.parent))
                        if rel not in downloaded_paths:
                            downloaded_paths.append(rel)
            except Exception:
                # 3. Fallback to local Cobalt instance if yt-dlp fails
                cobalt_paths = self._download_via_cobalt(tweet_url, target_dir)
                downloaded_paths.extend(cobalt_paths)

        return downloaded_paths

    def _download_via_cobalt(self, tweet_url: str, target_dir: Path) -> list[str]:
        saved_paths: list[str] = []
        try:
            with httpx.Client(timeout=30.0) as client:
                res = client.post(
                    self.cobalt_url,
                    json={"url": tweet_url},
                    headers={"Accept": "application/json"},
                )
                if res.status_code == 200:
                    data = res.json()
                    stream_url = data.get("url")
                    if stream_url:
                        media_resp = client.get(stream_url)
                        if media_resp.status_code == 200:
                            out_file = target_dir / "cobalt_download.mp4"
                            out_file.write_bytes(media_resp.content)
                            saved_paths.append(str(out_file.relative_to(self.media_dir.parent)))
        except Exception:
            pass
        return saved_paths
