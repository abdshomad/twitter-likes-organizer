import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any
from src.media.downloader import MediaDownloader
from src.storage.lancedb_client import LanceDBStore

DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
QUEUE_PATH = DEFAULT_DATA_DIR / "media_queue.json"


class MediaQueue:
    def __init__(self, queue_path: Path | str | None = None, store: LanceDBStore | None = None):
        self.queue_path = Path(queue_path or QUEUE_PATH)
        self.store = store or LanceDBStore()
        self.downloader = MediaDownloader()
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = asyncio.Lock()
        self._load_queue()

    def _load_queue(self) -> dict[str, Any]:
        if not self.queue_path.exists():
            data = {"pending": [], "completed_count": 0, "failed": []}
            self._save_queue(data)
            return data
        try:
            return json.loads(self.queue_path.read_text())
        except Exception:
            return {"pending": [], "completed_count": 0, "failed": []}

    def _save_queue(self, data: dict[str, Any]):
        try:
            self.queue_path.write_text(json.dumps(data, indent=2))
        except Exception:
            pass

    async def enqueue(self, tweet_id: str, media_urls: list[str]) -> int:
        if not media_urls:
            return 0
        async with self.lock:
            data = self._load_queue()
            existing_urls = {item["url"] for item in data["pending"]}
            added = 0
            for url in media_urls:
                if url not in existing_urls:
                    data["pending"].append({
                        "tweet_id": tweet_id,
                        "url": url,
                        "attempts": 0,
                        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    added += 1
            if added > 0:
                self._save_queue(data)
            return added

    def get_status(self) -> dict[str, Any]:
        data = self._load_queue()
        return {
            "pending_count": len(data.get("pending", [])),
            "completed_count": data.get("completed_count", 0),
            "failed_count": len(data.get("failed", [])),
        }

    async def process_next_batch(self, batch_size: int = 3) -> int:
        async with self.lock:
            data = self._load_queue()
            if not data.get("pending"):
                return 0
            batch = data["pending"][:batch_size]
            data["pending"] = data["pending"][batch_size:]
            self._save_queue(data)

        processed = 0
        for item in batch:
            tweet_id = item["tweet_id"]
            url = item["url"]
            try:
                tweet_mock = {"id": tweet_id, "media_urls": [url]}
                saved_paths = self.downloader.download_tweet_media(tweet_mock)
                if saved_paths:
                    async with self.lock:
                        data = self._load_queue()
                        data["completed_count"] = data.get("completed_count", 0) + 1
                        self._save_queue(data)
                    processed += 1
                else:
                    self._record_failure(item, "Empty downloaded paths")
            except Exception as e:
                self._record_failure(item, str(e))
        return processed

    def _record_failure(self, item: dict[str, Any], err: str):
        item["attempts"] = item.get("attempts", 0) + 1
        item["last_error"] = err
        data = self._load_queue()
        if item["attempts"] >= 3:
            data.setdefault("failed", []).append(item)
        else:
            data.setdefault("pending", []).append(item)
        self._save_queue(data)

    async def worker_loop(self):
        while True:
            try:
                status = self.get_status()
                if status["pending_count"] > 0:
                    await self.process_next_batch(batch_size=3)
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)
