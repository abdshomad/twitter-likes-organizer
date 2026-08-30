import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any
from src.storage.lancedb_client import LanceDBStore
from src.ai.tagger import AITagger
from src.ai.embedder import VectorEmbedder
from src.media.downloader import MediaDownloader
from src.ingestion.playwright_scraper import PlaywrightXScraper

SYNC_STATE_PATH = Path(os.getenv("DATA_DIR", "data")) / "sync_state.json"


class BackgroundSyncScheduler:
    def __init__(
        self,
        scraper: PlaywrightXScraper,
        store: LanceDBStore,
        tagger: AITagger,
        embedder: VectorEmbedder,
        downloader: MediaDownloader,
        interval_sec: int = 600,
    ):
        self.scraper = scraper
        self.store = store
        self.tagger = tagger
        self.embedder = embedder
        self.downloader = downloader
        self.interval_sec = interval_sec
        self.enabled = True
        self.is_running = False
        self.last_sync_time: float = 0
        self.total_synced_count: int = 0
        self.task: asyncio.Task | None = None
        self._load_state()

    def _load_state(self):
        if SYNC_STATE_PATH.exists():
            try:
                data = json.loads(SYNC_STATE_PATH.read_text())
                self.enabled = data.get("enabled", True)
                self.last_sync_time = data.get("last_sync_time", 0)
                self.total_synced_count = data.get("total_synced_count", 0)
            except Exception:
                pass

    def _save_state(self):
        SYNC_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "enabled": self.enabled,
            "last_sync_time": self.last_sync_time,
            "total_synced_count": self.total_synced_count,
            "interval_sec": self.interval_sec,
        }
        SYNC_STATE_PATH.write_text(json.dumps(data, indent=2))

    def get_status(self) -> dict[str, Any]:
        now = time.time()
        elapsed = now - self.last_sync_time if self.last_sync_time > 0 else self.interval_sec
        next_in = max(0, int(self.interval_sec - elapsed)) if self.enabled else 0
        return {
            "enabled": self.enabled,
            "is_running": self.is_running,
            "interval_sec": self.interval_sec,
            "next_sync_in_sec": next_in,
            "last_sync_time": self.last_sync_time,
            "total_synced_count": self.total_synced_count,
        }

    def toggle(self, enable: bool | None = None) -> bool:
        self.enabled = not self.enabled if enable is None else enable
        self._save_state()
        return self.enabled

    async def run_sync_cycle(self) -> int:
        if self.is_running:
            return 0
        status = self.scraper.get_session_status()
        if not status.get("connected"):
            return 0

        self.is_running = True
        inserted_count = 0
        try:
            # Query existing IDs to avoid re-visiting/re-processing
            existing_tweets = self.store.get_all_tweets(limit=100000)
            existing_ids = {t["tweet_id"] for t in existing_tweets if t.get("tweet_id")}

            # Uncapped infinite scroll (max_tweets=0) until reaching the end of timeline
            scraped = await self.scraper.scrape_likes(max_tweets=0)
            new_tweets = [t for t in scraped if t.get("id") not in existing_ids]

            for tweet in new_tweets:
                tweet["local_media_paths"] = self.downloader.download_tweet_media(tweet)
                tweet["tags"] = self.tagger.generate_tags(tweet["text"])
                try:
                    tweet["vector"] = self.embedder.embed_text(tweet["text"])
                except Exception:
                    tweet["vector"] = [0.0] * 1024
                
                self.store.upsert_tweets([tweet])
                inserted_count += 1

            self.last_sync_time = time.time()
            self.total_synced_count += inserted_count
            self._save_state()
        except Exception:
            pass
        finally:
            self.is_running = False
        return inserted_count

    async def start_loop(self):
        while True:
            try:
                await asyncio.sleep(10)
                if self.enabled:
                    now = time.time()
                    if (now - self.last_sync_time) >= self.interval_sec:
                        await self.run_sync_cycle()
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(10)
