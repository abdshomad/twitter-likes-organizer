import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any
from src.storage.lancedb_client import LanceDBStore
from src.storage.history_manager import HistoryManager
from src.media.media_queue import MediaQueue
from src.ai.tagger import AITagger
from src.ai.embedder import VectorEmbedder
from src.ingestion.playwright_scraper import PlaywrightXScraper
from src.ingestion.graphql_client import TwitterGraphQLClient
from src.ingestion.unliker import TwitterUnliker

SYNC_STATE_PATH = Path(os.getenv("DATA_DIR", "data")) / "sync_state.json"


class BackgroundSyncScheduler:
    def __init__(
        self,
        scraper: PlaywrightXScraper,
        store: LanceDBStore,
        tagger: AITagger,
        embedder: VectorEmbedder,
        media_queue: MediaQueue,
        interval_sec: int = 600,
    ):
        self.scraper = scraper
        self.gql_client = TwitterGraphQLClient(scraper.session_path)
        self.unliker = TwitterUnliker(scraper.session_path)
        self.store = store
        self.media_queue = media_queue
        self.history = HistoryManager()
        self.tagger = tagger
        self.embedder = embedder
        self.interval_sec = interval_sec
        self.enabled = True
        self.auto_unlike = True
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
                self.auto_unlike = data.get("auto_unlike", True)
                self.interval_sec = data.get("interval_sec", self.interval_sec)
                self.last_sync_time = data.get("last_sync_time", 0)
                self.total_synced_count = data.get("total_synced_count", 0)
            except Exception:
                pass
        else:
            self._save_state()

    def _save_state(self):
        SYNC_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "enabled": self.enabled,
            "auto_unlike": self.auto_unlike,
            "last_sync_time": self.last_sync_time,
            "total_synced_count": self.total_synced_count,
            "interval_sec": self.interval_sec,
        }
        SYNC_STATE_PATH.write_text(json.dumps(data, indent=2))

    def get_status(self) -> dict[str, Any]:
        now = time.time()
        elapsed = now - self.last_sync_time if self.last_sync_time > 0 else self.interval_sec
        next_in = max(0, int(self.interval_sec - elapsed)) if (self.enabled and self.interval_sec > 0) else 0
        return {
            "enabled": self.enabled,
            "auto_unlike": self.auto_unlike,
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

    def set_interval(self, interval_sec: int) -> int:
        self.interval_sec = max(0, interval_sec)
        self._save_state()
        return self.interval_sec

    def toggle_auto_unlike(self, enable: bool | None = None) -> bool:
        self.auto_unlike = not self.auto_unlike if enable is None else enable
        self._save_state()
        return self.auto_unlike

    async def run_sync_cycle(self) -> int:
        if self.is_running:
            return 0
        status = self.scraper.get_session_status()
        if not status.get("connected"):
            return 0

        self.is_running = True
        inserted_count = 0
        start_time = time.time()
        engine_used = "graphql"

        try:
            existing_tweets = self.store.get_all_tweets(limit=100000)
            existing_ids = {t["tweet_id"] for t in existing_tweets if t.get("tweet_id")}

            async def on_item_found(tweet: dict):
                nonlocal inserted_count
                if tweet.get("id") in existing_ids:
                    return
                media_urls = tweet.get("media_urls", [])
                if media_urls:
                    await self.media_queue.enqueue(tweet.get("id", ""), media_urls)

                tweet["tags"] = self.tagger.generate_tags(tweet["text"])
                try:
                    tweet["vector"] = self.embedder.embed_text(tweet["text"])
                except Exception:
                    tweet["vector"] = [0.0] * 1024
                
                self.store.upsert_tweets([tweet])
                existing_ids.add(tweet.get("id"))
                inserted_count += 1

                if self.auto_unlike:
                    try:
                        await self.unliker.ensure_unliked(tweet.get("id", ""), tweet.get("url", ""), max_attempts=3)
                    except Exception:
                        pass

            uname = status.get("username", "")
            try:
                await self.gql_client.fetch_all_likes_streaming(username=uname, max_tweets=0, on_item_found=on_item_found)
            except Exception:
                engine_used = "playwright"
                await self.scraper.scrape_likes(username=uname, max_tweets=0, on_item_found=on_item_found)

            self.last_sync_time = time.time()
            self.total_synced_count += inserted_count
            self._save_state()

            duration = time.time() - start_time
            total_db = self.store.get_stats().get("total_likes", 0)
            self.history.add_sync_log(
                trigger="auto-cron",
                engine=engine_used,
                status="success",
                new_likes=inserted_count,
                total_db_likes=total_db,
                message=f"Auto-sync completed (+{inserted_count} likes, Auto-Unlike: {self.auto_unlike}).",
                duration_sec=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            total_db = self.store.get_stats().get("total_likes", 0)
            self.history.add_sync_log(
                trigger="auto-cron",
                engine=engine_used,
                status="error",
                new_likes=inserted_count,
                total_db_likes=total_db,
                message=str(e),
                duration_sec=duration,
            )
        finally:
            self.is_running = False
        return inserted_count

    async def start_loop(self):
        while True:
            try:
                await asyncio.sleep(5)
                if self.enabled and self.interval_sec > 0:
                    now = time.time()
                    if (now - self.last_sync_time) >= self.interval_sec:
                        await self.run_sync_cycle()
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)
