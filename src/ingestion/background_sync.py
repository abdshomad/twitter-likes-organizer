import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, AsyncGenerator
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
        interval_sec: int = 300,
        on_telemetry_event: Any = None,
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
        self.on_telemetry_event = on_telemetry_event
        self.enabled = True
        self.auto_unlike = True
        self.is_running = False
        self.last_sync_time: float = 0
        self.total_synced_count: int = 0
        self.last_spared_tweet_id: str = ""
        self.swept_ghost_ids: set[str] = set()
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
                self.last_spared_tweet_id = data.get("last_spared_tweet_id", "")
                self.swept_ghost_ids = set(data.get("swept_ghost_ids", []))
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
            "last_spared_tweet_id": self.last_spared_tweet_id,
            "swept_ghost_ids": list(self.swept_ghost_ids),
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
            "last_spared_tweet_id": self.last_spared_tweet_id,
            "swept_ghost_count": len(self.swept_ghost_ids),
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
        unliked_count = 0
        start_time = time.time()
        max_duration_sec = max(60, self.interval_sec - 60)
        engine_used = "graphql"

        try:
            existing_tweets = self.store.get_all_tweets(limit=100000)
            existing_ids = {t["tweet_id"] for t in existing_tweets if t.get("tweet_id")}

            discovered_batch: list[dict[str, Any]] = []
            discovered_bookmarks: list[dict[str, Any]] = []

            async def on_like_found(tweet: dict):
                nonlocal inserted_count
                if (time.time() - start_time) >= max_duration_sec:
                    return

                tid = str(tweet.get("id") or tweet.get("tweet_id"))
                tweet["source"] = tweet.get("source") or "like"
                discovered_batch.append(tweet)

                if tid not in existing_ids:
                    media_urls = tweet.get("media_urls", [])
                    if media_urls:
                        await self.media_queue.enqueue(tid, media_urls)

                    tweet["tags"] = self.tagger.generate_tags(tweet.get("text", ""))
                    try:
                        tweet["vector"] = self.embedder.embed_text(tweet.get("text", ""))
                    except Exception:
                        tweet["vector"] = [0.0] * 1024

                    self.store.upsert_tweets([tweet], default_source="like")
                    existing_ids.add(tid)
                    inserted_count += 1

                    if self.on_telemetry_event:
                        try:
                            await self.on_telemetry_event("new_like", {
                                "tweet_id": tid,
                                "author_handle": tweet.get("author_handle", ""),
                                "author_name": tweet.get("author_name", ""),
                                "text": tweet.get("text", "")[:120],
                                "favorite_count": tweet.get("favorite_count", 0),
                            })
                        except Exception:
                            pass

            async def on_bookmark_found(tweet: dict):
                nonlocal inserted_count
                if (time.time() - start_time) >= max_duration_sec:
                    return

                tid = str(tweet.get("id") or tweet.get("tweet_id"))
                tweet["source"] = tweet.get("source") or "bookmark"
                discovered_bookmarks.append(tweet)

                if tid not in existing_ids:
                    media_urls = tweet.get("media_urls", [])
                    if media_urls:
                        await self.media_queue.enqueue(tid, media_urls)

                    tweet["tags"] = self.tagger.generate_tags(tweet.get("text", ""))
                    try:
                        tweet["vector"] = self.embedder.embed_text(tweet.get("text", ""))
                    except Exception:
                        tweet["vector"] = [0.0] * 1024

                    self.store.upsert_tweets([tweet], default_source="bookmark")
                    existing_ids.add(tid)
                    inserted_count += 1

                    if self.on_telemetry_event:
                        try:
                            await self.on_telemetry_event("new_bookmark", {
                                "tweet_id": tid,
                                "author_handle": tweet.get("author_handle", ""),
                                "author_name": tweet.get("author_name", ""),
                                "text": tweet.get("text", "")[:120],
                                "favorite_count": tweet.get("favorite_count", 0),
                            })
                        except Exception:
                            pass

            uname = status.get("username", "")
            # Sync Likes
            likes_count = 0
            try:
                async for tweet in self.gql_client.fetch_all_likes_streaming(username=uname, max_tweets=0):
                    likes_count += 1
                    await on_like_found(tweet)
                if likes_count == 0:
                    raise RuntimeError("GraphQL Likes returned 0 items; falling back to Playwright scraper")
            except Exception:
                engine_used = "playwright"
                await self.scraper.scrape_likes(username=uname, max_tweets=0, on_item_found=on_like_found)

            # Sync Bookmarks
            try:
                async for tweet in self.gql_client.fetch_all_bookmarks_streaming(max_tweets=0):
                    await on_bookmark_found(tweet)
            except Exception:
                if engine_used != "playwright":
                    engine_used = "graphql+playwright"
                try:
                    await self.scraper.scrape_bookmarks(max_tweets=0, on_item_found=on_bookmark_found)
                except Exception:
                    pass

            # Clean/Unlike/Unbookmark on X if auto_unlike is enabled
            if self.auto_unlike and (discovered_batch or discovered_bookmarks):
                for t in discovered_batch:
                    if (time.time() - start_time) >= max_duration_sec:
                        break
                    tid = str(t.get("id") or t.get("tweet_id"))
                    try:
                        success, _ = await self.unliker.ensure_unliked(tid, max_attempts=3)
                        if success:
                            unliked_count += 1
                        await asyncio.sleep(0.5)
                    except Exception:
                        pass

                for t in discovered_bookmarks:
                    if (time.time() - start_time) >= max_duration_sec:
                        break
                    tid = str(t.get("id") or t.get("tweet_id"))
                    try:
                        await self.unliker.ensure_unbookmarked(tid, max_attempts=3)
                        await asyncio.sleep(0.5)
                    except Exception:
                        pass

            # Ghost likes incremental batch sweeper: Purge next 60 unswept stored likes
            ghost_purged_count = 0
            if self.auto_unlike and (time.time() - start_time) < max_duration_sec:
                try:
                    stored_likes = self.store.get_all_tweets(limit=100000, source="like")
                    candidates = [
                        str(t.get("tweet_id") or t.get("id"))
                        for t in stored_likes
                        if str(t.get("tweet_id") or t.get("id")) and str(t.get("tweet_id") or t.get("id")) not in self.swept_ghost_ids
                    ]
                    batch_candidates = candidates[:60]
                    if batch_candidates:
                        p_count, swept_ids = await self.unliker.sweep_ghost_likes(batch_candidates)
                        ghost_purged_count = p_count
                        self.swept_ghost_ids.update(swept_ids)
                except Exception:
                    pass

            self.last_sync_time = time.time()
            self.total_synced_count += inserted_count
            self._save_state()

            duration = time.time() - start_time
            stats = self.store.get_stats()
            total_db = stats.get("total_items", stats.get("total_likes", 0))
            self.history.add_sync_log(
                trigger="auto-cron-5m",
                engine=engine_used,
                status="success",
                new_likes=inserted_count,
                total_db_likes=total_db,
                message=f"Sync cycle completed (+{inserted_count} saved, {unliked_count} live unliked, {ghost_purged_count} ghosts purged).",
                duration_sec=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            stats = self.store.get_stats()
            total_db = stats.get("total_items", stats.get("total_likes", 0))
            self.history.add_sync_log(
                trigger="auto-cron-5m",
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

    async def sweep_ghost_likes_stream(self, limit: int = 100) -> AsyncGenerator[str, None]:
        stored_likes = self.store.get_all_tweets(limit=100000, source="like")
        candidates = [
            str(t.get("tweet_id") or t.get("id"))
            for t in stored_likes
            if str(t.get("tweet_id") or t.get("id")) and str(t.get("tweet_id") or t.get("id")) not in self.swept_ghost_ids
        ]
        target_batch = candidates[:limit] if limit > 0 else candidates
        total = len(target_batch)
        if total == 0:
            yield f"data: {json.dumps({'stage': 'complete', 'total': 0, 'purged': 0, 'message': 'All stored likes have already been swept for ghost status.'})}\n\n"
            return

        yield f"data: {json.dumps({'stage': 'start', 'total': total, 'message': f'Starting ghost like purge for {total} candidates...'})}\n\n"

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

        async def on_prog(ev: dict[str, Any]):
            await queue.put(ev)

        async def run_worker():
            purged, swept = await self.unliker.sweep_ghost_likes(target_batch, on_progress=on_prog)
            self.swept_ghost_ids.update(swept)
            self._save_state()
            await queue.put({
                "stage": "complete",
                "total": total,
                "purged": purged,
                "message": f"Ghost purge completed. Cleaned {purged} ghost references on X.",
            })

        worker_task = asyncio.create_task(run_worker())

        while not worker_task.done() or not queue.empty():
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=0.2)
                yield f"data: {json.dumps(ev)}\n\n"
                if ev.get("stage") == "complete":
                    break
            except asyncio.TimeoutError:
                continue

        await worker_task

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
