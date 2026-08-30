import asyncio
import json
from typing import Any, AsyncGenerator
import httpx
from src.storage.lancedb_client import LanceDBStore


class TweetMetricsEnricher:
    def __init__(self, store: LanceDBStore | None = None):
        self.store = store or LanceDBStore()

    async def fetch_tweet_metrics(self, client: httpx.AsyncClient, tweet_id: str, sem: asyncio.Semaphore) -> dict[str, Any] | None:
        if not tweet_id or not str(tweet_id).isdigit():
            return None
        url = f"https://cdn.syndication.twimg.com/tweet-result?id={tweet_id}&token=1"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "*/*",
        }
        async with sem:
            try:
                resp = await client.get(url, headers=headers, timeout=10.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and data.get("favorite_count") is not None:
                        return {
                            "tweet_id": tweet_id,
                            "favorite_count": int(data.get("favorite_count", 0)),
                            "created_at": data.get("created_at", ""),
                            "author_name": data.get("user", {}).get("name", ""),
                            "author_handle": data.get("user", {}).get("screen_name", ""),
                        }
            except Exception:
                pass
        return None

    async def stream_enrich_all(self, batch_size: int = 50, concurrency: int = 15) -> AsyncGenerator[str, None]:
        all_tweets = self.store.get_all_tweets(limit=10000)
        total = len(all_tweets)
        if total == 0:
            yield json.dumps({"stage": "complete", "total": 0, "enriched": 0, "message": "No tweets in database."})
            return

        yield json.dumps({"stage": "start", "total": total, "message": f"Starting enrichment for {total} likes..."})

        sem = asyncio.Semaphore(concurrency)
        enriched_count = 0
        updated_batch: list[dict[str, Any]] = []

        async with httpx.AsyncClient() as client:
            tasks = []
            for t in all_tweets:
                tid = str(t.get("tweet_id") or t.get("id"))
                tasks.append((t, asyncio.create_task(self.fetch_tweet_metrics(client, tid, sem))))

            for i, (orig_t, task) in enumerate(tasks, start=1):
                res = await task
                if res and res.get("favorite_count") is not None:
                    enriched_count += 1
                    raw_dict = {}
                    try:
                        raw_dict = json.loads(orig_t.get("raw_json") or "{}")
                    except Exception:
                        pass
                    raw_dict["favorite_count"] = res["favorite_count"]
                    if res.get("created_at"):
                        raw_dict["created_at"] = res["created_at"]

                    updated_t = dict(orig_t)
                    updated_t["favorite_count"] = res["favorite_count"]
                    if res.get("created_at") and not updated_t.get("created_at"):
                        updated_t["created_at"] = res["created_at"]
                    if res.get("author_name") and not updated_t.get("author_name"):
                        updated_t["author_name"] = res["author_name"]
                    if res.get("author_handle") and not updated_t.get("author_handle"):
                        updated_t["author_handle"] = res["author_handle"]
                    updated_t["raw_json"] = json.dumps(raw_dict)
                    updated_batch.append(updated_t)

                if len(updated_batch) >= batch_size:
                    self.store.upsert_tweets(updated_batch)
                    updated_batch.clear()

                if i % 10 == 0 or i == total:
                    pct = round((i / total) * 100, 1)
                    yield json.dumps({
                        "stage": "progress",
                        "current": i,
                        "total": total,
                        "enriched": enriched_count,
                        "percent": pct,
                    })

            if updated_batch:
                self.store.upsert_tweets(updated_batch)
                updated_batch.clear()

        yield json.dumps({
            "stage": "complete",
            "total": total,
            "enriched": enriched_count,
            "message": f"Successfully enriched {enriched_count} / {total} likes with live like counts!",
        })
