import asyncio
import httpx
from typing import Any
from src.storage.lancedb_client import LanceDBStore


class AuthorHydrator:
    def __init__(self, store: LanceDBStore):
        self.store = store
        self.client = httpx.AsyncClient(timeout=10.0, follow_redirects=True)

    async def resolve_author(self, tweet_id: str) -> tuple[str, str]:
        url = f"https://publish.twitter.com/oembed?url=https://x.com/i/status/{tweet_id}"
        try:
            res = await self.client.get(url)
            if res.status_code == 200:
                data = res.json()
                author_name = data.get("author_name", "")
                author_url = data.get("author_url", "")
                author_handle = author_url.strip("/").split("/")[-1] if author_url else ""
                if author_handle and not author_handle.startswith("@"):
                    author_handle = f"@{author_handle}"
                return author_name, author_handle
        except Exception:
            pass
        return "", ""

    async def hydrate_missing_authors(self, batch_size: int = 50, concurrency: int = 5) -> int:
        tweets = self.store.get_all_tweets(limit=5000)
        missing = [t for t in tweets if not t.get("author_handle")][:batch_size]
        if not missing:
            return 0

        sem = asyncio.Semaphore(concurrency)
        updated: list[dict[str, Any]] = []

        async def worker(t: dict[str, Any]):
            async with sem:
                name, handle = await self.resolve_author(t["tweet_id"])
                if handle:
                    t["author_name"] = name or handle.lstrip("@")
                    t["author_handle"] = handle
                    t["url"] = f"https://x.com/{handle.lstrip('@')}/status/{t['tweet_id']}"
                    updated.append(t)
                await asyncio.sleep(0.1)

        await asyncio.gather(*(worker(t) for t in missing))
        if updated:
            self.store.upsert_tweets(updated)
        return len(updated)

    async def close(self):
        await self.client.aclose()
