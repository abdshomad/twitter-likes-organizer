import asyncio
import json
import os
import random
from pathlib import Path
from typing import Any, Callable, Awaitable
from curl_cffi.requests import AsyncSession

DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_SESSION_PATH = DEFAULT_DATA_DIR / "session.json"
BEARER_TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
UNLIKE_ENDPOINTS = [
    "https://x.com/i/api/1.1/favorites/destroy.json",
    "https://api.x.com/1.1/favorites/destroy.json",
    "https://twitter.com/i/api/1.1/favorites/destroy.json",
]


class TwitterUnliker:
    def __init__(self, session_path: Path | str | None = None):
        self.session_path = Path(session_path or DEFAULT_SESSION_PATH)

    def _get_auth_headers(self) -> dict[str, str]:
        if not self.session_path.exists():
            raise FileNotFoundError("Session file not found. Please connect Twitter first.")
        data = json.loads(self.session_path.read_text())
        cookies = data.get("cookies", [])
        
        auth_token = next((c["value"] for c in cookies if c["name"] == "auth_token"), "")
        ct0 = next((c["value"] for c in cookies if c["name"] == "ct0"), "")
        if not auth_token:
            raise ValueError("auth_token cookie missing from session.")

        cookie_header = f"auth_token={auth_token}" + (f"; ct0={ct0}" if ct0 else "")
        headers = {
            "authorization": BEARER_TOKEN,
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session",
            "x-twitter-client-language": "en",
            "cookie": cookie_header,
            "referer": "https://x.com/",
            "origin": "https://x.com",
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded",
        }
        if ct0:
            headers["x-csrf-token"] = ct0
        return headers

    async def unlike_tweet_api(self, tweet_id: str) -> tuple[bool, int]:
        headers = self._get_auth_headers()
        payload = {"id": str(tweet_id)}
        async with AsyncSession(impersonate="chrome120") as s:
            for url in UNLIKE_ENDPOINTS:
                try:
                    r = await s.post(url, headers=headers, data=payload, timeout=12)
                    if r.status_code == 200:
                        return True, 200
                    if r.status_code == 404:
                        # 404 on destroy means already unliked or deleted -> considered success
                        return True, 404
                    if r.status_code == 429:
                        return False, 429
                except Exception:
                    continue
        return False, 500

    async def ensure_unliked(self, tweet_id: str, tweet_url: str = "", max_attempts: int = 3) -> tuple[bool, str]:
        for attempt in range(1, max_attempts + 1):
            try:
                success, status_code = await self.unlike_tweet_api(tweet_id)
                if success:
                    return True, "api"
                if status_code == 429:
                    await asyncio.sleep(4.0)
            except Exception:
                pass

            if attempt < max_attempts:
                await asyncio.sleep(0.8 * attempt)

        return False, "failed"

    async def unlike_tweet(self, tweet_id: str, tweet_url: str = "") -> bool:
        success, _ = await self.ensure_unliked(tweet_id, tweet_url, max_attempts=2)
        return success

    async def bulk_unlike(
        self,
        tweets: list[dict[str, Any]],
        on_progress: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> int:
        unliked_count = 0
        total = len(tweets)

        for idx, t in enumerate(tweets, start=1):
            tweet_id = t.get("tweet_id") or t.get("id")
            if not tweet_id:
                continue

            success, _ = await self.ensure_unliked(str(tweet_id), max_attempts=3)
            if success:
                unliked_count += 1

            if on_progress:
                await on_progress({
                    "stage": "unliking",
                    "current": idx,
                    "total": total,
                    "unliked_count": unliked_count,
                    "tweet_id": tweet_id,
                    "success": success,
                })

            delay = random.uniform(0.6, 1.2)
            await asyncio.sleep(delay)

        return unliked_count
