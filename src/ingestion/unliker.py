import asyncio
import json
import os
import random
import re
from pathlib import Path
from typing import Any, Callable, Awaitable
from curl_cffi.requests import AsyncSession

DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_SESSION_PATH = DEFAULT_DATA_DIR / "session.json"
BEARER_TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
DEFAULT_UNFAVORITE_QUERY_ID = "ZYKSe-w7KEslx3JhSIk5LA"


class TwitterUnliker:
    def __init__(self, session_path: Path | str | None = None):
        self.session_path = Path(session_path or DEFAULT_SESSION_PATH)
        self.unfavorite_query_id = DEFAULT_UNFAVORITE_QUERY_ID

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
            "content-type": "application/json",
        }
        if ct0:
            headers["x-csrf-token"] = ct0
        return headers

    async def auto_refresh_query_id(self) -> str:
        try:
            async with AsyncSession(impersonate="chrome120") as s:
                r = await s.get("https://x.com")
                scripts = re.findall(r'https://abs\.twimg\.com/responsive-web/client-web/[a-zA-Z0-9_\-\.]+\.js', r.text)
                for sc in scripts:
                    sr = await s.get(sc)
                    matches = re.findall(r'queryId:\"([a-zA-Z0-9_\-]+)\",operationName:\"UnfavoriteTweet\"', sr.text)
                    if matches:
                        self.unfavorite_query_id = matches[0]
                        return self.unfavorite_query_id
        except Exception:
            pass
        return self.unfavorite_query_id

    async def unlike_tweet_graphql(self, tweet_id: str) -> tuple[bool, int]:
        headers = self._get_auth_headers()
        url = f"https://x.com/i/api/graphql/{self.unfavorite_query_id}/UnfavoriteTweet"
        payload = {"variables": {"tweet_id": str(tweet_id)}, "queryId": self.unfavorite_query_id}
        
        async with AsyncSession(impersonate="chrome120") as s:
            r = await s.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code == 200:
                data = r.json()
                res = data.get("data", {}).get("unfavorite_tweet")
                return res == "Done" or bool(res), 200
            if r.status_code == 404:
                # Refresh query ID if rotated
                await self.auto_refresh_query_id()
                url = f"https://x.com/i/api/graphql/{self.unfavorite_query_id}/UnfavoriteTweet"
                payload["queryId"] = self.unfavorite_query_id
                r2 = await s.post(url, headers=headers, json=payload, timeout=15)
                if r2.status_code == 200:
                    return True, 200
            return False, r.status_code

    async def unlike_tweet_rest(self, tweet_id: str) -> tuple[bool, int]:
        headers = self._get_auth_headers()
        headers["content-type"] = "application/x-www-form-urlencoded"
        url = "https://x.com/i/api/1.1/favorites/destroy.json"
        async with AsyncSession(impersonate="chrome120") as s:
            r = await s.post(url, headers=headers, data={"id": str(tweet_id)}, timeout=15)
            if r.status_code == 200:
                return True, 200
            return False, r.status_code

    async def ensure_unliked(self, tweet_id: str, tweet_url: str = "", max_attempts: int = 3) -> tuple[bool, str]:
        for attempt in range(1, max_attempts + 1):
            try:
                success, status_code = await self.unlike_tweet_graphql(tweet_id)
                if success:
                    # Also send REST destroy to clear legacy cluster cache
                    try:
                        await self.unlike_tweet_rest(tweet_id)
                    except Exception:
                        pass
                    return True, "graphql+rest"
                
                # Fallback to REST destroy
                rest_ok, rest_code = await self.unlike_tweet_rest(tweet_id)
                if rest_ok:
                    return True, "rest"

                if status_code == 429 or rest_code == 429:
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
