import asyncio
import json
import os
import random
from pathlib import Path
from typing import Any, Callable, Awaitable
from curl_cffi.requests import AsyncSession
from playwright.async_api import async_playwright

DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_SESSION_PATH = DEFAULT_DATA_DIR / "session.json"
BEARER_TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
UNFAVORITE_QUERY_ID = "ZYKfl-48MoIZplqd7Anwlg"


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
            "content-type": "application/json",
        }
        if ct0:
            headers["x-csrf-token"] = ct0
        return headers

    async def unlike_tweet_graphql(self, tweet_id: str) -> bool:
        headers = self._get_auth_headers()
        url = f"https://x.com/i/api/graphql/{UNFAVORITE_QUERY_ID}/UnfavoriteTweet"
        payload = {
            "variables": {"tweet_id": tweet_id},
            "queryId": UNFAVORITE_QUERY_ID,
        }
        async with AsyncSession(impersonate="chrome120") as s:
            r = await s.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code == 200:
                data = r.json()
                return bool(data.get("data", {}).get("unfavorite_tweet"))
            return False

    async def unlike_tweet_playwright(self, tweet_url: str) -> bool:
        if not self.session_path.exists():
            return False
        storage_data = json.loads(self.session_path.read_text())
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = await browser.new_context(storage_state=storage_data)
            page = await context.new_page()
            try:
                await page.goto(tweet_url, wait_until="domcontentloaded", timeout=15000)
                unlike_btn = page.locator("button[data-testid='unlike']").first
                if await unlike_btn.count() > 0:
                    await unlike_btn.click()
                    await page.wait_for_timeout(500)
                    await browser.close()
                    return True
                await browser.close()
                return False
            except Exception:
                await browser.close()
                return False

    async def unlike_tweet(self, tweet_id: str, tweet_url: str = "") -> bool:
        try:
            success = await self.unlike_tweet_graphql(tweet_id)
            if success:
                return True
        except Exception:
            pass

        if tweet_url:
            try:
                return await self.unlike_tweet_playwright(tweet_url)
            except Exception:
                pass
        return False

    async def bulk_unlike(
        self,
        tweets: list[dict[str, Any]],
        on_progress: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> int:
        unliked_count = 0
        total = len(tweets)

        for idx, t in enumerate(tweets, start=1):
            tweet_id = t.get("tweet_id") or t.get("id")
            url = t.get("url", f"https://x.com/i/web/status/{tweet_id}")
            if not tweet_id:
                continue

            success = await self.unlike_tweet(str(tweet_id), url)
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

            # Humanized anti-ban pacing (800ms - 1500ms)
            delay = random.uniform(0.8, 1.5)
            await asyncio.sleep(delay)

        return unliked_count
