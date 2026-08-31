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


DEFAULT_DELETE_BOOKMARK_QUERY_ID = "2v08qAahO0pn9vdqGkCqzg"


class TwitterUnliker:
    def __init__(self, session_path: Path | str | None = None):
        self.session_path = Path(session_path or DEFAULT_SESSION_PATH)
        self.unfavorite_query_id = DEFAULT_UNFAVORITE_QUERY_ID
        self.delete_bookmark_query_id = DEFAULT_DELETE_BOOKMARK_QUERY_ID

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
                    bm_matches = re.findall(r'queryId:\"([a-zA-Z0-9_\-]+)\",operationName:\"DeleteBookmark\"', sr.text)
                    if bm_matches:
                        self.delete_bookmark_query_id = bm_matches[0]
        except Exception:
            pass
        return self.unfavorite_query_id

    async def unlike_tweet_graphql(self, tweet_id: str) -> tuple[bool, int]:
        try:
            headers = self._get_auth_headers()
        except Exception:
            return False, 401
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
        try:
            headers = self._get_auth_headers()
        except Exception:
            return False, 401
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
                
                if status_code == 401:
                    return False, "unauthorized"

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

    async def unlike_tweet(self, tweet_id: str, tweet_url: str = "") -> bool:
        success, _ = await self.ensure_unliked(tweet_id, tweet_url, max_attempts=2)
        return success

    async def purge_ghost_like(self, tweet_id: str) -> tuple[bool, str]:
        """
        Aggressively purges a ghost like on X using dual GraphQL + REST destroy.
        Even if GraphQL returns 404 (deleted/suspended tweet), REST favorites/destroy
        forces X's legacy clusters to clear phantom like references and decrement counts.
        """
        tid = str(tweet_id)
        gql_ok, gql_code = await self.unlike_tweet_graphql(tid)
        rest_ok, rest_code = await self.unlike_tweet_rest(tid)

        if gql_ok and rest_ok:
            return True, "graphql+rest"
        elif gql_ok:
            return True, "graphql"
        elif rest_ok:
            return True, "rest_legacy"
        elif gql_code == 404 and (rest_code == 200 or rest_code == 404):
            # Tweet deleted by author or suspended; legacy cluster purged
            return True, "ghost_purged"
        elif gql_code == 401 or rest_code == 401:
            return False, "unauthorized"
        elif gql_code == 429 or rest_code == 429:
            return False, "rate_limited"
        return False, f"failed_gql{gql_code}_rest{rest_code}"

    async def sweep_ghost_likes(
        self,
        tweet_ids: list[str],
        on_progress: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> tuple[int, list[str]]:
        """
        Sweeps a batch of historical tweet IDs, unliking and purging ghost references.
        Returns (purged_count, successfully_swept_ids).
        """
        swept_ids: list[str] = []
        purged_count = 0
        total = len(tweet_ids)

        for idx, tid in enumerate(tweet_ids, start=1):
            if not tid:
                continue
            success, method = await self.purge_ghost_like(str(tid))
            if success:
                purged_count += 1
                swept_ids.append(str(tid))

            if on_progress:
                await on_progress({
                    "stage": "ghost_sweeping",
                    "current": idx,
                    "total": total,
                    "purged_count": purged_count,
                    "tweet_id": tid,
                    "method": method,
                    "success": success,
                })

            if method == "rate_limited":
                await asyncio.sleep(4.0)
            else:
                delay = random.uniform(0.4, 0.8)
                await asyncio.sleep(delay)

        return purged_count, swept_ids

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

    async def unbookmark_tweet_graphql(self, tweet_id: str) -> tuple[bool, int]:
        try:
            headers = self._get_auth_headers()
        except Exception:
            return False, 401
        url = f"https://x.com/i/api/graphql/{self.delete_bookmark_query_id}/DeleteBookmark"
        payload = {"variables": {"tweet_id": str(tweet_id)}, "queryId": self.delete_bookmark_query_id}
        
        async with AsyncSession(impersonate="chrome120") as s:
            r = await s.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code == 200:
                data = r.json()
                res = data.get("data", {}).get("delete_bookmark")
                return res == "Done" or bool(res), 200
            if r.status_code == 404:
                await self.auto_refresh_query_id()
                url = f"https://x.com/i/api/graphql/{self.delete_bookmark_query_id}/DeleteBookmark"
                payload["queryId"] = self.delete_bookmark_query_id
                r2 = await s.post(url, headers=headers, json=payload, timeout=15)
                if r2.status_code == 200:
                    return True, 200
            return False, r.status_code

    async def ensure_unbookmarked(self, tweet_id: str, max_attempts: int = 3) -> tuple[bool, str]:
        for attempt in range(1, max_attempts + 1):
            try:
                success, status_code = await self.unbookmark_tweet_graphql(tweet_id)
                if success:
                    return True, "graphql"
                if status_code == 401:
                    return False, "unauthorized"
                if status_code == 429:
                    await asyncio.sleep(4.0)
            except Exception:
                pass

            if attempt < max_attempts:
                await asyncio.sleep(0.8 * attempt)

        return False, "failed"

    async def unbookmark_tweet(self, tweet_id: str) -> bool:
        success, _ = await self.ensure_unbookmarked(tweet_id, max_attempts=2)
        return success

    async def bulk_unbookmark(
        self,
        tweets: list[dict[str, Any]],
        on_progress: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> int:
        unbookmarked_count = 0
        total = len(tweets)

        for idx, t in enumerate(tweets, start=1):
            tweet_id = t.get("tweet_id") or t.get("id")
            if not tweet_id:
                continue

            success, _ = await self.ensure_unbookmarked(str(tweet_id), max_attempts=3)
            if success:
                unbookmarked_count += 1

            if on_progress:
                await on_progress({
                    "stage": "unbookmarking",
                    "current": idx,
                    "total": total,
                    "unbookmarked_count": unbookmarked_count,
                    "tweet_id": tweet_id,
                    "success": success,
                })

            delay = random.uniform(0.6, 1.2)
            await asyncio.sleep(delay)

        return unbookmarked_count

