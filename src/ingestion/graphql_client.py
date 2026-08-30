import json
import os
from pathlib import Path
from typing import Any, Callable, Awaitable
from curl_cffi.requests import AsyncSession

DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_SESSION_PATH = DEFAULT_DATA_DIR / "session.json"
BEARER_TOKEN = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
LIKES_QUERY_ID = "nkWXnZ7yXjVf_rL-D8kLFA"
USER_BY_SCREEN_NAME_QUERY_ID = "G3KGOASz96M-Qu0nwmGXNg"


class TwitterGraphQLClient:
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
        }
        if ct0:
            headers["x-csrf-token"] = ct0
        return headers

    async def get_user_id(self, screen_name: str) -> str:
        headers = self._get_auth_headers()
        url = f"https://x.com/i/api/graphql/{USER_BY_SCREEN_NAME_QUERY_ID}/UserByScreenName"
        params = {
            "variables": json.dumps({"screen_name": screen_name.replace("@", ""), "withSafetyModeUserFields": True}),
            "features": json.dumps({"hidden_profile_likes_enabled": True, "responsive_web_graphql_exclude_directive_enabled": True}),
        }
        async with AsyncSession(impersonate="chrome120") as s:
            r = await s.get(url, headers=headers, params=params, timeout=15)
            if r.status_code != 200:
                raise RuntimeError(f"UserByScreenName failed ({r.status_code}): {r.text[:80]}")
            data = r.json()
            user_id = data.get("data", {}).get("user", {}).get("result", {}).get("rest_id", "")
            if not user_id:
                raise RuntimeError(f"Could not find rest_id for user '{screen_name}'.")
            return user_id

    async def fetch_likes_page(self, user_id: str, cursor: str = "", count: int = 100) -> tuple[list[dict[str, Any]], str]:
        headers = self._get_auth_headers()
        url = f"https://x.com/i/api/graphql/{LIKES_QUERY_ID}/Likes"
        variables = {"userId": user_id, "count": count, "includePromotedContent": False, "withVoice": False}
        if cursor:
            variables["cursor"] = cursor

        params = {
            "variables": json.dumps(variables),
            "features": json.dumps({
                "responsive_web_graphql_timeline_navigation_enabled": True,
                "responsive_web_graphql_exclude_directive_enabled": True,
                "responsive_web_media_download_video_enabled": True,
            }),
        }
        async with AsyncSession(impersonate="chrome120") as s:
            r = await s.get(url, headers=headers, params=params, timeout=20)
            if r.status_code != 200:
                raise RuntimeError(f"Likes GraphQL query failed ({r.status_code}): {r.text[:80]}")
            return self._parse_likes_response(r.json())

    def _parse_likes_response(self, data: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
        tweets: list[dict[str, Any]] = []
        next_cursor = ""
        try:
            instructions = data.get("data", {}).get("user", {}).get("result", {}).get("timeline_v2", {}).get("timeline", {}).get("instructions", [])
            for inst in instructions:
                if inst.get("type") != "TimelineAddEntries":
                    continue
                for entry in inst.get("entries", []):
                    entry_id = entry.get("entryId", "")
                    if entry_id.startswith("cursor-bottom-"):
                        next_cursor = entry.get("content", {}).get("value", "")
                        continue

                    item = entry.get("content", {}).get("itemContent", {}).get("tweet_results", {}).get("result", {})
                    if not item:
                        continue
                    legacy = item.get("legacy") or item.get("tweet", {}).get("legacy", {})
                    user_legacy = item.get("core", {}).get("user_results", {}).get("result", {}).get("legacy", {})
                    
                    tweet_id = legacy.get("id_str", "") or item.get("rest_id", "")
                    if not tweet_id:
                        continue

                    full_text = legacy.get("full_text", "")
                    author_name = user_legacy.get("name", "")
                    author_handle = user_legacy.get("screen_name", "")

                    media_urls: list[str] = []
                    for m in legacy.get("extended_entities", {}).get("media", []):
                        if m.get("media_url_https"):
                            media_urls.append(m["media_url_https"])

                    tweets.append({
                        "id": tweet_id,
                        "tweet_id": tweet_id,
                        "author_name": author_name,
                        "author_handle": author_handle,
                        "text": full_text,
                        "created_at": legacy.get("created_at", ""),
                        "liked_at": "",
                        "url": f"https://x.com/{author_handle}/status/{tweet_id}" if author_handle else f"https://x.com/i/web/status/{tweet_id}",
                        "media_urls": media_urls,
                        "local_media_paths": [],
                        "tags": [],
                        "raw_json": json.dumps({"id": tweet_id, "text": full_text, "user": author_handle}),
                    })
        except Exception:
            pass
        return tweets, next_cursor

    async def fetch_all_likes_streaming(
        self,
        username: str = "",
        max_tweets: int = 0,
        on_progress: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        on_item_found: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> list[dict[str, Any]]:
        all_tweets: list[dict[str, Any]] = []
        user_id = await self.get_user_id(username) if username else ""
        if not user_id:
            data = json.loads(self.session_path.read_text())
            user_id = data.get("metadata", {}).get("user_id", "")
        if not user_id:
            raise ValueError(f"Could not resolve Twitter user ID for '{username}'.")

        cursor = ""
        batch_num = 0
        seen_ids: set[str] = set()

        while True:
            batch_num += 1
            batch, next_cursor = await self.fetch_likes_page(user_id=user_id, cursor=cursor, count=100)
            if not batch and batch_num == 1:
                raise RuntimeError("GraphQL Likes returned empty batch on first page.")
            if not batch:
                break

            for t in batch:
                if t["id"] not in seen_ids:
                    seen_ids.add(t["id"])
                    all_tweets.append(t)
                    if on_item_found:
                        await on_item_found(t)
                    if max_tweets > 0 and len(all_tweets) >= max_tweets:
                        break

            if on_progress:
                await on_progress({
                    "stage": "scrolling",
                    "scroll_attempt": batch_num,
                    "tweets_found": len(all_tweets),
                    "height": batch_num * 100,
                    "page_url": f"GraphQL:Likes (batch #{batch_num})",
                })

            if max_tweets > 0 and len(all_tweets) >= max_tweets:
                break
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

        return all_tweets
