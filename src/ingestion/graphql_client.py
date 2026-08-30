import json
from pathlib import Path
import httpx
from typing import Any, AsyncGenerator

LIKES_QUERY_ID = "xA8fDIbrJfy4ojjjXmSR-A"
USER_BY_SCREEN_NAME_QUERY_ID = "Gb-d6r0vxPOADdG62OEBpQ"
LIKES_FEATURES = {
    "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
}


class TwitterGraphQLClient:
    def __init__(self, auth_token: str = "", ct0: str = "", session_path: Path | str | None = None):
        if session_path and Path(session_path).exists():
            try:
                data = json.loads(Path(session_path).read_text())
                cookies = data.get("cookies", [])
                auth_token = auth_token or next((c["value"] for c in cookies if c.get("name") == "auth_token"), "")
                ct0 = ct0 or next((c["value"] for c in cookies if c.get("name") == "ct0"), "")
            except Exception:
                pass

        self.auth_token = auth_token
        self.ct0 = ct0
        self.client = httpx.AsyncClient(
            headers=self._get_auth_headers(),
            timeout=20.0,
        )

    def _get_auth_headers(self) -> dict[str, str]:
        cookies_str = f"auth_token={self.auth_token}"
        if self.ct0:
            cookies_str += f"; ct0={self.ct0}"
        return {
            "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
            "x-twitter-auth-type": "OAuth2Session",
            "x-csrf-token": self.ct0,
            "x-twitter-active-user": "yes",
            "x-twitter-client-language": "en",
            "cookie": cookies_str,
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        }

    def _parse_likes_response(self, data: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
        tweets: list[dict[str, Any]] = []
        next_cursor = ""
        try:
            instructions = data.get("data", {}).get("user", {}).get("result", {}).get("timeline_v2", {}).get("timeline", {}).get("instructions", [])
            if not instructions:
                instructions = data.get("data", {}).get("user", {}).get("result", {}).get("timeline", {}).get("timeline", {}).get("instructions", [])
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
                    if item.get("__typename") == "TweetWithVisibilityResults":
                        item = item.get("tweet", {}) or item

                    legacy = item.get("legacy") or item.get("tweet", {}).get("legacy", {})
                    core = item.get("core", {}) or item.get("tweet", {}).get("core", {})
                    user_res = core.get("user_results", {}).get("result", {})
                    user_legacy = user_res.get("legacy", {}) or user_res
                    
                    tweet_id = legacy.get("id_str", "") or item.get("rest_id", "")
                    if not tweet_id:
                        continue

                    full_text = legacy.get("full_text", "")
                    author_name = user_legacy.get("name", "") or user_res.get("name", "")
                    author_handle = (user_legacy.get("screen_name", "") or user_res.get("screen_name", "")).lstrip("@")

                    media_urls: list[str] = []
                    for m in legacy.get("extended_entities", {}).get("media", []):
                        if m.get("media_url_https"):
                            media_urls.append(m["media_url_https"])

                    url = f"https://x.com/{author_handle}/status/{tweet_id}" if author_handle else f"https://x.com/i/web/status/{tweet_id}"

                    tweets.append({
                        "id": tweet_id,
                        "tweet_id": tweet_id,
                        "author_name": author_name,
                        "author_handle": author_handle,
                        "text": full_text,
                        "created_at": legacy.get("created_at", ""),
                        "liked_at": "",
                        "url": url,
                        "media_urls": media_urls,
                        "local_media_paths": [],
                        "tags": [],
                        "raw_json": json.dumps({"id": tweet_id, "text": full_text, "user": author_handle}),
                    })
        except Exception:
            pass
        return tweets, next_cursor

    async def get_user_id(self, screen_name: str) -> str:
        url = f"https://x.com/i/api/graphql/{USER_BY_SCREEN_NAME_QUERY_ID}/UserByScreenName"
        params = {"variables": json.dumps({"screen_name": screen_name.replace("@", "")})}
        res = await self.client.get(url, params=params)
        if res.status_code == 200:
            data = res.json()
            return data.get("data", {}).get("user", {}).get("result", {}).get("rest_id", "")
        return ""

    async def fetch_likes_page(self, user_id: str, count: int = 40, cursor: str = "") -> tuple[list[dict[str, Any]], str]:
        url = f"https://x.com/i/api/graphql/{LIKES_QUERY_ID}/Likes"
        variables: dict[str, Any] = {"userId": user_id, "count": count, "includePromotedContent": False}
        if cursor:
            variables["cursor"] = cursor

        params = {"variables": json.dumps(variables), "features": json.dumps(LIKES_FEATURES)}
        res = await self.client.get(url, params=params)
        if res.status_code != 200:
            return [], ""

        try:
            return self._parse_likes_response(res.json())
        except Exception:
            return [], ""

    async def fetch_all_likes_streaming(
        self,
        username: str = "",
        max_tweets: int = 0,
    ) -> AsyncGenerator[dict[str, Any], None]:
        user_id = await self.get_user_id(username) if username else ""
        if not user_id:
            return

        cursor = ""
        total_fetched = 0
        while True:
            batch, cursor = await self.fetch_likes_page(user_id, count=40, cursor=cursor)
            if not batch:
                break
            for tweet in batch:
                yield tweet
                total_fetched += 1
                if max_tweets > 0 and total_fetched >= max_tweets:
                    return
            if not cursor:
                break
            await asyncio.sleep(0.5)

    async def close(self):
        await self.client.aclose()
