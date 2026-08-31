import os
import json
import logging
from datetime import datetime, timezone
from typing import Any, Sequence
import meilisearch
from meilisearch.errors import MeilisearchError

logger = logging.getLogger(__name__)

INDEX_NAME = "likes"


class MeiliSearchStore:
    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        index_name: str = INDEX_NAME,
    ):
        self.url = url or os.getenv("MEILISEARCH_URL", "http://127.0.0.1:7700")
        self.api_key = api_key or os.getenv("MEILISEARCH_API_KEY", None)
        self.index_name = index_name
        self.client = meilisearch.Client(self.url, self.api_key)
        self._initialized = False
        try:
            self._ensure_index()
            self._initialized = True
        except Exception as e:
            logger.warning(f"[Meilisearch] Initialization deferred / server unavailable: {e}")

    def is_healthy(self) -> bool:
        try:
            return self.client.is_healthy()
        except Exception:
            return False

    def _ensure_index(self):
        try:
            self.index = self.client.get_index(self.index_name)
        except Exception:
            task = self.client.create_index(self.index_name, {"primaryKey": "tweet_id"})
            self.index = self.client.get_index(self.index_name)

        try:
            self.index.update_filterable_attributes([
                "tags",
                "source",
                "author_handle",
                "created_at",
                "tweet_id",
            ])
            self.index.update_searchable_attributes([
                "text",
                "author_name",
                "author_handle",
                "tags",
            ])
            self.index.update_sortable_attributes([
                "created_at",
                "liked_at",
            ])
        except Exception as e:
            logger.debug(f"[Meilisearch] Index attribute update notice: {e}")

    def upsert_tweets(self, tweets: Sequence[dict[str, Any]], default_source: str = "like") -> int:
        if not tweets:
            return 0
        if not self.is_healthy():
            logger.warning("[Meilisearch] Server not healthy, skipping upsert")
            return 0

        documents = []
        for t in tweets:
            tid = str(t.get("tweet_id") or t.get("id", ""))
            if not tid:
                continue

            raw_source = str(t.get("source") or default_source or "like").lower()

            doc = {
                "id": str(t.get("id") or tid),
                "tweet_id": tid,
                "author_name": str(t.get("author_name", "")),
                "author_handle": str(t.get("author_handle", "")),
                "text": str(t.get("text", "")),
                "created_at": str(t.get("created_at", "")),
                "liked_at": str(t.get("liked_at") or datetime.now(timezone.utc).isoformat()),
                "url": str(t.get("url", "")),
                "media_urls": list(t.get("media_urls", [])),
                "local_media_paths": list(t.get("local_media_paths", [])),
                "tags": list(t.get("tags", [])),
                "raw_json": str(t.get("raw_json", "")),
                "source": raw_source,
            }
            documents.append(doc)

        if not documents:
            return 0

        try:
            self.index.add_documents(documents, primary_key="tweet_id")
            return len(documents)
        except Exception as e:
            logger.error(f"[Meilisearch] Error adding documents: {e}")
            return 0

    def delete_tweets(self, tweet_ids: list[str]) -> int:
        if not tweet_ids or not self.is_healthy():
            return 0
        try:
            self.index.delete_documents(tweet_ids)
            return len(tweet_ids)
        except Exception as e:
            logger.error(f"[Meilisearch] Error deleting documents: {e}")
            return 0

    def search(
        self,
        query: str = "",
        tag: str | None = None,
        author_handle: str | None = None,
        source: str = "all",
        sort_by: str = "newest",
        offset: int = 0,
        limit: int = 24,
    ) -> list[dict[str, Any]]:
        if not self.is_healthy():
            return []

        filters = []
        if tag:
            clean_tag = tag.replace('"', '\\"')
            filters.append(f'tags = "{clean_tag}"')

        if author_handle:
            clean_author = author_handle.lstrip("@").replace('"', '\\"')
            filters.append(f'author_handle = "{clean_author}"')

        if source in ("like", "likes"):
            filters.append('(source = "like" OR source = "both")')
        elif source in ("bookmark", "bookmarks"):
            filters.append('(source = "bookmark" OR source = "both")')
        elif source in ("youtube", "yt"):
            filters.append('source = "youtube"')

        filter_str = " AND ".join(filters) if filters else None

        sort_params = []
        if sort_by in ("newest", "newest_liked"):
            sort_params = ["liked_at:desc", "created_at:desc"]
        elif sort_by in ("oldest", "oldest_liked"):
            sort_params = ["liked_at:asc", "created_at:asc"]
        elif sort_by == "newest_tweeted":
            sort_params = ["created_at:desc"]
        elif sort_by == "oldest_tweeted":
            sort_params = ["created_at:asc"]

        search_params: dict[str, Any] = {
            "offset": offset,
            "limit": limit,
        }
        if filter_str:
            search_params["filter"] = filter_str
        if sort_params:
            search_params["sort"] = sort_params

        try:
            res = self.index.search(query, search_params)
            hits = res.get("hits", [])
            for h in hits:
                if "source" not in h or not h["source"]:
                    h["source"] = "like"
                # Parse favorite count if available
                if "favorite_count" not in h:
                    raw_str = h.get("raw_json", "")
                    if raw_str and raw_str != "{}":
                        try:
                            raw = json.loads(raw_str)
                            h["favorite_count"] = int(
                                raw.get("favorite_count")
                                or raw.get("legacy", {}).get("favorite_count")
                                or raw.get("like_count")
                                or 0
                            )
                        except Exception:
                            h["favorite_count"] = 0
                    else:
                        h["favorite_count"] = 0
            return hits
        except Exception as e:
            logger.error(f"[Meilisearch] Search error: {e}")
            return []

    def get_stats(self) -> dict[str, int]:
        if not self.is_healthy():
            return {"total_tweets": 0}
        try:
            stats = self.index.get_stats()
            return {"total_tweets": stats.number_of_documents}
        except Exception:
            return {"total_tweets": 0}
