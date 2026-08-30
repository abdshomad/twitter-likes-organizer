import os
import json
import time
from pathlib import Path
from typing import Any, Sequence
import lancedb
import pyarrow as pa

SCHEMA = pa.schema([
    pa.field("id", pa.string()),
    pa.field("tweet_id", pa.string()),
    pa.field("author_name", pa.string()),
    pa.field("author_handle", pa.string()),
    pa.field("text", pa.string()),
    pa.field("created_at", pa.string()),
    pa.field("liked_at", pa.string()),
    pa.field("url", pa.string()),
    pa.field("media_urls", pa.list_(pa.string())),
    pa.field("local_media_paths", pa.list_(pa.string())),
    pa.field("tags", pa.list_(pa.string())),
    pa.field("vector", pa.list_(pa.float32(), 1024)),
    pa.field("raw_json", pa.string()),
])


class LanceDBStore:
    def __init__(self, db_path: Path | str | None = None):
        if db_path is None:
            db_path = Path(os.getenv("DATA_DIR", "data")) / "lancedb"
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        self.db = lancedb.connect(str(self.db_path))
        self.table_name = "likes"
        self._stats_cache: dict[str, int] | None = None
        self._stats_time: float = 0.0
        self._tags_cache: list[dict[str, Any]] | None = None
        self._tags_time: float = 0.0
        self._ensure_table()

    def _ensure_table(self):
        try:
            self.table = self.db.open_table(self.table_name)
        except Exception:
            self.table = self.db.create_table(
                self.table_name, schema=SCHEMA, mode="create"
            )

    def _invalidate_cache(self):
        self._stats_cache = None
        self._tags_cache = None

    def _ensure_fts_index(self):
        if len(self.table) > 0:
            try:
                self.table.create_fts_index("text", replace=True)
            except Exception:
                pass

    def upsert_tweets(self, tweets: Sequence[dict[str, Any]]) -> int:
        if not tweets:
            return 0
        cleaned = []
        for t in tweets:
            vec = t.get("vector") or [0.0] * 1024
            if len(vec) != 1024:
                vec = [0.0] * 1024
            cleaned.append({
                "id": str(t.get("id") or t.get("tweet_id", "")),
                "tweet_id": str(t.get("tweet_id") or t.get("id", "")),
                "author_name": str(t.get("author_name", "")),
                "author_handle": str(t.get("author_handle", "")),
                "text": str(t.get("text", "")),
                "created_at": str(t.get("created_at", "")),
                "liked_at": str(t.get("liked_at", "")),
                "url": str(t.get("url", "")),
                "media_urls": list(t.get("media_urls", [])),
                "local_media_paths": list(t.get("local_media_paths", [])),
                "tags": list(t.get("tags", [])),
                "vector": vec,
                "raw_json": str(t.get("raw_json", "")),
            })
        self.table.merge_insert("tweet_id").when_matched_update_all().when_not_matched_insert_all().execute(cleaned)
        self._invalidate_cache()
        return len(cleaned)

    def delete_tweets(self, tweet_ids: list[str]) -> int:
        if not tweet_ids:
            return 0
        clean_ids = [str(tid).replace("'", "''") for tid in tweet_ids if tid]
        if not clean_ids:
            return 0
        if len(clean_ids) == 1:
            where_expr = f"tweet_id = '{clean_ids[0]}'"
        else:
            in_clause = ", ".join(f"'{cid}'" for cid in clean_ids)
            where_expr = f"tweet_id IN ({in_clause})"
        try:
            self.table.delete(where_expr)
            self._invalidate_cache()
            return len(clean_ids)
        except Exception:
            deleted = 0
            for cid in clean_ids:
                try:
                    self.table.delete(f"tweet_id = '{cid}'")
                    deleted += 1
                except Exception:
                    pass
            self._invalidate_cache()
            return deleted

    def get_all_tweets(self, limit: int = 1000) -> list[dict[str, Any]]:
        return self.table.search().limit(limit).to_list()

    def search_hybrid(
        self,
        query: str = "",
        query_vector: list[float] | None = None,
        tag: str | None = None,
        sort_by: str = "newest",
        offset: int = 0,
        limit: int = 24,
    ) -> list[dict[str, Any]]:
        where_expr = f"array_contains(tags, '{tag}')" if tag else None

        def extract_favorite_count(t: dict[str, Any]) -> int:
            if "favorite_count" in t and t["favorite_count"] is not None:
                try:
                    return int(t["favorite_count"])
                except Exception:
                    pass
            raw_str = t.get("raw_json", "")
            if raw_str and raw_str != "{}":
                try:
                    raw = json.loads(raw_str)
                    fc = raw.get("favorite_count") or raw.get("legacy", {}).get("favorite_count") or raw.get("like_count")
                    if fc is not None:
                        return int(fc)
                except Exception:
                    pass
            return 0

        if query_vector and any(v != 0.0 for v in query_vector):
            q = self.table.search(query_vector)
            if where_expr:
                q = q.where(where_expr)
            res = q.offset(offset).limit(limit).to_list()
            for r in res:
                r["favorite_count"] = extract_favorite_count(r)
            return res

        if query and not where_expr:
            try:
                q = self.table.search(query, query_type="fts")
                results = q.offset(offset).limit(limit).to_list()
                if results:
                    for r in results:
                        r["favorite_count"] = extract_favorite_count(r)
                    return results
            except Exception:
                pass

        q = self.table.search()
        if where_expr:
            q = q.where(where_expr)

        items = q.to_list()
        if query:
            q_lower = query.lower()
            items = [t for t in items if q_lower in t.get("text", "").lower() or q_lower in t.get("author_handle", "").lower() or q_lower in t.get("author_name", "").lower()]

        def get_tweet_numeric_id(x: dict[str, Any]) -> int:
            tid = str(x.get("tweet_id") or x.get("id") or "0")
            digits = "".join(c for c in tid if c.isdigit())
            return int(digits) if digits else 0

        if sort_by in ("newest_liked", "newest"):
            items.reverse()
        elif sort_by in ("oldest_liked", "oldest"):
            # Earliest liked first (natural insertion order)
            pass
        elif sort_by == "newest_tweeted":
            items.sort(key=get_tweet_numeric_id, reverse=True)
        elif sort_by == "oldest_tweeted":
            items.sort(key=get_tweet_numeric_id)
        elif sort_by == "most_liked":
            items.sort(key=extract_favorite_count, reverse=True)
        elif sort_by == "media_only":
            items = [t for t in items if (t.get("media_urls") and len(t["media_urls"]) > 0) or (t.get("local_media_paths") and len(t["local_media_paths"]) > 0)]
            items.reverse()
        else:
            items.reverse()

        paged = items[offset : offset + limit]
        for p in paged:
            p["favorite_count"] = extract_favorite_count(p)
        return paged

    def get_stats(self, force: bool = False) -> dict[str, int]:
        now = time.time()
        if not force and self._stats_cache and (now - self._stats_time < 30.0):
            return self._stats_cache

        total = len(self.table) if hasattr(self, "table") else 0
        tag_set: set[str] = set()
        media_count = 0
        vectors_count = 0
        if total > 0:
            try:
                table_arrow = self.table.to_arrow()
                tags_col = table_arrow["tags"].to_pylist()
                for tags in tags_col:
                    if isinstance(tags, list):
                        tag_set.update(tags)
                media_col = table_arrow["local_media_paths"].to_pylist()
                for paths in media_col:
                    if isinstance(paths, list):
                        media_count += len(paths)
                vectors_col = table_arrow["vector"].to_pylist()
                for vec in vectors_col:
                    if vec and any(v != 0.0 for v in vec):
                        vectors_count += 1
            except Exception:
                pass
        res = {
            "total_likes": total,
            "indexed_vectors": vectors_count,
            "archived_media_files": media_count,
            "tags_count": len(tag_set),
        }
        self._stats_cache = res
        self._stats_time = now
        return res

    def get_all_tags(self, force: bool = False) -> list[dict[str, Any]]:
        now = time.time()
        if not force and self._tags_cache and (now - self._tags_time < 30.0):
            return self._tags_cache

        if len(self.table) == 0:
            return []
        table_arrow = self.table.to_arrow()
        tags_col = table_arrow["tags"].to_pylist()
        counts: dict[str, int] = {}
        for tags in tags_col:
            if isinstance(tags, list):
                for t in tags:
                    if t:
                        counts[t] = counts.get(t, 0) + 1
        res = [{"tag": k, "count": v} for k, v in sorted(counts.items(), key=lambda x: -x[1])]
        self._tags_cache = res
        self._tags_time = now
        return res
