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

        if query_vector and any(v != 0.0 for v in query_vector):
            q = self.table.search(query_vector)
            if where_expr:
                q = q.where(where_expr)
            return q.offset(offset).limit(limit).to_list()

        if query and not where_expr:
            try:
                q = self.table.search(query, query_type="fts")
                results = q.offset(offset).limit(limit).to_list()
                if results:
                    return results
            except Exception:
                pass

        q = self.table.search()
        if where_expr:
            q = q.where(where_expr)

        if not query and sort_by == "newest":
            return q.offset(offset).limit(limit).to_list()

        items = q.to_list()
        if query:
            q_lower = query.lower()
            items = [t for t in items if q_lower in t.get("text", "").lower() or q_lower in t.get("author_handle", "").lower() or q_lower in t.get("author_name", "").lower()]

        if sort_by == "oldest":
            items.sort(key=lambda x: x.get("created_at") or x.get("id") or "")
        elif sort_by == "author":
            items.sort(key=lambda x: (x.get("author_handle") or "").lower())
        elif sort_by == "media_only":
            items = [t for t in items if t.get("media_urls") or t.get("local_media_paths")]
        else:
            items.reverse()

        return items[offset : offset + limit]

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
