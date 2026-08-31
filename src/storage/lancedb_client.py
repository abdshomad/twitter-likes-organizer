import os
import json
import time
import email.utils
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence
import lancedb
import pyarrow as pa


def parse_date_to_timestamp(date_str: str) -> float:
    if not date_str:
        return 0.0
    try:
        return datetime.fromisoformat(date_str).timestamp()
    except Exception:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(date_str)
        return dt.timestamp()
    except Exception:
        pass
    return 0.0

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
    pa.field("source", pa.string()),
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
        self._tags_cache: dict[str, list[dict[str, Any]]] = {}
        self._tags_time: float = 0.0
        self._ensure_table()
        self.meili = None
        try:
            from src.storage.meilisearch_client import MeiliSearchStore
            self.meili = MeiliSearchStore()
        except Exception:
            pass

    def _ensure_table(self):
        try:
            self.table = self.db.open_table(self.table_name)
            table_schema = self.table.schema
            if "source" not in table_schema.names:
                try:
                    self.table.add_columns({"source": "'like'"})
                except Exception:
                    pass
        except Exception:
            self.table = self.db.create_table(
                self.table_name, schema=SCHEMA, mode="create"
            )

    def _invalidate_cache(self):
        self._stats_cache = None
        self._tags_cache.clear()

    def _ensure_fts_index(self):
        if len(self.table) > 0:
            try:
                self.table.create_fts_index("text", replace=True)
            except Exception:
                pass

    def upsert_tweets(self, tweets: Sequence[dict[str, Any]], default_source: str = "like") -> int:
        if not tweets:
            return 0

        incoming_ids = [str(t.get("tweet_id") or t.get("id", "")) for t in tweets if t.get("tweet_id") or t.get("id")]
        existing_sources: dict[str, str] = {}
        if incoming_ids and len(self.table) > 0:
            try:
                clean_check_ids = [cid.replace("'", "''") for cid in incoming_ids[:500] if cid]
                if clean_check_ids:
                    id_filter = ", ".join(f"'{cid}'" for cid in clean_check_ids)
                    res = self.table.search().where(f"tweet_id IN ({id_filter})").limit(len(clean_check_ids)).to_list()
                    for r in res:
                        existing_sources[r.get("tweet_id", "")] = r.get("source") or "like"
            except Exception:
                pass

        cleaned = []
        for t in tweets:
            tid = str(t.get("tweet_id") or t.get("id", ""))
            vec = t.get("vector") or [0.0] * 1024
            if len(vec) != 1024:
                vec = [0.0] * 1024
            
            raw_source = str(t.get("source") or default_source or "like").lower()
            existing_src = existing_sources.get(tid)
            if existing_src:
                if existing_src == "both" or raw_source == "both":
                    final_source = "both"
                elif (existing_src == "like" and raw_source == "bookmark") or (existing_src == "bookmark" and raw_source == "like"):
                    final_source = "both"
                else:
                    final_source = raw_source
            else:
                final_source = raw_source

            cleaned.append({
                "id": str(t.get("id") or t.get("tweet_id", "")),
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
                "vector": vec,
                "raw_json": str(t.get("raw_json", "")),
                "source": final_source,
            })
        self.table.merge_insert("tweet_id").when_matched_update_all().when_not_matched_insert_all().execute(cleaned)
        self._invalidate_cache()
        if self.meili and self.meili.is_healthy():
            try:
                self.meili.upsert_tweets(cleaned, default_source=default_source)
            except Exception:
                pass
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
            if self.meili and self.meili.is_healthy():
                try:
                    self.meili.delete_tweets(clean_ids)
                except Exception:
                    pass
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
            if self.meili and self.meili.is_healthy():
                try:
                    self.meili.delete_tweets(clean_ids)
                except Exception:
                    pass
            return deleted

    def unbookmark_tweets(self, tweet_ids: list[str]) -> int:
        if not tweet_ids or len(self.table) == 0:
            return 0
        clean_ids = [str(tid).replace("'", "''") for tid in tweet_ids if tid]
        if not clean_ids:
            return 0
        try:
            in_clause = ", ".join(f"'{cid}'" for cid in clean_ids)
            matches = self.table.search().where(f"tweet_id IN ({in_clause})").limit(len(clean_ids)).to_list()
            delete_ids = []
            update_tweets = []
            for m in matches:
                src = m.get("source", "bookmark")
                if src == "both":
                    m["source"] = "like"
                    update_tweets.append(m)
                else:
                    delete_ids.append(m.get("tweet_id"))
            
            if delete_ids:
                self.delete_tweets(delete_ids)
            if update_tweets:
                self.table.merge_insert("tweet_id").when_matched_update_all().execute(update_tweets)
            self._invalidate_cache()
            return len(matches)
        except Exception:
            return self.delete_tweets(tweet_ids)

    def get_all_tweets(self, limit: int = 1000, source: str = "all") -> list[dict[str, Any]]:
        items = self.table.search().limit(limit).to_list()
        if source in ("like", "likes"):
            return [t for t in items if (t.get("source") or "like") in ("like", "both")]
        elif source in ("bookmark", "bookmarks"):
            return [t for t in items if (t.get("source") or "like") in ("bookmark", "both")]
        elif source in ("youtube", "yt"):
            return [t for t in items if (t.get("source") or "") == "youtube"]
        return items

    def search_hybrid(
        self,
        query: str = "",
        query_vector: list[float] | None = None,
        tag: str | None = None,
        source: str = "all",
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

        def filter_source(items_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
            if source in ("like", "likes"):
                return [t for t in items_list if (t.get("source") or "like") in ("like", "both")]
            elif source in ("bookmark", "bookmarks"):
                return [t for t in items_list if (t.get("source") or "like") in ("bookmark", "both")]
            elif source in ("youtube", "yt"):
                return [t for t in items_list if (t.get("source") or "") == "youtube"]
            return items_list

        if query_vector and any(v != 0.0 for v in query_vector):
            q = self.table.search(query_vector)
            if where_expr:
                q = q.where(where_expr)
            res = q.limit(limit + offset + 200).to_list()
            res = filter_source(res)
            paged = res[offset : offset + limit]
            for r in paged:
                r["favorite_count"] = extract_favorite_count(r)
                r["source"] = r.get("source") or "like"
            return paged

        if query and not where_expr:
            try:
                q = self.table.search(query, query_type="fts")
                results = q.limit(limit + offset + 200).to_list()
                results = filter_source(results)
                if results:
                    paged = results[offset : offset + limit]
                    for r in paged:
                        r["favorite_count"] = extract_favorite_count(r)
                        r["source"] = r.get("source") or "like"
                    return paged
            except Exception:
                pass

        q = self.table.search()
        if where_expr:
            q = q.where(where_expr)

        items = q.to_list()
        items = filter_source(items)

        for item in items:
            item["favorite_count"] = extract_favorite_count(item)
            item["source"] = item.get("source") or "like"

        if sort_by == "most_liked":
            items.sort(key=lambda x: x["favorite_count"], reverse=True)
        elif sort_by == "oldest_liked":
            items.sort(key=lambda x: parse_date_to_timestamp(x.get("liked_at") or x.get("created_at") or ""))
        elif sort_by == "newest_tweeted":
            items.sort(key=lambda x: parse_date_to_timestamp(x.get("created_at") or ""), reverse=True)
        elif sort_by == "oldest_tweeted":
            items.sort(key=lambda x: parse_date_to_timestamp(x.get("created_at") or ""))
        elif sort_by == "media_only":
            items = [x for x in items if (x.get("media_urls") and len(x["media_urls"]) > 0) or (x.get("local_media_paths") and len(x["local_media_paths"]) > 0)]
            items.sort(key=lambda x: parse_date_to_timestamp(x.get("liked_at") or x.get("created_at") or ""), reverse=True)
        else:  # newest_liked (default)
            items.sort(key=lambda x: parse_date_to_timestamp(x.get("liked_at") or x.get("created_at") or ""), reverse=True)

        return items[offset : offset + limit]

    def get_stats(self) -> dict[str, int]:
        now = time.time()
        if self._stats_cache is not None and (now - self._stats_time < 10.0):
            return self._stats_cache

        total = len(self.table)
        tag_set = set()
        media_count = 0
        vectors_count = 0
        total_likes = 0
        total_bookmarks = 0
        total_youtube = 0

        if total > 0:
            try:
                table_arrow = self.table.to_arrow()
                if "source" in table_arrow.column_names:
                    source_col = table_arrow["source"].to_pylist()
                    for s in source_col:
                        src = str(s or "like").lower()
                        if src in ("like", "both"):
                            total_likes += 1
                        if src in ("bookmark", "both"):
                            total_bookmarks += 1
                        if src in ("youtube", "yt"):
                            total_youtube += 1
                else:
                    total_likes = total

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
            "total_items": total,
            "total_likes": total_likes,
            "total_bookmarks": total_bookmarks,
            "total_youtube": total_youtube,
            "indexed_vectors": vectors_count,
            "archived_media_files": media_count,
            "tags_count": len(tag_set),
        }
        self._stats_cache = res
        self._stats_time = now
        return res

    def get_all_tags(self, force: bool = False, source: str = "all") -> list[dict[str, Any]]:
        now = time.time()
        cache_key = source
        if not force and cache_key in self._tags_cache and (now - self._tags_time < 30.0):
            return self._tags_cache[cache_key]

        if len(self.table) == 0:
            return []
        table_arrow = self.table.to_arrow()
        tags_col = table_arrow["tags"].to_pylist()
        source_col = table_arrow["source"].to_pylist() if "source" in table_arrow.column_names else ["like"] * len(tags_col)
        counts: dict[str, int] = {}
        for tags, src in zip(tags_col, source_col):
            item_src = str(src or "like").lower()
            if source in ("like", "likes") and item_src not in ("like", "both"):
                continue
            if source in ("bookmark", "bookmarks") and item_src not in ("bookmark", "both"):
                continue
            if source in ("youtube", "yt") and item_src != "youtube":
                continue
            if isinstance(tags, list):
                for t in tags:
                    if t:
                        counts[t] = counts.get(t, 0) + 1
        res = [{"tag": k, "count": v} for k, v in sorted(counts.items(), key=lambda x: -x[1])]
        self._tags_cache[cache_key] = res
        self._tags_time = now
        return res

    def get_top_authors(
        self,
        source: str = "all",
        sort_by: str = "count",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if len(self.table) == 0:
            return []

        table_arrow = self.table.to_arrow()
        handles = table_arrow["author_handle"].to_pylist() if "author_handle" in table_arrow.column_names else [""] * len(table_arrow)
        names = table_arrow["author_name"].to_pylist() if "author_name" in table_arrow.column_names else [""] * len(table_arrow)
        tags_col = table_arrow["tags"].to_pylist() if "tags" in table_arrow.column_names else [[]] * len(table_arrow)
        sources = table_arrow["source"].to_pylist() if "source" in table_arrow.column_names else ["like"] * len(table_arrow)
        dates = table_arrow["liked_at"].to_pylist() if "liked_at" in table_arrow.column_names else [""] * len(table_arrow)

        total_matched = 0
        author_data: dict[str, dict[str, Any]] = {}

        for handle, name, item_tags, src, liked_date in zip(handles, names, tags_col, sources, dates):
            item_src = str(src or "like").lower()
            if source in ("like", "likes") and item_src not in ("like", "both"):
                continue
            if source in ("bookmark", "bookmarks") and item_src not in ("bookmark", "both"):
                continue
            if source in ("youtube", "yt") and item_src != "youtube":
                continue

            total_matched += 1
            clean_handle = (handle or "").strip().lstrip("@")
            clean_name = (name or "").strip()
            key = clean_handle.lower() if clean_handle else clean_name.lower()
            if not key:
                key = "unknown"

            if key not in author_data:
                author_data[key] = {
                    "author_handle": f"@{clean_handle}" if clean_handle else (f"@{clean_name}" if clean_name else "@unknown"),
                    "author_name": clean_name or clean_handle or "Unknown",
                    "count": 0,
                    "latest_date": str(liked_date or ""),
                    "tags_map": {},
                    "source": item_src,
                }

            entry = author_data[key]
            entry["count"] += 1
            if str(liked_date or "") > entry["latest_date"]:
                entry["latest_date"] = str(liked_date)
            if isinstance(item_tags, list):
                for t in item_tags:
                    if t:
                        entry["tags_map"][t] = entry["tags_map"].get(t, 0) + 1

        results = []
        for entry in author_data.values():
            sorted_tags = [t for t, _ in sorted(entry["tags_map"].items(), key=lambda x: -x[1])[:3]]
            pct = round((entry["count"] / total_matched * 100.0), 1) if total_matched > 0 else 0.0
            results.append({
                "author_handle": entry["author_handle"],
                "author_name": entry["author_name"],
                "count": entry["count"],
                "percentage": pct,
                "top_tags": sorted_tags,
                "latest_date": entry["latest_date"],
                "source": entry["source"],
            })

        if sort_by == "name":
            results.sort(key=lambda x: x["author_name"].lower())
        elif sort_by == "recent":
            results.sort(key=lambda x: x["latest_date"], reverse=True)
        else:  # count
            results.sort(key=lambda x: x["count"], reverse=True)

        for rank, r in enumerate(results, start=1):
            r["rank"] = rank

        return results[:limit]
