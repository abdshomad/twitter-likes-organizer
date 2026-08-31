import json
import re
from pathlib import Path
from typing import Any, Generator


def parse_archive_content(raw_text: str, default_source: str = "like") -> list[dict[str, Any]]:
    # Auto-detect source if present in header/wrapper
    detected_source = default_source
    if "window.YTD.bookmark" in raw_text or '"bookmark"' in raw_text or "account-bookmark" in raw_text:
        detected_source = "bookmark"
    elif "window.YTD.like" in raw_text or '"like"' in raw_text:
        detected_source = "like"

    # Strip leading variable assignment (e.g. window.YTD.like.part0 = ...)
    cleaned = raw_text.strip()
    if "=" in cleaned:
        cleaned = cleaned.split("=", 1)[1].strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        # Fallback to extracting array between [ and ]
        start = raw_text.find("[")
        end = raw_text.rfind("]")
        if start != -1 and end != -1:
            data = json.loads(raw_text[start:end+1])
        else:
            raise ValueError("Unable to parse valid JSON array from archive file.")

    items: list[dict[str, Any]] = []
    if isinstance(data, list):
        for entry in data:
            item_obj = entry.get("like") or entry.get("bookmark") or entry.get("tweet") or entry
            tweet_id = str(item_obj.get("tweetId", item_obj.get("id", item_obj.get("rest_id", ""))))
            expanded_url = str(item_obj.get("expandedUrl", item_obj.get("url", f"https://x.com/i/web/status/{tweet_id}")))
            full_text = str(item_obj.get("fullText", item_obj.get("text", "")))
            
            if tweet_id:
                items.append({
                    "id": tweet_id,
                    "tweet_id": tweet_id,
                    "author_name": str(item_obj.get("author_name", "")),
                    "author_handle": str(item_obj.get("author_handle", "")),
                    "text": full_text,
                    "created_at": str(item_obj.get("createdAt", item_obj.get("created_at", ""))),
                    "liked_at": "",
                    "url": expanded_url,
                    "media_urls": list(item_obj.get("media_urls", [])),
                    "local_media_paths": [],
                    "tags": [],
                    "raw_json": json.dumps(item_obj),
                    "source": detected_source,
                })
    return items


def parse_like_js_content(raw_text: str) -> list[dict[str, Any]]:
    return parse_archive_content(raw_text, default_source="like")


def parse_bookmarks_js_content(raw_text: str) -> list[dict[str, Any]]:
    return parse_archive_content(raw_text, default_source="bookmark")


def parse_archive_file(file_path: Path | str, default_source: str | None = None) -> list[dict[str, Any]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Archive file not found: {path}")
    raw_text = path.read_text(encoding="utf-8")
    
    src = default_source
    if not src:
        filename = path.name.lower()
        if "bookmark" in filename:
            src = "bookmark"
        else:
            src = "like"
    return parse_archive_content(raw_text, default_source=src)


def stream_archive_batches(
    file_path: Path | str, batch_size: int = 500, default_source: str | None = None
) -> Generator[list[dict[str, Any]], None, None]:
    items = parse_archive_file(file_path, default_source=default_source)
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]
