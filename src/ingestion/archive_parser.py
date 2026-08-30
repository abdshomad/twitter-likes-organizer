import json
import re
from pathlib import Path
from typing import Any, Generator


def parse_like_js_content(raw_text: str) -> list[dict[str, Any]]:
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
            like_obj = entry.get("like", entry)
            tweet_id = str(like_obj.get("tweetId", like_obj.get("id", "")))
            expanded_url = str(like_obj.get("expandedUrl", f"https://x.com/i/web/status/{tweet_id}"))
            full_text = str(like_obj.get("fullText", like_obj.get("text", "")))
            
            if tweet_id:
                items.append({
                    "id": tweet_id,
                    "tweet_id": tweet_id,
                    "author_name": "",
                    "author_handle": "",
                    "text": full_text,
                    "created_at": "",
                    "liked_at": "",
                    "url": expanded_url,
                    "media_urls": [],
                    "local_media_paths": [],
                    "tags": [],
                    "raw_json": json.dumps(like_obj),
                })
    return items


def parse_archive_file(file_path: Path | str) -> list[dict[str, Any]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Archive file not found: {path}")
    raw_text = path.read_text(encoding="utf-8")
    return parse_like_js_content(raw_text)


def stream_archive_batches(
    file_path: Path | str, batch_size: int = 500
) -> Generator[list[dict[str, Any]], None, None]:
    items = parse_archive_file(file_path)
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]
