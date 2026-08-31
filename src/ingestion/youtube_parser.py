import re
import json
import csv
import io
from typing import Any


def extract_video_id(url_or_id: str) -> str:
    if not url_or_id:
        return ""
    url_or_id = url_or_id.strip()
    
    if "http://" in url_or_id or "https://" in url_or_id or "youtube." in url_or_id or "youtu.be" in url_or_id:
        pattern = r"(?:v=|\/v\/|\/embed\/|\/shorts\/|youtu\.be\/|\/watch\?v=|\&v=)([A-Za-z0-9_-]{11})"
        match = re.search(pattern, url_or_id)
        return match.group(1) if match else ""

    if len(url_or_id) == 11 and re.match(r"^[A-Za-z0-9_-]{11}$", url_or_id) and not url_or_id.startswith("invalid"):
        return url_or_id
    
    return ""


def get_youtube_thumbnails(video_id: str) -> list[str]:
    if not video_id:
        return []
    return [
        f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg",
        f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
    ]


def parse_youtube_api_item(item: dict[str, Any]) -> dict[str, Any] | None:
    snippet = item.get("snippet", {})
    resource_id = snippet.get("resourceId", {})
    video_id = resource_id.get("videoId") or item.get("id") or item.get("videoId")
    if not video_id:
        return None

    video_id = extract_video_id(str(video_id))
    if not video_id:
        return None

    title = snippet.get("title", "").strip()
    if title == "Private video" or title == "Deleted video":
        return None

    description = snippet.get("description", "").strip()
    channel_title = snippet.get("videoOwnerChannelTitle") or snippet.get("channelTitle") or "YouTube Channel"
    channel_handle = "@" + re.sub(r"[^\w\d_]", "", channel_title.lower().replace(" ", "_"))
    
    published_at = snippet.get("publishedAt") or snippet.get("published_at") or ""
    liked_at = item.get("liked_at") or snippet.get("publishedAt") or ""

    thumbs = snippet.get("thumbnails", {})
    custom_thumb = (
        thumbs.get("maxres", {}).get("url")
        or thumbs.get("standard", {}).get("url")
        or thumbs.get("high", {}).get("url")
        or thumbs.get("medium", {}).get("url")
        or thumbs.get("default", {}).get("url")
    )
    media_urls = [custom_thumb] if custom_thumb else get_youtube_thumbnails(video_id)

    full_text = f"▶️ {title}\n\n{description[:500]}".strip() if description else f"▶️ {title}".strip()

    # Extract tags from title / description or snippet tags
    extracted_tags = snippet.get("tags") or []
    if not extracted_tags:
        lower_t = (title + " " + description).lower()
        candidates = ["AI", "Tech", "Coding", "Science", "Music", "Podcast", "Design", "Tutorial", "Gaming", "News"]
        for c in candidates:
            if c.lower() in lower_t:
                extracted_tags.append(c)

    return {
        "id": f"yt_{video_id}",
        "tweet_id": f"yt_{video_id}",
        "author_name": channel_title,
        "author_handle": channel_handle,
        "text": full_text,
        "created_at": published_at,
        "liked_at": liked_at,
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "media_urls": media_urls,
        "local_media_paths": [f"data/media/youtube/{video_id}.jpg"],
        "tags": list(set(extracted_tags)),
        "vector": [0.0] * 1024,
        "raw_json": json.dumps(item),
        "source": "youtube",
    }


def parse_youtube_takeout_content(content: str | bytes) -> list[dict[str, Any]]:
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")

    results = []
    content_str = content.strip()

    # Try JSON
    if content_str.startswith("[") or content_str.startswith("{"):
        try:
            data = json.loads(content_str)
            items = data if isinstance(data, list) else data.get("items", [data])
            for entry in items:
                # 1. Standard YouTube API item
                if "snippet" in entry:
                    parsed = parse_youtube_api_item(entry)
                    if parsed:
                        results.append(parsed)
                    continue

                # 2. Google Takeout watch-history / liked-videos item
                title_url = entry.get("titleUrl") or entry.get("url") or ""
                raw_title = entry.get("title", "")
                if raw_title.startswith("Watched "):
                    raw_title = raw_title[len("Watched "):]

                video_id = extract_video_id(title_url) or extract_video_id(entry.get("id", ""))
                if not video_id:
                    continue

                subs = entry.get("subtitles", [])
                channel_name = subs[0].get("name", "YouTube Channel") if subs else entry.get("channelTitle", "YouTube Channel")
                channel_handle = "@" + re.sub(r"[^\w\d_]", "", channel_name.lower().replace(" ", "_"))
                
                time_str = entry.get("time") or entry.get("timestamp") or ""
                desc = entry.get("description", "")
                full_text = f"▶️ {raw_title}\n\n{desc[:500]}".strip() if desc else f"▶️ {raw_title}".strip()

                parsed = {
                    "id": f"yt_{video_id}",
                    "tweet_id": f"yt_{video_id}",
                    "author_name": channel_name,
                    "author_handle": channel_handle,
                    "text": full_text,
                    "created_at": time_str,
                    "liked_at": time_str,
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "media_urls": get_youtube_thumbnails(video_id),
                    "local_media_paths": [f"data/media/youtube/{video_id}.jpg"],
                    "tags": ["YouTube"],
                    "vector": [0.0] * 1024,
                    "raw_json": json.dumps(entry),
                    "source": "youtube",
                }
                results.append(parsed)
            return results
        except Exception:
            pass

    # Try CSV
    try:
        reader = csv.DictReader(io.StringIO(content_str))
        for row in reader:
            vid = row.get("Video ID") or row.get("videoId") or row.get("id") or extract_video_id(row.get("URL") or row.get("url", ""))
            if not vid:
                continue
            vid = extract_video_id(vid)
            if not vid:
                continue

            title = row.get("Video Title") or row.get("title") or "YouTube Video"
            channel = row.get("Channel Title") or row.get("channel") or "YouTube Channel"
            desc = row.get("Description") or row.get("description") or ""
            pub_date = row.get("Published At") or row.get("time") or ""

            results.append({
                "id": f"yt_{vid}",
                "tweet_id": f"yt_{vid}",
                "author_name": channel,
                "author_handle": "@" + re.sub(r"[^\w\d_]", "", channel.lower().replace(" ", "_")),
                "text": f"▶️ {title}\n\n{desc[:500]}".strip() if desc else f"▶️ {title}".strip(),
                "created_at": pub_date,
                "liked_at": pub_date,
                "url": f"https://www.youtube.com/watch?v={vid}",
                "media_urls": get_youtube_thumbnails(vid),
                "local_media_paths": [f"data/media/youtube/{vid}.jpg"],
                "tags": ["YouTube"],
                "vector": [0.0] * 1024,
                "raw_json": json.dumps(row),
                "source": "youtube",
            })
    except Exception:
        pass

    return results
