import os
import re
import json
import logging
from pathlib import Path
from typing import Any, AsyncGenerator
import yt_dlp
from src.ingestion.youtube_parser import extract_video_id, get_youtube_thumbnails

logger = logging.getLogger(__name__)

TRANSCRIPTS_DIR = Path("data/transcripts")
YOUTUBE_MEDIA_DIR = Path("data/media/youtube")


def parse_vtt_text(vtt_content: str) -> list[dict[str, Any]]:
    """Parse WebVTT content into structured timestamp segments."""
    segments = []
    lines = vtt_content.splitlines()
    time_pattern = re.compile(
        r"(?:(\d{1,2}):)?(\d{2}):(\d{2})[.,](\d{3})\s*-->\s*(?:(\d{1,2}):)?(\d{2}):(\d{2})[.,](\d{3})"
    )

    def parse_seconds(h, m, s, ms):
        hours = int(h) if h else 0
        minutes = int(m)
        seconds = int(s)
        milliseconds = int(ms)
        return hours * 3600 + minutes * 60 + seconds + milliseconds / 1000.0

    current_start = 0.0
    current_end = 0.0
    current_text_lines = []
    has_timestamp = False

    for line in lines:
        line_clean = line.strip()
        if not line_clean or line_clean.startswith("WEBVTT") or line_clean.startswith("NOTE"):
            continue

        match = time_pattern.search(line_clean)
        if match:
            if current_text_lines and has_timestamp:
                text = " ".join(current_text_lines).strip()
                text = re.sub(r"<[^>]+>", "", text)  # Strip tags like <c> </c>
                if text:
                    segments.append({"start": round(current_start, 2), "end": round(current_end, 2), "text": text})
                current_text_lines = []

            h1, m1, s1, ms1, h2, m2, s2, ms2 = match.groups()
            current_start = parse_seconds(h1, m1, s1, ms1)
            current_end = parse_seconds(h2, m2, s2, ms2)
            has_timestamp = True
        else:
            if has_timestamp and not line_clean.isdigit():  # Skip cue numbers
                clean_t = re.sub(r"<[^>]+>", "", line_clean).strip()
                if clean_t and clean_t not in current_text_lines:
                    current_text_lines.append(clean_t)

    if current_text_lines and has_timestamp:
        text = " ".join(current_text_lines).strip()
        text = re.sub(r"<[^>]+>", "", text)
        if text:
            segments.append({"start": round(current_start, 2), "end": round(current_end, 2), "text": text})

    return segments


def parse_json3_subtitles(json3_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse YouTube JSON3 subtitle format into timestamp segments."""
    segments = []
    events = json3_data.get("events", [])
    for ev in events:
        start_ms = ev.get("tStartMs", 0)
        dur_ms = ev.get("dDurationMs", 0)
        segs = ev.get("segs", [])
        if not segs:
            continue
        text = "".join(s.get("utf8", "") for s in segs).strip()
        if text and text != "\n":
            start_sec = round(start_ms / 1000.0, 2)
            end_sec = round((start_ms + dur_ms) / 1000.0, 2)
            segments.append({"start": start_sec, "end": end_sec, "text": text})
    return segments


class YtDlpExtractor:
    def __init__(self, cookies_file: str | None = None):
        self.cookies_file = cookies_file
        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        YOUTUBE_MEDIA_DIR.mkdir(parents=True, exist_ok=True)

    def _get_ydl_opts(self, extract_subtitles: bool = True) -> dict[str, Any]:
        opts: dict[str, Any] = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
            "ignoreerrors": True,
        }
        if extract_subtitles:
            opts.update({
                "writesubtitles": True,
                "writeautomaticsub": True,
                "subtitleslangs": ["en.*", "en", "id", "all"],
                "subtitlesformat": "json3/vtt/srt/best",
            })
        if self.cookies_file and os.path.exists(self.cookies_file):
            opts["cookiefile"] = self.cookies_file
        return opts

    def extract_video(self, url_or_id: str, fetch_subtitles: bool = True) -> dict[str, Any] | None:
        """Extract metadata and subtitles for a single video with zero video downloads."""
        vid = extract_video_id(url_or_id)
        target_url = f"https://www.youtube.com/watch?v={vid}" if vid else url_or_id

        opts = self._get_ydl_opts(extract_subtitles=fetch_subtitles)
        with yt_dlp.YoutubeDL(opts) as ydl:
            try:
                info = ydl.extract_info(target_url, download=False)
                if not info:
                    return None
                return self._process_video_info(info)
            except Exception as e:
                logger.error(f"[yt-dlp error] {e}")
                return None

    def extract_playlist(self, playlist_url: str, max_items: int = 100) -> list[dict[str, Any]]:
        """Extract metadata and subtitles for items in a playlist."""
        opts = self._get_ydl_opts(extract_subtitles=True)
        opts["playlistend"] = max_items
        results = []

        with yt_dlp.YoutubeDL(opts) as ydl:
            try:
                info = ydl.extract_info(playlist_url, download=False)
                if not info:
                    return []
                
                entries = info.get("entries", [])
                for entry in entries:
                    if entry:
                        processed = self._process_video_info(entry)
                        if processed:
                            results.append(processed)
            except Exception as e:
                logger.error(f"[yt-dlp playlist error] {e}")

        return results

    def _process_video_info(self, info: dict[str, Any]) -> dict[str, Any] | None:
        video_id = info.get("id") or extract_video_id(info.get("webpage_url", ""))
        if not video_id:
            return None

        title = info.get("title") or "YouTube Video"
        description = info.get("description") or ""
        channel_name = info.get("uploader") or info.get("channel") or "YouTube Channel"
        uploader_id = info.get("uploader_id") or channel_name
        channel_handle = "@" + re.sub(r"[^\w\d_]", "", uploader_id.lower().replace(" ", "_"))

        upload_date = info.get("upload_date") or ""  # YYYYMMDD
        if len(upload_date) == 8:
            formatted_date = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}"
        else:
            formatted_date = str(upload_date)

        duration = info.get("duration") or 0
        view_count = info.get("view_count") or 0
        like_count = info.get("like_count") or 0

        # Subtitles / Transcript Extraction
        segments = []
        full_transcript = ""
        subtitles_dict = info.get("subtitles") or {}
        auto_subtitles_dict = info.get("automatic_captions") or {}

        # Look for English / primary subtitles
        chosen_subs = None
        for lang_key in ["en", "en-US", "en-GB", "id", "en-orig"]:
            if lang_key in subtitles_dict:
                chosen_subs = subtitles_dict[lang_key]
                break
        if not chosen_subs:
            for lang_key in ["en", "en-US", "en-GB", "id"]:
                if lang_key in auto_subtitles_dict:
                    chosen_subs = auto_subtitles_dict[lang_key]
                    break
        if not chosen_subs and subtitles_dict:
            chosen_subs = list(subtitles_dict.values())[0]
        if not chosen_subs and auto_subtitles_dict:
            chosen_subs = list(auto_subtitles_dict.values())[0]

        if chosen_subs:
            import urllib.request
            # Prefer json3, then vtt, then srt
            sub_url = None
            sub_ext = None
            for fmt in chosen_subs:
                ext = fmt.get("ext", "")
                if ext == "json3":
                    sub_url = fmt.get("url")
                    sub_ext = "json3"
                    break
                elif ext == "vtt" and not sub_url:
                    sub_url = fmt.get("url")
                    sub_ext = "vtt"

            if not sub_url and chosen_subs:
                sub_url = chosen_subs[0].get("url")
                sub_ext = chosen_subs[0].get("ext")

            if sub_url:
                try:
                    req = urllib.request.Request(sub_url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        sub_content = resp.read().decode("utf-8", errors="replace")
                        if sub_ext == "json3":
                            try:
                                json_sub = json.loads(sub_content)
                                segments = parse_json3_subtitles(json_sub)
                            except Exception:
                                segments = parse_vtt_text(sub_content)
                        else:
                            segments = parse_vtt_text(sub_content)
                except Exception as e:
                    logger.debug(f"[yt-dlp sub fetch failed] {e}")

        # Save transcript locally
        transcript_json_path = TRANSCRIPTS_DIR / f"{video_id}.json"
        transcript_txt_path = TRANSCRIPTS_DIR / f"{video_id}.txt"

        if segments:
            full_transcript = " ".join(s["text"] for s in segments)
            with open(transcript_json_path, "w", encoding="utf-8") as f:
                json.dump(segments, f, ensure_ascii=False, indent=2)
            with open(transcript_txt_path, "w", encoding="utf-8") as f:
                f.write(full_transcript)

        # Build full searchable text combining title, description, and transcript
        text_parts = [f"▶️ {title}"]
        if description:
            text_parts.append(description[:400])
        if full_transcript:
            text_parts.append(f"\n[Spoken Transcript]\n{full_transcript[:1200]}")
        combined_text = "\n\n".join(text_parts).strip()

        # Tags
        tags = info.get("tags") or []
        categories = info.get("categories") or []
        all_tags = list(set(["YouTube"] + tags + categories))

        # Thumbnails
        custom_thumb = info.get("thumbnail")
        media_urls = [custom_thumb] if custom_thumb else get_youtube_thumbnails(video_id)

        raw_meta = {
            "id": video_id,
            "title": title,
            "duration": duration,
            "view_count": view_count,
            "like_count": like_count,
            "upload_date": formatted_date,
            "has_transcript": bool(segments),
            "transcript_segments_count": len(segments),
        }

        return {
            "id": f"yt_{video_id}",
            "tweet_id": f"yt_{video_id}",
            "author_name": channel_name,
            "author_handle": channel_handle,
            "text": combined_text,
            "created_at": formatted_date,
            "liked_at": formatted_date,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "media_urls": media_urls,
            "local_media_paths": [f"data/media/youtube/{video_id}.jpg"],
            "tags": all_tags[:8],
            "vector": [0.0] * 1024,
            "raw_json": json.dumps(raw_meta),
            "source": "youtube",
        }


def get_saved_transcript(video_id: str) -> list[dict[str, Any]] | None:
    """Retrieve saved timestamped transcript segments for a video ID."""
    clean_id = extract_video_id(video_id) or video_id.replace("yt_", "")
    json_path = TRANSCRIPTS_DIR / f"{clean_id}.json"
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


async def stream_batch_fetch_transcripts(store: Any) -> AsyncGenerator[str, None]:
    """Background task to fetch missing transcripts for existing YouTube items."""
    extractor = YtDlpExtractor()
    yield json.dumps({"stage": "init", "message": "Scanning for YouTube items..."}) + "\n"

    all_yt = store.get_all_tweets(limit=10000, source="youtube")
    if not all_yt:
        yield json.dumps({"stage": "complete", "count": 0, "message": "No YouTube items in database."}) + "\n"
        return

    missing = []
    for item in all_yt:
        vid = item.get("tweet_id", "").replace("yt_", "")
        if vid and not (TRANSCRIPTS_DIR / f"{vid}.json").exists():
            missing.append(item)

    total_missing = len(missing)
    yield json.dumps({
        "stage": "start",
        "total": total_missing,
        "message": f"Found {total_missing} YouTube items missing transcripts."
    }) + "\n"

    if total_missing == 0:
        yield json.dumps({"stage": "complete", "count": 0, "message": "All YouTube items already have transcripts!"}) + "\n"
        return

    updated_items = []
    for idx, item in enumerate(missing, 1):
        vid = item.get("tweet_id", "").replace("yt_", "")
        yield json.dumps({
            "stage": "fetching",
            "current": idx,
            "total": total_missing,
            "video_id": vid,
            "title": item.get("author_name", "") + " - " + item.get("text", "")[:40],
        }) + "\n"

        extracted = extractor.extract_video(vid, fetch_subtitles=True)
        if extracted and (TRANSCRIPTS_DIR / f"{vid}.json").exists():
            item["text"] = extracted["text"]
            item["raw_json"] = extracted["raw_json"]
            item["tags"] = list(set(item.get("tags", []) + extracted.get("tags", [])))
            updated_items.append(item)

    if updated_items:
        store.upsert_tweets(updated_items, default_source="youtube")

    yield json.dumps({
        "stage": "complete",
        "count": len(updated_items),
        "total": total_missing,
        "message": f"Successfully fetched and indexed transcripts for {len(updated_items)}/{total_missing} videos!"
    }) + "\n"
