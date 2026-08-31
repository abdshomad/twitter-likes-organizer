from pathlib import Path
from typing import Any
import json
import yaml


def format_tweet_to_markdown(tweet: dict[str, Any]) -> str:
    source = str(tweet.get("source") or "like").lower()
    tweet_id = str(tweet.get("tweet_id") or tweet.get("id", ""))
    clean_id = tweet_id.replace("yt_", "")

    frontmatter = {
        "tweet_id": tweet_id,
        "id": tweet_id,
        "author_name": str(tweet.get("author_name", "")),
        "author_handle": str(tweet.get("author_handle", "")),
        "created_at": str(tweet.get("created_at", "")),
        "liked_at": str(tweet.get("liked_at", "")),
        "url": str(tweet.get("url", "")),
        "tags": list(tweet.get("tags") or []),
        "media_files": list(tweet.get("local_media_paths") or []),
        "source": source,
    }

    yaml_header = yaml.dump(frontmatter, sort_keys=False).strip()
    text = str(tweet.get("text", "")).strip()

    media_sections: list[str] = []
    for m in frontmatter["media_files"]:
        if m.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
            media_sections.append(f"![[ {m} ]]")
        elif m.endswith((".mp4", ".webm", ".mov")):
            media_sections.append(f"![[ {m} ]]")

    media_block = ("\n\n### Media\n" + "\n".join(media_sections)) if media_sections else ""

    # Check for transcript if YouTube
    transcript_block = ""
    if source == "youtube":
        transcript_json_path = Path("data/transcripts") / f"{clean_id}.json"
        if transcript_json_path.exists():
            try:
                with open(transcript_json_path, "r", encoding="utf-8") as f:
                    segments = json.load(f)
                    if segments:
                        ts_lines = []
                        for s in segments:
                            mins = int(s.get("start", 0) // 60)
                            secs = int(s.get("start", 0) % 60)
                            time_tag = f"`[{mins:02d}:{secs:02d}]`"
                            ts_lines.append(f"- {time_tag} {s.get('text', '')}")
                        transcript_block = "\n\n### 📝 Spoken Transcript & Moments\n" + "\n".join(ts_lines)
            except Exception:
                pass

    if source == "youtube":
        header_title = f"# ▶️ YouTube: {frontmatter['author_name'] or 'Video'}"
    else:
        header_title = f"# Tweet by @{frontmatter['author_handle'].lstrip('@') or 'unknown'}"

    return f"""---
{yaml_header}
---

{header_title}

{text}
{media_block}
{transcript_block}

---
*Source: [{frontmatter['url']}]({frontmatter['url']})*
"""


def export_tweets_to_directory(
    tweets: list[dict[str, Any]], export_dir: Path | str
) -> list[Path]:
    out_dir = Path(export_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    exported_files: list[Path] = []

    for t in tweets:
        t_id = str(t.get("tweet_id") or t.get("id", "item"))
        handle = str(t.get("author_handle") or "unknown").replace("@", "")
        filename = f"{handle}_{t_id}.md"
        filepath = out_dir / filename
        content = format_tweet_to_markdown(t)
        filepath.write_text(content, encoding="utf-8")
        exported_files.append(filepath)

    return exported_files
