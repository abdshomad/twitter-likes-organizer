import pytest
from httpx import ASGITransport, AsyncClient
from src.server.app import app
from src.ingestion.youtube_parser import extract_video_id, parse_youtube_takeout_content, parse_youtube_api_item
from src.storage.lancedb_client import LanceDBStore
from src.storage.meilisearch_client import MeiliSearchStore


def test_extract_video_id():
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://www.youtube.com/shorts/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("https://www.youtube.com/embed/dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("dQw4w9WgXcQ") == "dQw4w9WgXcQ"
    assert extract_video_id("invalid_url") == ""


def test_parse_youtube_api_item():
    api_item = {
        "id": "item123",
        "snippet": {
            "resourceId": {"videoId": "dQw4w9WgXcQ"},
            "title": "Never Gonna Give You Up",
            "description": "The official music video for Rick Astley.",
            "channelTitle": "Rick Astley",
            "publishedAt": "2009-10-25T06:57:33Z",
            "thumbnails": {
                "high": {"url": "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg"}
            }
        }
    }
    parsed = parse_youtube_api_item(api_item)
    assert parsed is not None
    assert parsed["tweet_id"] == "yt_dQw4w9WgXcQ"
    assert parsed["source"] == "youtube"
    assert parsed["author_name"] == "Rick Astley"
    assert "Never Gonna Give You Up" in parsed["text"]
    assert parsed["url"] == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    assert len(parsed["media_urls"]) > 0


def test_parse_youtube_takeout_json():
    takeout_json = """[
        {
            "header": "YouTube",
            "title": "Watched Andrej Karpathy - Let's build GPT: from scratch, in code",
            "titleUrl": "https://www.youtube.com/watch?v=kCc8FmEb1nY",
            "subtitles": [{"name": "Andrej Karpathy", "url": "https://www.youtube.com/channel/UCXUPK5V_LwYIuL3S2_E4Wdw"}],
            "time": "2023-01-20T15:30:00.000Z",
            "products": ["YouTube"]
        }
    ]"""
    videos = parse_youtube_takeout_content(takeout_json)
    assert len(videos) == 1
    v = videos[0]
    assert v["tweet_id"] == "yt_kCc8FmEb1nY"
    assert v["source"] == "youtube"
    assert v["author_name"] == "Andrej Karpathy"
    assert "Andrej Karpathy - Let's build GPT" in v["text"]


def test_parse_youtube_takeout_csv():
    takeout_csv = """Video ID,Video Title,Channel Title,Description,Published At
dQw4w9WgXcQ,Never Gonna Give You Up,Rick Astley,Official music video,2009-10-25
"""
    videos = parse_youtube_takeout_content(takeout_csv)
    assert len(videos) == 1
    assert videos[0]["tweet_id"] == "yt_dQw4w9WgXcQ"
    assert videos[0]["source"] == "youtube"
    assert videos[0]["author_name"] == "Rick Astley"


@pytest.mark.asyncio
async def test_youtube_takeout_ingest_and_search_endpoint():
    takeout_data = """[
        {
            "header": "YouTube",
            "title": "Watched 3Blue1Brown - Neural Networks",
            "titleUrl": "https://www.youtube.com/watch?v=aircAruvnKk",
            "subtitles": [{"name": "3Blue1Brown"}],
            "time": "2023-05-10T10:00:00.000Z"
        }
    ]"""

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        # Ingest YouTube Takeout
        files = {"file": ("watch-history.json", takeout_data.encode("utf-8"), "application/json")}
        res = await client.post("/api/ingest/youtube/takeout", files=files)
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "success"
        assert data["parsed"] == 1
        assert data["source"] == "youtube"

        # Search with source=youtube
        search_res = await client.get("/api/search?q=3Blue1Brown&source=youtube")
        assert search_res.status_code == 200
        search_data = search_res.json()
        assert search_data["count"] >= 1
        found = any(r.get("tweet_id") == "yt_aircAruvnKk" for r in search_data["results"])
        assert found
