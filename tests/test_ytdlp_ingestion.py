import pytest
import json
from unittest.mock import patch, MagicMock
from httpx import ASGITransport, AsyncClient
from src.server.app import app
from src.ingestion.ytdlp_client import (
    parse_vtt_text,
    parse_json3_subtitles,
    YtDlpExtractor,
    get_saved_transcript,
    TRANSCRIPTS_DIR,
)


def test_parse_vtt_text():
    vtt_sample = """WEBVTT
Kind: captions
Language: en

00:00:01.000 --> 00:00:04.500
Hello world, welcome to this video.

00:00:05.200 --> 00:00:09.800
Today we will talk about <c>AI agents</c> and LanceDB.
"""
    segments = parse_vtt_text(vtt_sample)
    assert len(segments) == 2
    assert segments[0]["start"] == 1.0
    assert segments[0]["end"] == 4.5
    assert segments[0]["text"] == "Hello world, welcome to this video."
    assert segments[1]["start"] == 5.2
    assert segments[1]["end"] == 9.8
    assert segments[1]["text"] == "Today we will talk about AI agents and LanceDB."


def test_parse_json3_subtitles():
    json3_sample = {
        "events": [
            {
                "tStartMs": 1500,
                "dDurationMs": 3000,
                "segs": [{"utf8": "Hello and welcome to "}, {"utf8": "the channel."}]
            },
            {
                "tStartMs": 5000,
                "dDurationMs": 4000,
                "segs": [{"utf8": "Building fast search systems."}]
            }
        ]
    }
    segments = parse_json3_subtitles(json3_sample)
    assert len(segments) == 2
    assert segments[0]["start"] == 1.5
    assert segments[0]["end"] == 4.5
    assert segments[0]["text"] == "Hello and welcome to the channel."
    assert segments[1]["start"] == 5.0
    assert segments[1]["end"] == 9.0
    assert segments[1]["text"] == "Building fast search systems."


def test_process_video_info_extracts_metadata_and_saves_transcript(tmp_path):
    extractor = YtDlpExtractor()
    dummy_info = {
        "id": "abc123testV",
        "title": "Introduction to Machine Learning",
        "description": "A comprehensive tutorial on neural networks.",
        "uploader": "AI Researcher",
        "uploader_id": "airesearcher",
        "upload_date": "20240115",
        "duration": 600,
        "view_count": 50000,
        "like_count": 3500,
        "tags": ["AI", "Neural Networks", "Python"],
        "categories": ["Education"],
        "subtitles": {},
        "automatic_captions": {}
    }

    processed = extractor._process_video_info(dummy_info)
    assert processed is not None
    assert processed["tweet_id"] == "yt_abc123testV"
    assert processed["source"] == "youtube"
    assert processed["author_name"] == "AI Researcher"
    assert processed["author_handle"] == "@airesearcher"
    assert "▶️ Introduction to Machine Learning" in processed["text"]
    assert "Neural Networks" in processed["tags"]


@pytest.mark.asyncio
async def test_youtube_ingest_url_endpoint():
    mock_extracted = {
        "id": "yt_xyz987mockV",
        "tweet_id": "yt_xyz987mockV",
        "author_name": "Tech Explainer",
        "author_handle": "@techexplainer",
        "text": "▶️ Deep Dive into Vector Databases\n\nHow LanceDB works under the hood.",
        "created_at": "2024-03-01",
        "liked_at": "2024-03-01",
        "url": "https://www.youtube.com/watch?v=xyz987mockV",
        "media_urls": ["https://i.ytimg.com/vi/xyz987mockV/hqdefault.jpg"],
        "local_media_paths": ["data/media/youtube/xyz987mockV.jpg"],
        "tags": ["YouTube", "VectorDB", "AI"],
        "vector": [0.0] * 1024,
        "raw_json": json.dumps({"id": "xyz987mockV", "title": "Deep Dive"}),
        "source": "youtube",
    }

    with patch.object(YtDlpExtractor, "extract_video", return_value=mock_extracted):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            res = await client.post(
                "/api/youtube/ingest-url",
                json={"url": "https://www.youtube.com/watch?v=xyz987mockV"}
            )
            assert res.status_code == 200
            data = res.json()
            assert data["status"] == "success"
            assert data["count"] == 1

            # Verify saved transcript endpoint
            t_res = await client.get("/api/youtube/transcript/xyz987mockV")
            assert t_res.status_code == 200
            t_data = t_res.json()
            assert t_data["video_id"] == "xyz987mockV"


@pytest.mark.asyncio
async def test_youtube_fetch_transcripts_stream():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.get("/api/youtube/fetch-transcripts/stream")
        assert res.status_code == 200
        assert "text/event-stream" in res.headers["content-type"]
