import asyncio
import json
import pytest
from pathlib import Path
from src.media.media_queue import MediaQueue
from src.ingestion.unliker import TwitterUnliker


@pytest.mark.asyncio
async def test_media_queue_enqueue_and_process(tmp_path):
    queue_file = tmp_path / "media_queue.json"
    mq = MediaQueue(queue_path=queue_file)

    added = await mq.enqueue("12345", ["https://example.com/img1.jpg", "https://example.com/img2.jpg"])
    assert added == 2
    assert mq.get_status()["pending_count"] == 2

    added_dup = await mq.enqueue("12345", ["https://example.com/img1.jpg"])
    assert added_dup == 0

    mq2 = MediaQueue(queue_path=queue_file)
    assert mq2.get_status()["pending_count"] == 2


@pytest.mark.asyncio
async def test_unliker_headers_and_payload(tmp_path):
    session_file = tmp_path / "session.json"
    session_file.write_text(json.dumps({
        "cookies": [
            {"name": "auth_token", "value": "test_token"},
            {"name": "ct0", "value": "test_ct0"},
        ]
    }))
    unliker = TwitterUnliker(session_path=session_file)
    headers = unliker._get_auth_headers()
    assert "authorization" in headers
    assert "auth_token=test_token" in headers["cookie"]
    assert headers["x-csrf-token"] == "test_ct0"
    assert headers["content-type"] == "application/json"


@pytest.mark.asyncio
async def test_ghost_likes_sweeper(tmp_path, monkeypatch):
    session_file = tmp_path / "session.json"
    session_file.write_text(json.dumps({
        "cookies": [
            {"name": "auth_token", "value": "test_token"},
            {"name": "ct0", "value": "test_ct0"},
        ]
    }))
    unliker = TwitterUnliker(session_path=session_file)

    async def mock_gql(tid):
        return False, 404

    async def mock_rest(tid):
        return True, 200

    monkeypatch.setattr(unliker, "unlike_tweet_graphql", mock_gql)
    monkeypatch.setattr(unliker, "unlike_tweet_rest", mock_rest)

    purged, method = await unliker.purge_ghost_like("9999999999")
    assert purged is True
    assert "ghost_purged" in method or "rest" in method

    count, swept = await unliker.sweep_ghost_likes(["111", "222"])
    assert count == 2
    assert swept == ["111", "222"]
