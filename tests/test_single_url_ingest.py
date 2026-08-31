import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, patch
from src.server.app import app


@pytest.mark.asyncio
async def test_ingest_url_validation():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.post("/api/ingest/url", json={})
        assert res.status_code == 400
        assert "URL is required" in res.json()["message"]


@pytest.mark.asyncio
async def test_ingest_tweet_url_mocked():
    mock_tweet = {
        "id": "2090168861912170972",
        "tweet_id": "2090168861912170972",
        "author_name": "Bot Creator",
        "author_handle": "bot",
        "text": "Deep AI Agents and LanceDB test tweet #AI",
        "created_at": "",
        "liked_at": "",
        "url": "https://x.com/bot/status/2090168861912170972",
        "media_urls": [],
        "local_media_paths": [],
        "tags": ["AI"],
        "favorite_count": 42,
        "source": "like",
    }

    with patch("src.server.app.scraper.scrape_single_tweet", new_callable=AsyncMock) as mock_scrape:
        mock_scrape.return_value = mock_tweet
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            res = await client.post("/api/ingest/url", json={"url": "https://x.com/bot/status/2090168861912170972"})
            assert res.status_code == 200
            data = res.json()
            assert data["status"] == "success"
            assert data["type"] == "tweet"
            assert data["item"]["tweet_id"] == "2090168861912170972"
