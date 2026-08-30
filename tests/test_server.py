import pytest
from httpx import ASGITransport, AsyncClient
from src.server.app import app


@pytest.mark.asyncio
async def test_health_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["port"] == 4024


@pytest.mark.asyncio
async def test_index_page():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/")
        assert response.status_code == 200
        assert "Likes" in response.text


@pytest.mark.asyncio
async def test_rag_chat_stream_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/chat/stream?q=AI%20tools")
        assert response.status_code == 200
        assert "text/event-stream" in response.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_similar_tweets_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/tweets/123456/similar?limit=4")
        assert response.status_code == 200
        data = response.json()
        assert "similar" in data
        assert "count" in data
        assert data["tweet_id"] == "123456"


@pytest.mark.asyncio
async def test_metrics_enricher_stream_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/maintenance/enrich-metrics/stream")
        assert response.status_code == 200
        assert "text/event-stream" in response.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_delete_tweets_endpoints():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res1 = await client.delete("/api/tweets/999999")
        assert res1.status_code == 200
        assert res1.json()["status"] == "success"

        res2 = await client.post("/api/tweets/bulk-delete", json={"tweet_ids": ["999998", "999997"]})
        assert res2.status_code == 200
        assert res2.json()["status"] == "success"
