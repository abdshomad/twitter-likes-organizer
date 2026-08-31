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
        async with client.stream("GET", "/api/chat/stream?q=AI%20tools") as response:
            assert response.status_code == 200
            assert "text/event-stream" in response.headers.get("content-type", "")
            first_line = await anext(response.aiter_lines())
            assert "data:" in first_line


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
async def test_metrics_enricher_stream_endpoint(monkeypatch):
    async def mock_stream(*args, **kwargs):
        yield '{"stage": "complete", "message": "done"}'

    from src.server import app as server_module
    monkeypatch.setattr(server_module.metrics_enricher, "stream_enrich_all", mock_stream)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        async with client.stream("GET", "/api/maintenance/enrich-metrics/stream") as response:
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


@pytest.mark.asyncio
async def test_telemetry_stream_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/telemetry/stream?once=true")
        assert response.status_code == 200
        assert "text/event-stream" in response.headers.get("content-type", "")
        assert "event: stats" in response.text


@pytest.mark.asyncio
async def test_bookmarks_sync_and_unbookmark_endpoints():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        # Search with source filter
        search_res = await client.get("/api/search?source=bookmark")
        assert search_res.status_code == 200
        data = search_res.json()
        assert "results" in data

        # Unbookmark single
        res1 = await client.post("/api/maintenance/unbookmark-single", json={"tweet_id": "1896000000000000001"})
        assert res1.status_code == 200
        assert "status" in res1.json()

        # Unbookmark synced
        res2 = await client.post("/api/maintenance/unbookmark-synced")
        assert res2.status_code == 200
        assert res2.json()["status"] == "started"

        # Export markdown with source
        exp_res = await client.post("/api/export/markdown?source=bookmark")
        assert exp_res.status_code == 200
        assert exp_res.json()["status"] == "success"
        assert exp_res.json()["source"] == "bookmark"


@pytest.mark.asyncio
async def test_ghost_sweep_stream_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        async with client.stream("GET", "/api/maintenance/ghost-sweep/stream?limit=5") as response:
            assert response.status_code == 200
            assert "text/event-stream" in response.headers.get("content-type", "")
            line = await anext(response.aiter_lines())
            assert "data:" in line

