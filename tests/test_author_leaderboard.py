import pytest
from httpx import ASGITransport, AsyncClient
from src.server.app import app
from src.storage.lancedb_client import LanceDBStore


def test_get_top_authors_store():
    store = LanceDBStore()
    authors = store.get_top_authors(source="all", sort_by="count", limit=10)
    assert isinstance(authors, list)
    if authors:
        first = authors[0]
        assert "author_handle" in first
        assert "author_name" in first
        assert "count" in first
        assert "percentage" in first
        assert "rank" in first
        assert first["rank"] == 1
        assert first["count"] >= (authors[1]["count"] if len(authors) > 1 else 0)


@pytest.mark.asyncio
async def test_authors_leaderboard_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.get("/api/authors/leaderboard?limit=10")
        assert res.status_code == 200
        data = res.json()
        assert "authors" in data
        assert "count" in data
        assert isinstance(data["authors"], list)

        # Test source filter
        res_yt = await client.get("/api/authors/leaderboard?source=youtube")
        assert res_yt.status_code == 200
        data_yt = res_yt.json()
        assert "authors" in data_yt


@pytest.mark.asyncio
async def test_authors_leaderboard_sorting():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res_name = await client.get("/api/authors/leaderboard?sort_by=name&limit=10")
        assert res_name.status_code == 200
        data_name = res_name.json()
        assert "authors" in data_name
