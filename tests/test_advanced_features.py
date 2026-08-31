import pytest
from httpx import ASGITransport, AsyncClient
from src.server.app import app, fuse_rrf_rankings
from src.exporter.markdown_exporter import format_tweet_to_markdown, export_tweets_to_directory


def test_fuse_rrf_rankings():
    meili_docs = [
        {"tweet_id": "doc1", "text": "Doc 1"},
        {"tweet_id": "doc2", "text": "Doc 2"},
        {"tweet_id": "doc3", "text": "Doc 3"},
    ]
    lance_docs = [
        {"tweet_id": "doc2", "text": "Doc 2"},
        {"tweet_id": "doc4", "text": "Doc 4"},
        {"tweet_id": "doc1", "text": "Doc 1"},
    ]

    fused = fuse_rrf_rankings(meili_docs, lance_docs, k=60, limit=4)
    assert len(fused) == 4
    # doc1 and doc2 appear in both, so they must be ranked top
    fused_ids = [d["tweet_id"] for d in fused]
    assert fused_ids[0] in ("doc1", "doc2")
    assert fused_ids[1] in ("doc1", "doc2")


def test_youtube_markdown_export(tmp_path):
    yt_doc = {
        "tweet_id": "yt_sampleVid123",
        "author_name": "Andrej Karpathy",
        "author_handle": "@karpathy",
        "text": "▶️ Let's build GPT from scratch",
        "created_at": "2023-01-20",
        "liked_at": "2023-01-20",
        "url": "https://www.youtube.com/watch?v=sampleVid123",
        "tags": ["AI", "Coding"],
        "local_media_paths": ["data/media/youtube/sampleVid123.jpg"],
        "source": "youtube",
    }

    md = format_tweet_to_markdown(yt_doc)
    assert "# ▶️ YouTube: Andrej Karpathy" in md
    assert "source: youtube" in md
    assert "![[ data/media/youtube/sampleVid123.jpg ]]" in md

    files = export_tweets_to_directory([yt_doc], tmp_path)
    assert len(files) == 1
    assert files[0].exists()
    assert "karpathy_yt_sampleVid123.md" in files[0].name


@pytest.mark.asyncio
async def test_reconcile_stores_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.post("/api/maintenance/reconcile-stores")
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "success"
        assert "lancedb_count" in data
        assert "meilisearch_count" in data


@pytest.mark.asyncio
async def test_search_rrf_hybrid_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.get("/api/search?q=python&engine=rrf")
        assert res.status_code == 200
        data = res.json()
        assert data["engine"] == "rrf"
        assert "results" in data
