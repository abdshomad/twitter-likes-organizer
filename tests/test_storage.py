import shutil
from pathlib import Path
import pytest
from src.storage.lancedb_client import LanceDBStore


@pytest.fixture
def temp_lancedb(tmp_path):
    db_dir = tmp_path / "test_lancedb"
    store = LanceDBStore(db_path=db_dir)
    yield store
    shutil.rmtree(db_dir, ignore_errors=True)


def test_lancedb_crud(temp_lancedb):
    sample_tweets = [
        {
            "id": "1001",
            "tweet_id": "1001",
            "author_name": "AI Researcher",
            "author_handle": "airesearcher",
            "text": "Optimizing local LLMs using quantization and KV cache compression.",
            "created_at": "2026-08-01T12:00:00Z",
            "liked_at": "2026-08-01T13:00:00Z",
            "url": "https://x.com/airesearcher/status/1001",
            "media_urls": [],
            "local_media_paths": [],
            "tags": ["AI", "LLM", "Optimization"],
            "vector": [0.1] * 1024,
            "raw_json": '{"favorite_count": 42}',
        },
        {
            "id": "1002",
            "tweet_id": "1002",
            "author_name": "Rust Dev",
            "author_handle": "rustacean",
            "text": "Building high-throughput search engines in Rust with LanceDB.",
            "created_at": "2026-08-02T12:00:00Z",
            "liked_at": "2026-08-02T13:00:00Z",
            "url": "https://x.com/rustacean/status/1002",
            "media_urls": [],
            "local_media_paths": [],
            "tags": ["Rust", "Search", "LanceDB"],
            "vector": [0.2] * 1024,
            "raw_json": '{"favorite_count": 950}',
        },
    ]

    inserted = temp_lancedb.upsert_tweets(sample_tweets)
    assert inserted == 2

    stats = temp_lancedb.get_stats()
    assert stats["total_likes"] == 2
    assert stats["indexed_vectors"] == 2
    assert stats["tags_count"] >= 5

    tags = temp_lancedb.get_all_tags()
    tag_names = [t["tag"] for t in tags]
    assert "LLM" in tag_names
    assert "Rust" in tag_names

    # Test FTS / vector search
    results = temp_lancedb.search_hybrid(query="LLMs")
    assert len(results) >= 1

    # Test Sorting
    newest_liked = temp_lancedb.search_hybrid(sort_by="newest_liked")
    assert newest_liked[0]["tweet_id"] == "1002"

    oldest_liked = temp_lancedb.search_hybrid(sort_by="oldest_liked")
    assert oldest_liked[0]["tweet_id"] == "1001"

    newest_tweeted = temp_lancedb.search_hybrid(sort_by="newest_tweeted")
    assert newest_tweeted[0]["tweet_id"] == "1002"

    oldest_tweeted = temp_lancedb.search_hybrid(sort_by="oldest_tweeted")
    assert oldest_tweeted[0]["tweet_id"] == "1001"

    most_liked = temp_lancedb.search_hybrid(sort_by="most_liked")
    assert most_liked[0]["tweet_id"] == "1002"
    assert most_liked[0]["favorite_count"] == 950
    assert most_liked[1]["favorite_count"] == 42
