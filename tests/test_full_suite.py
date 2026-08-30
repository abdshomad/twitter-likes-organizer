import json
from pathlib import Path
import pytest
from httpx import ASGITransport, AsyncClient
from src.server.app import app, store
from src.exporter.markdown_exporter import format_tweet_to_markdown, export_tweets_to_directory
from src.ai.tagger import AITagger
from src.ingestion.playwright_scraper import PlaywrightXScraper
from src.ingestion.graphql_client import TwitterGraphQLClient


def test_markdown_formatter():
    tweet = {
        "tweet_id": "8888",
        "author_handle": "jack",
        "author_name": "Jack",
        "text": "just setting up my twttr",
        "url": "https://x.com/jack/status/8888",
        "tags": ["Tech", "History"],
        "local_media_paths": [],
    }
    md = format_tweet_to_markdown(tweet)
    assert "tweet_id: '8888'" in md or 'tweet_id: "8888"' in md
    assert "@jack" in md
    assert "just setting up my twttr" in md


def test_export_directory(tmp_path):
    tweets = [
        {"id": "1", "tweet_id": "1", "author_handle": "dev", "text": "Hello world", "tags": ["code"]}
    ]
    files = export_tweets_to_directory(tweets, tmp_path)
    assert len(files) == 1
    assert files[0].exists()
    assert "Hello world" in files[0].read_text()


def test_heuristic_tagger():
    tagger = AITagger()
    tags = tagger._heuristic_tags("Exploring #LLM and #Python architectures with Linux")
    assert "LLM" in tags or "Python" in tags or "Linux" in tags


def test_isolated_scraper_persistence(tmp_path):
    session_file = tmp_path / "session.json"
    backup_file = tmp_path / "backup.json"
    s = PlaywrightXScraper(session_path=session_file, backup_path=backup_file)
    s.save_cookies("token_abc", "ct0_xyz", "myuser")

    assert s.get_session_status()["connected"] is True
    assert s.get_session_status()["username"] == "myuser"

    session_file.unlink()
    assert s.get_session_status()["connected"] is True
    assert session_file.exists()


def test_graphql_response_parser(tmp_path):
    session_file = tmp_path / "session.json"
    session_file.write_text(json.dumps({
        "cookies": [{"name": "auth_token", "value": "test_auth"}, {"name": "ct0", "value": "test_ct0"}],
        "metadata": {"username": "testuser", "user_id": "12345"}
    }))
    client = TwitterGraphQLClient(session_path=session_file)
    headers = client._get_auth_headers()
    assert "authorization" in headers
    assert "auth_token=test_auth" in headers["cookie"]

    # Mock GraphQL response payload
    mock_payload = {
        "data": {
            "user": {
                "result": {
                    "timeline_v2": {
                        "timeline": {
                            "instructions": [
                                {
                                    "type": "TimelineAddEntries",
                                    "entries": [
                                        {
                                            "entryId": "tweet-9999",
                                            "content": {
                                                "itemContent": {
                                                    "tweet_results": {
                                                        "result": {
                                                            "legacy": {
                                                                "id_str": "9999",
                                                                "full_text": "GraphQL is ultra fast!",
                                                                "created_at": "Sun Aug 30 00:00:00 +0000 2026",
                                                            },
                                                            "core": {
                                                                "user_results": {
                                                                    "result": {
                                                                        "legacy": {
                                                                            "name": "Dev",
                                                                            "screen_name": "developer"
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        {
                                            "entryId": "cursor-bottom-12345",
                                            "content": {"value": "cursor_next_token"}
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
    tweets, next_cursor = client._parse_likes_response(mock_payload)
    assert len(tweets) == 1
    assert tweets[0]["tweet_id"] == "9999"
    assert tweets[0]["author_handle"] == "developer"
    assert tweets[0]["text"] == "GraphQL is ultra fast!"
    assert next_cursor == "cursor_next_token"


@pytest.mark.asyncio
async def test_api_endpoints():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.get("/api/stats")
        assert res.status_code == 200
        data = res.json()
        assert "total_likes" in data

        res = await client.get("/api/tags")
        assert res.status_code == 200

        res = await client.get("/api/search?q=test")
        assert res.status_code == 200

        res = await client.get("/api/auth/status")
        assert res.status_code == 200

        # Scheduler endpoints
        res = await client.get("/api/scheduler/status")
        assert res.status_code == 200
        sched_data = res.json()
        assert "enabled" in sched_data
        assert "interval_sec" in sched_data

        res = await client.post("/api/scheduler/toggle")
        assert res.status_code == 200
        assert "enabled" in res.json()
