import json
import pytest
from src.ingestion.archive_parser import (
    parse_like_js_content,
    parse_bookmarks_js_content,
    parse_archive_content,
    parse_archive_file,
)


def test_parse_like_js_wrapper():
    sample_js = """window.YTD.like.part0 = [
      {
        "like" : {
          "tweetId" : "1895000000000000001",
          "fullText" : "Autonomous agent architectures with Cordis microkernel.",
          "expandedUrl" : "https://twitter.com/i/web/status/1895000000000000001"
        }
      },
      {
        "like" : {
          "tweetId" : "1895000000000000002",
          "fullText" : "High-throughput vector indexing with LanceDB.",
          "expandedUrl" : "https://twitter.com/i/web/status/1895000000000000002"
        }
      }
    ];"""

    tweets = parse_like_js_content(sample_js)
    assert len(tweets) == 2
    assert tweets[0]["tweet_id"] == "1895000000000000001"
    assert tweets[0]["source"] == "like"
    assert "Cordis" in tweets[0]["text"]
    assert tweets[1]["tweet_id"] == "1895000000000000002"
    assert "LanceDB" in tweets[1]["text"]


def test_parse_bookmarks_js_wrapper():
    sample_js = """window.YTD.bookmark.part0 = [
      {
        "bookmark" : {
          "tweetId" : "1896000000000000001",
          "fullText" : "Saved bookmark research paper on LLM memory.",
          "expandedUrl" : "https://twitter.com/i/web/status/1896000000000000001"
        }
      }
    ];"""

    tweets = parse_bookmarks_js_content(sample_js)
    assert len(tweets) == 1
    assert tweets[0]["tweet_id"] == "1896000000000000001"
    assert tweets[0]["source"] == "bookmark"
    assert "paper" in tweets[0]["text"]


def test_parse_archive_content_autodetect():
    like_js = """window.YTD.like.part0 = [{"like": {"tweetId": "101", "fullText": "Liked post"}}];"""
    bookmark_js = """window.YTD.bookmark.part0 = [{"bookmark": {"tweetId": "202", "fullText": "Bookmarked post"}}];"""

    likes = parse_archive_content(like_js)
    assert len(likes) == 1
    assert likes[0]["source"] == "like"
    assert likes[0]["tweet_id"] == "101"

    bmarks = parse_archive_content(bookmark_js)
    assert len(bmarks) == 1
    assert bmarks[0]["source"] == "bookmark"
    assert bmarks[0]["tweet_id"] == "202"


def test_parse_archive_file(tmp_path):
    file_path = tmp_path / "like.js"
    file_path.write_text("""window.YTD.like.part0 = [
      {
        "like" : {
          "tweetId" : "9999",
          "fullText" : "Testing file read"
        }
      }
    ];""")
    tweets = parse_archive_file(file_path)
    assert len(tweets) == 1
    assert tweets[0]["id"] == "9999"

    bm_path = tmp_path / "bookmarks.js"
    bm_path.write_text("""window.YTD.bookmark.part0 = [
      {
        "bookmark" : {
          "tweetId" : "8888",
          "fullText" : "Testing bookmark read"
        }
      }
    ];""")
    bm_tweets = parse_archive_file(bm_path)
    assert len(bm_tweets) == 1
    assert bm_tweets[0]["id"] == "8888"
    assert bm_tweets[0]["source"] == "bookmark"
