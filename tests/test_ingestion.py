import json
import pytest
from src.ingestion.archive_parser import parse_like_js_content, parse_archive_file


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
    assert "Cordis" in tweets[0]["text"]
    assert tweets[1]["tweet_id"] == "1895000000000000002"
    assert "LanceDB" in tweets[1]["text"]


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
