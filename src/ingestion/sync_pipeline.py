import json
from typing import AsyncGenerator, Any
from src.storage.lancedb_client import LanceDBStore
from src.ai.tagger import AITagger
from src.ai.embedder import VectorEmbedder
from src.media.downloader import MediaDownloader
from src.ingestion.playwright_scraper import PlaywrightXScraper


async def stream_likes_sync(
    scraper: PlaywrightXScraper,
    store: LanceDBStore,
    tagger: AITagger,
    embedder: VectorEmbedder,
    downloader: MediaDownloader,
    username: str = "",
    max_tweets: int = 50,
) -> AsyncGenerator[str, None]:
    status = scraper.get_session_status()
    if not status.get("connected"):
        yield f"data: {json.dumps({'error': 'Please connect Twitter account first.', 'stage': 'error'})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'scraping', 'percent': 5, 'message': 'Connecting to Twitter timeline...'})}\n\n"
    
    try:
        tweets = await scraper.scrape_likes(username=username, max_tweets=max_tweets)
    except Exception as e:
        yield f"data: {json.dumps({'error': f'Failed to scrape: {str(e)}', 'stage': 'error'})}\n\n"
        return

    total = len(tweets)
    if total == 0:
        yield f"data: {json.dumps({'stage': 'complete', 'percent': 100, 'message': 'No new likes found.', 'inserted': 0})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'processing', 'percent': 10, 'total': total, 'message': f'Found {total} likes. Ingesting...'})}\n\n"

    processed = []
    for idx, tweet in enumerate(tweets, start=1):
        # 1. Media
        media_paths = downloader.download_tweet_media(tweet)
        tweet["local_media_paths"] = media_paths

        # 2. AI Tags
        tags = tagger.generate_tags(tweet["text"])
        tweet["tags"] = tags

        # 3. Vector embedding
        try:
            tweet["vector"] = embedder.embed_text(tweet["text"])
        except Exception:
            tweet["vector"] = [0.0] * 1024

        store.upsert_tweets([tweet])
        processed.append(tweet)

        percent = int(10 + (idx / total) * 85)
        event_data = {
            "stage": "item_done",
            "current": idx,
            "total": total,
            "percent": percent,
            "tweet_id": tweet.get("id"),
            "author_handle": tweet.get("author_handle"),
            "author_name": tweet.get("author_name"),
            "text": tweet.get("text", "")[:120],
            "tags": tags,
            "media_count": len(media_paths),
        }
        yield f"data: {json.dumps(event_data)}\n\n"

    yield f"data: {json.dumps({'stage': 'complete', 'percent': 100, 'message': f'Successfully synced {len(processed)} likes!', 'inserted': len(processed)})}\n\n"
