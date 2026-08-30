import asyncio
import json
from typing import AsyncGenerator
from src.storage.lancedb_client import LanceDBStore
from src.ai.tagger import AITagger
from src.ai.embedder import VectorEmbedder
from src.media.downloader import MediaDownloader
from src.ingestion.playwright_scraper import PlaywrightXScraper
from src.ingestion.graphql_client import TwitterGraphQLClient


async def stream_likes_sync(
    scraper: PlaywrightXScraper,
    store: LanceDBStore,
    tagger: AITagger,
    embedder: VectorEmbedder,
    downloader: MediaDownloader,
    username: str = "",
    max_tweets: int = 0,
) -> AsyncGenerator[str, None]:
    status = scraper.get_session_status()
    if not status.get("connected"):
        yield f"data: {json.dumps({'error': 'Please connect Twitter account first.', 'stage': 'error'})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'scraping', 'percent': 5, 'message': 'Connecting via ultra-fast GraphQL interceptor...'})}\n\n"
    
    event_queue = asyncio.Queue()
    processed_count = 0
    gql_client = TwitterGraphQLClient(scraper.session_path)

    async def on_progress(data: dict):
        await event_queue.put(data)

    async def on_item_found(tweet: dict):
        nonlocal processed_count
        media_paths = downloader.download_tweet_media(tweet)
        tweet["local_media_paths"] = media_paths
        tags = tagger.generate_tags(tweet["text"])
        tweet["tags"] = tags

        try:
            tweet["vector"] = embedder.embed_text(tweet["text"])
        except Exception:
            tweet["vector"] = [0.0] * 1024

        store.upsert_tweets([tweet])
        processed_count += 1

        await event_queue.put({
            "stage": "item_done",
            "current": processed_count,
            "tweet_id": tweet.get("id"),
            "author_handle": tweet.get("author_handle"),
            "author_name": tweet.get("author_name"),
            "text": tweet.get("text", "")[:120],
            "tags": tags,
            "media_count": len(media_paths),
        })

    async def run_sync():
        try:
            # 1. Primary: Direct GraphQL Interceptor (100x faster)
            uname = username or status.get("username", "")
            return await gql_client.fetch_all_likes_streaming(
                username=uname,
                max_tweets=max_tweets,
                on_progress=on_progress,
                on_item_found=on_item_found,
            )
        except Exception as gql_err:
            await event_queue.put({
                "stage": "scrolling",
                "scroll_attempt": 1,
                "tweets_found": processed_count,
                "height": 0,
                "page_url": f"GraphQL fallback to Playwright: {str(gql_err)[:60]}...",
            })
            # 2. Fallback: Headless Playwright
            return await scraper.scrape_likes(
                username=username,
                max_tweets=max_tweets,
                on_progress=on_progress,
                on_item_found=on_item_found,
            )
        finally:
            await event_queue.put({"stage": "scrape_finished"})

    sync_task = asyncio.create_task(run_sync())

    while not sync_task.done() or not event_queue.empty():
        try:
            event = await asyncio.wait_for(event_queue.get(), timeout=0.2)
            if event.get("stage") == "scrape_finished":
                break
            yield f"data: {json.dumps(event)}\n\n"
        except asyncio.TimeoutError:
            continue

    try:
        await sync_task
    except Exception as e:
        yield f"data: {json.dumps({'error': f'Sync failed: {str(e)}', 'stage': 'error'})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'complete', 'percent': 100, 'message': f'Successfully synced {processed_count} likes!', 'inserted': processed_count})}\n\n"
