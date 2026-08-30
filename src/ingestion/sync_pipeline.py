import asyncio
import json
import time
from typing import AsyncGenerator
from src.storage.lancedb_client import LanceDBStore
from src.storage.history_manager import HistoryManager
from src.media.media_queue import MediaQueue
from src.ai.tagger import AITagger
from src.ai.embedder import VectorEmbedder
from src.ingestion.playwright_scraper import PlaywrightXScraper
from src.ingestion.graphql_client import TwitterGraphQLClient
from src.ingestion.unliker import TwitterUnliker


async def stream_likes_sync(
    scraper: PlaywrightXScraper,
    store: LanceDBStore,
    tagger: AITagger,
    embedder: VectorEmbedder,
    media_queue: MediaQueue,
    username: str = "",
    max_tweets: int = 0,
    auto_unlike: bool = False,
) -> AsyncGenerator[str, None]:
    start_time = time.time()
    history = HistoryManager()
    unliker = TwitterUnliker(scraper.session_path)
    status = scraper.get_session_status()
    if not status.get("connected"):
        yield f"data: {json.dumps({'error': 'Please connect Twitter account first.', 'stage': 'error'})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'scraping', 'percent': 5, 'message': 'Connecting to timeline...'})}\n\n"
    
    event_queue = asyncio.Queue()
    processed_count = 0
    engine_used = "graphql"
    gql_client = TwitterGraphQLClient(scraper.session_path)

    async def on_progress(data: dict):
        await event_queue.put(data)

    async def on_item_found(tweet: dict):
        nonlocal processed_count
        # 1. Enqueue media to async queue (Decoupled Stage 1)
        media_urls = tweet.get("media_urls", [])
        if media_urls:
            await media_queue.enqueue(tweet.get("id", ""), media_urls)

        # 2. AI tagging & vector embedding
        tags = tagger.generate_tags(tweet["text"])
        tweet["tags"] = tags
        try:
            tweet["vector"] = embedder.embed_text(tweet["text"])
        except Exception:
            tweet["vector"] = [0.0] * 1024

        # 3. Store locally in LanceDB
        store.upsert_tweets([tweet])
        processed_count += 1

        # 4. Optional Safe Auto-Unlike on X
        unliked = False
        if auto_unlike:
            try:
                unliked = await unliker.unlike_tweet(tweet.get("id", ""), tweet.get("url", ""))
            except Exception:
                pass

        await event_queue.put({
            "stage": "item_done",
            "current": processed_count,
            "tweet_id": tweet.get("id"),
            "author_handle": tweet.get("author_handle"),
            "author_name": tweet.get("author_name"),
            "text": tweet.get("text", "")[:120],
            "tags": tags,
            "media_count": len(media_urls),
            "unliked": unliked,
        })

    async def run_sync():
        nonlocal engine_used
        try:
            uname = username or status.get("username", "")
            return await gql_client.fetch_all_likes_streaming(
                username=uname, max_tweets=max_tweets, on_progress=on_progress, on_item_found=on_item_found
            )
        except Exception as gql_err:
            engine_used = "playwright"
            await event_queue.put({
                "stage": "scrolling",
                "scroll_attempt": 1,
                "tweets_found": processed_count,
                "height": 0,
                "page_url": f"GraphQL fallback to Playwright: {str(gql_err)[:50]}...",
            })
            return await scraper.scrape_likes(
                username=username, max_tweets=max_tweets, on_progress=on_progress, on_item_found=on_item_found
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

    duration = time.time() - start_time
    total_db = store.get_stats().get("total_likes", 0)

    try:
        await sync_task
        history.add_sync_log(
            trigger="manual-ui",
            engine=engine_used,
            status="success",
            new_likes=processed_count,
            total_db_likes=total_db,
            message=f"Synced {processed_count} likes (Decoupled Stage 1).",
            duration_sec=duration,
        )
    except Exception as e:
        history.add_sync_log(
            trigger="manual-ui",
            engine=engine_used,
            status="error",
            new_likes=processed_count,
            total_db_likes=total_db,
            message=str(e),
            duration_sec=duration,
        )
        yield f"data: {json.dumps({'error': f'Sync failed: {str(e)}', 'stage': 'error'})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'complete', 'percent': 100, 'message': f'Successfully synced {processed_count} likes!', 'inserted': processed_count})}\n\n"
