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

    yield f"data: {json.dumps({'stage': 'scraping', 'percent': 5, 'message': 'Connecting to likes timeline...'})}\n\n"
    
    event_queue = asyncio.Queue()
    processed_count = 0
    engine_used = "graphql"
    gql_client = TwitterGraphQLClient(scraper.session_path)

    async def on_progress(data: dict):
        await event_queue.put(data)

    async def on_item_found(tweet: dict):
        nonlocal processed_count
        tweet["source"] = tweet.get("source") or "like"
        media_urls = tweet.get("media_urls", [])
        if media_urls:
            await media_queue.enqueue(tweet.get("id", ""), media_urls)

        tags = tagger.generate_tags(tweet["text"])
        tweet["tags"] = tags
        try:
            tweet["vector"] = embedder.embed_text(tweet["text"])
        except Exception:
            tweet["vector"] = [0.0] * 1024

        store.upsert_tweets([tweet], default_source="like")
        processed_count += 1

        unliked = False
        unlike_method = ""
        if auto_unlike:
            unliked, unlike_method = await unliker.ensure_unliked(tweet.get("id", ""), tweet.get("url", ""), max_attempts=3)

        await event_queue.put({
            "stage": "item_done",
            "current": processed_count,
            "tweet_id": tweet.get("id"),
            "author_handle": tweet.get("author_handle"),
            "author_name": tweet.get("author_name"),
            "text": tweet.get("text", "")[:120],
            "tags": tags,
            "source": tweet.get("source", "like"),
            "media_count": len(media_urls),
            "unliked": unliked,
            "unlike_method": unlike_method,
        })

    async def run_sync():
        nonlocal engine_used
        try:
            uname = username or status.get("username", "")
            gql_count = 0
            async for tweet in gql_client.fetch_all_likes_streaming(username=uname, max_tweets=max_tweets):
                gql_count += 1
                await on_item_found(tweet)
            if gql_count == 0:
                raise RuntimeError("GraphQL Likes returned 0 items (Likes endpoint private). Falling back to Playwright scraper.")
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
            message=f"Synced {processed_count} likes (Auto-Unlike Verified: {auto_unlike}).",
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


async def stream_bookmarks_sync(
    scraper: PlaywrightXScraper,
    store: LanceDBStore,
    tagger: AITagger,
    embedder: VectorEmbedder,
    media_queue: MediaQueue,
    max_tweets: int = 0,
    auto_unbookmark: bool = False,
) -> AsyncGenerator[str, None]:
    start_time = time.time()
    history = HistoryManager()
    unliker = TwitterUnliker(scraper.session_path)
    status = scraper.get_session_status()
    if not status.get("connected"):
        yield f"data: {json.dumps({'error': 'Please connect Twitter account first.', 'stage': 'error'})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'scraping', 'percent': 5, 'message': 'Connecting to bookmarks timeline...'})}\n\n"
    
    event_queue = asyncio.Queue()
    processed_count = 0
    engine_used = "graphql"
    gql_client = TwitterGraphQLClient(scraper.session_path)

    async def on_progress(data: dict):
        await event_queue.put(data)

    async def on_item_found(tweet: dict):
        nonlocal processed_count
        tweet["source"] = tweet.get("source") or "bookmark"
        media_urls = tweet.get("media_urls", [])
        if media_urls:
            await media_queue.enqueue(tweet.get("id", ""), media_urls)

        tags = tagger.generate_tags(tweet["text"])
        tweet["tags"] = tags
        try:
            tweet["vector"] = embedder.embed_text(tweet["text"])
        except Exception:
            tweet["vector"] = [0.0] * 1024

        store.upsert_tweets([tweet], default_source="bookmark")
        processed_count += 1

        unbookmarked = False
        unbookmark_method = ""
        if auto_unbookmark:
            unbookmarked, unbookmark_method = await unliker.ensure_unbookmarked(tweet.get("id", ""), max_attempts=3)

        await event_queue.put({
            "stage": "item_done",
            "current": processed_count,
            "tweet_id": tweet.get("id"),
            "author_handle": tweet.get("author_handle"),
            "author_name": tweet.get("author_name"),
            "text": tweet.get("text", "")[:120],
            "tags": tags,
            "source": tweet.get("source", "bookmark"),
            "media_count": len(media_urls),
            "unbookmarked": unbookmarked,
            "unbookmark_method": unbookmark_method,
        })

    async def run_sync():
        nonlocal engine_used
        try:
            async for tweet in gql_client.fetch_all_bookmarks_streaming(max_tweets=max_tweets):
                await on_item_found(tweet)
        except Exception as gql_err:
            engine_used = "playwright"
            await event_queue.put({
                "stage": "scrolling",
                "scroll_attempt": 1,
                "tweets_found": processed_count,
                "height": 0,
                "page_url": f"GraphQL fallback to Playwright: {str(gql_err)[:50]}...",
            })
            return await scraper.scrape_bookmarks(
                max_tweets=max_tweets, on_progress=on_progress, on_item_found=on_item_found
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
    total_db = store.get_stats().get("total_bookmarks", 0)

    try:
        await sync_task
        history.add_sync_log(
            trigger="manual-ui",
            engine=engine_used,
            status="success",
            new_likes=processed_count,
            total_db_likes=total_db,
            message=f"Synced {processed_count} bookmarks (Auto-Unbookmark: {auto_unbookmark}).",
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
        yield f"data: {json.dumps({'error': f'Bookmarks sync failed: {str(e)}', 'stage': 'error'})}\n\n"
        return

    yield f"data: {json.dumps({'stage': 'complete', 'percent': 100, 'message': f'Successfully synced {processed_count} bookmarks!', 'inserted': processed_count})}\n\n"

