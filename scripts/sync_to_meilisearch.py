import sys
import time
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.storage.lancedb_client import LanceDBStore
from src.storage.meilisearch_client import MeiliSearchStore


def sync_all():
    print("[Sync] Connecting to LanceDB...")
    lance = LanceDBStore()
    
    print("[Sync] Connecting to Meilisearch...")
    meili = MeiliSearchStore()
    
    if not meili.is_healthy():
        print("[Sync ERROR] Meilisearch is not healthy or not reachable at http://127.0.0.1:7700")
        print("Run `./scripts/meilisearch_server.sh start` first.")
        sys.exit(1)

    print("[Sync] Fetching all tweets from LanceDB...")
    tweets = lance.get_all_tweets(limit=50000, source="all")
    print(f"[Sync] Found {len(tweets)} records in LanceDB.")

    if not tweets:
        print("[Sync] No records to sync.")
        return

    print(f"[Sync] Indexing {len(tweets)} records into Meilisearch...")
    start_t = time.perf_counter()
    count = meili.upsert_tweets(tweets)
    duration = time.perf_counter() - start_t
    print(f"[Sync DONE] Successfully indexed {count} tweets in {duration:.3f}s into Meilisearch!")


if __name__ == "__main__":
    sync_all()
