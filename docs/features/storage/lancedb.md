# LanceDB Embedded Storage

- **Built**: Unified zero-server embedded storage handling structured tweet metadata, Tantivy full-text search (FTS), and 1024-dim vector cosine similarity.
- **Paths**: `src/storage/lancedb_client.py`, `packages/plugin-lancedb/`
- **Usage**: `LanceDBStore().upsert_tweets(tweets)` / `LanceDBStore().search_hybrid(query, query_vector, tag)`
