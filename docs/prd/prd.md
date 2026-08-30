# Product Requirement Document: X-Likes Organizer

## 1. Overview
A locally hosted, plugin-driven application to ingest, archive, categorize, and semantically search user's liked posts on X (Twitter). Prioritizes local data ownership, local media preservation, and zero-server-overhead embedded storage via **LanceDB**.

## 2. Core Architecture & Stack
- **Plugin Architecture**: Cordis microkernel (TypeScript/Node.js) orchestrating plugin lifecycles, configuration, and API.
- **Unified Embedded Storage**: **LanceDB** as the single embedded database handling structured metadata (SQL filtering), full-text search (FTS), and high-dimensional vector embeddings without separate database daemon servers.
- **Python Sidecars**:
  - Playwright headed login & headless timeline scraper runner.
  - `yt-dlp` media extraction with local Cobalt fallback.
  - Ollama context tagger & FastEmbed (`BAAI/bge-m3`) vector generator.
- **Web & Dashboard**: Cordis HTTP plugin serving search endpoints, tag clouds, media viewer, and real-time status on port 4024.
- **Exporter**: Obsidian / Notion compatible Markdown exporter with YAML frontmatter.

## 3. Plugin Matrix (`packages/*`)
1. **`packages/core`**: Cordis microkernel loader, lifecycle management, and event bus.
2. **`packages/plugin-lancedb`**: LanceDB database adapter, table schemas, FTS indexing, and hybrid vector search queries.
3. **`packages/plugin-ingestion`**: Archive `like.js` streaming parser and Playwright scraper coordinator.
4. **`packages/plugin-media`**: Asset manager, `yt-dlp` extractor, Cobalt fallback, and local media organizer.
5. **`packages/plugin-ai`**: Local Ollama tag generator, OpenRouter fallback, and FastEmbed integration.
6. **`packages/plugin-web-dashboard`**: Web interface, instant search API, and live dashboard on port 4024.
7. **`packages/plugin-exporter`**: Markdown export engine for Obsidian/Notion.
