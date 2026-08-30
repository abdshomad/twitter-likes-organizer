# Next Enhancements Backlog (Cycle 1 - Pure LanceDB & Cordis Microkernel)

## Module 1: Cordis Microkernel Core (`packages/core`)
- [DONE] 1.1: Setup `pnpm` monorepo workspace configuration (`pnpm-workspace.yaml`, root `package.json`, `tsconfig.json`) and Cordis microkernel application bootstrap.
- [DONE] 1.2: Implement Cordis dynamic plugin loader, config schema validator, and service dependency injection container.
- [DONE] 1.3: Build unified event bus and error handling lifecycle for inter-plugin coordination.

## Module 2: LanceDB Unified Storage (`packages/plugin-lancedb`)
- [DONE] 2.1: Initialize LanceDB embedded database connection under `data/lancedb/` with zero external server dependencies.
- [DONE] 2.2: Define `tweets` table schema with structured fields (ID, author, text, timestamp, tags, media paths) and Tantivy full-text search (FTS) index.
- [DONE] 2.3: Implement hybrid search query builder combining SQL filters, keyword search, and vector cosine similarity.

## Module 3: Ingestion & Scraper Plugin (`packages/plugin-ingestion`)
- [DONE] 3.1: Implement X archive parser for fast streaming ingestion of `like.js` JSON dumps into LanceDB.
- [DONE] 3.2: Python Playwright runner sidecar for headed manual-intervention auth and session cookie serialization (`data/session.json`).
- [DONE] 3.3: Playwright headless incremental timeline scraper runner with Cordis job scheduler.

## Module 4: Media Processing Plugin (`packages/plugin-media`)
- [DONE] 4.1: Cordis media manager service coordinating downloads into `data/media/{tweet_id}/` with checksum verification.
- [DONE] 4.2: Python `yt-dlp` media extraction sidecar runner for direct video/photo extraction.
- [DONE] 4.3: Local Cobalt instance fallback client and offline asset path resolver.

## Module 5: AI Categorization & Embeddings (`packages/plugin-ai`)
- [DONE] 5.1: Autonomous context tagging engine plugin communicating with local Ollama model.
- [DONE] 5.2: Python FastEmbed vector sidecar generating dense embeddings (`BAAI/bge-m3`) for LanceDB vector search.
- [DONE] 5.3: Hybrid LLM fallback router (Ollama -> OpenRouter) with retry strategy and token efficiency.

## Module 6: Web Dashboard & Exporter (`packages/plugin-web-dashboard`)
- [DONE] 6.1: Cordis HTTP service serving REST API and WebSocket status streams on port 4024.
- [DONE] 6.2: Clean, modern search UI dashboard with live LanceDB instant search, tag facets, and embedded media viewer.
- [DONE] 6.3: Obsidian / Notion compatible Markdown export engine with custom YAML frontmatter formatting.
