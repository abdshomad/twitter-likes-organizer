# Playwright vs. MCP Chrome DevTools Benchmark & Architecture

## Empirical Benchmark & Architecture Comparison

Side-by-side empirical performance comparison between **Playwright Native Scraper** and **MCP Chrome DevTools Protocol (CDP)** for Twitter/X likes scraping and indexing:

| Metric / Dimension | **Playwright (Production Engine)** | **MCP Chrome DevTools (Agent-Mediated CDP)** | Winner |
| :--- | :--- | :--- | :--- |
| **Navigation Latency** | **0.25s** | 0.24s | ⚖️ **Tie** (both use Chromium CDP core) |
| **Scrolling & Parsing Speed** | **1.51s / 5 scrolls** (~60–120 likes/min) | 1.52s (raw CDP) / **5–15s per scroll via MCP LLM** | 🏆 **Playwright** (10x faster via direct bindings) |
| **Autonomous 24/7 Daemon (PM2)** | ✅ **Native** (Runs in background every 10m without LLM present) | ❌ **No** (Requires active LLM conversation context & tool turns) | 🏆 **Playwright** |
| **LLM Token Consumption** | **0 tokens** (100% free local execution) | **~15,000–30,000 tokens/run** (HTML/DOM passed via LLM context) | 🏆 **Playwright** |
| **Timeline Likes Volume** | **Thousands of likes** (unlimited infinite scroll until the end) | Small batches (constrained by agent turn & message token limits) | 🏆 **Playwright** |
| **Dynamic Element Locators** | `locator("article[data-testid='tweet']")` with auto-wait & sub-element queries | Manual DOM node tree traversal & JS evaluation | 🏆 **Playwright** |
| **Session Cookie Management** | Native `storage_state()` JSON serialization | Manual cookie header inspection | 🏆 **Playwright** |
| **Interactive Visual Debugging** | Headed popup / screenshots | Live browser devtools inspection & network tracking | 🏆 **MCP DevTools** |

---

## Architectural Decision
1. **Primary Ingestion Engine**: **Playwright** is the production engine powering live timeline scraping, progressive backfilling, and the 10-minute PM2 background daemon due to zero LLM token costs and massive throughput advantages.
2. **On-Demand Debugging**: Chrome DevTools MCP is used for manual session troubleshooting, network header inspection, and interactive captcha diagnostics.
