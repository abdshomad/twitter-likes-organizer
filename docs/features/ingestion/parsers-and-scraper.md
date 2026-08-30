# Ingestion & Scraping Pipeline

- **Built**: High-speed stream parser for `like.js` archive dumps and Playwright runner supporting interactive browser login and headless timeline likes scraping.
- **Paths**: `src/ingestion/archive_parser.py`, `src/ingestion/playwright_scraper.py`, `packages/plugin-ingestion/`
- **Usage**: `parse_archive_file("like.js")` / `await PlaywrightXScraper().scrape_likes("username")`
