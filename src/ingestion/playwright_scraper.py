import os
import json
import asyncio
from pathlib import Path
from typing import Any, Callable, Awaitable
from playwright.async_api import async_playwright
from src.server.config import SESSION_FILE, BACKUP_SESSION_FILE


class PlaywrightXScraper:
    def __init__(self, session_path: Path | str | None = None, backup_path: Path | str | None = None):
        self.session_path = Path(session_path) if session_path else SESSION_FILE
        self.backup_path = Path(backup_path) if backup_path else BACKUP_SESSION_FILE
        self.session_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_restored()

    def _ensure_restored(self):
        if not self.session_path.exists() and self.backup_path.exists():
            self.session_path.write_text(self.backup_path.read_text())

    def _persist(self, data: dict[str, Any]):
        content = json.dumps(data, indent=2)
        self.session_path.write_text(content)
        self.backup_path.parent.mkdir(parents=True, exist_ok=True)
        self.backup_path.write_text(content)

    def get_session_status(self) -> dict[str, Any]:
        self._ensure_restored()
        target = self.session_path if self.session_path.exists() else self.backup_path
        if not target.exists():
            return {"connected": False, "username": None}
        try:
            data = json.loads(target.read_text())
            cookies = data.get("cookies", [])
            auth_token = next((c["value"] for c in cookies if c.get("name") == "auth_token"), None)
            ct0 = next((c["value"] for c in cookies if c.get("name") == "ct0"), None)
            username = data.get("metadata", {}).get("username", None)
            return {"connected": bool(auth_token), "username": username, "has_ct0": bool(ct0)}
        except Exception:
            return {"connected": False, "username": None}

    def save_cookies(self, auth_token: str, ct0: str = "", username: str = "") -> dict[str, Any]:
        cookies = [
            {
                "name": "auth_token",
                "value": auth_token.strip(),
                "domain": ".x.com",
                "path": "/",
                "httpOnly": True,
                "secure": True,
                "sameSite": "None",
            }
        ]
        if ct0.strip():
            cookies.append({
                "name": "ct0",
                "value": ct0.strip(),
                "domain": ".x.com",
                "path": "/",
                "httpOnly": False,
                "secure": True,
                "sameSite": "Lax",
            })
        storage = {"cookies": cookies, "origins": [], "metadata": {"username": username.replace("@", ""), "avatar": ""}}
        self._persist(storage)
        return storage

    def disconnect(self):
        if self.session_path.exists():
            self.session_path.unlink()
        if self.backup_path.exists():
            self.backup_path.unlink()

    async def scrape_likes(
        self,
        username: str = "",
        max_tweets: int = 0,
        on_progress: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        on_item_found: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> list[dict[str, Any]]:
        self._ensure_restored()
        target = self.session_path if self.session_path.exists() else self.backup_path
        if not target.exists():
            raise FileNotFoundError("Session file not found. Please connect Twitter first.")

        storage_data = json.loads(target.read_text())
        if not username:
            username = storage_data.get("metadata", {}).get("username", "")

        extracted_tweets: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                storage_state=storage_data,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            )
            page = await context.new_page()

            for url in ["https://x.com/i/history/likes", f"https://x.com/{username}/likes" if username else None]:
                if not url:
                    continue
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    await page.wait_for_timeout(1500)
                    if "login" not in page.url:
                        break
                except Exception:
                    continue

            try:
                await page.wait_for_selector("article[data-testid='tweet']", timeout=10000)
            except Exception:
                pass

            consecutive_empty = 0
            last_height = 0
            scroll_count = 0

            while True:
                scroll_count += 1
                articles = await page.locator("article[data-testid='tweet']").all()
                for article in articles:
                    try:
                        tweet_link_el = article.locator("a[href*='/status/']").first
                        if await tweet_link_el.count() == 0:
                            continue
                        href = await tweet_link_el.get_attribute("href") or ""
                        tweet_id = href.split("/status/")[-1].split("?")[0].split("/")[0]
                        if not tweet_id or tweet_id in seen_ids:
                            continue

                        text_el = article.locator("div[data-testid='tweetText']")
                        tweet_text = await text_el.inner_text() if await text_el.count() > 0 else ""
                        
                        user_el = article.locator("div[data-testid='User-Name']")
                        user_text = await user_el.inner_text() if await user_el.count() > 0 else ""
                        lines = [line.strip() for line in user_text.split("\n") if line.strip()]
                        
                        author_name = lines[0] if len(lines) > 0 else ""
                        handle_part = href.strip("/").split("/status/")[0] if "/status/" in href else ""
                        if handle_part and handle_part not in ["i", "web"]:
                            author_handle = f"@{handle_part}"
                        else:
                            handle_line = next((l for l in lines if l.startswith("@")), "")
                            author_handle = handle_line or (f"@{author_name.lower().replace(' ', '')}" if author_name else "")

                        media_urls: list[str] = []
                        img_elements = await article.locator("div[data-testid='tweetPhoto'] img").all()
                        for img in img_elements:
                            src = await img.get_attribute("src")
                            if src:
                                media_urls.append(src)

                        seen_ids.add(tweet_id)
                        tweet_obj = {
                            "id": tweet_id,
                            "tweet_id": tweet_id,
                            "author_name": author_name or author_handle.lstrip("@"),
                            "author_handle": author_handle,
                            "text": tweet_text,
                            "created_at": "",
                            "liked_at": "",
                            "url": f"https://x.com{href}" if href.startswith("/") else href,
                            "media_urls": media_urls,
                            "local_media_paths": [],
                            "tags": [],
                            "raw_json": json.dumps({"id": tweet_id, "text": tweet_text, "user": author_handle}),
                        }
                        extracted_tweets.append(tweet_obj)

                        if on_item_found:
                            await on_item_found(tweet_obj)

                        if max_tweets > 0 and len(extracted_tweets) >= max_tweets:
                            break
                    except Exception:
                        continue

                if max_tweets > 0 and len(extracted_tweets) >= max_tweets:
                    break

                if on_progress:
                    await on_progress({
                        "stage": "scrolling",
                        "scroll_attempt": scroll_count,
                        "tweets_found": len(extracted_tweets),
                    })

                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    consecutive_empty += 1
                    if consecutive_empty >= 4:
                        break
                else:
                    consecutive_empty = 0
                last_height = new_height

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)

            await browser.close()
            return extracted_tweets
