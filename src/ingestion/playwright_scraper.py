import json
import os
import shutil
from pathlib import Path
from typing import Any, Callable, Awaitable
from playwright.async_api import async_playwright

DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
DEFAULT_SESSION_PATH = DEFAULT_DATA_DIR / "session.json"
BACKUP_SESSION_PATH = Path(".secrets") / "twitter_session.json"


class PlaywrightXScraper:
    def __init__(self, session_path: Path | str | None = None, backup_path: Path | str | None = None):
        self.session_path = Path(session_path or DEFAULT_SESSION_PATH)
        self.backup_path = Path(backup_path or BACKUP_SESSION_PATH)
        self.session_path.parent.mkdir(parents=True, exist_ok=True)
        self.backup_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_restored()

    def _ensure_restored(self):
        if not self.session_path.exists() and self.backup_path.exists():
            try:
                shutil.copyfile(self.backup_path, self.session_path)
            except Exception:
                pass

    def _persist(self, data: dict[str, Any]):
        content = json.dumps(data, indent=2)
        self.session_path.write_text(content)
        try:
            self.backup_path.write_text(content)
        except Exception:
            pass

    def get_session_status(self) -> dict[str, Any]:
        self._ensure_restored()
        target = self.session_path if self.session_path.exists() else self.backup_path
        if not target.exists():
            return {"connected": False, "username": "", "avatar": ""}
        try:
            data = json.loads(target.read_text())
            cookies = data.get("cookies", [])
            has_auth = any(c.get("name") == "auth_token" and c.get("value") for c in cookies)
            meta = data.get("metadata", {})
            return {"connected": bool(has_auth), "username": meta.get("username", ""), "avatar": meta.get("avatar", "")}
        except Exception:
            return {"connected": False, "username": "", "avatar": ""}

    def save_cookies(self, auth_token: str, ct0: str = "", username: str = "") -> dict[str, Any]:
        auth_token = auth_token.strip()
        ct0 = ct0.strip()
        username = username.strip().replace("@", "")
        cookies = [
            {"name": "auth_token", "value": auth_token, "domain": ".x.com", "path": "/", "httpOnly": True, "secure": True, "sameSite": "None"},
            {"name": "auth_token", "value": auth_token, "domain": ".twitter.com", "path": "/", "httpOnly": True, "secure": True, "sameSite": "None"},
        ]
        if ct0:
            cookies.extend([
                {"name": "ct0", "value": ct0, "domain": ".x.com", "path": "/", "secure": True, "sameSite": "Lax"},
                {"name": "ct0", "value": ct0, "domain": ".twitter.com", "path": "/", "secure": True, "sameSite": "Lax"},
            ])
        storage = {"cookies": cookies, "origins": [], "metadata": {"username": username, "avatar": ""}}
        self._persist(storage)
        return {"connected": True, "username": username}

    def disconnect(self) -> bool:
        deleted = False
        if self.session_path.exists():
            self.session_path.unlink()
            deleted = True
        if self.backup_path.exists():
            self.backup_path.unlink()
            deleted = True
        return deleted

    async def authenticate_interactive(self) -> bool:
        display = os.getenv("DISPLAY", ":0")
        env = os.environ.copy()
        env["DISPLAY"] = display
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=False, env=env)
            except Exception:
                browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto("https://x.com/login", wait_until="domcontentloaded")
            try:
                await page.wait_for_url("https://x.com/home", timeout=300000)
                storage = await context.storage_state()
                profile_link = page.locator("a[data-testid='AppTabBar_Profile_Link']")
                if await profile_link.count() > 0:
                    href = await profile_link.get_attribute("href") or ""
                    storage["metadata"] = {"username": href.replace("/", ""), "avatar": ""}
                self._persist(storage)
                await browser.close()
                return True
            except Exception:
                await browser.close()
                return False

    async def login_with_credentials(self, username: str, password: str, email_or_phone: str = "") -> dict[str, Any]:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            try:
                await page.goto("https://x.com/i/flow/login", wait_until="domcontentloaded", timeout=20000)
                username_input = page.locator("input[autocomplete='username']")
                await username_input.wait_for(timeout=15000)
                await username_input.fill(username)
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(2000)
                verify_input = page.locator("input[data-testid='ocfEnterTextTextInput']")
                if await verify_input.count() > 0 and email_or_phone:
                    await verify_input.fill(email_or_phone)
                    await page.keyboard.press("Enter")
                    await page.wait_for_timeout(2000)
                pass_input = page.locator("input[name='password']")
                await pass_input.wait_for(timeout=15000)
                await pass_input.fill(password)
                await page.keyboard.press("Enter")
                await page.wait_for_url(lambda url: "x.com/home" in url or "twitter.com/home" in url, timeout=20000)
                storage = await context.storage_state()
                storage["metadata"] = {"username": username.replace("@", ""), "avatar": ""}
                self._persist(storage)
                await browser.close()
                return {"status": "success", "username": username}
            except Exception as e:
                await browser.close()
                return {"status": "error", "message": f"Login failed: {str(e)}"}

    async def scrape_likes(
        self,
        username: str = "",
        max_tweets: int = 0,
        on_progress: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
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
                initial_count = len(extracted_tweets)
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
                        lines = user_text.split("\n")
                        author_name = lines[0] if len(lines) > 0 else ""
                        author_handle = lines[1] if len(lines) > 1 else ""

                        media_urls: list[str] = []
                        img_elements = await article.locator("div[data-testid='tweetPhoto'] img").all()
                        for img in img_elements:
                            src = await img.get_attribute("src")
                            if src:
                                media_urls.append(src)

                        seen_ids.add(tweet_id)
                        extracted_tweets.append({
                            "id": tweet_id,
                            "tweet_id": tweet_id,
                            "author_name": author_name,
                            "author_handle": author_handle,
                            "text": tweet_text,
                            "created_at": "",
                            "liked_at": "",
                            "url": f"https://x.com{href}" if href.startswith("/") else href,
                            "media_urls": media_urls,
                            "local_media_paths": [],
                            "tags": [],
                            "raw_json": json.dumps({"id": tweet_id, "text": tweet_text, "user": user_text}),
                        })
                        if max_tweets > 0 and len(extracted_tweets) >= max_tweets:
                            break
                    except Exception:
                        continue

                current_height = await page.evaluate("document.body.scrollHeight")
                if on_progress:
                    await on_progress({
                        "stage": "scrolling",
                        "scroll_attempt": scroll_count,
                        "tweets_found": len(extracted_tweets),
                        "height": current_height,
                        "page_url": page.url,
                    })

                if max_tweets > 0 and len(extracted_tweets) >= max_tweets:
                    break

                await page.evaluate("window.scrollBy(0, window.innerHeight * 2);")
                await page.wait_for_timeout(1000)

                if len(extracted_tweets) == initial_count and current_height == last_height:
                    consecutive_empty += 1
                    if consecutive_empty >= 5:
                        break
                else:
                    consecutive_empty = 0

                last_height = current_height

            await browser.close()
        return extracted_tweets
