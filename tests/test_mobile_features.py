import pytest
from httpx import ASGITransport, AsyncClient
from src.server.app import app


@pytest.mark.asyncio
async def test_pwa_manifest_endpoint():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.get("/manifest.json")
        assert res.status_code == 200
        manifest = res.json()
        assert manifest["name"] == "𝕏 & YouTube Likes Hub"
        assert manifest["short_name"] == "LikesHub"
        assert manifest["display"] == "standalone"
        assert manifest["theme_color"] == "#080b12"
        assert len(manifest["icons"]) >= 1


@pytest.mark.asyncio
async def test_mobile_html_elements():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.get("/")
        assert res.status_code == 200
        html = res.text
        # Check PWA meta tags
        assert '<meta name="theme-color" content="#080b12">' in html
        assert '<link rel="manifest" href="/manifest.json">' in html
        assert 'viewport-fit=cover' in html
        assert '<meta name="apple-mobile-web-app-capable" content="yes">' in html

        # Check mobile navigation dock & pull indicator
        assert 'class="hud-mobile-dock"' in html
        assert 'id="hud-pull-indicator"' in html
        assert 'id="mob-nav-all"' in html
        assert 'id="mob-nav-likes"' in html
        assert 'id="mob-nav-youtube"' in html
        assert 'id="mob-nav-chat"' in html
        assert 'id="mob-nav-cmd"' in html
