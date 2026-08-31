import pytest
from httpx import ASGITransport, AsyncClient
from src.server.app import app


@pytest.mark.asyncio
async def test_anti_collision_and_z_index_hierarchy():
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        res = await client.get("/")
        assert res.status_code == 200
        html = res.text

        # 1. Verify Mutual Panel Exclusion Engine
        assert "function closeConflictingPanels(except)" in html
        assert "closeConflictingPanels('chat')" in html
        assert "closeConflictingPanels('sidesheet')" in html
        assert "closeConflictingPanels('command-palette')" in html
        assert "closeConflictingPanels('tweet-modal')" in html

        # 2. Verify Z-Index Hierarchy in CSS
        assert ".hud-sidesheet" in html and "z-index: 800" in html
        assert ".hud-chat-drawer" in html and "z-index: 800" in html
        assert ".hud-floating-toast" in html and "z-index: 600" in html
        assert ".hud-bulk-bar" in html and "z-index: 500" in html

        # 3. Verify Mobile Spatial Clearance Offsets
        assert "bottom: calc(66px + env(safe-area-inset-bottom, 8px))" in html
        assert "bottom: calc(130px + env(safe-area-inset-bottom, 8px))" in html
