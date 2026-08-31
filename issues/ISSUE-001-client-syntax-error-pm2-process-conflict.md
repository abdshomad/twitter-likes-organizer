# Incident Report: ISSUE-001

## 📌 Title
Client-side JavaScript Syntax Errors & PM2 Port Conflict Causing Blank Feed and Unstyled Modal Overlays

---

## 📅 Incident Metadata
- **Date**: 2026-08-31
- **Severity**: High (UI rendering failure, empty feed)
- **Impacted Systems**: Web Frontend (`src/server/app.py`), PM2 Process Manager, Cloudflare Edge Delivery (`twitter-like-organizer.demoin.id`)
- **Status**: ✅ Resolved & Verified

---

## 🔍 Symptoms
1. **Unstyled Modal / Drawer Text Visible in Document Flow**:
   - Raw markup text for `#hud-tweet-modal`, `#hud-chat-drawer`, `#hud-sidesheet`, and `#hud-confirm-modal` rendered visibly at the bottom of the page (`Tweet Detail ✕ Similar Tweets You Might Enjoy ... Chat with LanceDB ... HUD Controls & Settings ... Confirm Delete`).
2. **Blank / Empty Likes Feed**:
   - No liked tweets or bookmarks were displayed in the `#results` container (`0 .hud-tweet-card` elements rendered).
3. **Persistent Across Devices**:
   - Observed across desktop browsers, mobile viewports, and direct server curls.

---

## 🔬 Root Cause Analysis (RCA)

### 1. Client-Side JavaScript Syntax Errors in Template
- **Orphaned Block & Extra Brace**: At the end of `startSyncStream()`, an extra `es.onerror = function() { ... }; }` was located outside of the function definition, throwing:
  ```text
  SyntaxError: Unexpected token '}'
  ```
- **Duplicate Function Declarations**: `toggleAutoSync()` and `changeSyncInterval()` were declared twice in the client script, throwing:
  ```text
  SyntaxError: Identifier 'toggleAutoSync' has already been declared
  ```
- **Consequence**: When modern JavaScript engines encounter top-level script syntax errors, script evaluation stops immediately before `DOMContentLoaded` handlers, `loadLikes()`, or `renderTagCloud()` can execute.

### 2. Missing Base CSS Rules for Modals
- `.hud-modal-backdrop` and `.hud-modal-box` had mobile bottom-sheet styling rules inside `@media (max-width: 768px)`, but lacked the base desktop CSS declaration `display: none; position: fixed; inset: 0;`.
- Because HTML `<div>` elements default to `display: block` in the normal document flow, the closed modals and confirm dialogs were visible at the bottom of the page.

### 3. PM2 Process Manager Port Lock & Restart Loop
- A previous PM2 service `twitter-likes-organizer-4024` was in a continuous restart loop after manual process kills, competing for TCP port `4024` and serving stale or interrupted HTTP connections.

### 4. Cloudflare Edge Caching
- HTTP responses from `GET /` previously lacked explicit `Cache-Control: no-cache` headers, allowing Cloudflare tunnel proxies and browser HTTP caches to cache the older broken HTML state.

---

## 🛠️ Resolutions Applied

### 1. Fixed JavaScript Template Script (`src/server/app.py`)
- Removed the orphaned `es.onerror` block and extra closing braces in `startSyncStream()`.
- Unified `toggleAutoSync()` and `changeSyncInterval()` into single functions that update both topbar status icons and sidesheet settings controls.
- Validated with Node.js parser (`node -c` returned exit code `0`).

### 2. Standardized Modal & Drawer CSS Overlay Architecture
- Added base CSS rules:
  ```css
  .hud-modal-backdrop {
    position: fixed;
    inset: 0;
    background: rgba(4, 6, 12, 0.85);
    backdrop-filter: blur(16px);
    z-index: 1000;
    display: none;
    align-items: center;
    justify-content: center;
  }
  .hud-modal-backdrop.open {
    display: flex !important;
  }
  .hud-chat-drawer, .hud-sidesheet {
    display: none;
  }
  .hud-chat-drawer.open, .hud-sidesheet.open {
    display: flex !important;
  }
  ```

### 3. Cleaned PM2 Daemon Process
- Freed port `4024` via `fuser -k 4024/tcp`.
- Recreated and saved the managed PM2 process:
  ```bash
  pm2 start "uv run uvicorn src.server.app:app --host 0.0.0.0 --port 4024" --name "twitter-likes-organizer-4024"
  pm2 save
  ```

### 4. Added Global Anti-Cache Middleware
- Injected strict cache-busting headers for dynamic HTML and JSON responses:
  ```http
  Cache-Control: no-cache, no-store, must-revalidate, max-age=0
  Pragma: no-cache
  Expires: 0
  ```
- Confirmed Cloudflare edge reports `cf-cache-status: DYNAMIC`.

---

## 🧪 Verification Matrix

| Verification Step | Test Tool | Result |
| :--- | :--- | :--- |
| JavaScript Syntax Validation | Node.js (`node -c`) | ✅ `PASSED (0 syntax errors)` |
| Desktop Viewport Render (1280x800) | Playwright Automation | ✅ `24 cards rendered, 0 JS errors` |
| Mobile Viewport Render (390x844) | Playwright Automation | ✅ `24 cards rendered, 0 JS errors` |
| Modal Open / Close Interactions | Playwright Automation | ✅ `Functional overlays` |
| Full Test Suite | Pytest | ✅ `47/47 tests passing` |

---

## 📁 Related Commits & Artifacts
- **Primary Source File**: [`src/server/app.py`](src/server/app.py)
- **Storage Clients**: [`src/storage/lancedb_client.py`](src/storage/lancedb_client.py), [`src/storage/meilisearch_client.py`](src/storage/meilisearch_client.py)
- **Live Endpoint**: [https://twitter-like-organizer.demoin.id/](https://twitter-like-organizer.demoin.id/)
