import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

DEFAULT_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
LOGS_PATH = DEFAULT_DATA_DIR / "logs_history.json"
NOTIFS_PATH = DEFAULT_DATA_DIR / "notifications.json"


class HistoryManager:
    def __init__(self, data_dir: Path | str | None = None):
        self.data_dir = Path(data_dir or DEFAULT_DATA_DIR)
        self.logs_path = self.data_dir / "logs_history.json"
        self.notifs_path = self.data_dir / "notifications.json"
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _read_json(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        try:
            return json.loads(path.read_text())
        except Exception:
            return []

    def _write_json(self, path: Path, data: list[dict[str, Any]], max_items: int = 200):
        try:
            trimmed = data[-max_items:]
            path.write_text(json.dumps(trimmed, indent=2))
        except Exception:
            pass

    def add_sync_log(
        self,
        trigger: str,
        engine: str,
        status: str,
        new_likes: int,
        total_db_likes: int,
        message: str = "",
        duration_sec: float = 0.0,
    ) -> dict[str, Any]:
        logs = self._read_json(self.logs_path)
        entry = {
            "id": str(uuid.uuid4())[:8],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "unix_time": time.time(),
            "trigger": trigger,
            "engine": engine,
            "status": status,
            "new_likes": new_likes,
            "total_db_likes": total_db_likes,
            "message": message,
            "duration_sec": round(duration_sec, 2),
        }
        logs.insert(0, entry)
        self._write_json(self.logs_path, logs)

        # Auto-create notification
        notif_type = "success" if status == "success" else "error"
        title = f"Sync {status.title()}: +{new_likes} Likes" if status == "success" else "Sync Failed"
        msg = f"Synced {new_likes} new likes via {engine} ({trigger}). Total DB: {total_db_likes} likes." if status == "success" else message
        self.add_notification(notif_type, title, msg)
        return entry

    def get_sync_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._read_json(self.logs_path)[:limit]

    def add_notification(self, notif_type: str, title: str, message: str) -> dict[str, Any]:
        notifs = self._read_json(self.notifs_path)
        entry = {
            "id": str(uuid.uuid4())[:8],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "unix_time": time.time(),
            "type": notif_type,
            "title": title,
            "message": message,
            "read": False,
        }
        notifs.insert(0, entry)
        self._write_json(self.notifs_path, notifs)
        return entry

    def get_notifications(self, limit: int = 50) -> dict[str, Any]:
        notifs = self._read_json(self.notifs_path)
        unread_count = sum(1 for n in notifs if not n.get("read", False))
        return {"unread_count": unread_count, "notifications": notifs[:limit]}

    def mark_all_read(self) -> int:
        notifs = self._read_json(self.notifs_path)
        for n in notifs:
            n["read"] = True
        self._write_json(self.notifs_path, notifs)
        return len(notifs)
