import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env if present
load_dotenv()

HOST: str = os.getenv("HOST", "0.0.0.0")
PORT: int = int(os.getenv("PORT", "4024"))
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "data"))
LANCEDB_DIR: Path = DATA_DIR / "lancedb"
MEDIA_DIR: Path = DATA_DIR / "media"
SESSION_FILE: Path = DATA_DIR / "session.json"
BACKUP_SESSION_FILE: Path = DATA_DIR / "auth" / "session.json"
