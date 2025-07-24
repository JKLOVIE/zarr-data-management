import logging
from pathlib import Path
from typing import Sequence
import json
from datetime import datetime
from .config import CATALOG_PATH

log = logging.getLogger("metazarr")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def ensure_empty_dir(path: Path):
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    elif any(path.iterdir()):
        raise ValueError(f"目标目录 {path} 必须为空。")

def scan_files(paths: Sequence[Path]):
    for p in paths:
        if p.is_dir():
            yield from p.rglob("*")
        else:
            yield p

def load_catalog():
    if CATALOG_PATH.exists():
        return json.loads(CATALOG_PATH.read_text())
    return {}

def save_catalog(cat: dict):
    CATALOG_PATH.write_text(json.dumps(cat, indent=2, ensure_ascii=False))

def timestamp():
    return datetime.utcnow().isoformat(timespec='seconds') + "Z"
