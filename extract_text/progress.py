"""断点续传：进度加载与保存。"""

import json
from pathlib import Path


def _progress_path(filepath: Path) -> Path:
    return filepath.with_suffix(".progress.json")


def load_progress(filepath: Path) -> dict[int, str]:
    p = _progress_path(filepath)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return {int(k): v for k, v in data.get("completed", {}).items()}
    except Exception:
        return {}


def save_progress(filepath: Path, results: dict[int, str]):
    p = _progress_path(filepath)
    data = {"completed": {str(k): v for k, v in results.items()}}
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def delete_progress(filepath: Path):
    p = _progress_path(filepath)
    if p.exists():
        p.unlink()
