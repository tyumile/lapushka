import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .project_storage import project_root


def append_event(project_id: str, event: dict[str, Any]) -> None:
    logs_path = project_root(project_id) / "04_logs" / "process_log.jsonl"
    logs_path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(event)
    payload.setdefault("timestamp", datetime.utcnow().isoformat())
    with logs_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_events(project_id: str) -> list[dict[str, Any]]:
    logs_path = project_root(project_id) / "04_logs" / "process_log.jsonl"
    if not logs_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in logs_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def latest_run_folder(project_id: str) -> Path | None:
    root = project_root(project_id) / "04_logs" / "runs"
    if not root.exists():
        return None
    folders = [p for p in root.rglob("*") if p.is_dir()]
    if not folders:
        return None
    return sorted(folders, key=lambda p: p.stat().st_mtime, reverse=True)[0]
