import json
from datetime import datetime
from pathlib import Path

from .project_storage import processing_path


STATUS_FILE = "ui_status.json"


def _load(project_id: str) -> dict:
    path = processing_path(project_id, STATUS_FILE)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def _save(project_id: str, payload: dict) -> None:
    path = processing_path(project_id, STATUS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def set_action_status(project_id: str, action_key: str, state: str, message: str) -> None:
    payload = _load(project_id)
    payload[action_key] = {
        "state": state,  # idle|running|success|error
        "message": message,
        "updated_at": datetime.utcnow().isoformat(),
    }
    _save(project_id, payload)


def get_action_statuses(project_id: str) -> dict:
    return _load(project_id)
