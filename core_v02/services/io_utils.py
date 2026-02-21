import json
from pathlib import Path

from .project_storage import project_root


def read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def read_processing(project_id: str, filename: str, default):
    return read_json(project_root(project_id) / "02_processing" / filename, default)
