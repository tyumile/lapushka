import json
from datetime import datetime

from .project_storage import project_root


def quality_row_key(row: dict) -> str:
    material = (row.get("material_norm_name") or "").strip()
    doc_kind = (row.get("doc_kind") or "").strip()
    doc_number = (row.get("doc_number") or "б/н").strip() or "б/н"
    doc_date = (row.get("doc_date") or "б/д").strip() or "б/д"
    file_ref = (row.get("file_ref") or "").strip()
    return "|".join([material, doc_kind, doc_number, doc_date, file_ref])


def append_quality_edit_log(project_id: str, edits: list[dict]) -> None:
    root = project_root(project_id)
    payload = []
    agg = root / "02_processing" / "edit_log_quality.json"
    if agg.exists():
        try:
            payload = json.loads(agg.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = []
    payload.extend(edits)
    agg.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    run_dir = root / "04_logs" / "runs" / "process_2" / datetime.utcnow().strftime("%Y%m%d%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "edit_log_quality.json").write_text(json.dumps(edits, ensure_ascii=False, indent=2), encoding="utf-8")
