import json
from datetime import datetime
from pathlib import Path

from .project_storage import project_root, save_reg_files_if_missing


def _append_versioned_rules(project_id: str, razdel_code: str, learning_diff: dict) -> None:
    save_reg_files_if_missing(project_id, razdel_code)
    reg_dir = project_root(project_id) / "06_regs" / razdel_code
    rules_md = reg_dir / "rules.md"
    rules_json = reg_dir / "rules.json"
    ts = datetime.utcnow().isoformat()
    md_entry = (
        f"\n## {ts}\n"
        f"- source_project_id: {project_id}\n"
        f"- note: auto-appended from user corrections\n"
    )
    rules_md.write_text(rules_md.read_text(encoding="utf-8") + md_entry, encoding="utf-8")
    payload = json.loads(rules_json.read_text(encoding="utf-8"))
    payload.setdefault("versions", []).append(
        {
            "timestamp": ts,
            "source_project_id": project_id,
            "learning_diff": learning_diff,
        }
    )
    rules_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_learning_diff(project_id: str, process_name: str, learning_diff: dict, razdel_code: str) -> None:
    root = project_root(project_id)
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    run_dir = root / "04_logs" / "runs" / process_name / ts
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "learning_diff.json").write_text(
        json.dumps(learning_diff, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (root / "02_processing" / "learning_diff.json").write_text(
        json.dumps(learning_diff, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _append_versioned_rules(project_id, razdel_code, learning_diff)
