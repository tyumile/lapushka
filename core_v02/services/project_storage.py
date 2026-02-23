import json
import secrets
import re
from datetime import datetime
from pathlib import Path

from django.conf import settings


def _safe_name(value: str) -> str:
    translit_map = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
        "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
        "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
        "э": "e", "ю": "yu", "я": "ya",
    }
    src = (value or "").strip()
    out = []
    for ch in src:
        low = ch.lower()
        if low in translit_map:
            part = translit_map[low]
            out.append(part.capitalize() if ch.isupper() and part else part)
            continue
        if ch.isascii() and (ch.isalnum() or ch in " _-"):
            out.append(ch)
            continue
        if ch.isascii():
            out.append(" ")
            continue
        # Keep latin-like characters from other alphabets out of folder id.
        out.append(" ")
    cleaned = "".join(out)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"[^A-Za-z0-9 _-]+", "", cleaned)
    return cleaned or "project"


def project_root(project_id: str) -> Path:
    return Path(settings.LOCAL_DRIVE_ROOT) / "Projects" / project_id


def generate_project_id(project_name: str) -> str:
    slug = _safe_name(project_name).replace(" ", "_")
    return f"{slug}__{secrets.token_hex(4)}"


def _ensure_file(path: Path, payload: dict | list) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def create_project_structure(project_name: str) -> str:
    p_id = generate_project_id(project_name)
    root = project_root(p_id)
    dirs = [
        root / "01_input" / "01_project",
        root / "01_input" / "02_quality_docs",
        root / "01_input" / "03_ojr",
        root / "01_input" / "04_samples",
        root / "01_input" / "04_samples" / "KJ" / "projects",
        root / "01_input" / "04_samples" / "KJ" / "id",
        root / "02_processing",
        root / "03_output",
        root / "04_logs" / "runs",
        root / "06_regs" / "KJ",
        root / "07_examples" / "files",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

    _ensure_file(
        root / "05_project_meta.json",
        {
            "project_id": p_id,
            "project_name": project_name,
            "created_at": datetime.utcnow().isoformat(),
            "comment": "",
            "step": 1,
        },
    )
    _ensure_file(root / "07_examples" / "examples_index.json", {"examples": []})
    _ensure_file(root / "06_regs" / "KJ" / "rules.json", {"versions": []})
    if not (root / "06_regs" / "KJ" / "rules.md").exists():
        (root / "06_regs" / "KJ" / "rules.md").write_text("# Rules\n", encoding="utf-8")
    return p_id


def load_project_meta(project_id: str) -> dict:
    path = project_root(project_id) / "05_project_meta.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_project_meta(project_id: str, meta: dict) -> None:
    path = project_root(project_id) / "05_project_meta.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def set_project_step(project_id: str, step: int) -> None:
    meta = load_project_meta(project_id)
    if not meta:
        return
    meta["step"] = step
    save_project_meta(project_id, meta)


def _block_relative_path(block: str, razdel_code: str | None = None, sample_kind: str | None = None) -> Path:
    if block == "project":
        return Path("01_input/01_project")
    if block == "quality":
        return Path("01_input/02_quality_docs")
    if block == "ojr":
        return Path("01_input/03_ojr")
    if block == "sample_project":
        return Path("01_input/04_samples") / (razdel_code or "UNKNOWN") / "projects"
    if block == "sample_id":
        return Path("01_input/04_samples") / (razdel_code or "UNKNOWN") / "id"
    if block == "sample_doc_type":
        return Path("01_input/04_samples") / (razdel_code or "UNKNOWN") / "doc_types" / (sample_kind or "general")
    return Path("01_input")


def save_uploaded_files(
    project_id: str,
    block: str,
    files,
    razdel_code: str | None = None,
    sample_kind: str | None = None,
) -> list[str]:
    root = project_root(project_id)
    rel = _block_relative_path(block, razdel_code=razdel_code, sample_kind=sample_kind)
    dst = root / rel
    dst.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    for f in files:
        name = Path(f.name or "").name
        if not name:
            continue
        path = dst / name
        with open(path, "wb") as out:
            for chunk in f.chunks():
                out.write(chunk)
        saved.append(str(rel / name).replace("\\", "/"))
    return saved


def list_uploaded_files(project_id: str) -> dict[str, list[str]]:
    root = project_root(project_id)
    blocks = {
        "project": root / "01_input" / "01_project",
        "quality": root / "01_input" / "02_quality_docs",
        "ojr": root / "01_input" / "03_ojr",
        "samples": root / "01_input" / "04_samples",
    }
    out: dict[str, list[str]] = {}
    for key, path in blocks.items():
        files: list[str] = []
        if path.exists():
            for p in sorted(path.rglob("*")):
                if p.is_file():
                    files.append(str(p.relative_to(root)).replace("\\", "/"))
        out[key] = files
    return out


def processing_path(project_id: str, filename: str) -> Path:
    return project_root(project_id) / "02_processing" / filename


def save_processing_json(project_id: str, filename: str, payload: dict | list) -> Path:
    path = processing_path(project_id, filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run_root(project_id: str, process_name: str, run_id: str) -> Path:
    return project_root(project_id) / "04_logs" / "runs" / process_name / run_id


def persist_run_artifact(
    project_id: str,
    process_name: str,
    run_id: str,
    name: str,
    content: str,
) -> Path:
    path = run_root(project_id, process_name, run_id) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def persist_run_json(
    project_id: str,
    process_name: str,
    run_id: str,
    name: str,
    payload: dict | list,
) -> Path:
    path = run_root(project_id, process_name, run_id) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def duplicate_output_to_run(
    project_id: str,
    process_name: str,
    run_id: str,
    processing_filename: str,
) -> None:
    src = processing_path(project_id, processing_filename)
    if not src.exists():
        return
    dst = run_root(project_id, process_name, run_id) / "outputs" / processing_filename
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(src.read_bytes())


def save_reg_files_if_missing(project_id: str, razdel_code: str) -> None:
    root = project_root(project_id) / "06_regs" / razdel_code
    root.mkdir(parents=True, exist_ok=True)
    rules_md = root / "rules.md"
    rules_json = root / "rules.json"
    if not rules_md.exists():
        rules_md.write_text("# Rules\n", encoding="utf-8")
    if not rules_json.exists():
        rules_json.write_text(json.dumps({"versions": []}, ensure_ascii=False, indent=2), encoding="utf-8")


def output_zip_path(project_id: str) -> Path:
    return project_root(project_id) / "03_output" / "output.zip"
