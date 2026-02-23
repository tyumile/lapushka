import json
import csv
import io
import os
import re
import secrets
import shutil
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from django.conf import settings

from ..llm.prompt_loader_v02 import load_prompt
from ..llm.responses_client_v02 import ResponsesClientV02
from .log_service import append_event
from .project_storage import persist_run_artifact, persist_run_json, project_root


SUPPORTED_CONTEXT_EXTS = {
    ".art", ".bat", ".brf", ".c", ".cls", ".css", ".diff", ".eml", ".es",
    ".h", ".hs", ".htm", ".html", ".ics", ".ifb", ".java", ".js", ".json",
    ".ksh", ".ltx", ".mail", ".markdown", ".md", ".mht", ".mhtml", ".mjs",
    ".nws", ".patch", ".pdf", ".pl", ".pm", ".pot", ".py", ".rst", ".scala",
    ".sh", ".shtml", ".srt", ".sty", ".tex", ".text", ".txt", ".vcf", ".vtt",
    ".xls", ".xlsx", ".csv", ".xml", ".yaml", ".yml",
}

TABLE_CONTEXT_EXTS = {".xls", ".xlsx", ".csv"}

TABLE_CONTEXT_PROMPT_NOTE = (
    "\n\nIMPORTANT: Any .xls/.xlsx/.csv inputs are converted to JSON before upload. "
    "Treat attached JSON files as the source for tabular data."
)

EXCLUDED_REL = "01_input/099_excluded"


def _json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def _read_csv_rows(path: Path, max_rows: int = 300) -> list[dict]:
    raw = path.read_bytes()
    text = ""
    for enc in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if not text:
        text = raw.decode("utf-8", errors="replace")
    sio = io.StringIO(text)
    rows: list[dict] = []
    reader = csv.DictReader(sio)
    if reader.fieldnames:
        for idx, row in enumerate(reader, start=1):
            rows.append({"row_index": idx, "values": {k: (v or "") for k, v in row.items()}})
            if idx >= max_rows:
                break
    else:
        sio.seek(0)
        plain_reader = csv.reader(sio)
        for idx, row in enumerate(plain_reader, start=1):
            rows.append({"row_index": idx, "values": {str(i + 1): (v or "") for i, v in enumerate(row)}})
            if idx >= max_rows:
                break
    return rows


def _read_xlsx_payload(path: Path, max_sheets: int = 8, max_rows: int = 250) -> dict:
    ns_main = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    ns_rel = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
    ns_pkg_rel = "{http://schemas.openxmlformats.org/package/2006/relationships}"
    payload: dict = {"file_name": path.name, "format": "xlsx", "sheets": []}
    with zipfile.ZipFile(path, "r") as zf:
        if "xl/workbook.xml" not in zf.namelist():
            payload["warning"] = "workbook.xml is missing"
            return payload

        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            s_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in s_root.findall(f".//{ns_main}si"):
                parts = [t.text or "" for t in si.findall(f".//{ns_main}t")]
                shared_strings.append("".join(parts))

        rels_map: dict[str, str] = {}
        rels_name = "xl/_rels/workbook.xml.rels"
        if rels_name in zf.namelist():
            r_root = ET.fromstring(zf.read(rels_name))
            for rel in r_root.findall(f".//{ns_pkg_rel}Relationship"):
                rid = rel.attrib.get("Id", "")
                target = rel.attrib.get("Target", "")
                if rid and target:
                    rels_map[rid] = target if target.startswith("xl/") else f"xl/{target.lstrip('/')}"

        wb_root = ET.fromstring(zf.read("xl/workbook.xml"))
        sheet_nodes = wb_root.findall(f".//{ns_main}sheet")
        for sheet_idx, sheet in enumerate(sheet_nodes, start=1):
            if sheet_idx > max_sheets:
                break
            sheet_name = sheet.attrib.get("name") or f"sheet_{sheet_idx}"
            rid = sheet.attrib.get(f"{ns_rel}id", "")
            target = rels_map.get(rid, f"xl/worksheets/sheet{sheet_idx}.xml")
            if target not in zf.namelist():
                payload["sheets"].append({"sheet_name": sheet_name, "warning": f"{target} is missing", "rows": []})
                continue

            sh_root = ET.fromstring(zf.read(target))
            rows_payload: list[dict] = []
            for row_idx, row in enumerate(sh_root.findall(f".//{ns_main}sheetData/{ns_main}row"), start=1):
                cell_values: dict[str, str] = {}
                for cell in row.findall(f"{ns_main}c"):
                    cell_ref = cell.attrib.get("r", "")
                    col_match = re.match(r"([A-Za-z]+)", cell_ref)
                    col = col_match.group(1) if col_match else str(len(cell_values) + 1)
                    ctype = cell.attrib.get("t", "")
                    value = ""
                    if ctype == "s":
                        v = cell.find(f"{ns_main}v")
                        if v is not None and (v.text or "").isdigit():
                            sid = int(v.text or "0")
                            if 0 <= sid < len(shared_strings):
                                value = shared_strings[sid]
                    elif ctype == "inlineStr":
                        t = cell.find(f"{ns_main}is/{ns_main}t")
                        value = (t.text or "") if t is not None else ""
                    else:
                        v = cell.find(f"{ns_main}v")
                        value = (v.text or "") if v is not None else ""
                    cell_values[col] = value
                rows_payload.append({"row_index": row_idx, "cells": cell_values})
                if row_idx >= max_rows:
                    break
            payload["sheets"].append(
                {
                    "sheet_name": sheet_name,
                    "rows": rows_payload,
                    "truncated_rows": len(rows_payload) >= max_rows,
                }
            )
    return payload


def _read_xls_payload(path: Path, max_sheets: int = 8, max_rows: int = 250) -> dict:
    payload: dict = {"file_name": path.name, "format": "xls", "sheets": []}
    try:
        import xlrd  # type: ignore
    except Exception:
        payload["warning"] = "xlrd is not installed; uploaded as metadata JSON."
        return payload

    wb = xlrd.open_workbook(filename=str(path), on_demand=True)
    for i in range(min(wb.nsheets, max_sheets)):
        sh = wb.sheet_by_index(i)
        rows_payload: list[dict] = []
        for r in range(min(sh.nrows, max_rows)):
            cells: dict[str, str] = {}
            for c in range(sh.ncols):
                col = str(c + 1)
                val = sh.cell_value(r, c)
                cells[col] = "" if val is None else str(val)
            rows_payload.append({"row_index": r + 1, "cells": cells})
        payload["sheets"].append(
            {
                "sheet_name": sh.name,
                "rows": rows_payload,
                "truncated_rows": sh.nrows > max_rows,
            }
        )
    return payload


def prepare_context_file_for_upload(file_path: Path) -> tuple[str, bytes, str | None]:
    ext = file_path.suffix.lower()
    if ext not in TABLE_CONTEXT_EXTS:
        return file_path.name, file_path.read_bytes(), None

    if ext == ".csv":
        rows = _read_csv_rows(file_path)
        payload = {
            "file_name": file_path.name,
            "format": "csv",
            "rows": rows,
            "truncated_rows": len(rows) >= 300,
        }
        return f"{file_path.stem}__csv_as_json.json", _json_bytes(payload), ext

    if ext == ".xlsx":
        payload = _read_xlsx_payload(file_path)
        return f"{file_path.stem}__xlsx_as_json.json", _json_bytes(payload), ext

    payload = _read_xls_payload(file_path)
    return f"{file_path.stem}__xls_as_json.json", _json_bytes(payload), ext


def _collect_paths(paths: list[Path]) -> list[Path]:
    return [
        p for p in paths
        if p.exists() and p.is_file() and p.suffix.lower() in SUPPORTED_CONTEXT_EXTS
    ]


def move_to_excluded(root: Path, path: Path, reason: str) -> Path:
    try:
        rel = path.relative_to(root)
    except ValueError:
        rel = Path(path.name)
    rel_str = str(rel).replace("\\", "/")
    if rel_str.startswith(EXCLUDED_REL):
        return path
    excluded_dir = root / EXCLUDED_REL
    excluded_dir.mkdir(parents=True, exist_ok=True)
    dest = excluded_dir / rel.name
    idx = 1
    while dest.exists() and dest != path:
        stem = dest.stem
        suf = dest.suffix
        dest = excluded_dir / f"{stem}_{idx}{suf}"
        idx += 1
    if path != dest:
        shutil.copy2(str(path), str(dest))
    reason_path = dest.with_suffix(dest.suffix + ".excluded_reason.txt")
    reason_path.write_text(reason, encoding="utf-8")
    return dest


def resolve_openai_model() -> str:
    model = (os.getenv("OPENAI_MODEL") or "").strip()
    if not model:
        raise ValueError("OPENAI_MODEL is not set in environment")
    return model


def run_llm_json_process(
    *,
    project_id: str,
    process_name: str,
    prompt_name: str,
    prompt_vars: dict,
    files: list[Path],
    output_filename: str,
    mock_payload: dict,
) -> tuple[str, dict]:
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)
    model = resolve_openai_model()
    append_event(project_id, {"process": process_name, "stage": "start", "run_id": run_id})

    if settings.MOCK_MODE:
        persist_run_json(project_id, process_name, run_id, output_filename, mock_payload)
        persist_run_json(
            project_id,
            process_name,
            run_id,
            "run_meta.json",
            {
                "process": process_name,
                "run_id": run_id,
                "model": model,
                "mock_mode": True,
                "started_at": datetime.utcnow().isoformat(),
                "success": True,
            },
        )
        persist_run_artifact(project_id, process_name, run_id, "raw_response.txt", json.dumps(mock_payload, ensure_ascii=False))
        persist_run_json(project_id, process_name, run_id, "uploaded_files.json", {"files": []})
        append_event(project_id, {"process": process_name, "stage": "finish", "run_id": run_id, "status": "success", "mock": True})
        return run_id, mock_payload

    root = project_root(project_id)
    existing = _collect_paths(files)
    client = ResponsesClientV02()
    upload_map: dict[str, dict] = {}
    file_ids: list[str] = []
    excluded: list[dict] = []
    for file_path in existing:
        rel = str(file_path.relative_to(root)).replace("\\", "/")
        try:
            upload_name, upload_bytes, converted_from = prepare_context_file_for_upload(file_path)
            fid = client.upload_file_bytes(upload_name, upload_bytes)
            upload_map[rel] = {"file_id": fid, "upload_name": upload_name, "converted_from": converted_from}
            file_ids.append(fid)
        except Exception as e:
            err = str(e).lower()
            if "invalid_request_error" in err or "unsupported" in err or "image_parse_error" in err:
                try:
                    move_to_excluded(root, file_path, f"Ошибка загрузки в LLM ({process_name}): {e}")
                except Exception:
                    pass
                excluded.append({"path": rel, "reason": str(e)[:300]})
            else:
                raise

    if not file_ids:
        details = "; ".join(f"{x['path']}: {x['reason'][:80]}" for x in excluded) if excluded else "нет"
        raise ValueError(
            f"Не удалось загрузить ни один файл. Исключенные файлы сохранены в {EXCLUDED_REL}. "
            f"Причины: {details}"
        )

    system_prompt = load_prompt("01_system_v02")
    user_prompt = load_prompt(prompt_name, prompt_vars) + TABLE_CONTEXT_PROMPT_NOTE
    response_json, raw_text = client.call_json_with_files(
        instructions=system_prompt,
        user_text=user_prompt,
        file_ids=file_ids,
        model=model,
        timeout_s=180,
    )
    persist_run_json(project_id, process_name, run_id, "uploaded_files.json", upload_map)
    if excluded:
        persist_run_json(project_id, process_name, run_id, "excluded_files.json", {"excluded": excluded, "dir": EXCLUDED_REL})
    persist_run_artifact(project_id, process_name, run_id, "raw_response.txt", raw_text)
    persist_run_json(project_id, process_name, run_id, output_filename, response_json)
    persist_run_json(
        project_id,
        process_name,
        run_id,
        "run_meta.json",
        {
            "process": process_name,
            "run_id": run_id,
            "model": model,
            "started_at": datetime.utcnow().isoformat(),
            "success": True,
        },
    )
    append_event(project_id, {"process": process_name, "stage": "finish", "run_id": run_id, "status": "success"})
    return run_id, response_json
