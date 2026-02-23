import json
import os
import secrets
import shutil
from datetime import datetime
from pathlib import Path

from ..llm.prompt_loader_v02 import load_prompt
from ..llm.responses_client_v02 import ResponsesClientV02
from .dictionary_service import get_razdel, load_dictionary
from .llm_runtime import (
    SUPPORTED_CONTEXT_EXTS,
    TABLE_CONTEXT_PROMPT_NOTE,
    prepare_context_file_for_upload,
    resolve_openai_model,
)
from .project_storage import persist_run_artifact, persist_run_json, project_root, save_processing_json

EXCLUDED_REL = "01_input/099_excluded"


def _move_to_excluded(root: Path, path: Path, reason: str) -> Path:
    """Copy file to 01_input/099_excluded, preserving name. Returns copied path."""
    try:
        rel = path.relative_to(root)
    except ValueError:
        rel = Path(path.name)
    rel_str = str(rel).replace("\\", "/")
    if rel_str.startswith(EXCLUDED_REL):
        return path
    excluded_dir = root / EXCLUDED_REL
    excluded_dir.mkdir(parents=True, exist_ok=True)
    base = rel.name
    dest = excluded_dir / base
    idx = 1
    while dest.exists() and dest != path:
        stem, suf = (dest.stem, dest.suffix) if dest.suffix else (dest.name, "")
        dest = excluded_dir / f"{stem}_{idx}{suf}"
        idx += 1
    if path != dest:
        shutil.copy2(str(path), str(dest))
    meta_path = dest.with_suffix(dest.suffix + ".excluded_reason.txt")
    meta_path.write_text(reason, encoding="utf-8")
    return dest


def _collect_input_files(project_id: str, razdel_code: str) -> list[Path]:
    root = project_root(project_id)
    excl_prefix = (root / EXCLUDED_REL).as_posix()

    def _skip_excluded(p: Path) -> bool:
        return not p.as_posix().startswith(excl_prefix)

    files: list[Path] = []
    files.extend([p for p in (root / "01_input" / "01_project").rglob("*") if p.is_file() and _skip_excluded(p)])
    qreg = root / "02_processing" / "p2_quality_registry_final.json"
    if qreg.exists():
        files.append(qreg)
    dict_path = Path(__file__).resolve().parent.parent / "data" / "id_dictionary_v02.json"
    if dict_path.exists():
        files.append(dict_path)
    regs_dir = root / "06_regs" / razdel_code
    for reg_name in ("rules.md", "rules.json"):
        p = regs_dir / reg_name
        if p.exists():
            files.append(p)
    sample_dirs = [
        root / "01_input" / "04_samples" / razdel_code / "projects",
        root / "01_input" / "04_samples" / razdel_code / "id",
    ]
    for d in sample_dirs:
        files.extend([p for p in d.rglob("*") if p.is_file() and _skip_excluded(p)])
    return [p for p in files if p.suffix.lower() in SUPPORTED_CONTEXT_EXTS and _skip_excluded(p)]


def _read_project_comment(project_id: str) -> str:
    meta_path = project_root(project_id) / "05_project_meta.json"
    if not meta_path.exists():
        return ""
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
        return (payload.get("comment") or "").strip()
    except json.JSONDecodeError:
        return ""


def _collect_excluded_non_reason_files(root: Path) -> list[Path]:
    excluded_dir = root / EXCLUDED_REL
    if not excluded_dir.exists():
        return []
    files: list[Path] = []
    for p in excluded_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.name.endswith(".excluded_reason.txt"):
            continue
        files.append(p)
    return files


def _build_supplemental_context(root: Path, files: list[Path], max_chars_per_file: int = 12000) -> str:
    chunks: list[str] = []
    for p in files:
        try:
            rel = str(p.relative_to(root)).replace("\\", "/")
        except ValueError:
            rel = p.name
        display_name = p.name
        try:
            upload_name, upload_bytes, _ = prepare_context_file_for_upload(p)
            display_name = upload_name
            text = upload_bytes.decode("utf-8", errors="replace")
        except Exception:
            # Best-effort fallback for plain text-like files.
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
        if len(text) > max_chars_per_file:
            text = text[:max_chars_per_file] + "\n... [truncated]"
        chunks.append(f"\n=== FILE: {rel} (as {display_name}) ===\n{text}")
    if not chunks:
        return ""
    return "\n\nSUPPLEMENTAL NON-PDF CONTEXT (treat as source material):\n" + "\n".join(chunks)


def _normalize_doc_instance(inst: dict, idx: int) -> dict:
    """Ensure doc_instance has required structure for templates and process_p5."""
    out = dict(inst) if isinstance(inst, dict) else {}
    out.setdefault("instance_id", out.get("instance_id") or f"i{idx}")
    out.setdefault("doc_id", "")
    out.setdefault("doc_type_id", "")
    out.setdefault("doc_name", "")
    out.setdefault("doc_number", "")
    out.setdefault("multi", False)
    mult = out.get("multiplier")
    if not isinstance(mult, dict):
        mult = {}
    out["multiplier"] = {
        "axis": mult.get("axis") if mult.get("axis") not in (None, "null") else "",
        "label": mult.get("label") or "",
        "confidence": mult.get("confidence") if isinstance(mult.get("confidence"), (int, float)) else 0,
    }
    out.setdefault("work_scope", [])
    if not isinstance(out["work_scope"], list):
        out["work_scope"] = []
    out.setdefault("fields", {})
    if not isinstance(out["fields"], dict):
        out["fields"] = {}
    out.setdefault("overall_status", "needs_extraction")
    out.setdefault("evidence", [])
    if not isinstance(out["evidence"], list):
        out["evidence"] = []
    out.setdefault("user_note", "")
    return out


def _normalize_payload(payload: dict, razdel_code: str, selected_doc_type_ids: list[str]) -> dict:
    out = payload if isinstance(payload, dict) else {}
    out.setdefault("razdel_code", razdel_code)
    out.setdefault("selected_doc_type_ids", selected_doc_type_ids)
    out.setdefault("doc_instances", [])
    out.setdefault("open_questions", [])
    out.setdefault("issues", [])
    if not isinstance(out["doc_instances"], list):
        out["doc_instances"] = []
    out["doc_instances"] = [_normalize_doc_instance(inst, i) for i, inst in enumerate(out["doc_instances"])]
    return out


def _mock_payload(razdel_code: str, selected_doc_type_ids: list[str]) -> dict:
    docs = []
    for idx, doc_type_id in enumerate(selected_doc_type_ids, start=1):
        docs.append(
            {
                "instance_id": f"{doc_type_id}-i{idx:02d}",
                "doc_id": "AOSR" if doc_type_id == "acts_hidden" else "AOOK",
                "doc_type_id": doc_type_id,
                "doc_name": "Акт освидетельствования скрытых работ" if doc_type_id == "acts_hidden" else "Акт освидетельствования ответственных конструкций",
                "doc_number": "",
                "multi": doc_type_id == "acts_hidden",
                "multiplier": {"axis": "section", "label": "Секция 1", "confidence": 0.7},
                "work_scope": [
                    {
                        "work_id": f"w{idx:02d}",
                        "work_name": "Армирование конструкций",
                        "work_group_id": "kj_foundation",
                        "materials_refs": [],
                        "gost_snip_refs": [],
                        "status": "needs_extraction",
                        "confidence": 0.4,
                        "source": {"file": "", "page": 0, "snippet": ""},
                    }
                ],
                "fields": {
                    "field_1_presented_works": {"value": "", "status": "needs_extraction", "confidence": 0.4, "sources": []},
                    "field_2_project_basis": {"value": "", "status": "needs_extraction", "confidence": 0.4, "sources": []},
                    "field_3_materials_used": {"value": "", "status": "needs_extraction", "confidence": 0.4, "sources": []},
                    "field_4_conformance_docs": {"value": "", "status": "needs_extraction", "confidence": 0.4, "sources": []},
                    "field_dates_start_end": {"value": "", "status": "needs_extraction", "confidence": 0.4, "sources": []},
                },
                "overall_status": "needs_extraction",
                "evidence": [],
            }
        )
    return {
        "razdel_code": razdel_code,
        "selected_doc_type_ids": selected_doc_type_ids,
        "doc_instances": docs,
        "open_questions": [],
        "issues": [],
    }


def run_process_p4b_build_doc_plan(
    project_id: str,
    razdel_code: str,
    selected_doc_type_ids: list[str],
    feedback_rules: str = "",
) -> tuple[str, dict]:
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)
    model = resolve_openai_model()
    root = project_root(project_id)
    input_files = _collect_input_files(project_id, razdel_code)
    excluded_files_for_context = _collect_excluded_non_reason_files(root)
    pdf_input_files = [p for p in input_files if p.suffix.lower() == ".pdf"]
    non_pdf_context_files = [p for p in input_files if p.suffix.lower() != ".pdf"]
    supplemental_context = _build_supplemental_context(root, non_pdf_context_files + excluded_files_for_context)
    razdel = get_razdel(razdel_code)
    user_comment = _read_project_comment(project_id)
    dictionary_hint = f"dict_size={len((load_dictionary().get('razdels') or []))}"
    quality_registry_hint = "p2_quality_registry_final.json attached"
    regs_hint = f"regs_dir=06_regs/{razdel_code}"
    samples_hint = f"samples_dir=01_input/04_samples/{razdel_code}"

    api_key = ""
    try:
        from django.conf import settings

        api_key = getattr(settings, "OPENAI_API_KEY", "") or ""
    except Exception:
        pass
    if not api_key:
        api_key = os.getenv("OPENAI_API_KEY", "") or ""

    # Mock mode path
    if root.exists() and (Path(__file__).resolve().parents[2] / "config" / "settings.py").exists():
        from django.conf import settings  # late import to avoid setup side effects in tests

        if settings.MOCK_MODE:
            payload = _mock_payload(razdel_code, selected_doc_type_ids)
            payload = _normalize_payload(payload, razdel_code, selected_doc_type_ids)
            save_processing_json(project_id, "p4_doc_types_selection.json", {"selected_doc_type_ids": selected_doc_type_ids})
            save_processing_json(project_id, "p4b_doc_instances_v1.json", payload)
            save_processing_json(project_id, "p4b_doc_instances_final.json", payload)
            persist_run_json(project_id, "process_p4b", run_id, "uploaded_files.json", {"files": []})
            persist_run_artifact(project_id, "process_p4b", run_id, "raw_response.txt", json.dumps(payload, ensure_ascii=False))
            persist_run_json(project_id, "process_p4b", run_id, "p4b_doc_instances_v1.json", payload)
            persist_run_json(
                project_id,
                "process_p4b",
                run_id,
                "run_meta.json",
                {"process": "process_p4b", "model": model, "success": True, "mock_mode": True},
            )
            return run_id, payload, []

    client = ResponsesClientV02(api_key=api_key or None)
    upload_map: dict[str, dict] = {}
    file_ids: list[str] = []
    items: list[tuple[Path, str, str]] = []
    excluded: list[dict] = []

    for p in pdf_input_files:
        try:
            rel = str(p.relative_to(root)).replace("\\", "/")
        except ValueError:
            rel = p.name
        try:
            upload_name, upload_bytes, converted_from = prepare_context_file_for_upload(p)
            fid = client.upload_file_bytes(upload_name, upload_bytes)
            upload_map[rel] = {"file_id": fid, "upload_name": upload_name, "converted_from": converted_from}
            file_ids.append(fid)
            items.append((p, rel, fid))
        except Exception as e:
            err_str = str(e).lower()
            if "image_parse_error" in err_str or "unsupported image" in err_str or "invalid_request_error" in err_str:
                try:
                    if root in p.parents or p == root or str(p).startswith(str(root)):
                        _move_to_excluded(root, p, f"Ошибка загрузки: {e}")
                except (ValueError, TypeError):
                    pass
                excluded.append({"path": rel, "reason": str(e)[:300]})
            else:
                raise

    if not file_ids:
        excluded_msg = "; ".join(f"{x['path']}: {x['reason'][:80]}" for x in excluded) if excluded else ""
        raise ValueError(
            f"Не удалось загрузить ни один файл. Исключённые файлы: {excluded_msg or 'нет'}. "
            "Проверьте формат файлов (PDF/изображения могут вызывать image_parse_error)."
        )

    def _call_with_ids(ids: list[str]):
        return client.call_json_with_files(
            instructions=system_prompt,
            user_text=user_prompt,
            file_ids=ids,
            model=model,
            timeout_s=1500,
        )

    system_prompt = load_prompt("01_system_v02")
    user_prompt = load_prompt(
        "04b_p4b_build_doc_instances",
        {
            "project_id": project_id,
            "razdel_code": razdel_code,
            "razdel_name": razdel.get("razdel_name", razdel_code),
            "selected_doc_type_ids_json": selected_doc_type_ids,
            "user_comment": user_comment,
            "dictionary_hint": dictionary_hint,
            "quality_registry_hint": quality_registry_hint,
            "regs_hint": regs_hint,
            "samples_hint": samples_hint,
            "feedback_rules": feedback_rules or "нет дополнительных правил",
        },
    ) + TABLE_CONTEXT_PROMPT_NOTE + supplemental_context

    def _is_file_support_error(msg: str) -> bool:
        m = (msg or "").lower()
        return (
            "image_parse_error" in m
            or "unsupported image" in m
            or "unsupported_file" in m
            or ("invalid_request_error" in m and "file type" in m)
        )

    try:
        payload, raw = _call_with_ids(file_ids)
    except Exception as e:
        err_str = str(e).lower()
        if _is_file_support_error(err_str):
            bad_path_rel = None
            for i, (path, rel, fid) in enumerate(items):
                trial_ids = [x[2] for j, x in enumerate(items) if j != i]
                try:
                    payload, raw = _call_with_ids(trial_ids)
                    bad_path_rel = rel
                    try:
                        if root in path.parents or path == root or str(path).startswith(str(root)):
                            _move_to_excluded(root, path, f"Ошибка обработки API: {e}")
                    except (ValueError, TypeError):
                        pass
                    excluded.append({"path": rel, "reason": str(e)[:300]})
                    upload_map.pop(rel, None)
                    items = [x for j, x in enumerate(items) if j != i]
                    break
                except Exception:
                    continue
            if bad_path_rel is None:
                # Fallback for APIs/models that only accept PDF as input_file.
                pdf_items = [x for x in items if x[0].suffix.lower() == ".pdf"]
                non_pdf_items = [x for x in items if x[0].suffix.lower() != ".pdf"]
                for path, rel, _ in non_pdf_items:
                    try:
                        if root in path.parents or path == root or str(path).startswith(str(root)):
                            _move_to_excluded(root, path, f"File type not accepted by API: {e}")
                    except (ValueError, TypeError):
                        pass
                    excluded.append({"path": rel, "reason": f"unsupported_file: {str(e)[:260]}"})
                    upload_map.pop(rel, None)
                payload, raw = _call_with_ids([x[2] for x in pdf_items])
        else:
            raise
    payload = _normalize_payload(payload, razdel_code, selected_doc_type_ids)
    if not isinstance(payload.get("doc_instances"), list):
        raise ValueError("p4b invalid payload: doc_instances missing")

    save_processing_json(project_id, "p4_doc_types_selection.json", {"selected_doc_type_ids": selected_doc_type_ids})
    save_processing_json(project_id, "p4b_doc_instances_v1.json", payload)
    save_processing_json(project_id, "p4b_doc_instances_final.json", payload)
    persist_run_json(project_id, "process_p4b", run_id, "uploaded_files.json", upload_map)
    persist_run_artifact(project_id, "process_p4b", run_id, "raw_response.txt", raw)
    persist_run_json(project_id, "process_p4b", run_id, "p4b_doc_instances_v1.json", payload)
    persist_run_json(
        project_id,
        "process_p4b",
        run_id,
        "run_meta.json",
        {"process": "process_p4b", "model": model, "success": True, "mock_mode": False, "excluded": excluded},
    )
    save_processing_json(project_id, "p4b_excluded_files.json", {"excluded": excluded, "run_id": run_id})
    return run_id, payload, excluded
