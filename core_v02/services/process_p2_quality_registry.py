from pathlib import Path
import os
import re

from .dictionary_service import get_razdel
from .llm_runtime import run_llm_json_process
from .log_service import append_event
from .quality_gate import run_quality_gate
from .project_storage import (
    duplicate_output_to_run,
    persist_run_json,
    processing_path,
    project_root,
    save_processing_json,
)


ALLOWED_STATUS = {"ok", "needs_extraction", "needs_disambiguation", "blocked_missing_source"}
PROJECT_CIPHER_BAD_HINTS = ("жилой комплекс", "расположенный по адресу", "этап")
TOO_LARGE_HINTS = (
    "rate_limit_exceeded",
    "tokens per min",
    "request too large",
    "error code: 429",
)


def _slug(value: str) -> str:
    cleaned = re.sub(r"\s+", "_", (value or "").strip().lower())
    cleaned = re.sub(r"[^a-zA-Z0-9а-яА-Я_]+", "", cleaned)
    return cleaned or "material"


def _default_source(file_ref: str = "") -> dict:
    return {"file": file_ref, "page": "", "snippet": ""}


def _quality_ref(path: Path) -> str:
    return f"01_input/02_quality_docs/{path.name}"


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _looks_like_too_large_error(exc: Exception) -> bool:
    text = (str(exc) or "").lower()
    return any(h in text for h in TOO_LARGE_HINTS)


def _chunk_by_limits(paths: list[Path], max_files: int, max_bytes: int) -> list[list[Path]]:
    if not paths:
        return []
    batches: list[list[Path]] = []
    current: list[Path] = []
    current_bytes = 0
    for p in paths:
        size = p.stat().st_size if p.exists() else 0
        can_fit = bool(current) and len(current) < max_files and (current_bytes + size) <= max_bytes
        if can_fit:
            current.append(p)
            current_bytes += size
            continue
        if current:
            batches.append(current)
        current = [p]
        current_bytes = size
    if current:
        batches.append(current)
    return batches


def _normalize_rel_path(value: str) -> str:
    return (value or "").strip().replace("\\", "/")


def _extract_protocol_number(value: str) -> str:
    text = (value or "").lower()
    patterns = (
        r"№\s*0*(\d+)",
        r"протокол[^0-9]{0,20}0*(\d+)",
        r"protocol[_\-\s]*0*(\d+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        raw = (match.group(1) or "").strip()
        if not raw:
            continue
        try:
            return str(int(raw))
        except ValueError:
            return raw
    return ""


def _looks_like_project_name_not_cipher(value: str) -> bool:
    text = (value or "").strip().lower()
    if not text:
        return False
    if any(hint in text for hint in PROJECT_CIPHER_BAD_HINTS):
        return True
    # Ciphers are usually compact; long narrative text is suspicious.
    return len(text) > 60 and text.count(" ") > 5


def _build_quality_indexes(quality_refs: list[str]) -> tuple[set[str], dict[str, str], dict[str, str]]:
    exact = set(quality_refs)
    by_name: dict[str, str] = {}
    by_protocol_no: dict[str, str] = {}
    for ref in quality_refs:
        name = Path(ref).name.lower()
        by_name[name] = ref
        proto_no = _extract_protocol_number(name) or _extract_protocol_number(ref)
        if proto_no and proto_no not in by_protocol_no:
            by_protocol_no[proto_no] = ref
    return exact, by_name, by_protocol_no


def _map_quality_ref(candidate: str, exact: set[str], by_name: dict[str, str], by_protocol_no: dict[str, str]) -> str:
    c = _normalize_rel_path(candidate)
    if not c:
        return ""
    if c in exact:
        return c
    name = Path(c).name.lower()
    if name in by_name:
        return by_name[name]
    proto_no = _extract_protocol_number(c)
    if proto_no and proto_no in by_protocol_no:
        return by_protocol_no[proto_no]
    return ""


def _enforce_quality_file_coverage(payload: dict, quality_refs: list[str]) -> tuple[int, int]:
    if not quality_refs:
        return 0, 0
    exact, by_name, by_protocol_no = _build_quality_indexes(quality_refs)
    remapped = 0
    synthetic_docs = 0
    unresolved_docs: list[dict] = []

    for material in payload.get("materials") or []:
        docs = material.get("docs") if isinstance(material.get("docs"), list) else []
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            source = doc.get("source") if isinstance(doc.get("source"), dict) else _default_source()
            mapped = _map_quality_ref(doc.get("file_ref", ""), exact, by_name, by_protocol_no)
            if not mapped:
                mapped = _map_quality_ref(source.get("file", ""), exact, by_name, by_protocol_no)
            if mapped:
                if doc.get("file_ref") != mapped:
                    remapped += 1
                doc["file_ref"] = mapped
                if not source.get("file") or source.get("file") != mapped:
                    source["file"] = mapped
                doc["source"] = source
            else:
                unresolved_docs.append(doc)

    covered: set[str] = set()
    for material in payload.get("materials") or []:
        for doc in material.get("docs") or []:
            file_ref = _normalize_rel_path(doc.get("file_ref", ""))
            if file_ref in exact:
                covered.add(file_ref)

    missing = [x for x in quality_refs if x not in covered]
    for doc in unresolved_docs:
        target = missing.pop(0) if missing else quality_refs[0]
        doc["file_ref"] = target
        source = doc.get("source") if isinstance(doc.get("source"), dict) else _default_source()
        source["file"] = target
        doc["source"] = source
        if doc.get("status") == "ok":
            doc["status"] = "needs_disambiguation"
        remapped += 1
        covered.add(target)

    missing = [x for x in quality_refs if x not in covered]
    for idx, file_ref in enumerate(missing, start=1):
        stem = Path(file_ref).stem.replace("_", " ").replace("-", " ").strip() or f"Материал missing {idx}"
        payload.setdefault("materials", []).append(
            {
                "material_id": f"mat-missing-{idx:03d}",
                "material_name": stem,
                "material_norm_name": _slug(stem),
                "status": "needs_disambiguation",
                "confidence": 0,
                "source": _default_source(file_ref),
                "docs": [
                    {
                        "doc_kind": "документ качества",
                        "doc_number": "б/н",
                        "doc_date": "б/д",
                        "volume": "needs_extraction",
                        "manufacturer": "needs_extraction",
                        "issuer": "needs_extraction",
                        "file_ref": file_ref,
                        "status": "needs_disambiguation",
                        "confidence": 0,
                        "source": _default_source(file_ref),
                    }
                ],
            }
        )
        synthetic_docs += 1

    if remapped or synthetic_docs:
        payload.setdefault("agent_comments", []).append(
            {
                "comment": (
                    f"Пост-валидация Process 2: remap file_ref={remapped}, "
                    f"добавлено синтетических docs для покрытия файлов={synthetic_docs}."
                ),
                "source": _default_source("01_input/02_quality_docs"),
            }
        )
    return remapped, synthetic_docs


def _normalize_doc(doc: dict, file_ref_fallback: str) -> dict:
    item = dict(doc or {})
    status = (item.get("status") or "needs_extraction").strip()
    if status not in ALLOWED_STATUS:
        status = "needs_extraction"
    file_ref = (item.get("file_ref") or file_ref_fallback or "").strip()
    if not file_ref and isinstance(item.get("source"), dict):
        file_ref = (item["source"].get("file") or "").strip()
    if file_ref and "/" not in file_ref and "\\" not in file_ref:
        file_ref = f"01_input/02_quality_docs/{file_ref}"
    source = item.get("source") if isinstance(item.get("source"), dict) else _default_source(file_ref)
    if not source.get("file"):
        source["file"] = file_ref
    return {
        "doc_kind": (item.get("doc_kind") or "документ качества").strip(),
        "doc_number": (item.get("doc_number") or "б/н").strip() or "б/н",
        "doc_date": (item.get("doc_date") or "б/д").strip() or "б/д",
        "volume": (item.get("volume") or "needs_extraction").strip() or "needs_extraction",
        "manufacturer": (item.get("manufacturer") or "needs_extraction").strip(),
        "issuer": (item.get("issuer") or "needs_extraction").strip(),
        "file_ref": file_ref,
        "status": status,
        "confidence": float(item.get("confidence") or 0),
        "source": source,
    }


def _normalize_payload(payload: dict, quality_files: list[Path], project_id: str) -> dict:
    data = payload if isinstance(payload, dict) else {}
    razdel = get_razdel("KJ")
    project_cipher_raw = ""
    project_cipher_status = "needs_extraction"
    project_cipher_confidence = 0.0
    project_cipher_source = _default_source()
    if isinstance(data.get("project_cipher"), dict):
        p = data.get("project_cipher") or {}
        project_cipher_raw = (p.get("value") or "").strip()
        project_cipher_status = (p.get("status") or "needs_extraction").strip() or "needs_extraction"
        project_cipher_confidence = float(p.get("confidence") or 0)
        project_cipher_source = p.get("source") if isinstance(p.get("source"), dict) else _default_source()
    # Reject accidental fallback to project id/name-like value.
    if (
        not project_cipher_raw
        or project_cipher_raw == project_id
        or "__" in project_cipher_raw
        or _looks_like_project_name_not_cipher(project_cipher_raw)
    ):
        project_cipher_raw = ""
        project_cipher_status = "needs_extraction"
        project_cipher_confidence = 0.0
        project_cipher_source = _default_source()

    out = {
        "project_cipher": {
            "value": project_cipher_raw,
            "status": project_cipher_status,
            "confidence": project_cipher_confidence,
            "source": project_cipher_source,
        },
        "razdel": {
            "razdel_code": ((data.get("razdel") or {}).get("razdel_code") if isinstance(data.get("razdel"), dict) else "") or razdel.get("razdel_code", "KJ"),
            "razdel_name": ((data.get("razdel") or {}).get("razdel_name") if isinstance(data.get("razdel"), dict) else "") or razdel.get("razdel_name", "КЖ"),
            "status": ((data.get("razdel") or {}).get("status") if isinstance(data.get("razdel"), dict) else "ok") or "ok",
            "confidence": float(((data.get("razdel") or {}).get("confidence") if isinstance(data.get("razdel"), dict) else 0) or 0),
            "source": ((data.get("razdel") or {}).get("source") if isinstance(data.get("razdel"), dict) else _default_source()),
        },
        "materials": [],
        "agent_comments": [],
    }

    quality_refs = [_quality_ref(p) for p in quality_files]
    raw_materials = data.get("materials") if isinstance(data.get("materials"), list) else []
    for idx, m in enumerate(raw_materials, start=1):
        if not isinstance(m, dict):
            continue
        m_name = (
            (m.get("material_name") or "").strip()
            or (m.get("name") or "").strip()
            or f"Материал {idx}"
        )
        material = {
            "material_id": (m.get("material_id") or f"mat-{idx:03d}").strip(),
            "material_name": m_name,
            "material_norm_name": (m.get("material_norm_name") or _slug(m_name)).strip(),
            "status": (m.get("status") or "needs_extraction").strip(),
            "confidence": float(m.get("confidence") or 0),
            "source": m.get("source") if isinstance(m.get("source"), dict) else _default_source(),
            "docs": [],
        }
        fallback_ref = ""
        if isinstance(material["source"], dict):
            fallback_ref = (material["source"].get("file") or "").strip()
        if not fallback_ref and quality_refs:
            fallback_ref = quality_refs[min(idx - 1, len(quality_refs) - 1)]
        docs = m.get("docs") if isinstance(m.get("docs"), list) else []
        if docs:
            material["docs"] = [_normalize_doc(d, fallback_ref) for d in docs if isinstance(d, dict)]
        else:
            material["docs"] = [
                _normalize_doc(
                    {
                        "doc_kind": "документ качества",
                        "doc_number": "б/н",
                        "doc_date": "б/д",
                        "volume": "needs_extraction",
                        "manufacturer": "needs_extraction",
                        "issuer": "needs_extraction",
                        "status": "needs_extraction",
                        "source": material["source"],
                    },
                    fallback_ref,
                )
            ]
        out["materials"].append(material)

    if not out["materials"] and quality_refs:
        for idx, file_ref in enumerate(quality_refs, start=1):
            name = Path(file_ref).stem.replace("_", " ").replace("-", " ").strip() or f"Материал {idx}"
            out["materials"].append(
                {
                    "material_id": f"mat-{idx:03d}",
                    "material_name": name,
                    "material_norm_name": _slug(name),
                    "status": "needs_extraction",
                    "confidence": 0,
                    "source": _default_source(file_ref),
                    "docs": [
                        _normalize_doc(
                            {
                                "doc_kind": "документ качества",
                                "doc_number": "б/н",
                                "doc_date": "б/д",
                                "volume": "needs_extraction",
                                "manufacturer": "needs_extraction",
                                "issuer": "needs_extraction",
                                "status": "needs_extraction",
                            },
                            file_ref,
                        )
                    ],
                }
            )
    raw_comments = data.get("agent_comments")
    if not isinstance(raw_comments, list):
        raw_comments = data.get("comments")
    if isinstance(raw_comments, list):
        for c in raw_comments:
            if not isinstance(c, dict):
                continue
            comment = (c.get("comment") or c.get("text") or "").strip()
            source = c.get("source") if isinstance(c.get("source"), dict) else _default_source()
            if comment:
                out["agent_comments"].append({"comment": comment, "source": source})
    if not out["agent_comments"]:
        for m in out["materials"][:5]:
            source = m.get("source") if isinstance(m.get("source"), dict) else _default_source()
            out["agent_comments"].append(
                {
                    "comment": f"Материал '{m.get('material_name')}' распознан из документа качества.",
                    "source": source,
                }
            )
    _enforce_quality_file_coverage(out, quality_refs)
    return out


def _mock_payload(project_id: str, comment: str, quality_files: list[Path]) -> dict:
    razdel = get_razdel("KJ")
    materials: list[dict] = []
    if not quality_files:
        materials = []
    else:
        for idx, file_path in enumerate(quality_files, start=1):
            file_ref = f"01_input/02_quality_docs/{file_path.name}"
            stem = file_path.stem.replace("_", " ").replace("-", " ").strip() or f"Материал {idx}"
            materials.append(
                {
                    "material_id": f"mat-{idx:03d}",
                    "material_name": stem,
                    "material_norm_name": stem.lower().replace(" ", "_"),
                    "status": "ok",
                    "confidence": 0.56,
                    "source": {"file": file_ref, "page": "1", "snippet": stem},
                    "docs": [
                        {
                            "doc_kind": "паспорт",
                            "doc_number": f"{idx:03d}/Q",
                            "doc_date": "01.01.2026",
                            "manufacturer": "Не определено",
                            "issuer": "Не определено",
                            "file_ref": file_ref,
                            "status": "needs_extraction",
                            "confidence": 0.4,
                            "source": {"file": file_ref, "page": "1", "snippet": "auto"},
                        }
                    ],
                }
            )
    return {
        "project_cipher": {
            "value": f"PRJ-{project_id.split('__')[-1]}",
            "status": "ok",
            "confidence": 0.66,
            "source": {"file": "01_input/01_project/mock_project.pdf", "page": "1", "snippet": comment or "auto"},
        },
        "razdel": {
            "razdel_code": razdel.get("razdel_code", "KJ"),
            "razdel_name": razdel.get("razdel_name", "КЖ"),
            "status": "ok",
            "confidence": 0.6,
            "source": {"file": "01_input/01_project/mock_project.pdf", "page": "1", "snippet": "КЖ"},
        },
        "materials": materials,
        "agent_comments": [
            {
                "comment": "Проверьте объемы в документах, где указано needs_extraction.",
                "source": {"file": "01_input/02_quality_docs", "page": "", "snippet": "auto"},
            }
        ],
    }


def _pick_project_cipher(payloads: list[dict], project_id: str) -> dict:
    for payload in payloads:
        project_cipher = payload.get("project_cipher")
        if not isinstance(project_cipher, dict):
            continue
        value = (project_cipher.get("value") or "").strip()
        if not value:
            continue
        if value == project_id or "__" in value or _looks_like_project_name_not_cipher(value):
            continue
        return project_cipher
    return {"value": "", "status": "needs_extraction", "confidence": 0, "source": _default_source()}


def _pick_razdel(payloads: list[dict]) -> dict:
    for payload in payloads:
        razdel = payload.get("razdel")
        if isinstance(razdel, dict):
            return razdel
    info = get_razdel("KJ")
    return {
        "razdel_code": info.get("razdel_code", "KJ"),
        "razdel_name": info.get("razdel_name", "КЖ"),
        "status": "ok",
        "confidence": 0.0,
        "source": _default_source(),
    }


def _merge_partial_payloads(payloads: list[dict], project_id: str) -> dict:
    merged = {
        "project_cipher": _pick_project_cipher(payloads, project_id),
        "razdel": _pick_razdel(payloads),
        "materials": [],
        "agent_comments": [],
    }
    for payload in payloads:
        mats = payload.get("materials")
        if isinstance(mats, list):
            merged["materials"].extend([x for x in mats if isinstance(x, dict)])
        comments = payload.get("agent_comments")
        if isinstance(comments, list):
            merged["agent_comments"].extend([x for x in comments if isinstance(x, dict)])
    return merged


def _run_batch_with_auto_split(
    *,
    project_id: str,
    prompt_vars: dict,
    comment: str,
    context_files: list[Path],
    quality_files: list[Path],
    batch_no: int,
    depth: int = 0,
) -> list[tuple[str, dict]]:
    if not quality_files:
        return []
    all_files = list(context_files) + list(quality_files)
    try:
        run_id, payload = run_llm_json_process(
            project_id=project_id,
            process_name="process_2",
            prompt_name="02_p2_quality_registry_v02",
            prompt_vars=prompt_vars,
            files=all_files,
            output_filename="p2_quality_registry_v1.json",
            mock_payload=_mock_payload(project_id, comment, quality_files),
        )
        append_event(
            project_id,
            {
                "process": "process_2",
                "stage": "batch_success",
                "batch_no": batch_no,
                "depth": depth,
                "quality_files": len(quality_files),
                "context_files": len(context_files),
                "run_id": run_id,
            },
        )
        return [(run_id, payload)]
    except Exception as exc:
        if not _looks_like_too_large_error(exc):
            raise
        if len(quality_files) > 1:
            mid = len(quality_files) // 2
            left = quality_files[:mid]
            right = quality_files[mid:]
            append_event(
                project_id,
                {
                    "process": "process_2",
                    "stage": "batch_split",
                    "batch_no": batch_no,
                    "depth": depth,
                    "left": len(left),
                    "right": len(right),
                    "reason": str(exc)[:300],
                },
            )
            out: list[tuple[str, dict]] = []
            out.extend(
                _run_batch_with_auto_split(
                    project_id=project_id,
                    prompt_vars=prompt_vars,
                    comment=comment,
                    context_files=context_files,
                    quality_files=left,
                    batch_no=batch_no,
                    depth=depth + 1,
                )
            )
            out.extend(
                _run_batch_with_auto_split(
                    project_id=project_id,
                    prompt_vars=prompt_vars,
                    comment=comment,
                    context_files=[],
                    quality_files=right,
                    batch_no=batch_no,
                    depth=depth + 1,
                )
            )
            return out
        if context_files:
            append_event(
                project_id,
                {
                    "process": "process_2",
                    "stage": "batch_retry_without_context",
                    "batch_no": batch_no,
                    "depth": depth,
                    "reason": str(exc)[:300],
                },
            )
            return _run_batch_with_auto_split(
                project_id=project_id,
                prompt_vars=prompt_vars,
                comment=comment,
                context_files=[],
                quality_files=quality_files,
                batch_no=batch_no,
                depth=depth + 1,
            )
        raise


def run_process_p2(
    project_id: str,
    comment: str,
    feedback_rules: str = "",
    agent_feedback_rules: str = "",
) -> tuple[str, dict]:
    root = project_root(project_id)
    files_project = [p for p in (root / "01_input" / "01_project").rglob("*") if p.is_file()]
    files_quality = [p for p in (root / "01_input" / "02_quality_docs").rglob("*") if p.is_file()]
    files_ojr = [p for p in (root / "01_input" / "03_ojr").rglob("*") if p.is_file()]
    prompt_vars = {
        "project_id": project_id,
        "comment": comment,
        "dictionary_json": get_razdel(None),
        "feedback_rules": feedback_rules or "нет дополнительных правил",
        "agent_feedback_rules": agent_feedback_rules or "нет самогенерированных правил",
    }
    if not files_quality:
        run_id, raw_payload = run_llm_json_process(
            project_id=project_id,
            process_name="process_2",
            prompt_name="02_p2_quality_registry_v02",
            prompt_vars=prompt_vars,
            files=list(files_project) + list(files_ojr),
            output_filename="p2_quality_registry_v1.json",
            mock_payload=_mock_payload(project_id, comment, files_quality),
        )
        payload = _normalize_payload(raw_payload, files_quality, project_id)
        all_input_files = list(files_project) + list(files_quality) + list(files_ojr)
        manifest, gate_report = run_quality_gate(
            process_name="process_2",
            root=root,
            payload=payload,
            input_files=all_input_files,
            excluded_refs=[],
            required_files=files_quality,
        )
        persist_run_json(project_id, "process_2", run_id, "input_manifest.json", manifest)
        persist_run_json(project_id, "process_2", run_id, "quality_gate_report.json", gate_report)
        if not gate_report.get("pass"):
            append_event(
                project_id,
                {"process": "process_2", "stage": "quality_gate_failed", "run_id": run_id, "reason": gate_report.get("summary", "")},
            )
            raise ValueError(f"Process 2 quality gate failed: {gate_report.get('summary', 'unknown reason')}")
        save_processing_json(project_id, "p2_quality_registry_v1.json", payload)
        duplicate_output_to_run(project_id, "process_2", run_id, "p2_quality_registry_v1.json")
        save_processing_json(project_id, "p2_quality_registry_final.json", payload)
        return run_id, payload
    max_files = _env_int("P2_MAX_FILES_PER_BATCH", default=3, minimum=1)
    max_bytes = _env_int("P2_MAX_BATCH_BYTES", default=2_000_000, minimum=100_000)
    quality_batches = _chunk_by_limits(files_quality, max_files=max_files, max_bytes=max_bytes)
    total_quality_bytes = sum((p.stat().st_size if p.exists() else 0) for p in files_quality)
    append_event(
        project_id,
        {
            "process": "process_2",
            "stage": "batch_plan",
            "quality_files": len(files_quality),
            "quality_total_bytes": total_quality_bytes,
            "project_files": len(files_project),
            "ojr_files": len(files_ojr),
            "batches_planned": len(quality_batches),
            "max_files_per_batch": max_files,
            "max_batch_bytes": max_bytes,
            "env_model": (os.getenv("OPENAI_MODEL") or "").strip(),
            "env_fallback_model": (os.getenv("OPENAI_FALLBACK_MODEL") or "").strip(),
        },
    )
    all_runs: list[tuple[str, dict]] = []
    for idx, batch in enumerate(quality_batches, start=1):
        context = (files_project + files_ojr) if idx == 1 else []
        all_runs.extend(
            _run_batch_with_auto_split(
                project_id=project_id,
                prompt_vars=prompt_vars,
                comment=comment,
                context_files=context,
                quality_files=batch,
                batch_no=idx,
            )
        )
    if not all_runs:
        raise ValueError("Process 2: no batch runs were produced.")
    run_id = all_runs[-1][0]
    merged_payload = _merge_partial_payloads([x[1] for x in all_runs], project_id)
    payload = _normalize_payload(merged_payload, files_quality, project_id)
    all_input_files = list(files_project) + list(files_quality) + list(files_ojr)
    manifest, gate_report = run_quality_gate(
        process_name="process_2",
        root=root,
        payload=payload,
        input_files=all_input_files,
        excluded_refs=[],
        required_files=files_quality,
    )
    persist_run_json(project_id, "process_2", run_id, "input_manifest.json", manifest)
    persist_run_json(project_id, "process_2", run_id, "quality_gate_report.json", gate_report)
    if not gate_report.get("pass"):
        append_event(
            project_id,
            {"process": "process_2", "stage": "quality_gate_failed", "run_id": run_id, "reason": gate_report.get("summary", "")},
        )
        raise ValueError(f"Process 2 quality gate failed: {gate_report.get('summary', 'unknown reason')}")
    save_processing_json(project_id, "p2_quality_registry_v1.json", payload)
    duplicate_output_to_run(project_id, "process_2", run_id, "p2_quality_registry_v1.json")
    # Always refresh final baseline after a new Process 2 run.
    save_processing_json(project_id, "p2_quality_registry_final.json", payload)
    return run_id, payload
