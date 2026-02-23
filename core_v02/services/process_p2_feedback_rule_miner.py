from .llm_runtime import run_llm_json_process
from .process_p2_quality_registry import _default_source
from .project_storage import duplicate_output_to_run, project_root, save_processing_json


def _parse_row_key_file_ref(row_key: str) -> str:
    parts = (row_key or "").split("|")
    if not parts:
        return ""
    tail = (parts[-1] or "").strip().replace("\\", "/")
    if tail.startswith("01_input/02_quality_docs/"):
        return tail
    return ""


def _compact_rules_text(text: str, max_lines: int = 30) -> str:
    lines = [x.strip() for x in (text or "").splitlines() if x.strip()]
    return "\n".join(lines[-max_lines:])


def _compact_registry(payload: dict, max_rows: int = 24) -> dict:
    data = payload if isinstance(payload, dict) else {}
    out = {
        "project_cipher_status": ((data.get("project_cipher") or {}).get("status") if isinstance(data.get("project_cipher"), dict) else ""),
        "materials_count": len(data.get("materials") or []),
        "docs_count": 0,
        "rows_sample": [],
    }
    for material in data.get("materials") or []:
        m_name = (material.get("material_name") or "").strip()
        m_norm = (material.get("material_norm_name") or "").strip()
        docs = material.get("docs") if isinstance(material.get("docs"), list) else []
        out["docs_count"] += len(docs)
        for doc in docs:
            if len(out["rows_sample"]) >= max_rows:
                break
            out["rows_sample"].append(
                {
                    "material_name": m_name,
                    "material_norm_name": m_norm,
                    "doc_kind": (doc.get("doc_kind") or "").strip(),
                    "doc_number": (doc.get("doc_number") or "").strip(),
                    "doc_date": (doc.get("doc_date") or "").strip(),
                    "volume": (doc.get("volume") or "").strip(),
                    "manufacturer": (doc.get("manufacturer") or "").strip(),
                    "issuer": (doc.get("issuer") or "").strip(),
                    "file_ref": (doc.get("file_ref") or "").strip(),
                    "status": (doc.get("status") or "").strip(),
                }
            )
        if len(out["rows_sample"]) >= max_rows:
            break
    return out


def _select_context_files(root, edits: list[dict], max_project_files: int = 1, max_quality_files: int = 4) -> list:
    project_files = [p for p in (root / "01_input" / "01_project").rglob("*") if p.is_file()]
    selected = project_files[:max_project_files]

    desired_refs: list[str] = []
    for edit in edits:
        ref = _parse_row_key_file_ref(edit.get("row_key") or "")
        if ref and ref not in desired_refs:
            desired_refs.append(ref)

    qdir = root / "01_input" / "02_quality_docs"
    by_ref = {}
    for p in qdir.rglob("*"):
        if p.is_file():
            rel = f"01_input/02_quality_docs/{p.name}"
            by_ref[rel] = p

    for ref in desired_refs:
        p = by_ref.get(ref)
        if p is not None and p not in selected:
            selected.append(p)
        if len([x for x in selected if x in by_ref.values()]) >= max_quality_files:
            break

    if len([x for x in selected if x in by_ref.values()]) < max_quality_files:
        for p in by_ref.values():
            if p in selected:
                continue
            selected.append(p)
            if len([x for x in selected if x in by_ref.values()]) >= max_quality_files:
                break
    return selected


def _normalize_feedback_payload(payload: dict) -> dict:
    data = payload if isinstance(payload, dict) else {}
    rules_raw = data.get("prompt_rules") if isinstance(data.get("prompt_rules"), list) else []
    rules: list[str] = []
    seen: set[str] = set()
    for item in rules_raw:
        text = (item or "").strip()
        if not text:
            continue
        if not text.startswith("- "):
            text = f"- {text.lstrip('-').strip()}"
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        rules.append(text)
        if len(rules) >= 12:
            break
    return {
        "prompt_rules": rules,
        "agent_comment": (data.get("agent_comment") or "").strip(),
        "source": data.get("source") if isinstance(data.get("source"), dict) else _default_source(),
    }


def _mock_payload_from_edits(edits: list[dict], general_comment: str) -> dict:
    rules: list[str] = []
    seen: set[str] = set()
    for edit in edits:
        field = (edit.get("field") or "").strip()
        if not field:
            continue
        rule = f"- Для поля '{field}' при конфликте с документом ставь status=needs_disambiguation и указывай source."
        key = rule.casefold()
        if key in seen:
            continue
        seen.add(key)
        rules.append(rule)
        if len(rules) >= 8:
            break
    if general_comment:
        rules.append(f"- Учитывай общий комментарий пользователя: {general_comment}")
    return {
        "prompt_rules": rules,
        "agent_comment": "Mock: правила сформированы на основе полей правок.",
        "source": _default_source("02_processing/edit_log_quality.json"),
    }


def run_process_p2_feedback_rule_miner(
    *,
    project_id: str,
    comment: str,
    edits: list[dict],
    quality_registry_before: dict,
    quality_registry_after: dict,
    general_comment: str = "",
    existing_user_rules: str = "",
    existing_agent_rules: str = "",
) -> tuple[str, dict]:
    root = project_root(project_id)
    files = _select_context_files(root, edits)
    compact_before = _compact_registry(quality_registry_before)
    compact_after = _compact_registry(quality_registry_after)
    mock_payload = _mock_payload_from_edits(edits, general_comment)
    run_id, payload = run_llm_json_process(
        project_id=project_id,
        process_name="process_2_feedback",
        prompt_name="02_p2_feedback_rule_miner_v02",
        prompt_vars={
            "project_id": project_id,
            "comment": comment,
            "general_comment": general_comment,
            "existing_user_rules": _compact_rules_text(existing_user_rules) or "нет пользовательских правил",
            "existing_agent_rules": _compact_rules_text(existing_agent_rules) or "нет агентских правил",
            "quality_registry_before_json": compact_before,
            "quality_registry_after_json": compact_after,
            "edits_json": edits,
        },
        files=files,
        output_filename="p2_feedback_rules_suggested.json",
        mock_payload=mock_payload,
    )
    normalized = _normalize_feedback_payload(payload)
    save_processing_json(project_id, "p2_feedback_rules_suggested.json", normalized)
    duplicate_output_to_run(project_id, "process_2_feedback", run_id, "p2_feedback_rules_suggested.json")
    return run_id, normalized
