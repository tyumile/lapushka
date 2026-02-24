import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from django.contrib import messages
from django.shortcuts import redirect, render

from .services.dictionary_service import get_doc_types
from .services.io_utils import read_processing
from .services.process_p4b_build_doc_plan import _normalize_doc_instance, run_process_p4b_build_doc_plan
from .services.project_storage import project_root, save_processing_json, set_project_step
from .services.ui_status import set_action_status
from .views_utils import common_context


DOC_FIELDS_ALLOWLIST_BY_DOC_ID: dict[str, set[str]] = {
    # Title sheet does not use structured extraction fields from AOSR/AOOK.
    "KJ_TITLE_SHEET": set(),
}


def _filter_fields_for_doc(inst: dict, fields_obj: dict) -> dict:
    doc_id = (inst.get("doc_id") or "").strip()
    allowlist = DOC_FIELDS_ALLOWLIST_BY_DOC_ID.get(doc_id)
    if allowlist is None:
        return fields_obj
    if not allowlist:
        return {}
    return {k: v for k, v in fields_obj.items() if k in allowlist}


def _append_doc_plan_edit_log(project_id: str, entries: list[dict]) -> None:
    root = project_root(project_id)
    log_path = root / "04_logs" / "edit_logs" / "edit_log_doc_plan.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = []
    if log_path.exists():
        try:
            payload = json.loads(log_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = []
    payload.extend(entries)
    log_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _p4_feedback_rules_path(project_id: str) -> Path:
    return project_root(project_id) / "02_processing" / "p4b_prompt_feedback.txt"


def _p4_feedback_rules_store_path(project_id: str) -> Path:
    return project_root(project_id) / "02_processing" / "p4b_feedback_rules.json"


def _p4_general_comment_path(project_id: str) -> Path:
    return project_root(project_id) / "02_processing" / "p4b_last_general_comment.txt"


def _load_p4_feedback_rules(project_id: str) -> str:
    # Use only structured rules store to avoid sending raw user edits into the prompt.
    entries = _load_p4_feedback_rules_store(project_id)
    if entries:
        return _render_p4_feedback_rules(entries)
    return ""


def _write_p4_feedback_rules_text(project_id: str, text: str) -> str:
    path = _p4_feedback_rules_path(project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (text or "").strip()
    if payload:
        path.write_text(payload + "\n", encoding="utf-8")
    elif path.exists():
        path.unlink()
    return payload


def _load_p4_feedback_rules_store(project_id: str) -> list[dict]:
    path = _p4_feedback_rules_store_path(project_id)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    rows = payload.get("rules") if isinstance(payload, dict) else []
    return rows if isinstance(rows, list) else []


def _save_p4_feedback_rules_store(project_id: str, rules: list[dict]) -> None:
    path = _p4_feedback_rules_store_path(project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"rules": rules}, ensure_ascii=False, indent=2), encoding="utf-8")


def _entry_type_from_path(path: str) -> str:
    if path.endswith(".fields"):
        return "fields"
    if path.endswith(".work_scope"):
        return "work_scope"
    if ".multiplier." in path:
        return "multiplier"
    if path.endswith(".doc_name"):
        return "doc_name"
    if path.endswith(".doc_number"):
        return "doc_number"
    return "generic"


def _derive_p4_rules_from_diff(diff: list[dict], general_comment: str) -> list[dict]:
    groups: dict[str, list[dict]] = {}
    for item in diff:
        path = (item.get("path") or "").strip()
        key = _entry_type_from_path(path)
        groups.setdefault(key, []).append(item)

    rules: list[dict] = []
    mapping = {
        "fields": {
            "reason": "Wrong field extraction strategy or weak source validation for structured fields.",
            "rule": "When extracting fields, trust only explicit source-backed values; keep uncertain values as needs_extraction or needs_disambiguation.",
        },
        "work_scope": {
            "reason": "Incorrect work-to-document mapping in work_scope.",
            "rule": "Rebuild work_scope from source-confirmed work breakdown and avoid assigning works without explicit evidence.",
        },
        "multiplier": {
            "reason": "Wrong assumption about instance splitting axis/label.",
            "rule": "Set multiplier axis/label only when splitting evidence exists in project files; otherwise keep axis empty and raise clarification.",
        },
        "doc_name": {
            "reason": "Document naming did not follow project/domain naming expected by user.",
            "rule": "Use stable domain naming for doc_name and keep wording consistent with source terminology.",
        },
        "doc_number": {
            "reason": "Document numbering strategy was incorrect or missing.",
            "rule": "Generate doc_number only from confirmed numbering evidence; keep blank when no source-backed numbering exists.",
        },
        "generic": {
            "reason": "General extraction mismatch between generated plan and user intent.",
            "rule": "When user correction pattern appears, re-check related entities in source files and prefer conservative statuses over unsupported values.",
        },
    }
    for key, entries in groups.items():
        payload = mapping.get(key, mapping["generic"])
        paths = []
        for e in entries:
            p = (e.get("path") or "").strip()
            if p and p not in paths:
                paths.append(p)
        rules.append(
            {
                "rule_key": key,
                "reason": payload["reason"],
                "rule": payload["rule"],
                "paths": paths[:8],
            }
        )

    if general_comment:
        rules.append(
            {
                "rule_key": "general_comment",
                "reason": "User provided global correction context for this project.",
                "rule": "Apply project-wide correction intent from general comment while still validating every value against source files.",
                "paths": [],
            }
        )
    return rules[:8]


def _merge_p4_feedback_rules(existing: list[dict], fresh: list[dict]) -> list[dict]:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out: list[dict] = []
    index: dict[str, int] = {}
    for row in existing:
        if not isinstance(row, dict):
            continue
        key = (row.get("rule") or "").strip().casefold()
        if not key:
            continue
        clean = {
            "added_on": row.get("added_on") or today,
            "rule_key": (row.get("rule_key") or "generic"),
            "reason": (row.get("reason") or "").strip(),
            "rule": (row.get("rule") or "").strip(),
            "paths": row.get("paths") if isinstance(row.get("paths"), list) else [],
            "hits": int(row.get("hits") or 1),
        }
        index[key] = len(out)
        out.append(clean)

    for row in fresh:
        key = (row.get("rule") or "").strip().casefold()
        if not key:
            continue
        if key in index:
            found = out[index[key]]
            found["hits"] = int(found.get("hits") or 1) + 1
            merged_paths = list(found.get("paths") or [])
            for p in (row.get("paths") or []):
                if p not in merged_paths:
                    merged_paths.append(p)
            found["paths"] = merged_paths[:12]
            if row.get("reason"):
                found["reason"] = row["reason"]
            continue
        index[key] = len(out)
        out.append(
            {
                "added_on": today,
                "rule_key": row.get("rule_key") or "generic",
                "reason": (row.get("reason") or "").strip(),
                "rule": (row.get("rule") or "").strip(),
                "paths": row.get("paths") if isinstance(row.get("paths"), list) else [],
                "hits": 1,
            }
        )
    return out


def _render_p4_feedback_rules(rules: list[dict]) -> str:
    lines: list[str] = []
    for row in rules:
        if not isinstance(row, dict):
            continue
        rule = (row.get("rule") or "").strip()
        if not rule:
            continue
        added_on = (row.get("added_on") or "").strip() or datetime.utcnow().strftime("%Y-%m-%d")
        reason = (row.get("reason") or "").strip()
        paths = ", ".join([p for p in (row.get("paths") or []) if isinstance(p, str) and p.strip()][:4])
        lines.append(f"[{added_on}] RULE: {rule}")
        if reason:
            lines.append(f"[{added_on}] CAUSE: {reason}")
        if paths:
            lines.append(f"[{added_on}] FROM_PATHS: {paths}")
    return "\n".join(lines).strip()


def _load_p4_general_comment(project_id: str) -> str:
    path = _p4_general_comment_path(project_id)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _save_p4_general_comment(project_id: str, comment: str) -> None:
    path = _p4_general_comment_path(project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text((comment or "").strip(), encoding="utf-8")


def _diff_entries(project_id: str, before: dict, after: dict) -> list[dict]:
    rows = []
    b_instances = before.get("doc_instances") or []
    a_instances = after.get("doc_instances") or []
    for i, (b, a) in enumerate(zip(b_instances, a_instances)):
        instance_id = a.get("instance_id") or b.get("instance_id") or f"i{i}"
        for key in ("doc_name", "doc_number"):
            if (b.get(key) or "") != (a.get(key) or ""):
                rows.append(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "object_id": project_id,
                        "instance_id": instance_id,
                        "path": f"doc_instances[{i}].{key}",
                        "before": b.get(key),
                        "after": a.get(key),
                    }
                )
        b_mult = b.get("multiplier") if isinstance(b.get("multiplier"), dict) else {}
        a_mult = a.get("multiplier") if isinstance(a.get("multiplier"), dict) else {}
        for key in ("axis", "label"):
            if (b_mult.get(key) or "") != (a_mult.get(key) or ""):
                rows.append(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "object_id": project_id,
                        "instance_id": instance_id,
                        "path": f"doc_instances[{i}].multiplier.{key}",
                        "before": b_mult.get(key),
                        "after": a_mult.get(key),
                    }
                )
        if json.dumps(b.get("work_scope") or [], ensure_ascii=False) != json.dumps(a.get("work_scope") or [], ensure_ascii=False):
            rows.append(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "object_id": project_id,
                    "instance_id": instance_id,
                    "path": f"doc_instances[{i}].work_scope",
                    "before": b.get("work_scope"),
                    "after": a.get("work_scope"),
                }
            )
        if json.dumps(b.get("fields") or {}, ensure_ascii=False) != json.dumps(a.get("fields") or {}, ensure_ascii=False):
            rows.append(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "object_id": project_id,
                    "instance_id": instance_id,
                    "path": f"doc_instances[{i}].fields",
                    "before": b.get("fields"),
                    "after": a.get("fields"),
                }
            )
    return rows


def _parse_json_or_keep(value: str, fallback):
    text = (value or "").strip()
    if not text:
        return fallback
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return fallback


def _value_to_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _fields_filled_score(payload: dict) -> int:
    score = 0
    for inst in (payload or {}).get("doc_instances") or []:
        fields = inst.get("fields")
        if isinstance(fields, dict):
            score += len(fields)
    return score


def _apply_diff_overrides(payload: dict, diff: list[dict]) -> dict:
    out = payload if isinstance(payload, dict) else {}
    instances = out.get("doc_instances")
    if not isinstance(instances, list):
        return out
    for item in diff:
        path = (item.get("path") or "").strip()
        if not path.startswith("doc_instances["):
            continue
        start = path.find("[") + 1
        end = path.find("]", start)
        if start <= 0 or end <= start:
            continue
        try:
            idx = int(path[start:end])
        except ValueError:
            continue
        if idx < 0 or idx >= len(instances):
            continue
        inst = instances[idx] if isinstance(instances[idx], dict) else {}
        after = item.get("after")
        if path.endswith(".doc_name"):
            inst["doc_name"] = after
        elif path.endswith(".doc_number"):
            inst["doc_number"] = after
        elif path.endswith(".multiplier.axis"):
            mult = inst.get("multiplier") if isinstance(inst.get("multiplier"), dict) else {}
            mult["axis"] = after
            inst["multiplier"] = mult
        elif path.endswith(".multiplier.label"):
            mult = inst.get("multiplier") if isinstance(inst.get("multiplier"), dict) else {}
            mult["label"] = after
            inst["multiplier"] = mult
        elif path.endswith(".work_scope"):
            inst["work_scope"] = after if isinstance(after, list) else inst.get("work_scope") or []
        elif path.endswith(".fields"):
            inst["fields"] = after if isinstance(after, dict) else inst.get("fields") or {}
        instances[idx] = inst
    out["doc_instances"] = instances
    return out


def run_p4b_build_doc_plan_view(request, project_id: str):
    if request.method != "POST":
        return redirect("v02_doc_plan", project_id=project_id)
    quality_final = read_processing(project_id, "p2_quality_registry_final.json", {})
    razdel_code = ((quality_final.get("razdel") or {}).get("razdel_code") or "KJ").strip() or "KJ"
    selected = [d.get("doc_type_id") for d in get_doc_types(razdel_code) if d.get("doc_type_id")]
    if not selected:
        messages.error(request, "Р”Р»СЏ СЂР°Р·РґРµР»Р° РЅРµ РЅР°Р№РґРµРЅРѕ С‚РёРїРѕРІ РґРѕРєСѓРјРµРЅС‚РѕРІ.")
        return redirect("v02_doc_types", project_id=project_id)
    set_action_status(project_id, "run_p4b", "running", "Process P4B Р·Р°РїСѓС‰РµРЅ...")
    try:
        _, payload, excluded = run_process_p4b_build_doc_plan(
            project_id,
            razdel_code,
            selected,
            feedback_rules=_load_p4_feedback_rules(project_id),
        )
        status_msg = f"Process P4B Р·Р°РІРµСЂС€С‘РЅ. РРЅСЃС‚Р°РЅСЃРѕРІ: {len(payload.get('doc_instances') or [])}."
        if excluded:
            for x in excluded:
                messages.warning(request, f"РСЃРєР»СЋС‡С‘РЅ: {x['path']}. РџСЂРёС‡РёРЅР°: {x['reason'][:150]}...")
            status_msg += f" РСЃРєР»СЋС‡РµРЅРѕ С„Р°Р№Р»РѕРІ: {len(excluded)} (СЃРј. 01_input/099_excluded)"
        set_action_status(project_id, "run_p4b", "success", status_msg)
        messages.success(request, f"P4B Р·Р°РІРµСЂС€С‘РЅ. РРЅСЃС‚Р°РЅСЃРѕРІ: {len(payload.get('doc_instances') or [])}.")
    except Exception as exc:
        set_action_status(project_id, "run_p4b", "error", f"Process P4B РѕС€РёР±РєР°: {exc}")
        messages.error(request, f"Process P4B РѕС€РёР±РєР°: {exc}")
    return redirect("v02_doc_plan", project_id=project_id)


def save_doc_plan_view(request, project_id: str):
    if request.method != "POST":
        return redirect("v02_doc_plan", project_id=project_id)
    source = read_processing(project_id, "p4b_doc_instances_final.json", read_processing(project_id, "p4b_doc_instances_v1.json", {}))
    if not source:
        messages.error(request, "Нет плана для сохранения. Сначала запустите P4B.")
        return redirect("v02_doc_plan", project_id=project_id)
    original = deepcopy(source)
    instances = source.get("doc_instances") or []
    order = request.POST.get("rows_order", "")
    idx_order = [int(x) for x in order.split(",") if x.strip().isdigit()]
    if idx_order:
        instances = [instances[i] for i in idx_order if 0 <= i < len(instances)]
    for i, inst in enumerate(instances):
        inst["doc_name"] = request.POST.get(f"doc_name_{i}", inst.get("doc_name", ""))
        inst["doc_number"] = request.POST.get(f"doc_number_{i}", inst.get("doc_number", ""))
        mult = inst.get("multiplier") if isinstance(inst.get("multiplier"), dict) else {}
        mult["axis"] = request.POST.get(f"mult_axis_{i}", mult.get("axis", ""))
        mult["label"] = request.POST.get(f"mult_label_{i}", mult.get("label", ""))
        inst["multiplier"] = mult
        inst["work_scope"] = _parse_json_or_keep(request.POST.get(f"work_scope_{i}", ""), inst.get("work_scope") or [])
        inst["fields"] = _parse_json_or_keep(request.POST.get(f"fields_{i}", ""), inst.get("fields") or {})
        note = request.POST.get(f"user_note_{i}", "").strip()
        if note:
            inst["user_note"] = note
    source["doc_instances"] = instances
    save_processing_json(project_id, "p4b_doc_instances_final.json", source)

    diff = _diff_entries(project_id, original, source)
    _append_doc_plan_edit_log(project_id, diff)
    general_comment = (request.POST.get("general_comment") or "").strip()
    _save_p4_general_comment(project_id, general_comment)

    fresh_rules = _derive_p4_rules_from_diff(diff, general_comment)
    merged_rules = _merge_p4_feedback_rules(_load_p4_feedback_rules_store(project_id), fresh_rules)
    _save_p4_feedback_rules_store(project_id, merged_rules)
    feedback_rules = _render_p4_feedback_rules(merged_rules)
    _write_p4_feedback_rules_text(project_id, feedback_rules)
    save_processing_json(
        project_id,
        "p4b_feedback_last.json",
        {
            "generated_at": datetime.utcnow().isoformat(),
            "diff_count": len(diff),
            "new_rules": fresh_rules,
            "all_rules_count": len(merged_rules),
        },
    )

    set_action_status(project_id, "save_doc_plan", "success", f"План сохранен. Изменений: {len(diff)}. Новых правил: {len(fresh_rules)}.")
    if fresh_rules:
        first_rule = (fresh_rules[0].get("rule") or "").strip()
        if first_rule:
            messages.success(request, f"План сохранен. Изменений: {len(diff)}. Новое правило: {first_rule}")
        else:
            messages.success(request, f"План сохранен. Изменений: {len(diff)}. Новых правил: {len(fresh_rules)}.")
    else:
        messages.success(request, f"План сохранен. Изменений: {len(diff)}.")

    quality_final = read_processing(project_id, "p2_quality_registry_final.json", {})
    razdel_code = ((quality_final.get("razdel") or {}).get("razdel_code") or "KJ").strip() or "KJ"
    selected = [d.get("doc_type_id") for d in get_doc_types(razdel_code) if d.get("doc_type_id")]
    if selected:
        set_action_status(project_id, "run_p4b", "running", "Process P4B повторно запущен с учетом правил после исправлений...")
        try:
            _, payload, excluded = run_process_p4b_build_doc_plan(
                project_id,
                razdel_code,
                selected,
                feedback_rules=feedback_rules,
            )
            payload = _apply_diff_overrides(payload, diff)
            save_processing_json(project_id, "p4b_doc_instances_final.json", payload)
            rows_count = len(payload.get("doc_instances") or [])
            status_msg = f"Process P4B (после исправлений) завершен. Инстансов: {rows_count}."
            if excluded:
                status_msg += f" Исключено файлов: {len(excluded)}."
            status_msg += f" Применено правил: {len(merged_rules)}."
            set_action_status(project_id, "run_p4b", "success", status_msg)
            messages.success(request, f"Process P4B перезапущен. Инстансов: {rows_count}. Правил: {len(merged_rules)}.")
        except Exception as exc:
            set_action_status(project_id, "run_p4b", "error", f"Повторный Process P4B завершился ошибкой: {exc}")
            messages.error(request, f"План сохранен, но повторный Process P4B завершился ошибкой: {exc}")
    return redirect("v02_doc_plan", project_id=project_id)
def doc_plan_view(request, project_id: str):
    v1 = read_processing(project_id, "p4b_doc_instances_v1.json", {})
    final = read_processing(project_id, "p4b_doc_instances_final.json", v1)
    if _fields_filled_score(v1) > _fields_filled_score(final):
        final = v1
        save_processing_json(project_id, "p4b_doc_instances_final.json", final)
    excluded_data = read_processing(project_id, "p4b_excluded_files.json", {})
    raw_instances = final.get("doc_instances") or []
    instances = [_normalize_doc_instance(inst, i) for i, inst in enumerate(raw_instances)]
    for idx, inst in enumerate(instances):
        inst["row_idx"] = idx
        inst["work_scope_text"] = json.dumps(inst.get("work_scope") or [], ensure_ascii=False, indent=2)
        inst["fields_text"] = json.dumps(inst.get("fields") or {}, ensure_ascii=False, indent=2)
        fields_rows = []
        fields_obj = inst.get("fields") if isinstance(inst.get("fields"), dict) else {}
        fields_obj = _filter_fields_for_doc(inst, fields_obj)
        for field_name, field_payload in fields_obj.items():
            if isinstance(field_payload, dict):
                sources = field_payload.get("sources")
                if not isinstance(sources, list):
                    source_single = field_payload.get("source")
                    sources = [source_single] if isinstance(source_single, dict) else []
                source_text = "; ".join(
                    f"{(s.get('file') or '').strip()}:{s.get('page')}" if isinstance(s, dict) else str(s)
                    for s in sources
                )
                fields_rows.append(
                    {
                        "name": field_name,
                        "value": _value_to_text(field_payload.get("value")),
                        "status": field_payload.get("status", ""),
                        "confidence": field_payload.get("confidence", ""),
                        "sources": source_text,
                    }
                )
            else:
                fields_rows.append(
                    {
                        "name": field_name,
                        "value": _value_to_text(field_payload),
                        "status": "",
                        "confidence": "",
                        "sources": "",
                    }
                )
        inst["fields_rows"] = fields_rows
    doc_type_groups = []
    groups_map = {}
    for inst in instances:
        doc_type_id = (inst.get("doc_type_id") or "").strip() or "unknown"
        group = groups_map.get(doc_type_id)
        if group is None:
            group = {"doc_type_id": doc_type_id, "instances": []}
            groups_map[doc_type_id] = group
            doc_type_groups.append(group)
        group["instances"].append(inst)
    set_project_step(project_id, 4)
    return render(
        request,
        "doc_plan_v02.html",
        {
            **common_context(project_id),
            "plan": final,
            "instances": instances,
            "doc_type_groups": doc_type_groups,
            "open_questions": final.get("open_questions") or [],
            "issues": final.get("issues") or [],
            "excluded_files": excluded_data.get("excluded") or [],
            "p4_general_comment": _load_p4_general_comment(project_id),
        },
    )

