import json
from copy import deepcopy
from datetime import datetime

from django.contrib import messages
from django.shortcuts import redirect, render

from .services.io_utils import read_processing
from .services.process_p4b_build_doc_plan import _normalize_doc_instance, run_process_p4b_build_doc_plan
from .services.project_storage import project_root, save_processing_json, set_project_step
from .services.ui_status import set_action_status
from .views_utils import common_context


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


def run_p4b_build_doc_plan_view(request, project_id: str):
    if request.method != "POST":
        return redirect("v02_doc_plan", project_id=project_id)
    quality_final = read_processing(project_id, "p2_quality_registry_final.json", {})
    razdel_code = ((quality_final.get("razdel") or {}).get("razdel_code") or "KJ").strip() or "KJ"
    selected = (read_processing(project_id, "p4_doc_types_selection.json", {}).get("selected_doc_type_ids") or [])
    if not selected:
        messages.error(request, "Сначала выберите типы документов на шаге 3.")
        return redirect("v02_doc_types", project_id=project_id)
    set_action_status(project_id, "run_p4b", "running", "Process P4B запущен...")
    try:
        _, payload, excluded = run_process_p4b_build_doc_plan(project_id, razdel_code, selected)
        status_msg = f"Process P4B завершён. Инстансов: {len(payload.get('doc_instances') or [])}."
        if excluded:
            for x in excluded:
                messages.warning(request, f"Исключён: {x['path']}. Причина: {x['reason'][:150]}...")
            status_msg += f" Исключено файлов: {len(excluded)} (см. 01_input/99_excluded)"
        set_action_status(project_id, "run_p4b", "success", status_msg)
        messages.success(request, f"P4B завершён. Инстансов: {len(payload.get('doc_instances') or [])}.")
    except Exception as exc:
        set_action_status(project_id, "run_p4b", "error", f"Process P4B ошибка: {exc}")
        messages.error(request, f"Process P4B ошибка: {exc}")
    return redirect("v02_doc_plan", project_id=project_id)


def save_doc_plan_view(request, project_id: str):
    if request.method != "POST":
        return redirect("v02_doc_plan", project_id=project_id)
    source = read_processing(project_id, "p4b_doc_instances_final.json", read_processing(project_id, "p4b_doc_instances_v1.json", {}))
    if not source:
        messages.error(request, "Нет плана для сохранения. Запустите P4B.")
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
    set_action_status(project_id, "save_doc_plan", "success", f"План сохранён. Изменений: {len(diff)}.")
    messages.success(request, f"План сохранён. Изменений: {len(diff)}.")
    return redirect("v02_doc_plan", project_id=project_id)


def doc_plan_view(request, project_id: str):
    v1 = read_processing(project_id, "p4b_doc_instances_v1.json", {})
    final = read_processing(project_id, "p4b_doc_instances_final.json", v1)
    excluded_data = read_processing(project_id, "p4b_excluded_files.json", {})
    raw_instances = final.get("doc_instances") or []
    instances = [_normalize_doc_instance(inst, i) for i, inst in enumerate(raw_instances)]
    for inst in instances:
        inst["work_scope_text"] = json.dumps(inst.get("work_scope") or [], ensure_ascii=False, indent=2)
        inst["fields_text"] = json.dumps(inst.get("fields") or {}, ensure_ascii=False, indent=2)
    set_project_step(project_id, 4)
    return render(
        request,
        "doc_plan_v02.html",
        {
            **common_context(project_id),
            "plan": final,
            "instances": instances,
            "open_questions": final.get("open_questions") or [],
            "issues": final.get("issues") or [],
            "excluded_files": excluded_data.get("excluded") or [],
        },
    )
