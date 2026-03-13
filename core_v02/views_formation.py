import json
from datetime import datetime

from django.contrib import messages
from django.shortcuts import redirect, render

from .services.document_generator import generate_from_fill_plan
from .services.io_utils import read_processing
from .services.process_p5_fill_plan import run_process_p5
from .services.project_storage import save_processing_json, set_project_step
from .services.rule_miner import save_learning_diff
from .services.ui_status import set_action_status
from .views_doc_plan import (
    _coerce_field_value,
    _collect_editable_field_rows,
    _filter_fields_for_doc,
    _normalize_doc_instance,
    _parse_json_or_keep,
    _set_nested_field_value,
)
from .views_utils import common_context, resolve_files_for_process


def _as_dict(payload) -> dict:
    return payload if isinstance(payload, dict) else {}


def _docs_rows(payload: dict) -> list[dict]:
    rows: list[dict] = []
    source = payload.get("doc_instances") if isinstance(payload.get("doc_instances"), list) else payload.get("docs_to_generate") or []
    for idx, d in enumerate(source, start=1):
        rows.append(
            {
                "idx": idx,
                "doc_id": d.get("doc_id", ""),
                "doc_name": d.get("doc_name", ""),
                "doc_number_suggestion": d.get("doc_number") or d.get("doc_number_suggestion", ""),
                "status": d.get("overall_status") or d.get("status", "ok"),
            }
        )
    return rows


def formation_view(request, project_id: str):
    p4_v1 = _as_dict(read_processing(project_id, "p4b_doc_instances_v1.json", read_processing(project_id, "p4_doc_list_v1.json", {})))
    p4_final = _as_dict(read_processing(project_id, "p4b_doc_instances_final.json", read_processing(project_id, "p4_doc_list_final.json", p4_v1)))
    quality_final = _as_dict(read_processing(project_id, "p2_quality_registry_final.json", {}))
    razdel_code = ((quality_final.get("razdel") or {}).get("razdel_code") or "KJ").strip() or "KJ"

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_doc_list_edits":
            original = p4_final.get("doc_instances") if isinstance(p4_final.get("doc_instances"), list) else p4_final.get("docs_to_generate") or []
            order = request.POST.get("rows_order", "")
            idx_order = [int(x) for x in order.split(",") if x.strip().isdigit()]
            edited = []
            for old_idx in idx_order if idx_order else range(len(original)):
                row = _normalize_doc_instance(original[old_idx], old_idx)
                item = dict(row)
                item["doc_name"] = request.POST.get(f"doc_name_{old_idx}", row.get("doc_name", ""))
                number = request.POST.get(
                    f"doc_number_suggestion_{old_idx}",
                    row.get("doc_number") or row.get("doc_number_suggestion", ""),
                )
                item["doc_number"] = number
                item["doc_number_suggestion"] = number
                mult = item.get("multiplier") if isinstance(item.get("multiplier"), dict) else {}
                mult["axis"] = request.POST.get(f"mult_axis_{old_idx}", mult.get("axis", ""))
                mult["label"] = request.POST.get(f"mult_label_{old_idx}", mult.get("label", ""))
                item["multiplier"] = mult
                item["work_scope"] = _parse_json_or_keep(request.POST.get(f"work_scope_{old_idx}", ""), item.get("work_scope") or [])
                fields_obj = item.get("fields") if isinstance(item.get("fields"), dict) else {}
                field_rows = _collect_editable_field_rows(_filter_fields_for_doc(item, fields_obj))
                for field_idx, field_row in enumerate(field_rows):
                    posted_value = request.POST.get(f"field_value_{old_idx}_{field_idx}")
                    if posted_value is None:
                        continue
                    _set_nested_field_value(
                        fields_obj,
                        field_row.get("path", ""),
                        _coerce_field_value(posted_value, field_row.get("value_kind", "string"), field_row.get("value_text", "")),
                    )
                item["fields"] = fields_obj
                item["user_note"] = request.POST.get(f"user_note_{old_idx}", item.get("user_note", ""))
                edited.append(item)
            if isinstance(p4_final.get("doc_instances"), list):
                p4_final["doc_instances"] = edited
                save_processing_json(project_id, "p4b_doc_instances_final.json", p4_final)
            else:
                p4_final["docs_to_generate"] = edited
                save_processing_json(project_id, "p4_doc_list_final.json", p4_final)
            save_learning_diff(
                project_id,
                "process_4",
                {"before": original, "after": edited, "saved_at": datetime.utcnow().isoformat()},
                razdel_code,
            )
            set_action_status(project_id, "save_doc_list_edits", "success", f"Исправления списка документов сохранены: {len(edited)}.")
            messages.success(request, "Правки сохранены.")
            return redirect("v02_formation", project_id=project_id)

        if action == "run_p5":
            set_action_status(project_id, "run_p5", "running", "Process 5 запущен...")
            try:
                files = resolve_files_for_process(project_id)
                _, fill_plan = run_process_p5(
                    project_id=project_id,
                    razdel_code=razdel_code,
                    doc_list=p4_final,
                    quality_registry=quality_final,
                    files=files,
                )
                generated = generate_from_fill_plan(project_id, fill_plan)
                save_learning_diff(
                    project_id,
                    "process_5",
                    {"docs_sent": p4_final.get("doc_instances") or p4_final.get("docs_to_generate", []), "generated": generated},
                    razdel_code,
                )
                set_action_status(project_id, "run_p5", "success", f"Process 5 завершён. Сформировано файлов: {len(generated)}.")
                messages.success(request, f"Документы сформированы: {len(generated)}")
            except Exception as exc:
                set_action_status(project_id, "run_p5", "error", f"Process 5 ошибка: {exc}")
                messages.error(request, f"Process 5 ошибка: {exc}")
            return redirect("v02_formation", project_id=project_id)

    fill_plan = _as_dict(read_processing(project_id, "p5_fill_plan.json", {}))
    outputs = fill_plan.get("outputs") or []
    set_project_step(project_id, 4)
    instances = [_normalize_doc_instance(inst, i) for i, inst in enumerate(p4_final.get("doc_instances") or [])]
    for idx, inst in enumerate(instances):
        inst["row_idx"] = idx
        inst["work_scope_text"] = json.dumps(inst.get("work_scope") or [], ensure_ascii=False, indent=2)
        fields_obj = inst.get("fields") if isinstance(inst.get("fields"), dict) else {}
        fields_obj = _filter_fields_for_doc(inst, fields_obj)
        inst["fields_rows"] = _collect_editable_field_rows(fields_obj)

    return render(
        request,
        "formation_v02.html",
        {
            **common_context(project_id),
            "p4_final": p4_final,
            "p4_comments": p4_v1.get("agent_comments") or [],
            "rows": _docs_rows(p4_final),
            "instances": instances,
            "outputs": outputs,
            "p5_comments": fill_plan.get("agent_comments") or [],
        },
    )
