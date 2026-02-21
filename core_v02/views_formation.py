from datetime import datetime

from django.contrib import messages
from django.shortcuts import redirect, render

from .services.document_generator import generate_from_fill_plan
from .services.io_utils import read_processing
from .services.process_p5_fill_plan import run_process_p5
from .services.project_storage import save_processing_json, set_project_step
from .services.rule_miner import save_learning_diff
from .services.ui_status import set_action_status
from .views_utils import common_context, resolve_files_for_process


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
    p4_v1 = read_processing(project_id, "p4b_doc_instances_v1.json", read_processing(project_id, "p4_doc_list_v1.json", {}))
    p4_final = read_processing(project_id, "p4b_doc_instances_final.json", read_processing(project_id, "p4_doc_list_final.json", p4_v1))
    quality_final = read_processing(project_id, "p2_quality_registry_final.json", {})
    razdel_code = ((quality_final.get("razdel") or {}).get("razdel_code") or "KJ").strip() or "KJ"

    if request.method == "POST":
        action = request.POST.get("action")
        if action == "save_doc_list_edits":
            original = p4_final.get("doc_instances") if isinstance(p4_final.get("doc_instances"), list) else p4_final.get("docs_to_generate") or []
            order = request.POST.get("rows_order", "")
            idx_order = [int(x) for x in order.split(",") if x.strip().isdigit()]
            edited = []
            for old_idx in idx_order if idx_order else range(len(original)):
                row = original[old_idx]
                item = dict(row)
                item["doc_name"] = request.POST.get(f"doc_name_{old_idx}", row.get("doc_name", ""))
                number = request.POST.get(
                    f"doc_number_suggestion_{old_idx}",
                    row.get("doc_number") or row.get("doc_number_suggestion", ""),
                )
                item["doc_number"] = number
                item["doc_number_suggestion"] = number
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
            return redirect("v02_formation", project_id=project_id)

    fill_plan = read_processing(project_id, "p5_fill_plan.json", {})
    outputs = fill_plan.get("outputs") or []
    set_project_step(project_id, 4)
    return render(
        request,
        "formation_v02.html",
        {
            **common_context(project_id),
            "p4_final": p4_final,
            "p4_comments": p4_v1.get("agent_comments") or [],
            "rows": _docs_rows(p4_final),
            "outputs": outputs,
            "p5_comments": fill_plan.get("agent_comments") or [],
        },
    )
