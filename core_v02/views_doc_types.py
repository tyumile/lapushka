from pathlib import Path

from django.contrib import messages
from django.shortcuts import redirect, render

from .services.dictionary_service import get_doc_types
from .services.io_utils import read_processing
from .services.process_p4b_build_doc_plan import run_process_p4b_build_doc_plan
from .services.project_storage import list_uploaded_files, save_uploaded_files, set_project_step
from .services.ui_status import set_action_status
from .views_utils import common_context


def doc_types_view(request, project_id: str):
    quality_final = read_processing(project_id, "p2_quality_registry_final.json", {})
    p4_v1 = read_processing(project_id, "p4b_doc_instances_v1.json", read_processing(project_id, "p4_doc_list_v1.json", {}))
    razdel_code = ((quality_final.get("razdel") or {}).get("razdel_code") or "KJ").strip() or "KJ"
    if request.method == "POST":
        action = request.POST.get("action")
        if action in {"upload_sample_project", "upload_sample_id", "upload_doc_type_sample"}:
            files = request.FILES.getlist("files")
            doc_type_id = (request.POST.get("doc_type_id") or "").strip()
            block = {
                "upload_sample_project": "sample_project",
                "upload_sample_id": "sample_id",
                "upload_doc_type_sample": "sample_doc_type",
            }[action]
            saved = save_uploaded_files(project_id, block, files, razdel_code=razdel_code, sample_kind=doc_type_id)
            labels = {
                "upload_sample_project": "Образцы проектов",
                "upload_sample_id": "Образцы ИД",
                "upload_doc_type_sample": f"Образцы типа {doc_type_id or 'general'}",
            }
            set_action_status(project_id, action, "success", f"{labels[action]}: загружено {len(saved)} файл(ов).")
            messages.success(request, f"Загружено образцов: {len(saved)}")
            return redirect("v02_doc_types", project_id=project_id)

        if action == "run_p4b":
            set_action_status(project_id, "run_p4b", "running", "Process P4B запущен...")
            selected_doc_type_ids = [d.get("doc_type_id") for d in get_doc_types(razdel_code) if d.get("doc_type_id")]
            if not selected_doc_type_ids:
                set_action_status(project_id, "run_p4b", "error", "Для раздела не найдено типов документов.")
                messages.error(request, "Для раздела не найдено типов документов.")
                return redirect("v02_doc_types", project_id=project_id)
            try:
                _, payload, excluded = run_process_p4b_build_doc_plan(project_id, razdel_code, selected_doc_type_ids)
                status_msg = f"Process P4B завершён. Инстансов: {len(payload.get('doc_instances') or [])}."
                if excluded:
                    for x in excluded:
                        messages.warning(request, f"Исключён: {x['path']}. Причина: {x['reason'][:150]}...")
                    status_msg += f" Исключено файлов: {len(excluded)} (см. 01_input/099_excluded)"
                set_action_status(project_id, "run_p4b", "success", status_msg)
                messages.success(request, f"Сформирован план документов: {len(payload.get('doc_instances') or [])}")
            except Exception as exc:
                set_action_status(project_id, "run_p4b", "error", f"Process P4B завершился ошибкой: {exc}")
                messages.error(request, f"Process P4B ошибка: {exc}")
                return redirect("v02_doc_types", project_id=project_id)
            return redirect("v02_doc_plan", project_id=project_id)

    set_project_step(project_id, 3)
    return render(
        request,
        "doc_types_v02.html",
        {
            **common_context(project_id),
            "quality_final": quality_final,
            "p4_comments": p4_v1.get("agent_comments") or [],
            "razdel_code": razdel_code,
            "doc_types": get_doc_types(razdel_code),
            "files_map": list_uploaded_files(project_id),
        },
    )
