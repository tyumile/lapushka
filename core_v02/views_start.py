from django.contrib import messages
from django.shortcuts import redirect, render

from .services.dictionary_service import get_razdels
from .services.project_storage import (
    create_project_structure,
    list_uploaded_files,
    load_project_meta,
    project_root,
    save_project_meta,
    save_uploaded_files,
    set_project_step,
)
from .services.ui_status import set_action_status
from .views_utils import common_context, open_last_logs_view


def start_view(request):
    if request.method == "POST":
        action = request.POST.get("action")
        if action == "create_project":
            project_name = (request.POST.get("project_name") or "").strip()
            comment = (request.POST.get("comment") or "").strip()
            if not project_name:
                messages.error(request, "Введите название проекта.")
                return redirect("v02_start")
            project_id = create_project_structure(project_name)
            meta = load_project_meta(project_id)
            meta["comment"] = comment
            save_project_meta(project_id, meta)
            set_action_status(project_id, "create_project", "success", "Проект создан.")
            messages.success(request, f"Проект создан: {project_id}")
            return redirect("v02_quality", project_id=project_id)

        project_id = request.POST.get("project_id") or ""
        razdel_code = request.POST.get("razdel_code") or "KJ"
        if action in {"upload_project", "upload_quality", "upload_ojr"}:
            files = request.FILES.getlist("files")
            block = {"upload_project": "project", "upload_quality": "quality", "upload_ojr": "ojr"}[action]
            saved = save_uploaded_files(project_id, block, files, razdel_code=razdel_code)
            labels = {
                "upload_project": "Проект",
                "upload_quality": "Документы качества",
                "upload_ojr": "ОЖР",
            }
            set_action_status(project_id, action, "success", f"{labels[action]}: загружено {len(saved)} файл(ов).")
            messages.success(request, f"Загружено файлов: {len(saved)}")
            return redirect(f"/start/?project_id={project_id}")

    selected_project_id = request.GET.get("project_id") or ""
    context = {"razdels": get_razdels(), "selected_project_id": selected_project_id}
    if selected_project_id:
        meta = load_project_meta(selected_project_id)
        files_map = list_uploaded_files(selected_project_id)
        context.update(
            {
                "meta": meta,
                "files_map": files_map,
                "exists": project_root(selected_project_id).exists(),
                **common_context(selected_project_id),
            }
        )
        set_project_step(selected_project_id, 1)
    return render(request, "start_v02.html", context)
