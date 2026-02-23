from datetime import datetime
from pathlib import Path

from django.contrib import messages
from django.shortcuts import redirect, render

from .services.edit_logs import append_quality_edit_log, quality_row_key
from .services.io_utils import read_processing
from .services.process_p2_quality_registry import run_process_p2
from .services.project_storage import (
    list_uploaded_files,
    load_project_meta,
    save_processing_json,
    save_uploaded_files,
    set_project_step,
    project_root,
)
from .services.rule_miner import save_learning_diff
from .services.ui_status import set_action_status
from .views_utils import common_context

UI_NOT_FOUND = "не нашел данные"


def _to_ui_value(value: str) -> str:
    return UI_NOT_FOUND if (value or "").strip() == "needs_extraction" else value


def _from_ui_value(value: str) -> str:
    return "needs_extraction" if (value or "").strip().lower() == UI_NOT_FOUND else value


def _flatten_quality_rows(payload: dict) -> list[dict]:
    rows: list[dict] = []
    for material in payload.get("materials") or []:
        m_norm = material.get("material_norm_name") or ""
        for doc in material.get("docs") or []:
            rows.append(
                {
                    "material_id": material.get("material_id"),
                    "material_name": material.get("material_name"),
                    "material_norm_name": m_norm,
                    "doc_kind": _to_ui_value(doc.get("doc_kind", "")),
                    "doc_number": _to_ui_value(doc.get("doc_number", "б/н")),
                    "doc_date": _to_ui_value(doc.get("doc_date", "б/д")),
                    "volume": _to_ui_value(doc.get("volume", "needs_extraction")),
                    "manufacturer": _to_ui_value(doc.get("manufacturer", "")),
                    "issuer": _to_ui_value(doc.get("issuer", "")),
                    "file_ref": doc.get("file_ref", ""),
                    "status": doc.get("status", "ok"),
                    "status_display": _to_ui_value(doc.get("status", "ok")),
                }
            )
    return rows


def _feedback_rules_path(project_id: str) -> Path:
    return project_root(project_id) / "02_processing" / "p2_prompt_feedback.txt"


def _general_comment_path(project_id: str) -> Path:
    return project_root(project_id) / "02_processing" / "p2_last_general_comment.txt"


def _build_rules_from_edits(edits: list[dict]) -> list[str]:
    rules: list[str] = []
    for e in edits:
        before = (e.get("before") or "").strip()
        after = (e.get("after") or "").strip()
        field = e.get("field") or ""
        row_key = e.get("row_key") or ""
        if not after:
            continue
        rules.append(
            f"- Для поля '{field}' по ключу '{row_key}' использовать значение '{after}' (вместо '{before}')."
        )
    return rules


def _append_feedback_rules(project_id: str, rules: list[str]) -> str:
    if not rules:
        return ""
    path = _feedback_rules_path(project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    merged = (existing.rstrip() + "\n" if existing.strip() else "") + "\n".join(rules) + "\n"
    path.write_text(merged, encoding="utf-8")
    return merged


def _load_general_comment(project_id: str) -> str:
    path = _general_comment_path(project_id)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _save_general_comment(project_id: str, comment: str) -> None:
    path = _general_comment_path(project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text((comment or "").strip(), encoding="utf-8")


def quality_view(request, project_id: str):
    if request.method == "POST":
        action = request.POST.get("action")
        if action in {"upload_project", "upload_quality", "upload_ojr"}:
            files = request.FILES.getlist("files")
            block = {"upload_project": "project", "upload_quality": "quality", "upload_ojr": "ojr"}[action]
            saved = save_uploaded_files(project_id, block, files)
            labels = {
                "upload_project": "Проект",
                "upload_quality": "Документы качества",
                "upload_ojr": "ОЖР",
            }
            set_action_status(project_id, action, "success", f"{labels[action]}: загружено {len(saved)} файл(ов).")
            messages.success(request, f"Загружено файлов: {len(saved)}")
            return redirect("v02_quality", project_id=project_id)

        if action == "run_p2":
            set_action_status(project_id, "run_p2", "running", "Process 2 запущен...")
            files_map = list_uploaded_files(project_id)
            if not (files_map.get("quality") or []):
                set_action_status(project_id, "run_p2", "error", "Process 2 не запущен: сначала загрузите документы качества.")
                messages.error(request, "Сначала загрузите документы качества.")
                return redirect("v02_quality", project_id=project_id)
            try:
                meta = load_project_meta(project_id)
                feedback_rules = ""
                fp = _feedback_rules_path(project_id)
                if fp.exists():
                    feedback_rules = fp.read_text(encoding="utf-8")
                _, payload = run_process_p2(project_id, meta.get("comment", ""), feedback_rules=feedback_rules)
                save_processing_json(project_id, "p2_quality_registry_final.json", payload)
                rows_count = len(_flatten_quality_rows(payload))
                set_action_status(project_id, "run_p2", "success", f"Process 2 завершён. Строк в реестре: {rows_count}.")
                messages.success(request, "Process 2 завершён.")
            except Exception as exc:
                set_action_status(project_id, "run_p2", "error", f"Process 2 завершился ошибкой: {exc}")
                messages.error(request, f"Process 2 ошибка: {exc}")
            return redirect("v02_quality", project_id=project_id)

        if action == "save_quality_edits":
            set_action_status(project_id, "save_quality_edits", "running", "Сохраняем правки и переобучаем prompt Process 2...")
            before = read_processing(project_id, "p2_quality_registry_final.json", {})
            before_rows = _flatten_quality_rows(before)
            edited_rows: list[dict] = []
            for idx, row in enumerate(before_rows):
                edited = dict(row)
                edited["material_name"] = _from_ui_value(request.POST.get(f"material_name_{idx}", row.get("material_name", "")))
                for key in ("doc_kind", "doc_number", "doc_date", "volume", "manufacturer", "issuer"):
                    edited[key] = _from_ui_value(request.POST.get(f"{key}_{idx}", row.get(key, "")))
                edited_rows.append(edited)
            edits: list[dict] = []
            for old, new in zip(before_rows, edited_rows):
                row_key = quality_row_key(new)
                for field in ("material_name", "doc_kind", "doc_number", "doc_date", "volume", "manufacturer", "issuer"):
                    if (old.get(field) or "") != (new.get(field) or ""):
                        edits.append(
                            {
                                "timestamp": datetime.utcnow().isoformat(),
                                "table": "quality_registry",
                                "row_key": row_key,
                                "field": field,
                                "before": old.get(field),
                                "after": new.get(field),
                            }
                        )
            append_quality_edit_log(project_id, edits)
            general_comment = (request.POST.get("general_comment") or "").strip()
            _save_general_comment(project_id, general_comment)
            # Rebuild payload with edited values.
            final = dict(before)
            pointer = 0
            for material in final.get("materials") or []:
                for doc in material.get("docs") or []:
                    row = edited_rows[pointer]
                    pointer += 1
                    material["material_name"] = row.get("material_name", material.get("material_name"))
                    for key in ("doc_kind", "doc_number", "doc_date", "volume", "manufacturer", "issuer"):
                        doc[key] = row.get(key, doc.get(key))
            save_processing_json(project_id, "p2_quality_registry_final.json", final)
            rules = _build_rules_from_edits(edits)
            if general_comment:
                rules.append(f"- Общий комментарий пользователя по исправлению реестра: {general_comment}")
            feedback_rules = _append_feedback_rules(project_id, rules)
            razdel_code = (final.get("razdel") or {}).get("razdel_code", "KJ")
            save_learning_diff(
                project_id,
                "process_2",
                {
                    "table": "quality_registry",
                    "edits_count": len(edits),
                    "rules_added": rules,
                },
                razdel_code,
            )
            set_action_status(project_id, "run_p2", "running", "Process 2 повторно запущен с учетом исправлений...")
            try:
                meta = load_project_meta(project_id)
                _, rerun_payload = run_process_p2(project_id, meta.get("comment", ""), feedback_rules=feedback_rules)
                save_processing_json(project_id, "p2_quality_registry_final.json", rerun_payload)
                rows_count = len(_flatten_quality_rows(rerun_payload))
                set_action_status(project_id, "run_p2", "success", f"Process 2 (после исправлений) завершён. Строк: {rows_count}.")
                set_action_status(project_id, "save_quality_edits", "success", f"Исправления сохранены и учтены в prompt: {len(edits)}.")
                messages.success(request, f"Исправления сохранены, prompt обновлен, Process 2 перезапущен. Строк: {rows_count}.")
            except Exception as exc:
                set_action_status(project_id, "run_p2", "error", f"Повторный Process 2 завершился ошибкой: {exc}")
                set_action_status(project_id, "save_quality_edits", "error", f"Исправления сохранены, но повторный запуск упал: {exc}")
                messages.error(request, f"Исправления сохранены, но повторный Process 2 завершился ошибкой: {exc}")
            return redirect("v02_quality", project_id=project_id)

        if action == "next_to_doc_types":
            set_action_status(project_id, "next_to_doc_types", "success", "Переход к шагу 3 выполнен.")
            set_project_step(project_id, 3)
            return redirect("v02_doc_types", project_id=project_id)

    p2_v1 = read_processing(project_id, "p2_quality_registry_v1.json", {})
    p2_final = read_processing(project_id, "p2_quality_registry_final.json", p2_v1)
    rows = _flatten_quality_rows(p2_final)
    files_map = list_uploaded_files(project_id)
    set_project_step(project_id, 2)
    return render(
        request,
        "quality_v02.html",
        {
            **common_context(project_id),
            "meta": load_project_meta(project_id),
            "files_map": files_map,
            "p2": p2_final,
            "rows": rows,
            "razdel_code": (p2_final.get("razdel") or {}).get("razdel_code", "KJ"),
            "quality_general_comment": _load_general_comment(project_id),
        },
    )
