from pathlib import Path
import re
import mimetypes
import json

from django.http import FileResponse, Http404, JsonResponse
from django.shortcuts import redirect, render

from .services.log_service import latest_run_folder
from .services.project_storage import load_project_meta, project_root
from .services.ui_status import get_action_statuses


def common_context(project_id: str) -> dict:
    root = project_root(project_id)
    return {
        "project_id": project_id,
        "project_root": str(root),
        "meta": load_project_meta(project_id),
        "ui_status": get_action_statuses(project_id),
    }


def download_output_zip_view(request, project_id: str):
    raise Http404("output.zip disabled")


def download_input_file_view(request, project_id: str, relative_path: str):
    root = project_root(project_id)
    candidate = (root / relative_path).resolve()
    # Restrict to project root and input tree only.
    if not str(candidate).startswith(str(root.resolve())):
        raise Http404("invalid path")
    if "01_input" not in candidate.parts:
        raise Http404("only input files are allowed")
    if not candidate.exists() or not candidate.is_file():
        # LLM may return normalized/transliterated file_ref.
        # Try to resolve the closest existing file in quality docs.
        quality_dir = root / "01_input" / "02_quality_docs"
        req_name = Path(relative_path).name
        req_stem = Path(req_name).stem.lower()
        req_ext = Path(req_name).suffix.lower()
        req_tokens = set(re.findall(r"[a-zA-Zа-яА-Я0-9]+", req_stem))
        req_num_tokens = set(re.findall(r"\d+(?:[-_]\d+)*", req_stem))

        best_path = None
        best_score = -1
        if quality_dir.exists():
            for p in quality_dir.glob("*"):
                if not p.is_file():
                    continue
                cand_stem = p.stem.lower()
                cand_tokens = set(re.findall(r"[a-zA-Zа-яА-Я0-9]+", cand_stem))
                cand_num_tokens = set(re.findall(r"\d+(?:[-_]\d+)*", cand_stem))
                score = 0
                if req_ext and p.suffix.lower() == req_ext:
                    score += 2
                score += len(req_tokens & cand_tokens)
                score += 5 * len(req_num_tokens & cand_num_tokens)
                if score > best_score:
                    best_score = score
                    best_path = p
        if best_path is not None and best_score > 0:
            candidate = best_path.resolve()
        else:
            raise Http404("file not found")
    content_type, _ = mimetypes.guess_type(str(candidate))
    response = FileResponse(open(candidate, "rb"), as_attachment=False, filename=candidate.name, content_type=content_type)
    response["Content-Disposition"] = f'inline; filename="{candidate.name}"'
    return response


def open_last_logs_view(request, project_id: str):
    return redirect("v02_logs", project_id=project_id)


def logs_view(request, project_id: str):
    runs_root = project_root(project_id) / "04_logs" / "runs"
    runs = []
    if runs_root.exists():
        for process_dir in sorted([p for p in runs_root.iterdir() if p.is_dir()], key=lambda p: p.name):
            for run_dir in sorted([p for p in process_dir.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True):
                item = {
                    "process": process_dir.name,
                    "run_id": run_dir.name,
                    "folder": str(run_dir.relative_to(project_root(project_id))).replace("\\", "/"),
                    "run_meta": {},
                    "uploaded_files": {},
                    "raw_response": "",
                    "outputs": [],
                }
                meta_path = run_dir / "run_meta.json"
                if meta_path.exists():
                    try:
                        item["run_meta"] = json.loads(meta_path.read_text(encoding="utf-8"))
                    except json.JSONDecodeError:
                        item["run_meta"] = {"_raw": meta_path.read_text(encoding="utf-8", errors="replace")}
                uploaded_path = run_dir / "uploaded_files.json"
                if uploaded_path.exists():
                    try:
                        item["uploaded_files"] = json.loads(uploaded_path.read_text(encoding="utf-8"))
                    except json.JSONDecodeError:
                        item["uploaded_files"] = {"_raw": uploaded_path.read_text(encoding="utf-8", errors="replace")}
                raw_path = run_dir / "raw_response.txt"
                if raw_path.exists():
                    item["raw_response"] = raw_path.read_text(encoding="utf-8", errors="replace")
                outputs_dir = run_dir / "outputs"
                if outputs_dir.exists():
                    item["outputs"] = [str(p.relative_to(project_root(project_id))).replace("\\", "/") for p in outputs_dir.rglob("*") if p.is_file()]
                item["run_meta_text"] = json.dumps(item["run_meta"], ensure_ascii=False, indent=2) if item["run_meta"] else "{}"
                item["uploaded_files_text"] = json.dumps(item["uploaded_files"], ensure_ascii=False, indent=2) if item["uploaded_files"] else "{}"
                runs.append(item)
    runs.sort(key=lambda x: x["run_id"], reverse=True)
    return render(
        request,
        "logs_v02.html",
        {
            **common_context(project_id),
            "runs": runs,
            "latest_folder": str(latest_run_folder(project_id) or ""),
        },
    )


def resolve_files_for_process(project_id: str) -> list[Path]:
    root = project_root(project_id)
    all_files = [p for p in (root / "01_input").rglob("*") if p.is_file()]
    return all_files


def project_status_view(request, project_id: str):
    return JsonResponse(
        {
            "project_id": project_id,
            "ui_status": get_action_statuses(project_id),
        }
    )
