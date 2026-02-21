from pathlib import Path
import re
import mimetypes

from django.http import FileResponse, Http404, JsonResponse
from django.shortcuts import redirect

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
    folder = latest_run_folder(project_id)
    if not folder:
        return redirect("v02_start")
    # Browser-safe fallback: redirect to start page with hint.
    # Keeping this endpoint for UX parity ("open last logs").
    return redirect(f"/start/?logs={folder.as_posix()}")


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
