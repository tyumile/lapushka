import json
import secrets
from datetime import datetime
from pathlib import Path

from django.conf import settings

from ..llm.prompt_loader_v02 import load_prompt
from ..llm.responses_client_v02 import ResponsesClientV02
from .log_service import append_event
from .project_storage import persist_run_artifact, persist_run_json, project_root


SUPPORTED_CONTEXT_EXTS = {
    ".art", ".bat", ".brf", ".c", ".cls", ".css", ".diff", ".eml", ".es",
    ".h", ".hs", ".htm", ".html", ".ics", ".ifb", ".java", ".js", ".json",
    ".ksh", ".ltx", ".mail", ".markdown", ".md", ".mht", ".mhtml", ".mjs",
    ".nws", ".patch", ".pdf", ".pl", ".pm", ".pot", ".py", ".rst", ".scala",
    ".sh", ".shtml", ".srt", ".sty", ".tex", ".text", ".txt", ".vcf", ".vtt",
    ".xls", ".xlsx", ".csv", ".xml", ".yaml", ".yml",
}


def _collect_paths(paths: list[Path]) -> list[Path]:
    return [
        p for p in paths
        if p.exists() and p.is_file() and p.suffix.lower() in SUPPORTED_CONTEXT_EXTS
    ]


def run_llm_json_process(
    *,
    project_id: str,
    process_name: str,
    prompt_name: str,
    prompt_vars: dict,
    files: list[Path],
    output_filename: str,
    mock_payload: dict,
) -> tuple[str, dict]:
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S") + "-" + secrets.token_hex(3)
    model = "gpt-4.1-mini"
    append_event(project_id, {"process": process_name, "stage": "start", "run_id": run_id})

    if settings.MOCK_MODE:
        persist_run_json(project_id, process_name, run_id, output_filename, mock_payload)
        persist_run_json(
            project_id,
            process_name,
            run_id,
            "run_meta.json",
            {
                "process": process_name,
                "run_id": run_id,
                "model": model,
                "mock_mode": True,
                "started_at": datetime.utcnow().isoformat(),
                "success": True,
            },
        )
        persist_run_artifact(project_id, process_name, run_id, "raw_response.txt", json.dumps(mock_payload, ensure_ascii=False))
        persist_run_json(project_id, process_name, run_id, "uploaded_files.json", {"files": []})
        append_event(project_id, {"process": process_name, "stage": "finish", "run_id": run_id, "status": "success", "mock": True})
        return run_id, mock_payload

    existing = _collect_paths(files)
    client = ResponsesClientV02()
    upload_map: dict[str, str] = {}
    file_ids: list[str] = []
    for file_path in existing:
        fid = client.upload_file_bytes(file_path.name, file_path.read_bytes())
        upload_map[str(file_path.relative_to(project_root(project_id))).replace("\\", "/")] = fid
        file_ids.append(fid)

    system_prompt = load_prompt("01_system_v02")
    user_prompt = load_prompt(prompt_name, prompt_vars)
    response_json, raw_text = client.call_json_with_files(
        instructions=system_prompt,
        user_text=user_prompt,
        file_ids=file_ids,
        model=model,
        timeout_s=180,
    )
    persist_run_json(project_id, process_name, run_id, "uploaded_files.json", upload_map)
    persist_run_artifact(project_id, process_name, run_id, "raw_response.txt", raw_text)
    persist_run_json(project_id, process_name, run_id, output_filename, response_json)
    persist_run_json(
        project_id,
        process_name,
        run_id,
        "run_meta.json",
        {
            "process": process_name,
            "run_id": run_id,
            "model": model,
            "started_at": datetime.utcnow().isoformat(),
            "success": True,
        },
    )
    append_event(project_id, {"process": process_name, "stage": "finish", "run_id": run_id, "status": "success"})
    return run_id, response_json
