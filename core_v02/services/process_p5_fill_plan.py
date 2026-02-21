from pathlib import Path

from .llm_runtime import run_llm_json_process
from .project_storage import duplicate_output_to_run, save_processing_json


def _normalize_agent_comments(payload: dict) -> dict:
    out = dict(payload or {})
    comments = out.get("agent_comments")
    if not isinstance(comments, list):
        comments = out.get("comments")
    normalized = []
    if isinstance(comments, list):
        for c in comments:
            if not isinstance(c, dict):
                continue
            text = (c.get("comment") or c.get("text") or "").strip()
            source = c.get("source") if isinstance(c.get("source"), dict) else {"file": "", "page": "", "snippet": ""}
            if text:
                normalized.append({"comment": text, "source": source})
    if not normalized:
        for out_item in (out.get("outputs") or [])[:5]:
            fills = out_item.get("fills") or []
            source = {}
            if fills and isinstance(fills[0], dict):
                source = fills[0].get("source") if isinstance(fills[0].get("source"), dict) else {}
            normalized.append(
                {
                    "comment": f"План заполнения подготовлен для '{out_item.get('output_path', '')}'.",
                    "source": source or {"file": "", "page": "", "snippet": ""},
                }
            )
    out["agent_comments"] = normalized
    return out


def _mock_payload(razdel_code: str, doc_list: dict) -> dict:
    docs = doc_list.get("docs_to_generate")
    if not isinstance(docs, list):
        docs = doc_list.get("doc_instances") or []
    outputs = []
    for idx, row in enumerate(docs, start=1):
        fmt = "xlsx"
        outputs.append(
            {
                "output_path": f"{razdel_code}/{idx:03d}_{row.get('doc_id')}.{fmt}",
                "template_ref": "samples/auto",
                "format": fmt,
                "fills": [
                    {
                        "target": {"sheet": "Лист1", "cell": "A1"},
                        "value": row.get("doc_name"),
                        "status": "ok",
                        "source": {"file": "01_input/01_project/mock_project.pdf", "page": "1", "snippet": row.get("doc_name")},
                    }
                ],
            }
        )
    return {
        "outputs": outputs,
        "missing_inputs": [],
        "agent_comments": [
            {
                "comment": "План заполнения построен по доступным образцам и реестру качества.",
                "source": {"file": "01_input/04_samples", "page": "", "snippet": "auto"},
            }
        ],
    }


def run_process_p5(
    project_id: str,
    razdel_code: str,
    doc_list: dict,
    quality_registry: dict,
    files: list[Path],
) -> tuple[str, dict]:
    prompt_doc_list = dict(doc_list or {})
    if not isinstance(prompt_doc_list.get("docs_to_generate"), list) and isinstance(prompt_doc_list.get("doc_instances"), list):
        prompt_doc_list["docs_to_generate"] = prompt_doc_list.get("doc_instances") or []
    run_id, payload = run_llm_json_process(
        project_id=project_id,
        process_name="process_5",
        prompt_name="04_p5_fill_plan_v02",
        prompt_vars={"doc_list_json": prompt_doc_list, "quality_registry_json": quality_registry},
        files=files,
        output_filename="p5_fill_plan.json",
        mock_payload=_mock_payload(razdel_code, doc_list),
    )
    payload = _normalize_agent_comments(payload)
    save_processing_json(project_id, "p5_fill_plan.json", payload)
    duplicate_output_to_run(project_id, "process_5", run_id, "p5_fill_plan.json")
    return run_id, payload
