from .dictionary_service import get_docs_for_doc_types
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
        for d in (out.get("docs_to_generate") or [])[:5]:
            ev = (d.get("evidence") or [])
            source = ev[0] if ev and isinstance(ev[0], dict) else {"file": "", "page": "", "snippet": ""}
            normalized.append(
                {
                    "comment": f"Документ '{d.get('doc_name') or d.get('doc_id')}' выбран по результатам анализа входных файлов.",
                    "source": source,
                }
            )
    out["agent_comments"] = normalized
    return out


def _mock_payload(razdel_code: str, selected_doc_type_ids: list[str], docs: list[dict]) -> dict:
    docs_to_generate = []
    for idx, d in enumerate(docs, start=1):
        multi = (d.get("multi_rule") or "single") == "multi"
        docs_to_generate.append(
            {
                "doc_id": d.get("doc_id"),
                "doc_name": d.get("name"),
                "multi": multi,
                "instances": (
                    [
                        {"instance_id": "i01", "instance_label": "Секция 1"},
                        {"instance_id": "i02", "instance_label": "Секция 2"},
                    ]
                    if multi
                    else []
                ),
                "status": "ok",
                "confidence": 0.55,
                "evidence": [{"file": "01_input/01_project/mock_project.pdf", "page": "1", "snippet": d.get("name")}],
                "doc_number_suggestion": f"ИД-{idx:03d}",
            }
        )
    return {
        "razdel_code": razdel_code,
        "selected_doc_type_ids": selected_doc_type_ids,
        "docs_to_generate": docs_to_generate,
        "agent_comments": [
            {
                "comment": "Список документов сформирован по выбранным типам и найденным признакам в файлах.",
                "source": {"file": "01_input/01_project", "page": "", "snippet": "auto"},
            }
        ],
    }


def run_process_p4(
    project_id: str,
    razdel_code: str,
    selected_doc_type_ids: list[str],
    quality_registry: dict,
    files: list[Path],
) -> tuple[str, dict]:
    docs = get_docs_for_doc_types(razdel_code, selected_doc_type_ids)
    run_id, payload = run_llm_json_process(
        project_id=project_id,
        process_name="process_4",
        prompt_name="03_p4_doc_list_v02",
        prompt_vars={
            "selected_doc_type_ids": selected_doc_type_ids,
            "dictionary_json": docs,
            "quality_registry_json": quality_registry,
        },
        files=files,
        output_filename="p4_doc_list_v1.json",
        mock_payload=_mock_payload(razdel_code, selected_doc_type_ids, docs),
    )
    payload = _normalize_agent_comments(payload)
    save_processing_json(project_id, "p4_doc_types_selection.json", {"selected_doc_type_ids": selected_doc_type_ids})
    save_processing_json(project_id, "p4_doc_list_v1.json", payload)
    save_processing_json(project_id, "p4_doc_list_final.json", payload)
    duplicate_output_to_run(project_id, "process_4", run_id, "p4_doc_list_v1.json")
    return run_id, payload
