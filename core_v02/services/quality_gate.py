from typing import Any
from pathlib import Path

from .input_manifest import build_input_manifest, build_manifest_indexes, match_manifest_row, norm_ref, parse_pages_checked

MISSING_ALLOWED_STATUSES = {"needs_extraction", "needs_disambiguation", "blocked_missing_source"}


def _status_allows_missing(obj: dict) -> bool:
    status = (obj.get("status") or obj.get("overall_status") or "").strip().lower()
    return status in MISSING_ALLOWED_STATUSES


def _match_path(file_ref: str, doc_id: str, indexes: dict) -> str:
    row = match_manifest_row(file_ref, doc_id, indexes)
    return (row or {}).get("path", "")


def _collect_payload_refs(payload: Any, out: list[dict] | None = None) -> list[dict]:
    if out is None:
        out = []
    if isinstance(payload, dict):
        if "file_ref" in payload or "file_doc_id" in payload:
            out.append(
                {
                    "file": payload.get("file_ref") if isinstance(payload.get("file_ref"), str) else "",
                    "doc_id": payload.get("file_doc_id") if isinstance(payload.get("file_doc_id"), str) else "",
                }
            )
        if "source" in payload and isinstance(payload.get("source"), dict):
            src = payload.get("source") or {}
            out.append(
                {
                    "file": src.get("file") if isinstance(src.get("file"), str) else "",
                    "doc_id": src.get("doc_id") if isinstance(src.get("doc_id"), str) else "",
                }
            )
        if "sources" in payload and isinstance(payload.get("sources"), list):
            for src in payload.get("sources") or []:
                if not isinstance(src, dict):
                    continue
                out.append(
                    {
                        "file": src.get("file") if isinstance(src.get("file"), str) else "",
                        "doc_id": src.get("doc_id") if isinstance(src.get("doc_id"), str) else "",
                    }
                )
        for v in payload.values():
            _collect_payload_refs(v, out)
    elif isinstance(payload, list):
        for item in payload:
            _collect_payload_refs(item, out)
    return out


def _collect_file_coverage(payload: Any, out: list[dict] | None = None) -> list[dict]:
    if out is None:
        out = []
    if isinstance(payload, dict):
        entries = payload.get("agent_file_coverage")
        if isinstance(entries, list):
            out.extend([x for x in entries if isinstance(x, dict)])
        for v in payload.values():
            _collect_file_coverage(v, out)
    elif isinstance(payload, list):
        for item in payload:
            _collect_file_coverage(item, out)
    return out


def _collect_traceability_errors(
    payload: Any,
    indexes: dict,
    *,
    enforce_membership: bool = True,
    path: str = "$",
    out: list[dict] | None = None,
) -> list[dict]:
    if out is None:
        out = []
    if isinstance(payload, dict):
        allows_missing = _status_allows_missing(payload)
        if "file_ref" in payload:
            file_ref = payload.get("file_ref")
            file_doc_id = payload.get("file_doc_id") if isinstance(payload.get("file_doc_id"), str) else ""
            if isinstance(file_ref, str):
                if not file_ref.strip() and not file_doc_id and not allows_missing:
                    out.append({"path": f"{path}.file_ref", "issue": "missing_file_ref"})
                elif (file_ref.strip() or file_doc_id) and not _match_path(file_ref, file_doc_id, indexes):
                    out.append({"path": f"{path}.file_ref", "issue": "unknown_file_ref", "value": file_ref, "doc_id": file_doc_id})
            elif not allows_missing:
                out.append({"path": f"{path}.file_ref", "issue": "invalid_file_ref_type"})
        if "source" in payload:
            src = payload.get("source")
            if isinstance(src, dict):
                f = (src.get("file") or "").strip() if isinstance(src.get("file"), str) else ""
                doc_id = (src.get("doc_id") or "").strip() if isinstance(src.get("doc_id"), str) else ""
                if not f and not doc_id and not allows_missing:
                    out.append({"path": f"{path}.source.file", "issue": "missing_source_file"})
                elif enforce_membership and (f or doc_id) and not _match_path(f, doc_id, indexes):
                    out.append({"path": f"{path}.source.file", "issue": "unknown_source_file", "value": f, "doc_id": doc_id})
            elif not allows_missing:
                out.append({"path": f"{path}.source", "issue": "invalid_source_type"})
        if "sources" in payload:
            sources = payload.get("sources")
            if isinstance(sources, list):
                if not sources and not allows_missing:
                    out.append({"path": f"{path}.sources", "issue": "empty_sources"})
                for i, s in enumerate(sources):
                    if not isinstance(s, dict):
                        out.append({"path": f"{path}.sources[{i}]", "issue": "invalid_source_entry"})
                        continue
                    f = (s.get("file") or "").strip() if isinstance(s.get("file"), str) else ""
                    doc_id = (s.get("doc_id") or "").strip() if isinstance(s.get("doc_id"), str) else ""
                    if not f and not doc_id and not allows_missing:
                        out.append({"path": f"{path}.sources[{i}].file", "issue": "missing_source_file"})
                    elif enforce_membership and (f or doc_id) and not _match_path(f, doc_id, indexes):
                        out.append({"path": f"{path}.sources[{i}].file", "issue": "unknown_source_file", "value": f, "doc_id": doc_id})
            elif not allows_missing:
                out.append({"path": f"{path}.sources", "issue": "invalid_sources_type"})
        for k, v in payload.items():
            _collect_traceability_errors(
                v,
                indexes,
                enforce_membership=enforce_membership,
                path=f"{path}.{k}",
                out=out,
            )
    elif isinstance(payload, list):
        for i, item in enumerate(payload):
            _collect_traceability_errors(
                item,
                indexes,
                enforce_membership=enforce_membership,
                path=f"{path}[{i}]",
                out=out,
            )
    return out


def _collect_page_coverage_errors(entries: list[dict], indexes: dict, required_rows: dict[str, dict]) -> tuple[list[dict], dict[str, set[int]]]:
    errors: list[dict] = []
    coverage_by_path: dict[str, set[int]] = {path: set() for path in required_rows}
    for idx, entry in enumerate(entries):
        file_ref = entry.get("file_ref") if isinstance(entry.get("file_ref"), str) else ""
        doc_id = entry.get("doc_id") if isinstance(entry.get("doc_id"), str) else ""
        row = match_manifest_row(file_ref, doc_id, indexes)
        if not row:
            errors.append({"path": f"$.agent_file_coverage[{idx}]", "issue": "unknown_coverage_file", "value": file_ref, "doc_id": doc_id})
            continue
        pages_total = int(row.get("pages_total") or 1)
        provided_total = entry.get("pages_total")
        if isinstance(provided_total, int) and provided_total != pages_total:
            errors.append(
                {
                    "path": f"$.agent_file_coverage[{idx}].pages_total",
                    "issue": "wrong_pages_total",
                    "expected": pages_total,
                    "value": provided_total,
                }
            )
        covered_pages = parse_pages_checked(entry.get("pages_checked"), pages_total)
        if not covered_pages:
            errors.append({"path": f"$.agent_file_coverage[{idx}].pages_checked", "issue": "empty_pages_checked"})
            continue
        coverage_by_path.setdefault(row["path"], set()).update(covered_pages)

    for path, row in required_rows.items():
        pages_total = int(row.get("pages_total") or 1)
        required_pages = set(range(1, pages_total + 1))
        missing = sorted(required_pages - coverage_by_path.get(path, set()))
        if missing:
            errors.append({"path": path, "issue": "missing_page_coverage", "missing_pages": missing[:200]})
    return errors, coverage_by_path


def run_quality_gate(
    *,
    process_name: str,
    root: Path,
    payload: dict,
    input_files: list[Path],
    excluded_refs: list[str] | None = None,
    required_files: list[Path] | None = None,
) -> tuple[dict, dict]:
    excluded = [norm_ref(x) for x in (excluded_refs or []) if norm_ref(x)]
    manifest = build_input_manifest(root, input_files, excluded)
    indexes = build_manifest_indexes(manifest)

    required_paths = {}
    for p in (required_files or input_files):
        if not p.exists() or not p.is_file():
            continue
        row = match_manifest_row(str(p), "", indexes)
        if row and row["path"] not in excluded:
            required_paths[row["path"]] = row

    raw_refs = _collect_payload_refs(payload)
    matched_refs = sorted(
        {
            _match_path(str(ref.get("file") or ""), str(ref.get("doc_id") or ""), indexes)
            for ref in raw_refs
            if _match_path(str(ref.get("file") or ""), str(ref.get("doc_id") or ""), indexes)
        }
    )
    covered_required = sorted([x for x in required_paths if x in set(matched_refs)])
    uncovered_required = sorted([x for x in required_paths if x not in set(matched_refs)])

    trace_errors = _collect_traceability_errors(
        payload,
        indexes,
        enforce_membership=bool(indexes.get("by_path")),
    )
    trace_errors = trace_errors[:200]
    coverage_entries = _collect_file_coverage(payload)
    page_coverage_errors, coverage_by_path = _collect_page_coverage_errors(coverage_entries, indexes, required_paths)
    page_coverage_errors = page_coverage_errors[:200]
    files_with_full_page_coverage = sorted(
        [
            path for path, row in required_paths.items()
            if len(coverage_by_path.get(path, set())) >= int(row.get("pages_total") or 1)
        ]
    )
    ok = not uncovered_required and not trace_errors and not page_coverage_errors
    report = {
        "process": process_name,
        "pass": ok,
        "summary": (
            f"quality_gate=PASS; required={len(required_paths)}; covered={len(covered_required)}; page_coverage={len(files_with_full_page_coverage)}; trace_errors=0"
            if ok
            else (
                f"quality_gate=FAIL; uncovered_required={len(uncovered_required)}; "
                f"page_errors={len(page_coverage_errors)}; trace_errors={len(trace_errors)}"
            )
        ),
        "totals": {
            "input_files": len(indexes.get("by_path") or {}),
            "required_files": len(required_paths),
            "excluded_files": len(excluded),
            "matched_refs": len(matched_refs),
            "covered_required": len(covered_required),
            "uncovered_required": len(uncovered_required),
            "files_with_full_page_coverage": len(files_with_full_page_coverage),
            "files_missing_page_coverage": max(0, len(required_paths) - len(files_with_full_page_coverage)),
            "page_coverage_errors": len(page_coverage_errors),
            "traceability_errors": len(trace_errors),
        },
        "uncovered_required_files": uncovered_required,
        "matched_refs": matched_refs,
        "page_coverage_errors": page_coverage_errors,
        "files_with_full_page_coverage": files_with_full_page_coverage,
        "traceability_errors": trace_errors,
    }
    return manifest, report
