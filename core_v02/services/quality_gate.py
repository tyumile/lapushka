import hashlib
from pathlib import Path
from typing import Any


MISSING_ALLOWED_STATUSES = {"needs_extraction", "needs_disambiguation", "blocked_missing_source"}


def _rel(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root)).replace("\\", "/")
    except ValueError:
        return path.name


def _norm_ref(value: str) -> str:
    return (value or "").strip().replace("\\", "/")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_input_manifest(root: Path, input_files: list[Path], excluded_refs: list[str]) -> dict:
    files_rows: list[dict] = []
    for p in input_files:
        if not p.exists() or not p.is_file():
            continue
        files_rows.append(
            {
                "path": _rel(root, p),
                "size_bytes": p.stat().st_size,
                "sha256": _sha256(p),
            }
        )
    files_rows.sort(key=lambda x: x["path"])
    return {
        "total_files": len(files_rows),
        "files": files_rows,
        "excluded_refs": sorted({_norm_ref(x) for x in excluded_refs if _norm_ref(x)}),
    }


def _collect_refs(payload: Any, refs: list[str]) -> None:
    if isinstance(payload, dict):
        for k, v in payload.items():
            if k in {"file_ref", "file"} and isinstance(v, str):
                refs.append(_norm_ref(v))
            _collect_refs(v, refs)
        return
    if isinstance(payload, list):
        for item in payload:
            _collect_refs(item, refs)


def _build_name_index(paths: list[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for p in paths:
        name = Path(p).name.lower()
        out.setdefault(name, []).append(p)
    return out


def _match_ref(ref: str, known_paths: set[str], by_name: dict[str, list[str]]) -> str:
    value = _norm_ref(ref)
    if not value:
        return ""
    if value in known_paths:
        return value
    # Skip folder-like references.
    if not Path(value).suffix:
        return ""
    name = Path(value).name.lower()
    candidates = by_name.get(name) or []
    if len(candidates) == 1:
        return candidates[0]
    return ""


def _status_allows_missing(obj: dict) -> bool:
    status = (obj.get("status") or obj.get("overall_status") or "").strip().lower()
    return status in MISSING_ALLOWED_STATUSES


def _collect_traceability_errors(
    payload: Any,
    known_paths: set[str],
    by_name: dict[str, list[str]],
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
            if isinstance(file_ref, str):
                if not file_ref.strip() and not allows_missing:
                    out.append({"path": f"{path}.file_ref", "issue": "missing_file_ref"})
                elif file_ref.strip() and not _match_ref(file_ref, known_paths, by_name):
                    out.append({"path": f"{path}.file_ref", "issue": "unknown_file_ref", "value": file_ref})
            elif not allows_missing:
                out.append({"path": f"{path}.file_ref", "issue": "invalid_file_ref_type"})
        if "source" in payload:
            src = payload.get("source")
            if isinstance(src, dict):
                f = (src.get("file") or "").strip() if isinstance(src.get("file"), str) else ""
                if not f and not allows_missing:
                    out.append({"path": f"{path}.source.file", "issue": "missing_source_file"})
                elif enforce_membership and f and not _match_ref(f, known_paths, by_name):
                    out.append({"path": f"{path}.source.file", "issue": "unknown_source_file", "value": f})
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
                    if not f and not allows_missing:
                        out.append({"path": f"{path}.sources[{i}].file", "issue": "missing_source_file"})
                    elif enforce_membership and f and not _match_ref(f, known_paths, by_name):
                        out.append({"path": f"{path}.sources[{i}].file", "issue": "unknown_source_file", "value": f})
            elif not allows_missing:
                out.append({"path": f"{path}.sources", "issue": "invalid_sources_type"})
        for k, v in payload.items():
            _collect_traceability_errors(
                v,
                known_paths,
                by_name,
                enforce_membership=enforce_membership,
                path=f"{path}.{k}",
                out=out,
            )
    elif isinstance(payload, list):
        for i, item in enumerate(payload):
            _collect_traceability_errors(
                item,
                known_paths,
                by_name,
                enforce_membership=enforce_membership,
                path=f"{path}[{i}]",
                out=out,
            )
    return out


def run_quality_gate(
    *,
    process_name: str,
    root: Path,
    payload: dict,
    input_files: list[Path],
    excluded_refs: list[str] | None = None,
    required_files: list[Path] | None = None,
) -> tuple[dict, dict]:
    excluded = [_norm_ref(x) for x in (excluded_refs or []) if _norm_ref(x)]
    manifest = build_input_manifest(root, input_files, excluded)

    known_paths = {row["path"] for row in manifest.get("files") or []}
    by_name = _build_name_index(sorted(known_paths))
    required_paths = {
        _rel(root, p) for p in (required_files or input_files) if p.exists() and p.is_file() and _rel(root, p) not in excluded
    }

    raw_refs: list[str] = []
    _collect_refs(payload, raw_refs)
    matched_refs = sorted({_match_ref(r, known_paths, by_name) for r in raw_refs if _match_ref(r, known_paths, by_name)})
    covered_required = sorted([x for x in required_paths if x in set(matched_refs)])
    uncovered_required = sorted([x for x in required_paths if x not in set(matched_refs)])

    trace_errors = _collect_traceability_errors(
        payload,
        known_paths,
        by_name,
        enforce_membership=bool(known_paths),
    )
    trace_errors = trace_errors[:200]
    ok = not uncovered_required and not trace_errors
    report = {
        "process": process_name,
        "pass": ok,
        "summary": (
            f"quality_gate=PASS; required={len(required_paths)}; covered={len(covered_required)}; trace_errors=0"
            if ok
            else (
                f"quality_gate=FAIL; uncovered_required={len(uncovered_required)}; "
                f"trace_errors={len(trace_errors)}"
            )
        ),
        "totals": {
            "input_files": len(known_paths),
            "required_files": len(required_paths),
            "excluded_files": len(excluded),
            "matched_refs": len(matched_refs),
            "covered_required": len(covered_required),
            "uncovered_required": len(uncovered_required),
            "traceability_errors": len(trace_errors),
        },
        "uncovered_required_files": uncovered_required,
        "matched_refs": matched_refs,
        "traceability_errors": trace_errors,
    }
    return manifest, report
