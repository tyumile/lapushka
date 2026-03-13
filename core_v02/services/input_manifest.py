import hashlib
import re
from pathlib import Path


def rel_path(root: Path, path: Path) -> str:
    try:
        return str(path.relative_to(root)).replace("\\", "/")
    except ValueError:
        return path.name


def norm_ref(value: str) -> str:
    return (value or "").strip().replace("\\", "/")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stable_doc_id(rel: str) -> str:
    digest = hashlib.sha1(rel.encode("utf-8")).hexdigest()[:12].upper()
    return f"DOC-{digest}"


def detect_pages_total(path: Path) -> int:
    ext = path.suffix.lower()
    if ext != ".pdf":
        return 1
    try:
        raw = path.read_bytes()
    except OSError:
        return 1
    matches = re.findall(rb"/Type\s*/Page\b", raw)
    return max(1, len(matches))


def build_input_manifest(root: Path, input_files: list[Path], excluded_refs: list[str]) -> dict:
    files_rows: list[dict] = []
    for p in input_files:
        if not p.exists() or not p.is_file():
            continue
        rel = rel_path(root, p)
        files_rows.append(
            {
                "path": rel,
                "file_name": p.name,
                "doc_id": stable_doc_id(rel),
                "pages_total": detect_pages_total(p),
                "size_bytes": p.stat().st_size,
                "sha256": sha256_file(p),
            }
        )
    files_rows.sort(key=lambda x: x["path"])
    return {
        "total_files": len(files_rows),
        "files": files_rows,
        "excluded_refs": sorted({norm_ref(x) for x in excluded_refs if norm_ref(x)}),
    }


def build_manifest_indexes(manifest: dict) -> dict:
    rows = manifest.get("files") or []
    by_path: dict[str, dict] = {}
    by_name: dict[str, list[dict]] = {}
    by_doc_id: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        path = norm_ref(row.get("path", ""))
        if not path:
            continue
        by_path[path] = row
        by_name.setdefault(Path(path).name.lower(), []).append(row)
        doc_id = (row.get("doc_id") or "").strip()
        if doc_id:
            by_doc_id[doc_id] = row
    return {
        "by_path": by_path,
        "by_name": by_name,
        "by_doc_id": by_doc_id,
    }


def match_manifest_row(file_ref: str, doc_id: str, indexes: dict) -> dict | None:
    file_value = norm_ref(file_ref)
    doc_value = (doc_id or "").strip()
    if doc_value:
        row = (indexes.get("by_doc_id") or {}).get(doc_value)
        if row:
            return row
    if file_value:
        row = (indexes.get("by_path") or {}).get(file_value)
        if row:
            return row
        if Path(file_value).suffix:
            candidates = (indexes.get("by_name") or {}).get(Path(file_value).name.lower()) or []
            if len(candidates) == 1:
                return candidates[0]
    return None


def match_manifest_row_strict(file_ref: str, doc_id: str, indexes: dict) -> dict | None:
    file_value = norm_ref(file_ref)
    doc_value = (doc_id or "").strip()
    if doc_value:
        row = (indexes.get("by_doc_id") or {}).get(doc_value)
        if not row:
            return None
        if file_value and norm_ref(row.get("path", "")) != file_value:
            return None
        return row
    if file_value:
        return (indexes.get("by_path") or {}).get(file_value)
    return None


def parse_pages_checked(value, pages_total: int) -> set[int]:
    if value is None or value == "":
        return set()
    if isinstance(value, int):
        return {value} if value > 0 else set()
    if isinstance(value, list):
        out: set[int] = set()
        for item in value:
            out.update(parse_pages_checked(item, pages_total))
        return {p for p in out if 1 <= p <= max(1, pages_total)}
    text = str(value).strip().lower()
    if not text:
        return set()
    if text in {"all", "все"}:
        return set(range(1, max(1, pages_total) + 1))
    out: set[int] = set()
    for part in [x.strip() for x in text.split(",") if x.strip()]:
        if "-" in part:
            left, right = [x.strip() for x in part.split("-", 1)]
            if left.isdigit() and right.isdigit():
                start = int(left)
                end = int(right)
                if start > end:
                    start, end = end, start
                out.update(range(start, end + 1))
            continue
        if part.isdigit():
            out.add(int(part))
    return {p for p in out if 1 <= p <= max(1, pages_total)}
