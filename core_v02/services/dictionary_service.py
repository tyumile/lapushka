import json
from pathlib import Path


def _default_dictionary() -> dict:
    return {
        "version": "0.2.0-fallback",
        "default_razdel_code": "KJ",
        "razdels": [
            {
                "razdel_code": "KJ",
                "razdel_name": "КЖ",
                "doc_types": [],
            }
        ],
    }


def _is_executive_scheme_name(name: str) -> bool:
    lowered = (name or "").strip().lower()
    return "исполнитель" in lowered and ("схем" in lowered or "чертеж" in lowered)


def _sanitize_remove_executive_schemes(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {}
    out = dict(payload)
    razdels = []
    for razdel in out.get("razdels") or []:
        if not isinstance(razdel, dict):
            continue
        r = dict(razdel)
        kept_types = []
        for doc_type in r.get("doc_types") or []:
            if not isinstance(doc_type, dict):
                continue
            type_name = doc_type.get("doc_type_name") or ""
            if _is_executive_scheme_name(type_name):
                continue
            docs = []
            for doc in doc_type.get("docs") or []:
                if not isinstance(doc, dict):
                    continue
                if _is_executive_scheme_name(doc.get("name") or ""):
                    continue
                docs.append(doc)
            if docs:
                d = dict(doc_type)
                d["docs"] = docs
                kept_types.append(d)
        r["doc_types"] = kept_types
        razdels.append(r)
    out["razdels"] = razdels
    return out


def _v02_dictionary_path() -> Path:
    return Path(__file__).resolve().parent.parent / "data" / "id_dictionary_v02.json"


def _v01_dictionary_path() -> Path:
    return Path(__file__).resolve().parents[3] / "virtual_engineer" / "id_dictionary_for_agent.json"


def _doc_from_v01(doc_id: str, catalog_item: dict) -> dict:
    multiplicity = (catalog_item.get("multiplicity") or "single").strip().lower()
    name = (catalog_item.get("name") or doc_id).strip()
    if _is_executive_scheme_name(name):
        return {}
    return {
        "doc_id": doc_id,
        "name": name,
        "multi_rule": "multi" if multiplicity in {"multiple", "multi"} else "single",
        "multi_axis_allowed": catalog_item.get("binding") or [],
        "default_output_format": "xlsx",
        "template_kind": "table",
    }


def adapt_v01_to_v02(v01: dict) -> dict:
    catalog = v01.get("справочник_документов") or {}
    sections = v01.get("разделы") or {}
    default_code = ((v01.get("схема") or {}).get("раздел_по_умолчанию") or "KJ").strip() or "KJ"

    razdels: list[dict] = []
    for razdel_code, section_payload in sections.items():
        doc_ids: list[str] = []
        docs_block = section_payload.get("документы") if isinstance(section_payload, dict) else {}
        for k in ("обязательные", "условные_по_требованию"):
            for doc_id in docs_block.get(k) or []:
                if isinstance(doc_id, str) and doc_id.strip() and doc_id not in doc_ids:
                    doc_ids.append(doc_id)
        docs: list[dict] = []
        for doc_id in doc_ids:
            meta = catalog.get(doc_id) if isinstance(catalog, dict) else None
            if not isinstance(meta, dict):
                meta = {"name": doc_id, "multiplicity": "single", "binding": []}
            item = _doc_from_v01(doc_id, meta)
            if item:
                docs.append(item)
        razdels.append(
            {
                "razdel_code": razdel_code,
                "razdel_name": (section_payload.get("название") or razdel_code) if isinstance(section_payload, dict) else razdel_code,
                "doc_types": [
                    {
                        "doc_type_id": "base",
                        "doc_type_name": "Основные документы",
                        "docs": docs,
                    }
                ],
            }
        )
    return _sanitize_remove_executive_schemes(
        {"version": "0.2.0-migrated", "default_razdel_code": default_code, "razdels": razdels}
    )


def validate_dictionary(payload: dict) -> list[str]:
    issues: list[str] = []
    if not isinstance(payload, dict):
        return ["dictionary must be object"]
    ids: set[str] = set()
    for razdel in payload.get("razdels") or []:
        if not isinstance(razdel, dict):
            issues.append("razdel is not object")
            continue
        if not razdel.get("razdel_code"):
            issues.append("missing razdel_code")
        for doc_type in razdel.get("doc_types") or []:
            if not isinstance(doc_type, dict):
                issues.append("doc_type is not object")
                continue
            if not doc_type.get("doc_type_id"):
                issues.append(f"missing doc_type_id in {razdel.get('razdel_code')}")
            for doc in doc_type.get("docs") or []:
                if not isinstance(doc, dict):
                    issues.append("doc is not object")
                    continue
                doc_id = (doc.get("doc_id") or "").strip()
                if not doc_id:
                    issues.append("missing doc_id")
                if doc_id in ids:
                    issues.append(f"duplicate doc_id: {doc_id}")
                ids.add(doc_id)
    return issues


def load_dictionary() -> dict:
    path = _v02_dictionary_path()
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload = _sanitize_remove_executive_schemes(payload)
            issues = validate_dictionary(payload)
            if not issues:
                return payload
            # Best-effort mode: keep usable v0.2 dictionary even with non-critical issues.
            if payload.get("razdels"):
                return payload
        except (json.JSONDecodeError, OSError):
            pass
    v01_path = _v01_dictionary_path()
    if v01_path.exists():
        try:
            v01 = json.loads(v01_path.read_text(encoding="utf-8"))
            converted = adapt_v01_to_v02(v01)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(converted, ensure_ascii=False, indent=2), encoding="utf-8")
            return converted
        except (json.JSONDecodeError, OSError):
            pass

    # Never crash Process 2 because of dictionary file issues.
    # Important: do not overwrite repository dictionary file with fallback.
    fallback = _default_dictionary()
    return fallback


def get_razdels() -> list[dict]:
    return load_dictionary().get("razdels") or []


def get_razdel(razdel_code: str | None = None) -> dict:
    payload = load_dictionary()
    razdels = payload.get("razdels") or []
    target = (razdel_code or payload.get("default_razdel_code") or "").strip()
    for razdel in razdels:
        if (razdel.get("razdel_code") or "").strip() == target:
            return razdel
    return razdels[0] if razdels else {}


def get_doc_types(razdel_code: str) -> list[dict]:
    return get_razdel(razdel_code).get("doc_types") or []


def get_docs_for_doc_types(razdel_code: str, selected_doc_type_ids: list[str]) -> list[dict]:
    selected = set((x or "").strip() for x in selected_doc_type_ids if x)
    docs: list[dict] = []
    seen: set[str] = set()
    for d_type in get_doc_types(razdel_code):
        if (d_type.get("doc_type_id") or "") not in selected:
            continue
        for doc in d_type.get("docs") or []:
            doc_id = (doc.get("doc_id") or "").strip()
            if not doc_id or doc_id in seen:
                continue
            seen.add(doc_id)
            docs.append(doc)
    return docs
