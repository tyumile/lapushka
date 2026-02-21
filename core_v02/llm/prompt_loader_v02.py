import json
from pathlib import Path


def _prompts_dir() -> Path:
    return Path(__file__).resolve().parent / "prompts"


def render_prompt(text: str, variables: dict) -> str:
    out = text
    for key, value in variables.items():
        token = "{{" + key + "}}"
        if token not in out:
            continue
        replacement = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        out = out.replace(token, replacement)
    return out


def load_prompt(name: str, variables: dict | None = None) -> str:
    filename = name if name.endswith(".txt") else f"{name}.txt"
    path = _prompts_dir() / filename
    text = path.read_text(encoding="utf-8")
    return render_prompt(text, variables or {})
