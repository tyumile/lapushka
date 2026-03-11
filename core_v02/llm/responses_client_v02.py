import io
import json
import os
import re
import time
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import APIConnectionError, APIStatusError, OpenAI, RateLimitError

# Ensure .env is loaded before reading OPENAI_API_KEY (handles cases when Django settings aren't loaded yet)
_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path, override=True)


def _extract_output_text(response: Any) -> str:
    output = getattr(response, "output", None) or []
    for item in output:
        if getattr(item, "type", None) != "message":
            continue
        content = getattr(item, "content", None) or []
        for block in content:
            if getattr(block, "type", None) == "output_text":
                return getattr(block, "text", "") or ""
    return ""


def _parse_json_or_none(raw: str) -> dict | None:
    value = (raw or "").strip()
    candidates = [value] + re.findall(r"```(?:json)?\s*\n?(.*?)\n?```", value, re.DOTALL)
    extracted = _extract_json_object(value)
    if extracted and extracted != value:
        candidates.append(extracted)
    for candidate in candidates:
        if not candidate:
            continue
        try:
            parsed = json.loads(candidate)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except json.JSONDecodeError:
            continue
    return None


def _extract_json_object(text: str) -> str | None:
    """Extract first complete {...} or [...] from text."""
    if not text:
        return None
    start = text.find("{")
    if start < 0:
        start = text.find("[")
        if start < 0:
            return None
    depth = 0
    open_ch, close_ch = ("{", "}") if text[start] == "{" else ("[", "]")
    for i in range(start, len(text)):
        if text[i] == open_ch:
            depth += 1
        elif text[i] == close_ch:
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _is_connection_like_error(exc: Exception) -> bool:
    if isinstance(exc, APIConnectionError):
        return True
    text = (str(exc) or "").lower()
    hints = (
        "connection error",
        "connection reset",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "dns",
        "name resolution",
    )
    return any(h in text for h in hints)


def _retry_pauses_from_env(var_name: str, default: str) -> list[int]:
    raw = (os.getenv(var_name) or default).strip()
    parts = [x.strip() for x in raw.split(",") if x.strip()]
    out: list[int] = []
    for p in parts:
        try:
            val = int(p)
        except ValueError:
            continue
        if val > 0:
            out.append(val)
    return out or [1, 2, 4, 8, 16]


class ResponsesClientV02:
    def __init__(self, api_key: str | None = None, on_retry=None):
        self._api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        if not self._api_key:
            raise ValueError("OPENAI_API_KEY is not set")
        self._on_retry = on_retry

    def _client(self, timeout_s: int = 120) -> OpenAI:
        return OpenAI(api_key=self._api_key, timeout=timeout_s)

    def upload_file_bytes(self, filename: str, content: bytes, timeout_s: int = 300) -> str:
        pauses = _retry_pauses_from_env("OPENAI_UPLOAD_RETRY_PAUSES", "1,2,4,8,16")
        for i, pause in enumerate(pauses, start=1):
            try:
                stream = io.BytesIO(content)
                file_obj = (filename, stream)
                f = self._client(timeout_s=timeout_s).files.create(file=file_obj, purpose="user_data")
                return f.id
            except Exception as e:
                if not _is_connection_like_error(e):
                    raise
                if i == len(pauses):
                    raise
                if callable(self._on_retry):
                    try:
                        self._on_retry(
                            {
                                "kind": "upload",
                                "attempt": i,
                                "next_pause_s": pause,
                                "error": str(e)[:300],
                                "filename": filename,
                            }
                        )
                    except Exception:
                        pass
                time.sleep(pause)
        raise RuntimeError("Upload retries exhausted")

    def call_json_with_files(
        self,
        *,
        instructions: str,
        user_text: str,
        file_ids: list[str],
        model: str,
        timeout_s: int = 120,
    ) -> tuple[dict, str]:
        pauses = _retry_pauses_from_env("OPENAI_CALL_RETRY_PAUSES", "1,2,4,8,16,24")
        last_raw = ""
        for i, pause in enumerate(pauses, start=1):
            try:
                raw = self._request_with_files(
                    instructions=instructions,
                    user_text=user_text if i == 1 else (user_text + "\n\nReturn valid JSON only."),
                    file_ids=file_ids,
                    model=model,
                    timeout_s=timeout_s,
                )
            except (APIConnectionError, APIStatusError, RateLimitError) as e:
                last_raw = str(e)
                if i < len(pauses):
                    if callable(self._on_retry):
                        try:
                            self._on_retry(
                                {
                                    "kind": "call",
                                    "attempt": i,
                                    "next_pause_s": pause,
                                    "error": str(e)[:300],
                                    "model": model,
                                }
                            )
                        except Exception:
                            pass
                    time.sleep(pause)
                    continue
                raise
            except Exception as e:
                last_raw = str(e)
                if _is_connection_like_error(e) and i < len(pauses):
                    if callable(self._on_retry):
                        try:
                            self._on_retry(
                                {
                                    "kind": "call",
                                    "attempt": i,
                                    "next_pause_s": pause,
                                    "error": str(e)[:300],
                                    "model": model,
                                }
                            )
                        except Exception:
                            pass
                    time.sleep(pause)
                    continue
                raise
            last_raw = raw
            parsed = _parse_json_or_none(raw)
            if parsed is not None:
                return parsed, raw
            if i < len(pauses):
                time.sleep(pause)
        raise ValueError(f"Model returned invalid JSON after retries: {last_raw[:2000]}")

    def _request_with_files(
        self,
        *,
        instructions: str,
        user_text: str,
        file_ids: list[str],
        model: str,
        timeout_s: int,
    ) -> str:
        content = [{"type": "input_file", "file_id": fid} for fid in file_ids]
        content.append({"type": "input_text", "text": user_text})
        response = self._client(timeout_s=timeout_s).responses.create(
            model=model,
            instructions=instructions,
            input=[{"role": "user", "content": content}],
        )
        return _extract_output_text(response)
