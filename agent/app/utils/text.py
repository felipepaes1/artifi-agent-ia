import hashlib
import json
import re
import unicodedata
from typing import Any


def _stringify_text(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return "\n".join(_stringify_text(item) for item in value if item)
    if isinstance(value, dict):
        chunks = []
        for key in ("text", "content", "value"):
            if key in value:
                text = _stringify_text(value.get(key))
                if text:
                    chunks.append(text)
        if chunks:
            return "\n".join(chunks)
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(value)
    return str(value)


def normalize_text(text: Any) -> str:
    if not text:
        return ""
    raw = _stringify_text(text)
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFD", raw.lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_service_text(text: str) -> str:
    normalized = normalize_text(text)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def contains_normalized_term(text: str, term: str) -> bool:
    haystack = normalize_service_text(text)
    needle = normalize_service_text(term)
    if not haystack or not needle:
        return False
    if haystack == needle:
        return True
    return re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack) is not None


def strip_list_prefix(text: str) -> str:
    if not text:
        return ""
    stripped = text.lstrip()
    stripped = re.sub(r"^(?:#|[-*•]+)\s*", "", stripped)
    stripped = re.sub(r"^\d+[.)]\s*", "", stripped)
    return stripped.strip()


def short_hash(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]
