import hashlib
import re
import unicodedata


def normalize_text(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text.lower())
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

