from typing import Any, Optional

from ...utils.text import normalize_text


def matches_ariane_alias(value: str) -> bool:
    normalized = normalize_text(value)
    if not normalized:
        return False
    if normalized == "ariane" or "ariane" in normalized:
        return True
    return normalized in ("estetica capilar", "estetica e terapia capilar")


def is_ariane_profile(
    profile_id: Optional[str],
    *,
    resolved_profile_id: str = "",
    prompt_profile: str = "",
) -> bool:
    if matches_ariane_alias(profile_id or ""):
        return True
    if profile_id:
        return False
    if matches_ariane_alias(resolved_profile_id):
        return True
    if resolved_profile_id:
        return False
    return matches_ariane_alias(prompt_profile)


def is_ariane_context_from_items(items: list[dict[str, Any]], user_text: str = "") -> bool:
    if not items and not user_text:
        return False
    chunks: list[str] = []
    for item in items[-16:]:
        if item.get("role") not in ("assistant", "user"):
            continue
        content = (item.get("content") or "").strip()
        if content:
            chunks.append(content)
    if user_text:
        chunks.append(user_text)
    corpus = normalize_text("\n".join(chunks))
    if not corpus:
        return False
    markers = (
        "ariane estevam",
        "terapia capilar",
        "consulta capilar",
        "queda associada a rarefacao",
        "rarefacao no couro cabeludo",
        "couro cabeludo e fios",
        "4x de r$82",
    )
    score = 0
    for marker in markers:
        if marker in corpus:
            score += 1
    return score >= 2

