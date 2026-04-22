import re
from typing import Optional

from ..config.settings import (
    MESSAGE_SPLIT_MAX_CHARS,
    MESSAGE_SPLIT_PRESENTATION_MAX_CHARS,
    MESSAGE_SPLIT_SENTENCE_THRESHOLD,
    MESSAGE_SPLIT_TARGET_CHARS,
    PROMPT_PROFILE,
)
from ..core.profiles import (
    get_profile_first_message_delay_ms,
    get_profile_message_delay_ms,
    get_profile_schedule_delay_ms,
    profile_uses_direct_response_style,
)
from ..profiles.ariane.rules import matches_ariane_alias
from ..utils.text import normalize_text, strip_list_prefix


_SENT_BOUNDARY = re.compile(r"([.!?…])(\s+)(?=[A-ZÁÉÍÓÚÂÊÔÃÕÀÇÜ0-9])")
_ABBREVIATIONS = frozenset(
    {
        "dr",
        "dra",
        "sr",
        "sra",
        "prof",
        "profa",
        "etc",
        "obs",
        "vs",
        "ex",
        "n",
        "no",
        "nos",
        "pe",
        "fl",
        "fls",
    }
)


def _split_into_sentences(text: str) -> list[str]:
    """Split text on sentence boundaries, respecting common PT-BR abbreviations.

    Keeps terminal punctuation attached to each sentence. Boundaries require
    whitespace followed by an uppercase letter or digit, to avoid cutting
    decimals like "R$ 600,00" or initials.
    """
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    pieces: list[str] = []
    last_pos = 0
    for match in _SENT_BOUNDARY.finditer(cleaned):
        punct_pos = match.start(1)
        k = punct_pos - 1
        while k >= 0 and cleaned[k].isalpha():
            k -= 1
        word = cleaned[k + 1 : punct_pos].lower()
        if word in _ABBREVIATIONS:
            continue
        end = match.end(1)
        pieces.append(cleaned[last_pos:end].strip())
        last_pos = match.end()
    if last_pos < len(cleaned):
        pieces.append(cleaned[last_pos:].strip())
    return [p for p in pieces if p]


def clamp_int(raw_value: str, *, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(value, max_value))


def delay_seconds_from_ms(
    raw_value: str,
    *,
    default_ms: int,
    min_ms: int = 0,
    max_ms: int = 5000,
) -> float:
    ms = clamp_int(raw_value, default=default_ms, min_value=min_ms, max_value=max_ms)
    return ms / 1000.0


def looks_like_presentation_message(text: str, profile_id: Optional[str] = None) -> bool:
    cleaned = (text or "").strip()
    if not cleaned:
        return False
    lowered = normalize_text(cleaned)
    if cleaned.count("\n✅ ") >= 2:
        return True
    is_ariane = matches_ariane_alias(profile_id or "") or (not profile_id and matches_ariane_alias(PROMPT_PROFILE))
    if not is_ariane:
        return False
    if cleaned.count("\n") < 2:
        return False
    return any(
        marker in lowered
        for marker in (
            "voce pode esperar",
            "beneficios",
            "o que voce recebe",
            "inclui",
            "consulta capilar:",
            "terapia capilar:",
            "estetica facial:",
        )
    )


def is_emoji_or_punctuation_only(text: str) -> bool:
    cleaned = (text or "").strip()
    if not cleaned:
        return False
    if any(ch.isalnum() for ch in cleaned):
        return False
    return len(cleaned) <= 8


def normalize_whatsapp_part(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""
    if cleaned.endswith(".") and not cleaned.endswith("..."):
        last_token = cleaned.split()[-1].lower() if cleaned.split() else ""
        abbreviations = {"dr.", "dra.", "sr.", "sra.", "etc.", "obs.", "prof.", "profa."}
        if last_token not in abbreviations:
            cleaned = cleaned[:-1].rstrip()
    return cleaned


def split_long_chunk(text: str, max_chars: int) -> list[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]

    chunks: list[str] = []
    remaining = cleaned
    while len(remaining) > max_chars:
        window = remaining[:max_chars]
        minimum = int(max_chars * 0.45)

        split_at = -1
        for pattern in ("\n\n", "\n"):
            idx = window.rfind(pattern)
            if idx >= minimum:
                split_at = idx
                break
        if split_at < 0:
            for ender in (". ", "! ", "? ", "… "):
                idx = window.rfind(ender)
                if idx >= minimum:
                    split_at = idx + len(ender) - 1
                    break
        if split_at < 0:
            idx = window.rfind(" ")
            if idx >= minimum:
                split_at = idx
        if split_at < 0:
            # No good boundary inside window — look for the next space AFTER max_chars
            # so we never cut mid-word, even if the resulting chunk exceeds max_chars.
            next_space = remaining.find(" ", max_chars)
            if next_space > 0:
                split_at = next_space
            else:
                # No whitespace anywhere ahead — emit the whole remainder.
                split_at = len(remaining)

        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


def merge_short_whatsapp_parts(
    parts: list[str],
    target_chars: int,
    short_limit: Optional[int] = None,
) -> list[str]:
    if not parts:
        return []
    merged: list[str] = []
    if short_limit is None:
        short_limit = max(60, min(120, int(target_chars * 0.35)))

    for part in parts:
        text = (part or "").strip()
        if not text:
            continue
        if not merged:
            merged.append(text)
            continue

        prev = merged[-1]
        is_short = len(text) <= short_limit
        emoji_or_punct_only = is_emoji_or_punctuation_only(text)
        same_message_budget = len(prev) + len(text) + 2 <= target_chars
        is_question = text.endswith("?")
        # Previous part ending in ":" is an introducer — always keep its
        # follow-up attached so users don't get a "me informe:" dangling alone.
        prev_is_introducer = prev.rstrip().endswith(":")

        if same_message_budget and prev_is_introducer:
            merged[-1] = f"{prev}\n{text}".strip()
        elif same_message_budget and (is_short or emoji_or_punct_only) and not is_question:
            joiner = " " if emoji_or_punct_only else "\n"
            merged[-1] = f"{prev}{joiner}{text}".strip()
        else:
            merged.append(text)

    return merged


def split_messages(text: str, profile_id: Optional[str] = None) -> list[str]:
    if not text:
        return []
    cleaned = text.replace("\r\n", "\n").strip()
    if not cleaned:
        return []

    target_chars = clamp_int(
        MESSAGE_SPLIT_TARGET_CHARS,
        default=420,
        min_value=120,
        max_value=1800,
    )
    max_chars = clamp_int(
        MESSAGE_SPLIT_MAX_CHARS,
        default=720,
        min_value=target_chars,
        max_value=3000,
    )
    presentation_max_chars = clamp_int(
        MESSAGE_SPLIT_PRESENTATION_MAX_CHARS,
        default=max_chars,
        min_value=max_chars,
        max_value=4000,
    )
    if looks_like_presentation_message(cleaned, profile_id):
        max_chars = presentation_max_chars
        target_chars = max(target_chars, presentation_max_chars)

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", cleaned) if part and part.strip()]
    if not paragraphs:
        paragraphs = [cleaned]

    parts: list[str] = []
    for paragraph in paragraphs:
        if len(paragraph) > max_chars:
            parts.extend(split_long_chunk(paragraph, max_chars))
        else:
            parts.append(paragraph)

    resolved_profile_id = profile_id or PROMPT_PROFILE or None
    sentence_threshold = clamp_int(
        MESSAGE_SPLIT_SENTENCE_THRESHOLD,
        default=180,
        min_value=0,
        max_value=max_chars,
    )
    sentence_split_done = False
    if (
        sentence_threshold > 0
        and profile_uses_direct_response_style(resolved_profile_id)
        and not looks_like_presentation_message(cleaned, profile_id)
    ):
        expanded: list[str] = []
        for part in parts:
            if len(part) > sentence_threshold:
                sentences = _split_into_sentences(part)
                if len(sentences) > 1:
                    expanded.extend(sentences)
                    sentence_split_done = True
                    continue
            expanded.append(part)
        parts = expanded

    normalized = [normalize_whatsapp_part(part) for part in parts]
    filtered = [part for part in normalized if part]
    merge_short_limit = 30 if sentence_split_done else None
    merged = merge_short_whatsapp_parts(
        filtered,
        target_chars,
        short_limit=merge_short_limit,
    )
    return [part for part in merged if part]


def is_reply_with_schedule_options(reply: str) -> bool:
    if not reply:
        return False
    lowered = normalize_text(reply)
    if any(
        day in lowered
        for day in (
            "segunda",
            "terca",
            "quarta",
            "quinta",
            "sexta",
            "sabado",
            "domingo",
        )
    ):
        if re.search(r"\b\d{1,2}(?::\d{2}|h\d{0,2})\b", lowered):
            return True
    if re.search(r"\b\d{1,2}h(\d{2})?\b", lowered):
        return True
    if re.search(r"\b\d{1,2}:\d{2}\b", lowered):
        return True
    return False


def message_delay_seconds(profile_id: Optional[str] = None) -> float:
    resolved_profile_id = profile_id or PROMPT_PROFILE or None
    return delay_seconds_from_ms(
        str(get_profile_message_delay_ms(resolved_profile_id)),
        default_ms=350,
        min_ms=0,
        max_ms=60000,
    )


def schedule_delay_seconds(profile_id: Optional[str] = None) -> float:
    resolved_profile_id = profile_id or PROMPT_PROFILE or None
    return delay_seconds_from_ms(
        str(get_profile_schedule_delay_ms(resolved_profile_id)),
        default_ms=350,
        min_ms=0,
        max_ms=60000,
    )


def first_message_delay_seconds(profile_id: Optional[str] = None) -> float:
    resolved_profile_id = profile_id or PROMPT_PROFILE or None
    return delay_seconds_from_ms(
        str(get_profile_first_message_delay_ms(resolved_profile_id)),
        default_ms=180,
        min_ms=0,
        max_ms=60000,
    )
