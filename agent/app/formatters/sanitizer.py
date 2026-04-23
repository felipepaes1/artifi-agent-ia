import logging
import re
from typing import Optional

from ..config.settings import PROMPT_PROFILE
from ..core.profiles import get_profile_max_reply_chars
from ..profiles.ariane.formatting import (
    format_ariane_checklists,
    split_ariane_trailing_question_blocks,
)
from ..profiles.ariane.rules import matches_ariane_alias
from ..utils.text import normalize_text, strip_list_prefix


logger = logging.getLogger("agent")


_TERMINAL_PUNCT = ".!?…;:"
_SENTENCE_ENDERS = (". ", "! ", "? ", "… ", "…")


def _smart_cut(text: str, max_chars: int) -> str:
    """Truncate text to <= max_chars, preferring sentence/word boundaries.

    Never cuts in the middle of a word when avoidable.
    """
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    window = text[:max_chars]
    minimum = int(max_chars * 0.5)

    idx = window.rfind("\n\n")
    if idx >= minimum:
        return window[:idx].rstrip()
    idx = window.rfind("\n")
    if idx >= minimum:
        return window[:idx].rstrip()
    for ender in _SENTENCE_ENDERS:
        idx = window.rfind(ender)
        if idx >= minimum:
            return window[: idx + len(ender)].rstrip()
    idx = window.rfind(" ")
    if idx > 0:
        return window[:idx].rstrip()
    return window


def _drop_orphan_header(text: str) -> str:
    """Drop a trailing line that ends in ':' when its list was cut off.

    Only runs after a truncate, when the final non-empty line is an
    introducer like "Por favor me informe:" left without the items
    it was introducing. Prevents confusing outputs where the user
    is asked to provide data but no list appears.
    """
    if not text:
        return text
    stripped = text.rstrip()
    while stripped:
        newline_pos = stripped.rfind("\n")
        last_line = stripped[newline_pos + 1 :].strip()
        if not last_line.endswith(":"):
            break
        logger.info("Dropped orphan header after truncate: %r", last_line[:60])
        stripped = stripped[:newline_pos].rstrip() if newline_pos >= 0 else ""
    return stripped


def truncate(text: str, profile_id: Optional[str] = None) -> str:
    max_chars = get_profile_max_reply_chars(profile_id or PROMPT_PROFILE or None)
    if max_chars <= 0:
        return text
    if len(text) <= max_chars:
        return text
    cut = _smart_cut(text, max_chars)
    cut = _drop_orphan_header(cut)
    logger.info(
        "Reply truncated from %d to %d chars (max=%d, profile=%s)",
        len(text),
        len(cut),
        max_chars,
        profile_id or PROMPT_PROFILE or "",
    )
    return cut


def _trim_dangling_tail_in_paragraph(paragraph: str) -> str:
    """Drop a short, unterminated trailing fragment in a paragraph.

    Guards against LLM outputs like a paragraph ending in a short dangling
    word (e.g. "... adicional.\\nVou"). Only trims when the tail after the
    last terminal punctuation is a single short word — stays conservative
    to avoid false positives on short legit utterances.
    """
    stripped = paragraph.rstrip()
    if not stripped:
        return paragraph
    if stripped[-1] in _TERMINAL_PUNCT:
        return stripped
    last_terminal = -1
    for ch in ".!?…":
        pos = stripped.rfind(ch)
        if pos > last_terminal:
            last_terminal = pos
    if last_terminal < 0:
        return stripped
    tail = stripped[last_terminal + 1 :].strip()
    if not tail:
        return stripped
    if " " not in tail and len(tail) <= 15:
        logger.warning("Dropped dangling tail fragment from reply: %r", tail[:40])
        return stripped[: last_terminal + 1].rstrip()
    return stripped


def trim_dangling_tails(text: str) -> str:
    if not text:
        return text
    cleaned = text.replace("\r\n", "\n")
    paragraphs = cleaned.split("\n\n")
    trimmed = [_trim_dangling_tail_in_paragraph(part) for part in paragraphs]
    return "\n\n".join(part for part in trimmed if part)


def sanitize_plain_text(text: str, profile_id: Optional[str] = None) -> str:
    if not text:
        return text
    cleaned = text.replace("**", "").replace("__", "").replace("`", "")
    # Remove em-dash and en-dash (travessão) — forbidden by all profiles
    cleaned = cleaned.replace("\u2014", " -").replace("\u2013", "-")
    if matches_ariane_alias(profile_id or "") or (not profile_id and matches_ariane_alias(PROMPT_PROFILE)):
        sanitized = split_ariane_trailing_question_blocks(format_ariane_checklists(cleaned))
    else:
        sanitized = "\n".join(strip_list_prefix(line) for line in cleaned.splitlines())
    sanitized = sanitize_internal_knowledge_references(sanitized)
    sanitized = sanitize_phone_number_requests(sanitized)
    sanitized = trim_dangling_tails(sanitized)
    if (profile_id or PROMPT_PROFILE or "").strip().lower() == "biovita":
        sanitized = sanitize_biovita_gender(sanitized)
    return sanitized


_PHONE_REQUEST_PATTERN = re.compile(
    r"(?:qual|informe?|me\s+passe?|poderia?\s+(?:me\s+)?(?:passar|informar|enviar))\s+(?:o\s+)?(?:seu\s+)?(?:n[uú]mero|telefone|celular)(?:\s+(?:de\s+)?(?:telefone|celular|whatsapp))?\b",
    re.IGNORECASE,
)


def sanitize_phone_number_requests(text: str) -> str:
    """Drop lines where the agent asks the user for their phone number.

    Users are already on WhatsApp, so requesting phone/celular is always wrong.
    Only narrow patterns are matched to avoid false positives.
    """
    if not text:
        return text
    kept: list[str] = []
    for line in text.splitlines():
        if _PHONE_REQUEST_PATTERN.search(line):
            logger.warning("Dropped phone-request line from reply: %r", line[:120])
            continue
        kept.append(line)
    result = "\n".join(kept).strip()
    return result if result else text


_BIOVITA_GENDER_REPLACEMENTS = (
    # Combined: article + "Clínica Biovita" -> masculine article + "Laboratório Biovita"
    (
        re.compile(r"\b([Aa])\s+Cl[ií]nica\s+Biovita\b"),
        lambda m: ("O" if m.group(1) == "A" else "o") + " Laboratório Biovita",
    ),
    (
        re.compile(r"\b([Dd])a\s+Cl[ií]nica\s+Biovita\b"),
        lambda m: m.group(1) + "o Laboratório Biovita",
    ),
    (
        re.compile(r"\b([Nn])a\s+Cl[ií]nica\s+Biovita\b"),
        lambda m: m.group(1) + "o Laboratório Biovita",
    ),
    (
        re.compile(r"\b([Àà])\s+Cl[ií]nica\s+Biovita\b"),
        lambda m: ("Ao" if m.group(1) == "À" else "ao") + " Laboratório Biovita",
    ),
    (
        re.compile(r"\b([Pp])ela\s+Cl[ií]nica\s+Biovita\b"),
        lambda m: m.group(1) + "elo Laboratório Biovita",
    ),
    # Bare article + Biovita
    (
        re.compile(r"\b([Aa])\s+Biovita\b"),
        lambda m: ("O" if m.group(1) == "A" else "o") + " Biovita",
    ),
    (
        re.compile(r"\b([Dd])a\s+Biovita\b"),
        lambda m: m.group(1) + "o Biovita",
    ),
    (
        re.compile(r"\b([Nn])a\s+Biovita\b"),
        lambda m: m.group(1) + "o Biovita",
    ),
    (
        re.compile(r"\b([Àà])\s+Biovita\b"),
        lambda m: ("Ao" if m.group(1) == "À" else "ao") + " Biovita",
    ),
    (
        re.compile(r"\b([Pp])ela\s+Biovita\b"),
        lambda m: m.group(1) + "elo Biovita",
    ),
    # Bare "Clínica Biovita" (without article) -> "Laboratório Biovita"
    (re.compile(r"\bCl[ií]nica\s+Biovita\b"), "Laboratório Biovita"),
    (re.compile(r"\bcl[ií]nica\s+Biovita\b"), "laboratório Biovita"),
)


def sanitize_biovita_gender(text: str) -> str:
    """Force masculine gender for Biovita references (laboratório, not clínica).

    Biovita is a laboratory; the LLM occasionally uses feminine articles.
    Only applied for the biovita profile via sanitize_plain_text.
    """
    if not text:
        return text
    for pattern, replacement in _BIOVITA_GENDER_REPLACEMENTS:
        text = pattern.sub(replacement, text)
    return text


def sanitize_internal_knowledge_references(text: str) -> str:
    if not text:
        return text

    drop_line_patterns = (
        r"voc[eê]\s+enviou.*arquiv",
        r"aproveitando.*arquiv",
        r"posso ajudar.*relacionad[oa].*arquiv",
        r"arquivos?\s+que\s+voc[eê]\s+enviou",
        r"obrigad[oa].*pel[ao]s?\s+(?:envio\s+d[oa]s?\s+)?(?:arquiv|document|anex)",
        r"obrigad[oa].*pel[ao]s?\s+(?:arquiv|document|anex)",
        r"recebi.*(?:seus?\s+)?(?:arquiv|document|anex|exame)",
        r"em\s+rela[cç][aã]o\s+a\s+esses?\s+(?:arquiv|document|anex)",
        r"sobre\s+(?:esses?|os)\s+(?:arquiv|document|anex)",
        r"vi\s+(?:os?|seus?)\s+(?:arquiv|document|anex)",
    )
    kept_lines: list[str] = []
    for line in text.splitlines():
        lowered = normalize_text(line)
        if any(re.search(pattern, lowered) for pattern in drop_line_patterns):
            continue
        kept_lines.append(line)
    sanitized = "\n".join(kept_lines)

    replacements = (
        (
            r"n[aã]o\s+est[aá]\s+especificad[oa]\s+nos?\s+documentos?(?:\s+que\s+consultei)?",
            "não tenho esse valor confirmado no momento",
        ),
        (
            r"n[aã]o\s+consta\s+nos?\s+documentos?(?:\s+que\s+consultei)?",
            "não tenho essa informação confirmada no momento",
        ),
        (
            r"nos?\s+documentos?\s+que\s+consultei",
            "nas informações da clínica",
        ),
        (
            r"nos?\s+arquivos?\s+que\s+consultei",
            "nas informações da clínica",
        ),
        (
            r"base\s+(?:de\s+conhecimento|interna)",
            "informações da clínica",
        ),
        (
            r"documentos?\s+internos?",
            "informações da clínica",
        ),
        (
            r"arquivos?\s+internos?",
            "informações da clínica",
        ),
        (
            r"vector\s+store",
            "informações da clínica",
        ),
        (
            r"(?:na\s+)?(?:minha\s+)?base\s+(?:de\s+dados|de\s+informacoes|de\s+informações)",
            "nas informações da clínica",
        ),
    )
    for pattern, replacement in replacements:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)

    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized).strip()
    if not sanitized:
        return "Posso te ajudar com as informações da clínica e com o agendamento."
    return sanitized
