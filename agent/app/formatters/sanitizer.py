import re
from typing import Optional

from ..config.settings import MAX_REPLY_CHARS, PROMPT_PROFILE
from ..profiles.ariane.formatting import (
    format_ariane_checklists,
    split_ariane_trailing_question_blocks,
)
from ..profiles.ariane.rules import matches_ariane_alias
from ..utils.text import normalize_text, strip_list_prefix


def truncate(text: str) -> str:
    if MAX_REPLY_CHARS <= 0:
        return text
    return text[:MAX_REPLY_CHARS]


def sanitize_plain_text(text: str, profile_id: Optional[str] = None) -> str:
    if not text:
        return text
    cleaned = text.replace("**", "").replace("__", "").replace("`", "")
    if matches_ariane_alias(profile_id or "") or (not profile_id and matches_ariane_alias(PROMPT_PROFILE)):
        sanitized = split_ariane_trailing_question_blocks(format_ariane_checklists(cleaned))
    else:
        sanitized = "\n".join(strip_list_prefix(line) for line in cleaned.splitlines())
    return sanitize_internal_knowledge_references(sanitized)


def sanitize_internal_knowledge_references(text: str) -> str:
    if not text:
        return text

    drop_line_patterns = (
        r"voc[eê]\s+enviou.*arquiv",
        r"aproveitando.*arquiv",
        r"posso ajudar.*relacionad[oa].*arquiv",
        r"arquivos?\s+que\s+voc[eê]\s+enviou",
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
    )
    for pattern, replacement in replacements:
        sanitized = re.sub(pattern, replacement, sanitized, flags=re.IGNORECASE)

    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized).strip()
    if not sanitized:
        return "Posso te ajudar com as informações da clínica e com o agendamento."
    return sanitized

