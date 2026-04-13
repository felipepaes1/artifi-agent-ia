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


def truncate(text: str, profile_id: Optional[str] = None) -> str:
    max_chars = get_profile_max_reply_chars(profile_id or PROMPT_PROFILE or None)
    if max_chars <= 0:
        return text
    return text[:max_chars]


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
