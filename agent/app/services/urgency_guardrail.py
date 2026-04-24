"""Guardrail deterministico de urgencia (atualmente apenas perfil mariano).

Quando a mensagem do paciente contem sinais claros de urgencia ou
atendimento no MESMO DIA, o fluxo normal do agente LLM e curto-circuitado
e devolvemos direto a frase canonica definida pelo perfil mariano
(URGENT_SAME_DAY_FALLBACK em agent/app/prompts/mariano.txt).

Motivacao: o LLM sozinho falhava de forma intermitente — em alguns casos
oferecia horarios antes de aplicar a regra, mesmo com o paciente dizendo
"estou com dor" ou "preciso com urgencia". Um matcher por keyword no
codigo elimina essa variabilidade sem depender da boa vontade do modelo.

Escopo: apenas perfis em URGENCY_PROFILE_IDS sao afetados. Os demais
perfis passam batido (detect_urgency_reply devolve None).
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from ..utils.text import normalize_text


logger = logging.getLogger("agent")


URGENCY_PROFILE_IDS = frozenset({"mariano"})


URGENT_SAME_DAY_REPLY = (
    "No momento, nossa agenda encontra-se completa. "
    "Contudo, iremos verificar a possibilidade de um encaixe para hoje "
    "e entraremos em contato assim que houver disponibilidade"
)


_URGENCY_KEYWORDS: tuple[str, ...] = (
    # Urgencia explicita
    r"urgencia",
    r"urgente",
    r"emergencia",
    # Quadro clinico agudo
    r"dor",
    r"dores",
    r"doendo",
    r"doi",
    r"doer",
    r"doeu",
    r"doido de dor",
    r"latejando",
    r"latejante",
    r"pulsando",
    r"sangrando",
    r"sangramento",
    r"sangrar",
    r"trauma",
    r"inchaco",
    r"inchado",
    r"inchada",
    r"abscesso",
    r"infeccionado",
    r"infeccao",
    r"pus",
    # Trauma dental especifico
    r"quebrei o dente",
    r"quebrou o dente",
    r"quebrei um dente",
    r"caiu o dente",
    r"caiu um dente",
    r"perdi o dente",
    r"cai o dente",
    # Intensidade / incapacidade
    r"nao aguento mais",
    r"nao aguento",
    r"nao suporto",
    r"nao da pra esperar",
    r"nao posso esperar",
    r"muita dor",
    r"dor forte",
    r"dor intensa",
    r"dor horrivel",
    r"dor insuportavel",
    # Pedido explicito de atendimento no mesmo dia
    r"preciso agora",
    r"preciso hoje",
    r"preciso ja",
    r"preciso urgente",
    r"preciso com urgencia",
    r"ainda hoje",
    r"pra hoje",
    r"para hoje",
    r"atendimento hoje",
    r"consulta hoje",
    r"marcar hoje",
    r"atendido hoje",
    r"atendida hoje",
    r"atender hoje",
    r"atendem hoje",
    r"me atende hoje",
    r"agora mesmo",
    r"hoje mesmo",
)


# Compila cada keyword com fronteiras de palavra (\b) em cima do texto
# normalizado (minusculo, sem acentos). O normalize_text remove acentos
# mas preserva pontuacao — \b cobre o resto.
_URGENCY_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(rf"\b{keyword}\b") for keyword in _URGENCY_KEYWORDS
)


def _matches_urgency(text: str) -> Optional[str]:
    normalized = normalize_text(text)
    if not normalized:
        return None
    for pattern in _URGENCY_PATTERNS:
        match = pattern.search(normalized)
        if match:
            return match.group(0)
    return None


def detect_urgency_reply(profile_id: Optional[str], user_text: str) -> Optional[str]:
    """Retorna a frase canonica de urgencia ou None.

    So atua nos perfis listados em URGENCY_PROFILE_IDS. Para qualquer
    outro perfil (ou profile_id vazio), devolve None — o fluxo normal
    do agente segue sem alteracao.
    """

    if not profile_id or profile_id not in URGENCY_PROFILE_IDS:
        return None
    if not user_text:
        return None
    matched = _matches_urgency(user_text)
    if not matched:
        return None
    logger.info(
        "urgency_guardrail_triggered profile=%s matched_keyword=%s",
        profile_id,
        matched,
    )
    return URGENT_SAME_DAY_REPLY


async def maybe_handle_urgency(
    profile_id: Optional[str],
    user_text: str,
    session,
) -> Optional[str]:
    """Detecta urgencia e, se aplicavel, persiste o par user/assistant na
    session para manter o historico consistente com o shortcut.

    Retorna a frase canonica ou None.
    """

    reply = detect_urgency_reply(profile_id, user_text)
    if reply is None:
        return None
    try:
        await session.add_items(
            [
                {"role": "user", "content": user_text},
                {"role": "assistant", "content": reply},
            ]
        )
    except Exception as exc:
        logger.warning("Failed to persist urgency shortcut to session: %s", exc)
    return reply
