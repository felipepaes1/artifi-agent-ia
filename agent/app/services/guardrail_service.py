import logging
import re
from typing import Any, Dict, Optional

from ..services.knowledge_service import search_profile_vector_knowledge
from ..utils.text import contains_normalized_term, normalize_service_text, normalize_text


logger = logging.getLogger("agent")

TEMPORAL_TOKENS = {
    "amanha",
    "amanhã",
    "hoje",
    "segunda",
    "terca",
    "terça",
    "quarta",
    "quinta",
    "sexta",
    "sabado",
    "sábado",
    "domingo",
    "manha",
    "manhã",
    "tarde",
    "noite",
    "cedo",
    "depois",
}
SCHEDULING_FILLER_TOKENS = {
    "agenda",
    "agendar",
    "agendamento",
    "agendaria",
    "atendimento",
    "com",
    "consulta",
    "consulta",
    "da",
    "das",
    "de",
    "do",
    "dos",
    "em",
    "encaixe",
    "fazer",
    "horario",
    "horarios",
    "horário",
    "horários",
    "marcar",
    "na",
    "nas",
    "no",
    "nos",
    "para",
    "periodo",
    "período",
    "por",
    "quero",
    "retorno",
    "sessao",
    "sessão",
    "um",
    "uma",
}
GENERIC_ENTITY_TOKENS = {
    "avaliacao",
    "avaliação",
    "consulta",
    "exame",
    "procedimento",
    "profissional",
    "retorno",
    "servico",
    "serviço",
    "tratamento",
}
DOCTOR_PATTERN = re.compile(
    r"\b(?:dr(?:a)?|doutor(?:a)?)\.?\s+([a-zà-ÿ]+(?:\s+(?:[a-zà-ÿ]+|da|de|do|das|dos)){0,4})",
    re.IGNORECASE,
)
ENTITY_PATTERNS = (
    ("doctor", re.compile(r"\b(?:consulta|retorno|atendimento)\s+com\s+([a-zà-ÿ][a-zà-ÿ\s]{2,50})", re.IGNORECASE)),
    ("service", re.compile(r"\b(?:exame|procedimento|tratamento|servico|serviço|sessao|sessão)\s+(?:de|do|da)?\s*([a-zà-ÿ0-9][a-zà-ÿ0-9\s]{2,60})", re.IGNORECASE)),
    ("service", re.compile(r"\b(?:agendar|marcar|fazer)\s+(?:o|a|um|uma)?\s*([a-zà-ÿ0-9][a-zà-ÿ0-9\s]{2,60})", re.IGNORECASE)),
)
SCHEDULING_REPLY_MARKERS = (
    "horarios disponiveis",
    "horários disponíveis",
    "vou consultar a agenda",
    "vou verificar os horarios",
    "vou ver os horarios",
    "vou reservar",
    "horario confirmado",
    "horário confirmado",
)


def _clean_candidate_text(text: str, *, keep_title: bool = False) -> str:
    cleaned = normalize_service_text(text)
    if not cleaned:
        return ""
    tokens: list[str] = []
    for token in cleaned.split():
        if token in TEMPORAL_TOKENS:
            break
        if token.isdigit():
            break
        tokens.append(token)
        if len(tokens) >= 5:
            break
    if not keep_title:
        tokens = [token for token in tokens if token not in {"dr", "dra", "doutor", "doutora"}]
    return " ".join(tokens).strip()


def _doctor_candidates(text: str) -> list[Dict[str, str]]:
    candidates: list[Dict[str, str]] = []
    for match in DOCTOR_PATTERN.finditer(text or ""):
        raw_name = (match.group(1) or "").strip()
        cleaned_name = _clean_candidate_text(raw_name)
        if len(cleaned_name.split()) < 1:
            continue
        candidates.append(
            {
                "kind": "doctor",
                "label": cleaned_name,
                "query": f"profissional {cleaned_name}",
            }
        )
    return candidates


def _service_candidates(text: str) -> list[Dict[str, str]]:
    lowered = text or ""
    candidates: list[Dict[str, str]] = []
    for kind, pattern in ENTITY_PATTERNS:
        for match in pattern.finditer(lowered):
            raw_candidate = (match.group(1) or "").strip()
            cleaned = _clean_candidate_text(raw_candidate)
            if not cleaned:
                continue
            filtered_tokens = [
                token
                for token in cleaned.split()
                if token not in SCHEDULING_FILLER_TOKENS and token not in GENERIC_ENTITY_TOKENS
            ]
            if not filtered_tokens:
                continue
            label = " ".join(filtered_tokens[:4]).strip()
            if not label:
                continue
            candidates.append(
                {
                    "kind": kind,
                    "label": label,
                    "query": label,
                }
            )
    return candidates


def extract_scheduling_validation_candidates(text: str) -> list[Dict[str, str]]:
    combined: list[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in _doctor_candidates(text) + _service_candidates(text):
        key = (candidate.get("kind", ""), candidate.get("label", ""))
        if not key[0] or not key[1] or key in seen:
            continue
        seen.add(key)
        combined.append(candidate)
    return combined


def _candidate_matches_result(candidate: Dict[str, str], result: Dict[str, Any]) -> bool:
    label = str(candidate.get("label") or "").strip()
    if not label:
        return False
    normalized_label = normalize_service_text(label)
    if not normalized_label:
        return False
    searchable = " ".join(
        [
            str(result.get("source") or ""),
            str(result.get("content") or ""),
        ]
    ).strip()
    searchable_normalized = normalize_service_text(searchable)
    if not searchable_normalized:
        return False
    if contains_normalized_term(searchable_normalized, normalized_label):
        return True

    candidate_tokens = [
        token
        for token in normalized_label.split()
        if token not in GENERIC_ENTITY_TOKENS and token not in {"dr", "dra", "doutor", "doutora"}
    ]
    if not candidate_tokens:
        return False
    matched_tokens = [token for token in candidate_tokens if contains_normalized_term(searchable_normalized, token)]
    if candidate.get("kind") == "doctor":
        return len(matched_tokens) >= min(len(candidate_tokens), 2)
    return len(matched_tokens) >= max(1, min(len(candidate_tokens), 2))


def validate_scheduling_entities(profile_id: Optional[str], text: str) -> Dict[str, Any]:
    # Guardrail desativado: o regex-based extractor produzia falsos positivos
    # em fala natural (pronomes como "ele", fragmentos como "estou muita"),
    # bloqueando agendamentos legítimos. Mantido como passthrough para não
    # quebrar callers existentes (ex.: tool_agente_scheduling).
    return {"status": "allowed", "candidates": []}


def reply_advances_scheduling(reply: str) -> bool:
    normalized = normalize_text(reply or "")
    if not normalized:
        return False
    if any(marker in normalized for marker in SCHEDULING_REPLY_MARKERS):
        return True
    if any(
        day in normalized
        for day in ("segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo")
    ) and re.search(r"\b\d{1,2}(?::\d{2}|h\d{0,2})\b", normalized):
        return True
    if "qual prefere" in normalized and re.search(r"\b\d{1,2}(?::\d{2}|h\d{0,2})\b", normalized):
        return True
    return False


def enforce_scheduling_entity_guardrail(
    profile_id: Optional[str],
    user_text: str,
    reply: str,
) -> str:
    # Guardrail desativado. Estava substituindo a resposta real do agente por
    # uma mensagem fixa ("Nao encontrei servico ou procedimento confirmado…")
    # sempre que o extractor de entidades falhava em validar fragmentos da
    # fala do usuario contra o vector store do perfil. Ficou atrapalhando a
    # troca de atendimento e o fluxo natural de conversa.
    return reply
