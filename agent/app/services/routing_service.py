from typing import Optional

from ..config.settings import PROFILE_ROUTING_ENABLED, PROMPT_PROFILE
from ..core.profiles import PROFILE_DEFAULT_ID, is_ariane_profile as profile_is_ariane_profile
from ..core.state import get_profile_state
from ..utils.text import normalize_text


def resolve_profile_for_chat(chat_id: str) -> Optional[str]:
    if PROFILE_ROUTING_ENABLED and chat_id:
        state = get_profile_state(str(chat_id))
        profile_id = (state.get("profile_id") or "").strip()
        if profile_id:
            return profile_id
    return PROMPT_PROFILE or PROFILE_DEFAULT_ID or None


def is_ariane_profile(profile_id: Optional[str], chat_id: str = "") -> bool:
    resolved = resolve_profile_for_chat(chat_id) if chat_id else ""
    return profile_is_ariane_profile(profile_id, resolved_profile_id=resolved or "")


def wants_profile_switch(text: str) -> bool:
    if not text:
        return False
    lowered = normalize_text(text)
    if any(token in lowered for token in ("trocar", "mudar", "alterar", "testar", "outro", "outra")):
        if any(token in lowered for token in ("assistente", "clinica", "perfil", "setor", "atendimento")):
            return True
    return False


def is_greeting_only(text: str) -> bool:
    if not text:
        return False
    lowered = normalize_text(text)
    if any(
        keyword in lowered
        for keyword in (
            "agendar",
            "marcar",
            "consulta",
            "avaliacao",
            "horario",
            "hora",
            "dor",
            "dente",
            "implante",
            "coroa",
            "canal",
            "emergencia",
        )
    ):
        return False
    tokens = [token for token in lowered.replace("?", " ").replace("!", " ").split() if token]
    if not tokens:
        return False
    allowed = {
        "oi",
        "oii",
        "oiii",
        "ola",
        "e",
        "ai",
        "opa",
        "bom",
        "dia",
        "boa",
        "tarde",
        "noite",
        "tudo",
        "bem",
        "bem?",
    }
    return all(token in allowed for token in tokens) or lowered in (
        "bom dia",
        "boa tarde",
        "boa noite",
        "oi tudo bem",
        "ola tudo bem",
    )

