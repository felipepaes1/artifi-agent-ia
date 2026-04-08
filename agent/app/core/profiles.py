import json
import logging
import os
from typing import Any, Dict, Optional

from ..booking_flow import BookingFlow, load_profile_flows
from ..config.settings import (
    CRIOLASER_AUDIO_BUCKET,
    INSTRUCTIONS_PATH,
    PROMPT_PROFILE,
    PROFILES_PATH,
    SYSTEM_PROMPT,
)
from ..profiles.ariane.rules import is_ariane_profile as is_ariane_profile_rule
from ..utils.text import normalize_text


logger = logging.getLogger("agent")

DIRECT_RESPONSE_STYLE = "direct"
PRESERVE_RESPONSE_STYLE = "preserve"
DIRECT_RESPONSE_STYLE_GUIDANCE = (
    "\n\nEstilo de resposta\n"
    "- Responda de forma direta e objetiva\n"
    "- Priorize a proxima acao ou orientacao mais util\n"
    "- Evite rodeios, repeticoes e explicacoes longas\n"
    "- Prefira no maximo 3 frases curtas ou uma lista curta quando necessario\n"
)


DEFAULT_PROFILES_DATA = {
    "pollName": "Olá. Para testar o atendimento, qual segmento você prefere?",
    "profiles": [
        {
            "id": "mariano",
            "label": "Odontologia",
            "instructions_path": "assistant_instructions.txt",
            "greeting_name": "Odontologia",
            "greeting_message": "tudo bem? Sou a assistente da Odontologia 👩🏼‍⚕️\nComo posso te ajudar hoje?",
        },
        {
            "id": "ariane",
            "label": "Estética Capilar",
            "instructions_path": "assistant_instructions_ariane.txt",
            "greeting_name": "Estética Capilar",
            "greeting_message": "tudo bem? Sou a assistente da Estética Capilar 👩🏼‍⚕️\nComo posso te ajudar hoje?",
        },
        {
            "id": "mais_vision",
            "label": "Oftalmologia",
            "instructions_path": "assistant_instructions_mais_vision.txt",
            "greeting_name": "Mais Vision",
            "greeting_message": "tudo bem? Sou a assistente da Mais Vision 👩🏼‍⚕️\nComo posso te ajudar hoje?",
        },
    ],
}


def normalize_vector_store_ids(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        return [part.strip() for part in text.split(",") if part.strip()]
    return []


def load_profiles_data() -> Dict[str, Any]:
    if not PROFILES_PATH:
        return DEFAULT_PROFILES_DATA
    try:
        with open(PROFILES_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict) and data.get("profiles"):
                return data
    except FileNotFoundError:
        logger.warning("Profiles file not found: %s", PROFILES_PATH)
    except Exception as exc:
        logger.warning("Failed to load profiles file %s: %s", PROFILES_PATH, exc)
    return DEFAULT_PROFILES_DATA


PROFILES_DATA = load_profiles_data()
PROFILE_LIST = list(PROFILES_DATA.get("profiles") or [])
PROFILES: Dict[str, Dict[str, Any]] = {p.get("id"): p for p in PROFILE_LIST if p.get("id")}
PROFILE_POLL_NAME = PROFILES_DATA.get("pollName") or "Olá. Para testar o atendimento, qual segmento você prefere?"
PROFILE_OPTIONS = [p.get("label") for p in PROFILE_LIST if p.get("label")]
PROFILE_LABEL_TO_ID = {
    (p.get("label") or "").strip().lower(): p.get("id")
    for p in PROFILE_LIST
    if p.get("label") and p.get("id")
}
PROFILE_DEFAULT_ID = PROFILE_LIST[0].get("id") if PROFILE_LIST else ""
PROFILES_BASE_DIR = os.path.dirname(PROFILES_PATH) or os.path.dirname(os.path.dirname(__file__))
PROFILE_FLOWS: Dict[str, BookingFlow] = load_profile_flows(PROFILE_LIST, PROFILES_BASE_DIR)


def load_vector_store_map_from_env() -> Dict[str, list[str]]:
    mapping: Dict[str, list[str]] = {}
    raw = os.getenv("AGENT_VECTOR_STORE_IDS", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                for key, value in data.items():
                    ids = normalize_vector_store_ids(value)
                    if ids:
                        mapping[str(key).strip()] = ids
        except Exception as exc:
            logger.warning("Failed to parse AGENT_VECTOR_STORE_IDS: %s", exc)
    for profile_id in PROFILES:
        env_key = f"AGENT_VECTOR_STORE_{profile_id.upper()}"
        env_value = os.getenv(env_key, "").strip()
        ids = normalize_vector_store_ids(env_value)
        if ids:
            mapping[profile_id] = ids
    return mapping


def load_profile_vector_store_ids() -> Dict[str, list[str]]:
    mapping: Dict[str, list[str]] = {}
    for profile in PROFILE_LIST:
        profile_id = profile.get("id")
        if not profile_id:
            continue
        ids = normalize_vector_store_ids(
            profile.get("vector_store_ids") or profile.get("vector_store_id")
        )
        if ids:
            mapping[profile_id] = ids
    mapping.update(load_vector_store_map_from_env())
    return mapping


def load_audio_bucket_map() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    raw = os.getenv("AGENT_AUDIO_BUCKETS", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                for key, value in data.items():
                    profile_key = str(key or "").strip()
                    bucket_name = str(value or "").strip()
                    if profile_key and bucket_name:
                        mapping[profile_key] = bucket_name
        except Exception as exc:
            logger.warning("Failed to parse AGENT_AUDIO_BUCKETS: %s", exc)
    for profile in PROFILE_LIST:
        profile_id = str(profile.get("id") or "").strip()
        bucket_name = str(profile.get("audio_bucket") or profile.get("audioBucket") or "").strip()
        if profile_id and bucket_name:
            mapping[profile_id] = bucket_name
    if CRIOLASER_AUDIO_BUCKET:
        mapping.setdefault("criolaser", CRIOLASER_AUDIO_BUCKET)
    return mapping


PROFILE_VECTOR_STORE_IDS = load_profile_vector_store_ids()
PROFILE_AUDIO_BUCKETS = load_audio_bucket_map()
DEFAULT_VECTOR_STORE_IDS = normalize_vector_store_ids(os.getenv("AGENT_VECTOR_STORE_ID", ""))


def get_vector_store_ids(profile_id: Optional[str]) -> list[str]:
    if profile_id:
        ids = PROFILE_VECTOR_STORE_IDS.get(profile_id)
        if ids:
            return list(ids)
    return list(DEFAULT_VECTOR_STORE_IDS)


def get_audio_bucket_for_profile(profile_id: Optional[str]) -> str:
    if not profile_id:
        return ""
    return str(PROFILE_AUDIO_BUCKETS.get(profile_id) or "").strip()


def get_profile_response_style(profile_id: Optional[str]) -> str:
    if not profile_id:
        return PRESERVE_RESPONSE_STYLE
    profile = PROFILES.get(profile_id) or {}
    response_style = str(profile.get("response_style") or profile.get("responseStyle") or "").strip().lower()
    if response_style == DIRECT_RESPONSE_STYLE:
        return DIRECT_RESPONSE_STYLE
    return PRESERVE_RESPONSE_STYLE


def profile_uses_direct_response_style(profile_id: Optional[str]) -> bool:
    return get_profile_response_style(profile_id) == DIRECT_RESPONSE_STYLE


def get_profile_temperature(profile_id: Optional[str]) -> Optional[float]:
    if not profile_id:
        return None
    profile = PROFILES.get(profile_id) or {}
    raw_value = profile.get("temperature")
    if raw_value is None:
        return None
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        logger.warning("Invalid temperature for profile %s: %s", profile_id, raw_value)
        return None


def get_profile_max_tokens(profile_id: Optional[str]) -> Optional[int]:
    if not profile_id:
        return None
    profile = PROFILES.get(profile_id) or {}
    raw_value = profile.get("max_tokens") or profile.get("maxTokens")
    if raw_value is None:
        return None
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        logger.warning("Invalid max_tokens for profile %s: %s", profile_id, raw_value)
        return None
    return parsed if parsed > 0 else None


def get_docs_dir_for_profile(profile_id: Optional[str]) -> str:
    if not profile_id:
        return ""
    profile = PROFILES.get(profile_id) or {}
    path = str(profile.get("docs_dir") or profile.get("docsDir") or "").strip()
    if not path:
        return ""
    candidates: list[str] = []
    if os.path.isabs(path):
        candidates.append(path)
    else:
        candidates.append(os.path.abspath(os.path.join(PROFILES_BASE_DIR, path)))

    profile_basename = os.path.basename(os.path.normpath(path))
    if profile_basename:
        candidates.extend(
            [
                os.path.join("/storage", profile_basename),
                os.path.join(os.path.dirname(__file__), "..", "..", "..", "storage", profile_basename),
                os.path.join(os.getcwd(), "storage", profile_basename),
            ]
        )

    seen: set[str] = set()
    normalized_candidates: list[str] = []
    for candidate in candidates:
        normalized = os.path.abspath(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_candidates.append(normalized)
        if os.path.isdir(normalized):
            return normalized

    return normalized_candidates[0] if normalized_candidates else ""


def log_profile_knowledge_status() -> None:
    for profile in PROFILE_LIST:
        profile_id = str(profile.get("id") or "").strip()
        if not profile_id:
            continue
        docs_dir = get_docs_dir_for_profile(profile_id)
        logger.info(
            "ProfileKnowledge profile=%s docs_dir=%s exists=%s vector_store_ids=%s",
            profile_id,
            docs_dir,
            os.path.isdir(docs_dir) if docs_dir else False,
            get_vector_store_ids(profile_id),
        )


def load_instructions() -> str:
    if SYSTEM_PROMPT:
        return SYSTEM_PROMPT
    try:
        with open(INSTRUCTIONS_PATH, "r", encoding="utf-8") as handle:
            content = handle.read().strip()
            if content:
                return content
    except FileNotFoundError:
        logger.warning("Instructions file not found: %s", INSTRUCTIONS_PATH)
    except OSError as exc:
        logger.warning("Failed to read instructions file %s: %s", INSTRUCTIONS_PATH, exc)
    return "You are a helpful assistant. Reply in a concise and practical way."


def append_audio_tool_instructions(instructions: str, profile_id: Optional[str]) -> str:
    bucket_name = get_audio_bucket_for_profile(profile_id)
    if not bucket_name:
        return instructions
    guidance = (
        "\n\nFerramentas de audio para WhatsApp\n"
        "- Sempre que o usuario indicar claramente um procedimento com audio disponivel ou quiser avancar/agendar um procedimento com audio disponivel, primeiro use a tool `buscar_audio_atendimento`\n"
        "- Passe um nome curto e direto do procedimento ou da area, nao envie a conversa inteira\n"
        "- So use a tool `enviar_audio_atendimento` depois que a tool de busca retornar um `filename` valido\n"
        "- Nunca invente nome de arquivo e nunca diga que enviou audio se a tool de envio falhar\n"
        "- Se nao houver correspondencia clara, siga apenas com texto\n"
        "- Se o usuario escrever com erro leve ou nome aproximado, use a tool de busca mesmo assim para tentar a associacao\n"
    )
    return f"{instructions.rstrip()}{guidance}"


def append_response_style_instructions(instructions: str, profile_id: Optional[str]) -> str:
    if not profile_uses_direct_response_style(profile_id):
        return instructions
    return f"{instructions.rstrip()}{DIRECT_RESPONSE_STYLE_GUIDANCE}"


def append_profile_runtime_instructions(instructions: str, profile_id: Optional[str]) -> str:
    return append_audio_tool_instructions(
        append_response_style_instructions(instructions, profile_id),
        profile_id,
    )


def resolve_profile_instructions_path(path: str) -> str:
    if not path:
        return ""
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(PROFILES_BASE_DIR, path))


def load_profile_instructions(profile_id: str) -> str:
    profile = PROFILES.get(profile_id) or {}
    path = resolve_profile_instructions_path(profile.get("instructions_path", ""))
    if not path:
        return append_profile_runtime_instructions(load_instructions(), profile_id)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            content = handle.read().strip()
            if content:
                return append_profile_runtime_instructions(content, profile_id)
    except FileNotFoundError:
        logger.warning("Instructions file not found: %s", path)
    except OSError as exc:
        logger.warning("Failed to read instructions file %s: %s", path, exc)
    return append_profile_runtime_instructions(load_instructions(), profile_id)


def profile_greeting_name(profile_id: Optional[str]) -> str:
    profile = PROFILES.get(profile_id) if profile_id else None
    if profile and profile.get("greeting_name"):
        return profile["greeting_name"]
    if profile and profile.get("label"):
        return profile["label"]
    return "Mariano Odontologia"


def profile_greeting_message(profile_id: Optional[str]) -> str:
    profile = PROFILES.get(profile_id) if profile_id else None
    if profile:
        message = str(profile.get("greeting_message") or "").strip()
        if message:
            return message
    greeting_name = profile_greeting_name(profile_id)
    return (
        f"tudo bem? Sou a assistente da {greeting_name} 👩🏼‍⚕️\n"
        "Como posso te ajudar hoje?"
    )


def has_profile_greeting(items: list[dict[str, Any]], profile_id: Optional[str]) -> bool:
    if not items:
        return False
    marker = normalize_text(profile_greeting_message(profile_id))
    for item in items:
        if item.get("role") != "assistant":
            continue
        content = normalize_text(item.get("content") or "")
        if marker and marker in content:
            return True
    return False


def build_greeting(first_name: Optional[str], profile_id: Optional[str]) -> str:
    greeting_message = profile_greeting_message(profile_id)
    if first_name:
        return f"Oii {first_name}, {greeting_message}"
    return f"Oii, {greeting_message}"


def resolve_profile_id_from_option(option: str) -> Optional[str]:
    if not option:
        return None
    raw = option.strip()
    if not raw:
        return None
    normalized = normalize_text(raw)
    for profile in PROFILE_LIST:
        profile_id = (profile.get("id") or "").strip()
        if profile_id and normalize_text(profile_id) == normalized:
            return profile_id
        label = normalize_text(profile.get("label") or "")
        if label and label == normalized:
            return profile.get("id")
        if label and normalized and label in normalized:
            return profile.get("id")
    lowered = raw.lower()
    if lowered in PROFILE_LABEL_TO_ID:
        return PROFILE_LABEL_TO_ID.get(lowered)
    return None


def normalize_selected_options(raw: Any) -> list[str]:
    if not raw:
        return []
    options: list[str] = []
    if not isinstance(raw, list):
        raw = [raw]
    for entry in raw:
        if isinstance(entry, dict):
            value = (
                entry.get("name")
                or entry.get("label")
                or entry.get("title")
                or entry.get("option")
                or entry.get("value")
                or entry.get("id")
            )
            if value is not None:
                options.append(str(value))
        else:
            options.append(str(entry))
    return options


def resolve_profile_id_from_vote(selected_options: Any) -> Optional[str]:
    options = normalize_selected_options(selected_options)
    if not options:
        return None
    option = options[0].strip()
    resolved_option = resolve_profile_id_from_option(option)
    if resolved_option:
        return resolved_option
    if option.isdigit():
        idx = int(option)
        if 1 <= idx <= len(PROFILE_OPTIONS):
            return resolve_profile_id_from_option(PROFILE_OPTIONS[idx - 1])
        if idx == 0 and PROFILE_OPTIONS:
            return resolve_profile_id_from_option(PROFILE_OPTIONS[0])
    if option in PROFILES:
        return option
    return None


def is_criolaser_profile(profile_id: Optional[str], resolved_profile_id: str = "") -> bool:
    resolved = (profile_id or "").strip() or resolved_profile_id.strip()
    return normalize_text(resolved) == "criolaser"


def is_ariane_profile(profile_id: Optional[str], resolved_profile_id: str = "") -> bool:
    return is_ariane_profile_rule(
        profile_id,
        resolved_profile_id=resolved_profile_id,
        prompt_profile=PROMPT_PROFILE,
    )
