import difflib
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import tempfile
import time
import unicodedata
import contextvars
from datetime import date, timedelta
from typing import Any, Dict, Optional
from urllib.parse import quote, urlparse, urlunparse
import anyio
import httpx
from agents import Agent, ModelSettings, Runner, SQLiteSession


try:
    from agents import FileSearchTool
except Exception:  # pragma: no cover - depends on openai-agents version
    FileSearchTool = None
try:
    from agents import function_tool
except Exception:  # pragma: no cover - depends on openai-agents version
    function_tool = None
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from openai import OpenAI

try:
    from supabase import Client as SupabaseClient
    from supabase import create_client as supabase_create_client
except Exception:
    SupabaseClient = None
    supabase_create_client = None

from .booking_flow import (
    BookingFlow,
    build_prebooking_message,
    build_proof_received_message,
    load_profile_flows,
)
from .chatwoot_integration import build_chatwoot_router, get_chatwoot_service

app = FastAPI()
logger = logging.getLogger("agent")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return default


OPENAI_API_KEY = _env_first("OPENAI_API_KEY", "API_OPENAI_KEY")
OPENAI_MODEL = _env_first("OPENAI_MODEL", "OPENAI_DEFAULT_MODEL")
OPENAI_MAX_TOKENS = _env_first("OPENAI_MAX_TOKENS")
OPENAI_TRANSCRIBE_MODEL = _env_first("OPENAI_TRANSCRIBE_MODEL", default="gpt-4o-mini-transcribe")
OPENAI_TRANSCRIBE_LANGUAGE = _env_first("OPENAI_TRANSCRIBE_LANGUAGE", default="pt")
SYSTEM_PROMPT = os.getenv("SYSTEM_PROMPT", "").strip()
PROMPT_PROFILE = os.getenv("AGENT_PROMPT_PROFILE", "").strip()
PROMPTS_DIR = os.path.join(os.path.dirname(__file__), "prompts")
INSTRUCTIONS_PATH = os.getenv("AGENT_INSTRUCTIONS_PATH", "").strip()
if not INSTRUCTIONS_PATH:
    if PROMPT_PROFILE:
        INSTRUCTIONS_PATH = os.path.join(PROMPTS_DIR, f"{PROMPT_PROFILE}.txt")
    else:
        INSTRUCTIONS_PATH = os.path.join(os.path.dirname(__file__), "assistant_instructions.txt")
SESSION_DB_PATH = os.getenv("AGENT_SESSION_DB", "sessions.db").strip() or "sessions.db"
ALLOW_GROUPS = os.getenv("ALLOW_GROUPS", "false").lower() == "true"
MAX_REPLY_CHARS = int(os.getenv("MAX_REPLY_CHARS", "1200"))
SESSION_MAX_ITEMS = int(os.getenv("SESSION_MAX_ITEMS", "0") or "0")
MESSAGE_DELAY_MS = os.getenv("MESSAGE_DELAY_MS", "350").strip()
FIRST_MESSAGE_DELAY_MS = os.getenv("FIRST_MESSAGE_DELAY_MS", "180").strip()
SCHEDULE_DELAY_MS = os.getenv("SCHEDULE_DELAY_MS", "350").strip()
ARIANE_MESSAGE_DELAY_MS = os.getenv("ARIANE_MESSAGE_DELAY_MS", "350").strip()
ARIANE_FIRST_MESSAGE_DELAY_MS = os.getenv("ARIANE_FIRST_MESSAGE_DELAY_MS", "180").strip()
ARIANE_SCHEDULE_DELAY_MS = os.getenv("ARIANE_SCHEDULE_DELAY_MS", "350").strip()
USER_MESSAGE_COALESCE_MS = os.getenv("USER_MESSAGE_COALESCE_MS", "800").strip()
USER_MESSAGE_COALESCE_MAX_MS = os.getenv("USER_MESSAGE_COALESCE_MAX_MS", "2500").strip()
MESSAGE_SPLIT_TARGET_CHARS = os.getenv("MESSAGE_SPLIT_TARGET_CHARS", "420").strip()
MESSAGE_SPLIT_MAX_CHARS = os.getenv("MESSAGE_SPLIT_MAX_CHARS", "720").strip()
MESSAGE_SPLIT_PRESENTATION_MAX_CHARS = os.getenv("MESSAGE_SPLIT_PRESENTATION_MAX_CHARS", "").strip()
PROFILES_PATH = os.getenv(
    "AGENT_PROFILES_PATH",
    os.path.join(os.path.dirname(__file__), "profiles.json"),
).strip()
PROFILE_STATE_DB = os.getenv("AGENT_PROFILE_DB", "profile_state.db").strip() or "profile_state.db"
PROFILE_ROUTING_ENABLED = os.getenv("AGENT_PROFILE_ROUTING", "true").lower() == "true"
LOG_WEBHOOK_PAYLOADS = os.getenv("LOG_WEBHOOK_PAYLOADS", "false").lower() == "true"
LOG_WEBHOOK_DEBUG = os.getenv("LOG_WEBHOOK_DEBUG", "false").lower() == "true"
RECENT_EVENT_TTL_SECONDS = int(os.getenv("RECENT_EVENT_TTL_SECONDS", "60") or "60")
POLL_THROTTLE_SECONDS = int(os.getenv("POLL_THROTTLE_SECONDS", "6") or "6")
DEDUP_DB_TTL_SECONDS = int(os.getenv("DEDUP_DB_TTL_SECONDS", "21600") or "21600")
OUTBOUND_ECHO_TTL_SECONDS = int(os.getenv("OUTBOUND_ECHO_TTL_SECONDS", "300") or "300")
PENDING_SIGNAL_TTL_SECONDS = int(os.getenv("PENDING_SIGNAL_TTL_SECONDS", "172800") or "172800")
SERVICE_AUDIO_REPEAT_TTL_SECONDS = int(
    os.getenv("SERVICE_AUDIO_REPEAT_TTL_SECONDS", "21600") or "21600"
)

WAHA_BASE_URL = os.getenv("WAHA_BASE_URL", "http://waha:3000").rstrip("/")
WAHA_API_KEY = os.getenv("WAHA_API_KEY_PLAIN", os.getenv("WAHA_API_KEY", "")).strip()
WAHA_SESSION = os.getenv("WAHA_SESSION", "default").strip()
WAHA_RECORDING_PREVIEW_MS = os.getenv("WAHA_RECORDING_PREVIEW_MS", "1400").strip()
WAHA_SENDVOICE_CONVERT = os.getenv("WAHA_SENDVOICE_CONVERT", "true").lower() == "true"

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = _env_first("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "conversations_agent_sessions").strip()
SUPABASE_APP = os.getenv("SUPABASE_APP", "delivery").strip()
SUPABASE_ENABLED = os.getenv("SUPABASE_ENABLED", "true").lower() == "true"
SUPABASE_SESSION_LIMIT = int(os.getenv("SUPABASE_SESSION_LIMIT", "12") or "12")
CRIOLASER_AUDIO_BUCKET = os.getenv("CRIOLASER_AUDIO_BUCKET", "audios_criolaser").strip()
CRIOLASER_AUDIO_PUBLIC_BUCKET = os.getenv("CRIOLASER_AUDIO_PUBLIC_BUCKET", "false").lower() == "true"
CRIOLASER_AUDIO_SIGN_TTL = int(os.getenv("CRIOLASER_AUDIO_SIGN_TTL", "3600") or "3600")
CRIOLASER_AUDIO_CACHE_TTL_SECONDS = int(
    os.getenv("CRIOLASER_AUDIO_CACHE_TTL_SECONDS", "600") or "600"
)
CRIOLASER_AUDIO_MAX_MATCHES = int(os.getenv("CRIOLASER_AUDIO_MAX_MATCHES", "2") or "2")

if OPENAI_API_KEY and not os.getenv("OPENAI_API_KEY"):
    os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY (or API_OPENAI_KEY) is not set. The agent will fail on first request.")

if SUPABASE_ENABLED and (not SUPABASE_URL or not SUPABASE_KEY):
    logger.warning("Supabase is enabled but SUPABASE_URL / SUPABASE_KEY are missing.")
if SUPABASE_ENABLED and supabase_create_client is None:
    logger.warning("Supabase client is not installed. Run `pip install supabase` in the agent environment.")


@app.get("/healthz")
async def healthz() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/chat-ui", response_class=HTMLResponse)
async def chat_ui() -> HTMLResponse:
    html_path = os.path.join(os.path.dirname(__file__), "chat_tester.html")
    try:
        with open(html_path, "r", encoding="utf-8") as handle:
            return HTMLResponse(content=handle.read())
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="chat_tester.html not found") from exc
    except OSError as exc:
        logger.exception("Failed to read chat tester UI: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to load chat tester UI") from exc


@app.get("/chat/profiles")
async def chat_profiles() -> Dict[str, Any]:
    profiles = []
    for profile in _PROFILE_LIST:
        profile_id = str(profile.get("id") or "").strip()
        if not profile_id:
            continue
        profiles.append(
            {
                "id": profile_id,
                "label": str(profile.get("label") or profile_id).strip(),
            }
        )
    return {
        "poll_name": PROFILE_POLL_NAME,
        "default_profile_id": PROFILE_DEFAULT_ID or None,
        "profiles": profiles,
    }


@app.post("/chat")
async def chat(request: Request) -> Dict[str, Any]:
    data = await request.json()
    message = (data.get("message") or "").strip()
    session_id = (data.get("session_id") or "local-test").strip()
    profile_id = (data.get("profile_id") or "").strip() or None
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    if not session_id:
        session_id = "local-test"

    if not profile_id:
        profile_id = PROMPT_PROFILE or PROFILE_DEFAULT_ID or None

    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

    session = _get_session(session_id)
    agent = _get_agent(profile_id)
    try:
        result = await _run_agent(agent, message, session, session_id, profile_id)
        reply = _truncate(_sanitize_plain_text(_extract_text_from_result(result), profile_id))
    except Exception as exc:
        logger.exception("Agent run failed: %s", exc)
        raise HTTPException(status_code=502, detail="Agent run failed") from exc

    if not reply:
        _log_empty_output_diagnostics(result, "chat_endpoint")
        reply = "Desculpe, nao consegui responder agora."
    reply = _inject_fake_schedule(session_id, message, reply)

    return {
        "reply": reply,
        "session_id": session_id,
        "profile_id": profile_id,
    }


def _parse_float(value: str) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        logger.warning("Invalid float value: %s", value)
        return None


def _parse_int(value: str) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid int value: %s", value)
        return None


_CURRENT_CHAT_ID: contextvars.ContextVar[str] = contextvars.ContextVar("CURRENT_CHAT_ID", default="")
_CURRENT_PROFILE_ID: contextvars.ContextVar[str] = contextvars.ContextVar("CURRENT_PROFILE_ID", default="")


def _normalize_vector_store_ids(raw: Any) -> list[str]:
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


_DEFAULT_PROFILES_DATA = {
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
        }
    ],
}


def _load_profiles_data() -> Dict[str, Any]:
    if not PROFILES_PATH:
        return _DEFAULT_PROFILES_DATA
    try:
        with open(PROFILES_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            if isinstance(data, dict) and data.get("profiles"):
                return data
    except FileNotFoundError:
        logger.warning("Profiles file not found: %s", PROFILES_PATH)
    except Exception as exc:
        logger.warning("Failed to load profiles file %s: %s", PROFILES_PATH, exc)
    return _DEFAULT_PROFILES_DATA


_PROFILES_DATA = _load_profiles_data()
_PROFILE_LIST = list(_PROFILES_DATA.get("profiles") or [])
PROFILES: Dict[str, Dict[str, Any]] = {p.get("id"): p for p in _PROFILE_LIST if p.get("id")}
PROFILE_POLL_NAME = _PROFILES_DATA.get("pollName") or "Olá. Para testar o atendimento, qual segmento você prefere?"
PROFILE_OPTIONS = [p.get("label") for p in _PROFILE_LIST if p.get("label")]
PROFILE_LABEL_TO_ID = {
    (p.get("label") or "").strip().lower(): p.get("id")
    for p in _PROFILE_LIST
    if p.get("label") and p.get("id")
}
PROFILE_DEFAULT_ID = _PROFILE_LIST[0].get("id") if _PROFILE_LIST else ""
_PROFILES_BASE_DIR = os.path.dirname(PROFILES_PATH) or os.path.dirname(__file__)
_PROFILE_FLOWS: Dict[str, BookingFlow] = load_profile_flows(_PROFILE_LIST, _PROFILES_BASE_DIR)


def _resolve_flow_profile_id(
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> str:
    if force_ariane:
        return "ariane"
    if profile_id:
        return profile_id
    if chat_id:
        resolved = _resolve_profile_for_chat(chat_id)
        if resolved:
            return resolved
    if _is_ariane_profile(profile_id, chat_id):
        return "ariane"
    return PROFILE_DEFAULT_ID or ""


def _get_booking_flow(
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> Optional[BookingFlow]:
    flow_profile_id = _resolve_flow_profile_id(profile_id, chat_id, force_ariane=force_ariane)
    if not flow_profile_id:
        return None
    return _PROFILE_FLOWS.get(flow_profile_id)


def _load_vector_store_map_from_env() -> Dict[str, list[str]]:
    mapping: Dict[str, list[str]] = {}
    raw = os.getenv("AGENT_VECTOR_STORE_IDS", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                for key, value in data.items():
                    ids = _normalize_vector_store_ids(value)
                    if ids:
                        mapping[str(key).strip()] = ids
        except Exception as exc:
            logger.warning("Failed to parse AGENT_VECTOR_STORE_IDS: %s", exc)
    for profile_id in PROFILES:
        env_key = f"AGENT_VECTOR_STORE_{profile_id.upper()}"
        env_value = os.getenv(env_key, "").strip()
        ids = _normalize_vector_store_ids(env_value)
        if ids:
            mapping[profile_id] = ids
    return mapping


def _load_profile_vector_store_ids() -> Dict[str, list[str]]:
    mapping: Dict[str, list[str]] = {}
    for profile in _PROFILE_LIST:
        profile_id = profile.get("id")
        if not profile_id:
            continue
        ids = _normalize_vector_store_ids(
            profile.get("vector_store_ids") or profile.get("vector_store_id")
        )
        if ids:
            mapping[profile_id] = ids
    mapping.update(_load_vector_store_map_from_env())
    return mapping


def _load_audio_bucket_map() -> Dict[str, str]:
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
    for profile in _PROFILE_LIST:
        profile_id = str(profile.get("id") or "").strip()
        bucket_name = str(profile.get("audio_bucket") or profile.get("audioBucket") or "").strip()
        if profile_id and bucket_name:
            mapping[profile_id] = bucket_name
    if CRIOLASER_AUDIO_BUCKET:
        mapping.setdefault("criolaser", CRIOLASER_AUDIO_BUCKET)
    return mapping


_PROFILE_VECTOR_STORE_IDS = _load_profile_vector_store_ids()
_PROFILE_AUDIO_BUCKETS = _load_audio_bucket_map()
_DEFAULT_VECTOR_STORE_IDS = _normalize_vector_store_ids(os.getenv("AGENT_VECTOR_STORE_ID", ""))
_SCHEDULING_TOOL: Optional[Any] = None
_KNOWLEDGE_TOOL: Optional[Any] = None
_AUDIO_MATCH_TOOL: Optional[Any] = None
_AUDIO_SEND_TOOL: Optional[Any] = None


def _get_vector_store_ids(profile_id: Optional[str]) -> list[str]:
    if profile_id:
        ids = _PROFILE_VECTOR_STORE_IDS.get(profile_id)
        if ids:
            return list(ids)
    return list(_DEFAULT_VECTOR_STORE_IDS)


def _get_audio_bucket_for_profile(profile_id: Optional[str]) -> str:
    if not profile_id:
        return ""
    return str(_PROFILE_AUDIO_BUCKETS.get(profile_id) or "").strip()


def _get_docs_dir_for_profile(profile_id: Optional[str]) -> str:
    if not profile_id:
        return ""
    profile = PROFILES.get(profile_id) or {}
    path = str(profile.get("docs_dir") or profile.get("docsDir") or "").strip()
    if not path:
        return ""
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(_PROFILES_BASE_DIR, path))


def _build_tools_for_profile(profile_id: Optional[str]) -> list[Any]:
    tools: list[Any] = []
    ids = _get_vector_store_ids(profile_id)
    if ids:
        if FileSearchTool is None:
            logger.warning("FileSearchTool not available. Update openai-agents to enable file search.")
        else:
            tools.append(
                FileSearchTool(
                    vector_store_ids=ids,
                    max_num_results=4,
                )
            )
    if _SCHEDULING_TOOL is not None:
        tools.append(_SCHEDULING_TOOL)
    if _KNOWLEDGE_TOOL is not None:
        tools.append(_KNOWLEDGE_TOOL)
    if _get_audio_bucket_for_profile(profile_id):
        if _AUDIO_MATCH_TOOL is not None:
            tools.append(_AUDIO_MATCH_TOOL)
        if _AUDIO_SEND_TOOL is not None:
            tools.append(_AUDIO_SEND_TOOL)
    return tools




def _load_instructions() -> str:
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


def _append_audio_tool_instructions(instructions: str, profile_id: Optional[str]) -> str:
    bucket_name = _get_audio_bucket_for_profile(profile_id)
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


def _resolve_profile_instructions_path(path: str) -> str:
    if not path:
        return ""
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(_PROFILES_BASE_DIR, path))


def _load_profile_instructions(profile_id: str) -> str:
    profile = PROFILES.get(profile_id) or {}
    path = _resolve_profile_instructions_path(profile.get("instructions_path", ""))
    if not path:
        return _append_audio_tool_instructions(_load_instructions(), profile_id)
    try:
        with open(path, "r", encoding="utf-8") as handle:
            content = handle.read().strip()
            if content:
                return _append_audio_tool_instructions(content, profile_id)
    except FileNotFoundError:
        logger.warning("Instructions file not found: %s", path)
    except OSError as exc:
        logger.warning("Failed to read instructions file %s: %s", path, exc)
    return _append_audio_tool_instructions(_load_instructions(), profile_id)


def _build_model_settings() -> Optional[ModelSettings]:
    max_tokens = _parse_int(OPENAI_MAX_TOKENS)
    model_name = (OPENAI_MODEL or "").strip().lower()
    if model_name.startswith("gpt-5"):
        # GPT-5 + tools can consume output budget quickly; keep this floor to avoid empty final text.
        if max_tokens is None or max_tokens < 1024:
            if max_tokens is not None:
                logger.info(
                    "OPENAI_MAX_TOKENS=%s is low for %s; using 1024 to avoid empty responses.",
                    max_tokens,
                    OPENAI_MODEL,
                )
            max_tokens = 1024
        return ModelSettings(
            max_tokens=max_tokens,
            reasoning={"effort": "low"},
            verbosity="low",
        )
    if max_tokens is None:
        return None
    return ModelSettings(
        max_tokens=max_tokens,
    )


def _extract_text_from_result(result: Any) -> str:
    final_output = getattr(result, "final_output", None)
    if isinstance(final_output, str):
        text = final_output.strip()
        if text:
            return text

    raw_responses = getattr(result, "raw_responses", None) or []
    for raw in reversed(raw_responses):
        output_items = getattr(raw, "output", None) or []
        for item in reversed(output_items):
            content = getattr(item, "content", None) or []
            parts: list[str] = []
            for part in content:
                part_type = getattr(part, "type", "")
                part_text = getattr(part, "text", None)
                if (
                    isinstance(part_text, str)
                    and part_text.strip()
                    and part_type in ("output_text", "text")
                ):
                    parts.append(part_text.strip())
            if parts:
                return "\n".join(parts).strip()

            item_text = getattr(item, "text", None)
            if isinstance(item_text, str) and item_text.strip():
                return item_text.strip()

    return ""


def _log_empty_output_diagnostics(result: Any, context: str) -> None:
    try:
        raw_responses = getattr(result, "raw_responses", None) or []
        if not raw_responses:
            logger.warning("Agent returned empty output (%s): no raw_responses", context)
            return
        last_response = raw_responses[-1]
        output_items = getattr(last_response, "output", None) or []
        output_types = [getattr(item, "type", item.__class__.__name__) for item in output_items]
        logger.warning(
            "Agent returned empty output (%s): response_id=%s output_types=%s output_items=%s",
            context,
            getattr(last_response, "response_id", None),
            output_types,
            len(output_items),
        )
    except Exception as exc:
        logger.warning("Failed to build empty output diagnostics (%s): %s", context, exc)


def _build_agent() -> Agent:
    profile_hint = PROMPT_PROFILE or PROFILE_DEFAULT_ID or None
    kwargs: Dict[str, Any] = {
        "name": "Assistente",
        "instructions": _load_instructions(),
    }
    tools = _build_tools_for_profile(profile_hint)
    if tools:
        kwargs["tools"] = tools
    if OPENAI_MODEL:
        kwargs["model"] = OPENAI_MODEL
    model_settings = _build_model_settings()
    if model_settings:
        kwargs["model_settings"] = model_settings
    return Agent(**kwargs)


def _build_agent_for_profile(profile_id: str) -> Agent:
    profile = PROFILES.get(profile_id) or {}
    kwargs: Dict[str, Any] = {
        "name": profile.get("label") or "Assistente",
        "instructions": _load_profile_instructions(profile_id),
    }
    tools = _build_tools_for_profile(profile_id)
    if tools:
        kwargs["tools"] = tools
    if OPENAI_MODEL:
        kwargs["model"] = OPENAI_MODEL
    model_settings = _build_model_settings()
    if model_settings:
        kwargs["model_settings"] = model_settings
    return Agent(**kwargs)


def _truncate(text: str) -> str:
    if MAX_REPLY_CHARS <= 0:
        return text
    return text[:MAX_REPLY_CHARS]


def _strip_list_prefix(text: str) -> str:
    if not text:
        return ""
    stripped = text.lstrip()
    stripped = re.sub(r"^(?:#|[-*•]+)\s*", "", stripped)
    stripped = re.sub(r"^\d+[.)]\s*", "", stripped)
    return stripped.strip()


def _looks_like_check_item(text: str) -> bool:
    cleaned = _strip_list_prefix(text)
    if not cleaned:
        return False
    if cleaned.endswith("?"):
        return False
    if len(cleaned) > 180:
        return False
    lowered = _normalize_text(cleaned)
    if lowered.startswith("gostaria de") or lowered.startswith("quer que eu"):
        return False
    return True


def _normalize_ariane_inline_blocks(text: str) -> str:
    normalized = (text or "").strip()
    if not normalized:
        return ""
    # Expand inline checklist markers and inline service labels into their own lines.
    normalized = re.sub(r"\s*✅\s*", "\n✅ ", normalized)
    normalized = re.sub(
        r"(?<!\n)(Consulta Capilar:|Terapia Capilar:|Est[eé]tica Facial:)",
        r"\n\1",
        normalized,
    )
    if normalized.count("\n✅ ") >= 2:
        normalized = re.sub(
            r"\s+(?=(?:Me conta,|Quer que eu|Gostaria de|Posso |Prefere |Restou alguma duvida))",
            "\n\n",
            normalized,
            count=1,
            flags=re.IGNORECASE,
        )
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def _format_ariane_checklists(text: str) -> str:
    if not text:
        return text

    formatted_paragraphs: list[str] = []
    paragraphs = re.split(r"\n\s*\n", _normalize_ariane_inline_blocks(text))

    for paragraph in paragraphs:
        raw_lines = [line.rstrip() for line in paragraph.splitlines() if line.strip()]
        if len(raw_lines) < 2:
            formatted_paragraphs.append("\n".join(_strip_list_prefix(line) for line in raw_lines).strip())
            continue

        normalized_lines = [_strip_list_prefix(line) for line in raw_lines]
        first_line = normalized_lines[0]
        first_lowered = _normalize_text(first_line)
        has_heading = first_line.endswith(":") or any(
            marker in first_lowered
            for marker in (
                "voce pode esperar",
                "servicos",
                "beneficios",
                "o que voce recebe",
                "inclui",
                "entregaveis",
            )
        )

        start_idx = 1 if has_heading else 0
        candidate_items = normalized_lines[start_idx:]
        checklist_candidates = [line for line in candidate_items if _looks_like_check_item(line)]
        colon_style_items = [line for line in candidate_items if ":" in line and not line.endswith("?")]
        existing_checks = [line for line in candidate_items if line.startswith("✅")]

        should_format = False
        if len(existing_checks) >= 2:
            should_format = True
        elif len(checklist_candidates) >= 2 and len(checklist_candidates) == len(candidate_items):
            should_format = True
        elif len(colon_style_items) >= 2 and len(colon_style_items) == len(candidate_items):
            should_format = True

        if not should_format:
            formatted_paragraphs.append("\n".join(normalized_lines).strip())
            continue

        trailing_questions: list[str] = []
        while candidate_items and candidate_items[-1].strip().endswith("?"):
            trailing_questions.insert(0, candidate_items.pop().strip())

        lines: list[str] = []
        if has_heading:
            lines.append(first_line)
        for item in candidate_items:
            item_text = _strip_list_prefix(item)
            if not item_text:
                continue
            if item_text.startswith("✅"):
                item_text = item_text.lstrip("✅").strip()
            if _looks_like_check_item(item_text) or ":" in item_text:
                lines.append(f"✅ {item_text}")
            else:
                lines.append(item_text)
        formatted_paragraphs.append("\n".join(lines).strip())
        if trailing_questions:
            formatted_paragraphs.append("\n".join(trailing_questions).strip())

    return "\n\n".join(part for part in formatted_paragraphs if part).strip()


def _split_ariane_trailing_question_blocks(text: str) -> str:
    if not text:
        return text

    formatted_paragraphs: list[str] = []
    paragraphs = re.split(r"\n\s*\n", text.strip())

    for paragraph in paragraphs:
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        if not lines:
            continue
        if any(line.startswith("✅ ") for line in lines):
            formatted_paragraphs.append("\n".join(lines))
            continue

        paragraph_text = " ".join(lines).strip()
        if paragraph_text.count("?") == 0:
            formatted_paragraphs.append(paragraph_text)
            continue

        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", paragraph_text) if part.strip()]
        if len(sentences) < 2:
            formatted_paragraphs.append(paragraph_text)
            continue

        trailing_questions: list[str] = []
        while sentences and sentences[-1].endswith("?"):
            trailing_questions.insert(0, sentences.pop())

        explanation = " ".join(sentences).strip()
        question_block = " ".join(trailing_questions).strip()
        if explanation and question_block and len(explanation) >= 90:
            formatted_paragraphs.append(explanation)
            formatted_paragraphs.append(question_block)
        else:
            formatted_paragraphs.append(paragraph_text)

    return "\n\n".join(part for part in formatted_paragraphs if part).strip()


def _sanitize_plain_text(text: str, profile_id: Optional[str] = None) -> str:
    if not text:
        return text
    cleaned = text.replace("**", "").replace("__", "").replace("`", "")
    if _is_ariane_profile(profile_id):
        sanitized = _split_ariane_trailing_question_blocks(_format_ariane_checklists(cleaned))
    else:
        sanitized = "\n".join(_strip_list_prefix(line) for line in cleaned.splitlines())
    return _sanitize_internal_knowledge_references(sanitized)


def _sanitize_internal_knowledge_references(text: str) -> str:
    if not text:
        return text

    # Nunca atribuir a origem das informacoes a arquivos/documentos enviados pelo usuario.
    drop_line_patterns = (
        r"voc[eê]\s+enviou.*arquiv",
        r"aproveitando.*arquiv",
        r"posso ajudar.*relacionad[oa].*arquiv",
        r"arquivos?\s+que\s+voc[eê]\s+enviou",
    )
    kept_lines: list[str] = []
    for line in text.splitlines():
        lowered = _normalize_text(line)
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


_RECENT_EVENT_IDS: Dict[str, float] = {}
_RECENT_MESSAGE_KEYS: Dict[str, float] = {}
_RECENT_POLL_SENT: Dict[str, float] = {}
_RECENT_OUTBOUND_MESSAGE_IDS: Dict[str, float] = {}
_RECENT_AUDIO_SENT_CHATS: Dict[str, float] = {}
_RECENT_SERVICE_AUDIO_KEYS: Dict[str, float] = {}
_LAST_SCHEDULE_OPTIONS: Dict[str, Dict[str, Any]] = {}
_PENDING_USER_MESSAGES: Dict[str, Dict[str, Any]] = {}
_PENDING_USER_LOCKS: Dict[str, Any] = {}
_CHAT_ACTIVE_TURN: Dict[str, int] = {}
_AUDIO_FILE_CACHE: Dict[str, Any] = {"expires_at": 0.0, "files": []}


def _is_duplicate_key(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> bool:
    if not key:
        return False
    now = time.time()
    if len(store) > 5000:
        store.clear()
    expired = [k for k, ts in store.items() if now - ts > ttl_seconds]
    for k in expired:
        store.pop(k, None)
    if key in store:
        return True
    store[key] = now
    return False


def _has_recent_key(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> bool:
    if not key:
        return False
    now = time.time()
    if len(store) > 5000:
        store.clear()
    expired = [k for k, ts in store.items() if now - ts > ttl_seconds]
    for k in expired:
        store.pop(k, None)
    return key in store


def _remember_recent_key(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> None:
    if not key:
        return
    _is_duplicate_key(store, key, ttl_seconds)


def _remember_recent_audio_sent(chat_id: str) -> None:
    if not chat_id:
        return
    _remember_recent_key(_RECENT_AUDIO_SENT_CHATS, str(chat_id), 45)


def _has_recent_audio_sent(chat_id: str) -> bool:
    if not chat_id:
        return False
    return _has_recent_key(_RECENT_AUDIO_SENT_CHATS, str(chat_id), 45)


def _service_audio_key(chat_id: str, filename: str) -> str:
    return f"{str(chat_id or '').strip()}::{str(filename or '').strip()}"


def _remember_service_audio_sent(chat_id: str, filename: str) -> None:
    key = _service_audio_key(chat_id, filename)
    if not key.strip(":"):
        return
    _remember_recent_key(
        _RECENT_SERVICE_AUDIO_KEYS,
        key,
        max(SERVICE_AUDIO_REPEAT_TTL_SECONDS, 60),
    )


def _has_recent_service_audio_sent(chat_id: str, filename: str) -> bool:
    key = _service_audio_key(chat_id, filename)
    if not key.strip(":"):
        return False
    return _has_recent_key(
        _RECENT_SERVICE_AUDIO_KEYS,
        key,
        max(SERVICE_AUDIO_REPEAT_TTL_SECONDS, 60),
    )


def _next_chat_turn(chat_id: str) -> int:
    key = str(chat_id or "").strip()
    if not key:
        return 0
    if len(_CHAT_ACTIVE_TURN) > 5000:
        _CHAT_ACTIVE_TURN.clear()
    turn = (_CHAT_ACTIVE_TURN.get(key) or 0) + 1
    _CHAT_ACTIVE_TURN[key] = turn
    return turn


def _is_chat_turn_current(chat_id: str, turn: Optional[int]) -> bool:
    if not turn:
        return True
    key = str(chat_id or "").strip()
    if not key:
        return True
    return (_CHAT_ACTIVE_TURN.get(key) or 0) == turn


def _is_duplicate_key_db(key: Optional[str], ttl_seconds: int) -> bool:
    if not key or ttl_seconds <= 0:
        return False
    now = int(time.time())
    try:
        conn = sqlite3.connect(PROFILE_STATE_DB, timeout=2)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS recent_events (
                key TEXT PRIMARY KEY,
                created_at INTEGER
            )
            """
        )
        conn.execute(
            "DELETE FROM recent_events WHERE created_at < ?",
            (now - ttl_seconds,),
        )
        cur = conn.execute(
            "INSERT OR IGNORE INTO recent_events (key, created_at) VALUES (?, ?)",
            (key, now),
        )
        conn.commit()
        return cur.rowcount == 0
    except Exception as exc:
        logger.warning("Failed to read recent_events: %s", exc)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _is_duplicate_key_global(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> bool:
    memory_dup = _is_duplicate_key(store, key, ttl_seconds)
    db_dup = _is_duplicate_key_db(key, DEDUP_DB_TTL_SECONDS)
    return memory_dup or db_dup


def _get_session(session_id: str) -> SQLiteSession:
    return SQLiteSession(session_id, SESSION_DB_PATH)


_UNSET = object()


def _init_profile_state_db() -> None:
    try:
        conn = sqlite3.connect(PROFILE_STATE_DB)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS profile_state (
                chat_id TEXT PRIMARY KEY,
                profile_id TEXT,
                poll_id TEXT,
                pending_message TEXT,
                flow_state TEXT,
                flow_data TEXT,
                updated_at INTEGER
            )
            """
        )
        existing_columns = {
            str(row[1]).strip().lower()
            for row in conn.execute("PRAGMA table_info(profile_state)").fetchall()
            if row and len(row) > 1
        }
        if "flow_state" not in existing_columns:
            conn.execute("ALTER TABLE profile_state ADD COLUMN flow_state TEXT")
        if "flow_data" not in existing_columns:
            conn.execute("ALTER TABLE profile_state ADD COLUMN flow_data TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS recent_events (
                key TEXT PRIMARY KEY,
                created_at INTEGER
            )
            """
        )
        conn.commit()
    except Exception as exc:
        logger.warning("Failed to init profile state db: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _get_profile_state(chat_id: str) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "profile_id": None,
        "poll_id": None,
        "pending_message": None,
        "flow_state": None,
        "flow_data": {},
    }
    if not chat_id:
        return state
    try:
        conn = sqlite3.connect(PROFILE_STATE_DB)
        row = conn.execute(
            """
            SELECT profile_id, poll_id, pending_message, flow_state, flow_data
            FROM profile_state
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()
        if row:
            state["profile_id"] = row[0]
            state["poll_id"] = row[1]
            state["pending_message"] = row[2]
            state["flow_state"] = row[3]
            raw_flow_data = row[4]
            if raw_flow_data:
                try:
                    parsed = json.loads(raw_flow_data)
                    if isinstance(parsed, dict):
                        state["flow_data"] = parsed
                    else:
                        state["flow_data"] = {"value": parsed}
                except Exception:
                    state["flow_data"] = {}
    except Exception as exc:
        logger.warning("Failed to read profile state: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return state


def _update_profile_state(
    chat_id: str,
    profile_id: Any = _UNSET,
    poll_id: Any = _UNSET,
    pending_message: Any = _UNSET,
    flow_state: Any = _UNSET,
    flow_data: Any = _UNSET,
) -> None:
    if not chat_id:
        return
    state = _get_profile_state(chat_id)

    def _coerce_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    def _coerce_json_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text or None
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return json.dumps({"value": str(value)}, ensure_ascii=False)

    if profile_id is not _UNSET:
        state["profile_id"] = _coerce_text(profile_id)
    if poll_id is not _UNSET:
        state["poll_id"] = _coerce_text(poll_id)
    if pending_message is not _UNSET:
        state["pending_message"] = _coerce_text(pending_message)
    if flow_state is not _UNSET:
        state["flow_state"] = _coerce_text(flow_state)
    if flow_data is not _UNSET:
        state["flow_data"] = flow_data

    try:
        conn = sqlite3.connect(PROFILE_STATE_DB)
        conn.execute(
            """
            INSERT INTO profile_state (
                chat_id,
                profile_id,
                poll_id,
                pending_message,
                flow_state,
                flow_data,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                profile_id=excluded.profile_id,
                poll_id=excluded.poll_id,
                pending_message=excluded.pending_message,
                flow_state=excluded.flow_state,
                flow_data=excluded.flow_data,
                updated_at=excluded.updated_at
            """,
            (
                chat_id,
                state["profile_id"],
                state["poll_id"],
                state["pending_message"],
                state["flow_state"],
                _coerce_json_text(state.get("flow_data")),
                int(time.time()),
            ),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("Failed to update profile state: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _clear_profile_state(chat_id: str) -> None:
    if not chat_id:
        return
    try:
        conn = sqlite3.connect(PROFILE_STATE_DB)
        conn.execute("DELETE FROM profile_state WHERE chat_id = ?", (chat_id,))
        conn.commit()
    except Exception as exc:
        logger.warning("Failed to clear profile state: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass


_init_profile_state_db()


def _resolve_profile_for_chat(chat_id: str) -> Optional[str]:
    if PROFILE_ROUTING_ENABLED and chat_id:
        state = _get_profile_state(str(chat_id))
        profile_id = (state.get("profile_id") or "").strip()
        if profile_id:
            return profile_id
    return PROMPT_PROFILE or PROFILE_DEFAULT_ID or None


def _is_criolaser_profile(profile_id: Optional[str], chat_id: str = "") -> bool:
    resolved = (profile_id or "").strip()
    if not resolved and chat_id:
        resolved = (_resolve_profile_for_chat(chat_id) or "").strip()
    return _normalize_text(resolved) == "criolaser"


def _first_name(full_name: str) -> Optional[str]:
    if not full_name:
        return None
    parts = full_name.strip().split()
    if not parts:
        return None
    return parts[0]


def _profile_greeting_name(profile_id: Optional[str]) -> str:
    profile = PROFILES.get(profile_id) if profile_id else None
    if profile and profile.get("greeting_name"):
        return profile["greeting_name"]
    if profile and profile.get("label"):
        return profile["label"]
    return "Mariano Odontologia"


def _profile_greeting_message(profile_id: Optional[str]) -> str:
    profile = PROFILES.get(profile_id) if profile_id else None
    if profile:
        message = str(profile.get("greeting_message") or "").strip()
        if message:
            return message
    greeting_name = _profile_greeting_name(profile_id)
    return (
        f"tudo bem? Sou a assistente da {greeting_name} 👩🏼‍⚕️\n"
        "Como posso te ajudar hoje?"
    )


def _has_profile_greeting(items: list[dict[str, Any]], profile_id: Optional[str]) -> bool:
    if not items:
        return False
    marker = _normalize_text(_profile_greeting_message(profile_id))
    for item in items:
        if item.get("role") != "assistant":
            continue
        content = _normalize_text(_coerce_session_item_content(item.get("content")))
        if marker and marker in content:
            return True
    return False


def _build_greeting(first_name: Optional[str], profile_id: Optional[str]) -> str:
    greeting_message = _profile_greeting_message(profile_id)
    if first_name:
        return f"Oii {first_name}, {greeting_message}"
    return f"Oii, {greeting_message}"


def _name_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    for key in ("pushName", "pushname", "notifyName", "name", "senderName", "contactName"):
        value = (payload.get(key) or "").strip()
        if value:
            return value
    return None


def _is_audio_payload(payload: Dict[str, Any]) -> bool:
    if not payload:
        return False
    msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
    if msg_type in ("audio", "voice", "ptt", "voicenote", "voice_note"):
        return True
    mimetype = (payload.get("mimetype") or payload.get("mimeType") or "").lower()
    if mimetype.startswith("audio/"):
        return True
    media = payload.get("media")
    if isinstance(media, dict):
        media_type = (media.get("type") or "").lower()
        if media_type in ("audio", "voice", "ptt", "voicenote", "voice_note"):
            return True
        media_mime = (media.get("mimetype") or media.get("mimeType") or "").lower()
        if media_mime.startswith("audio/"):
            return True
    return False


def _is_non_text_media(payload: Dict[str, Any]) -> bool:
    if not payload:
        return False
    if _is_audio_payload(payload):
        return False
    msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
    if msg_type in (
        "image",
        "video",
        "document",
        "file",
        "sticker",
        "ptv",
        "media",
    ):
        return True
    has_media = payload.get("hasMedia")
    if has_media is True or str(has_media).strip().lower() in ("1", "true", "yes", "sim"):
        return True
    media = payload.get("media")
    if isinstance(media, dict):
        media_type = (media.get("type") or "").lower()
        if media_type in (
            "image",
            "video",
            "document",
            "file",
            "sticker",
            "ptv",
            "media",
        ):
            return True
        if (
            media.get("mimetype")
            or media.get("mimeType")
            or media.get("mediaUrl")
            or media.get("fileUrl")
            or media.get("downloadUrl")
            or media.get("url")
            or media.get("fileName")
            or media.get("filename")
        ):
            return True
    if media:
        return True
    if payload.get("mimetype") or payload.get("mimeType"):
        return True
    if payload.get("mediaUrl") or payload.get("fileUrl") or payload.get("downloadUrl"):
        return True
    return False


def _extract_media_url(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    for key in ("mediaUrl", "fileUrl", "downloadUrl", "url"):
        value = (payload.get(key) or "").strip()
        if value:
            return _normalize_media_url(value)
    media = payload.get("media")
    if isinstance(media, dict):
        for key in ("mediaUrl", "fileUrl", "downloadUrl", "url"):
            value = (media.get(key) or "").strip()
            if value:
                return _normalize_media_url(value)
    return None


def _normalize_media_url(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if not parsed.scheme:
        base = urlparse(WAHA_BASE_URL)
        path = url if url.startswith("/") else f"/{url}"
        return urlunparse((base.scheme, base.netloc, path, "", "", ""))
    host = parsed.hostname or ""
    if host in ("localhost", "127.0.0.1"):
        base = urlparse(WAHA_BASE_URL)
        return urlunparse(
            (base.scheme, base.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
        )
    return url


def _normalize_mimetype(value: str) -> str:
    if not value:
        return ""
    return value.split(";", 1)[0].strip().lower()


def _extract_mimetype(payload: Dict[str, Any]) -> str:
    mimetype = _normalize_mimetype((payload.get("mimetype") or payload.get("mimeType") or ""))
    if not mimetype and isinstance(payload.get("media"), dict):
        mimetype = _normalize_mimetype(
            (payload["media"].get("mimetype") or payload["media"].get("mimeType") or "")
        )
    return mimetype


def _normalize_phone(chat_id: str) -> str:
    if not chat_id:
        return ""
    base = chat_id.split("@", 1)[0]
    digits = "".join(ch for ch in base if ch.isdigit())
    return digits or base


def _get_supabase_client() -> Optional["SupabaseClient"]:
    if not SUPABASE_ENABLED:
        return None
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    if supabase_create_client is None:
        return None
    global _SUPABASE_CLIENT
    if _SUPABASE_CLIENT is None:
        _SUPABASE_CLIENT = supabase_create_client(SUPABASE_URL, SUPABASE_KEY)
    return _SUPABASE_CLIENT


async def _supabase_insert(row: Dict[str, Any]) -> None:
    client = _get_supabase_client()
    if not client or not SUPABASE_TABLE:
        return

    def _insert() -> None:
        client.table(SUPABASE_TABLE).insert(row).execute()

    try:
        await anyio.to_thread.run_sync(_insert)
    except Exception as exc:
        logger.warning("Supabase insert failed: %s", exc)


async def _supabase_fetch_recent(phone: str, chat_id: Optional[str] = None) -> list[Dict[str, Any]]:
    client = _get_supabase_client()
    if not client or not SUPABASE_TABLE or not phone or SUPABASE_SESSION_LIMIT <= 0:
        return []

    def _fetch() -> list[Dict[str, Any]]:
        query = client.table(SUPABASE_TABLE).select("user_message, bot_message, created_at").eq(
            "phone", phone
        )
        if SUPABASE_APP:
            query = query.eq("app", SUPABASE_APP)
        if chat_id:
            query = query.eq("conversation_id", chat_id)
        resp = query.order("created_at", desc=True).limit(SUPABASE_SESSION_LIMIT).execute()
        data = list(resp.data or [])
        if data or not chat_id:
            return data
        fallback = client.table(SUPABASE_TABLE).select("user_message, bot_message, created_at").eq(
            "phone", phone
        )
        if SUPABASE_APP:
            fallback = fallback.eq("app", SUPABASE_APP)
        resp = fallback.order("created_at", desc=True).limit(SUPABASE_SESSION_LIMIT).execute()
        return list(resp.data or [])

    try:
        return await anyio.to_thread.run_sync(_fetch)
    except Exception as exc:
        logger.warning("Supabase fetch failed: %s", exc)
        return []


def _supabase_storage_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if SUPABASE_KEY:
        headers["Authorization"] = f"Bearer {SUPABASE_KEY}"
        headers["apikey"] = SUPABASE_KEY
    return headers


async def _list_bucket_audio_files(bucket: str) -> list[Dict[str, str]]:
    result = await anyio.to_thread.run_sync(_list_bucket_audio_files_sync_detailed, bucket)
    if result.get("error"):
        logger.warning("Supabase storage list failed bucket=%s: %s", bucket, result["error"])
    return list(result.get("files") or [])


async def _build_bucket_audio_url(bucket: str, file_name: str) -> Optional[str]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket or not file_name:
        return None
    encoded_path = quote(file_name, safe="/")
    if CRIOLASER_AUDIO_PUBLIC_BUCKET:
        return f"{SUPABASE_URL}/storage/v1/object/public/{quote(bucket, safe='')}/{encoded_path}"

    url = f"{SUPABASE_URL}/storage/v1/object/sign/{quote(bucket, safe='')}/{encoded_path}"
    payload = {"expiresIn": max(CRIOLASER_AUDIO_SIGN_TTL, 60)}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload, headers=_supabase_storage_headers())
        if resp.status_code >= 400:
            logger.warning("Supabase signed URL failed: %s %s", resp.status_code, resp.text)
            return None
        data = resp.json()
    except Exception as exc:
        logger.warning("Supabase signed URL request failed: %s", exc)
        return None

    signed_url = str(data.get("signedURL") or data.get("signedUrl") or "").strip()
    if not signed_url:
        return None
    if signed_url.startswith("http://") or signed_url.startswith("https://"):
        return signed_url
    if signed_url.startswith("/storage/v1/"):
        return f"{SUPABASE_URL}{signed_url}"
    if signed_url.startswith("/"):
        return f"{SUPABASE_URL}/storage/v1{signed_url}"
    return f"{SUPABASE_URL}/storage/v1/{signed_url.lstrip('/')}"


def _audio_file_variants(file_info: Dict[str, str]) -> list[str]:
    stem = _normalize_service_text(file_info.get("stem") or file_info.get("normalized_stem") or "")
    if not stem:
        return []
    tokens = [token for token in stem.split() if token]
    variants: set[str] = {stem}
    max_ngram = min(len(tokens), 3)
    for size in range(1, max_ngram + 1):
        for idx in range(0, len(tokens) - size + 1):
            chunk = " ".join(tokens[idx : idx + size]).strip()
            if len(chunk) >= 4:
                variants.add(chunk)
    return sorted(variants, key=len, reverse=True)


def _score_audio_match(query: str, file_info: Dict[str, str]) -> float:
    normalized_query = _normalize_service_text(query)
    if not normalized_query:
        return 0.0
    query_tokens = {token for token in normalized_query.split() if token}
    best_score = 0.0
    for variant in _audio_file_variants(file_info):
        variant_tokens = {token for token in variant.split() if token}
        if normalized_query == variant:
            return 1.0
        if normalized_query in variant or variant in normalized_query:
            best_score = max(best_score, 0.93)
        token_overlap = 0.0
        if query_tokens and variant_tokens:
            token_overlap = len(query_tokens & variant_tokens) / max(len(query_tokens), len(variant_tokens))
            if query_tokens.issubset(variant_tokens):
                best_score = max(best_score, 0.9)
        ratio = difflib.SequenceMatcher(None, normalized_query, variant).ratio()
        candidate_score = (ratio * 0.72) + (token_overlap * 0.28)
        best_score = max(best_score, candidate_score)
    return round(best_score, 4)


def _match_audio_files(
    query: str,
    available_files: list[Dict[str, str]],
    *,
    limit: int = 3,
    min_score: float = 0.55,
) -> list[Dict[str, Any]]:
    normalized_query = _normalize_service_text(query)
    if not normalized_query:
        return []
    ranked: list[Dict[str, Any]] = []
    for file_info in available_files:
        score = _score_audio_match(normalized_query, file_info)
        if score < min_score:
            continue
        ranked.append(
            {
                "filename": str(file_info.get("name") or "").strip(),
                "display_name": str(file_info.get("stem") or "").strip(),
                "score": score,
            }
        )
    ranked.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    return ranked[: max(limit, 1)]


def _looks_like_booking_or_interest_intent(text: str) -> bool:
    lowered = _normalize_text(text or "")
    if not lowered:
        return False
    triggers = (
        "quero agendar",
        "quero marcar",
        "quero fazer",
        "tenho interesse",
        "tenho interesse em",
        "quero esse",
        "quero este",
        "gostei desse",
        "gostei desse procedimento",
        "vamos agendar",
        "como agenda",
        "como agendar",
        "podemos agendar",
        "quero saber desse",
        "me interessei",
    )
    return any(trigger in lowered for trigger in triggers)


def _humanize_audio_display_name(name: str) -> str:
    cleaned = str(name or "").strip().replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or "o procedimento"


async def _try_send_service_audio_for_message(
    chat_id: str,
    profile_id: Optional[str],
    user_text: str,
    *,
    active_turn: Optional[int] = None,
    min_score: float = 0.72,
) -> Optional[Dict[str, str]]:
    bucket = _get_audio_bucket_for_profile(profile_id)
    if not bucket or not chat_id:
        return None
    if _has_recent_audio_sent(chat_id):
        return None
    if not _is_chat_turn_current(str(chat_id), active_turn):
        return None

    available_files = await _list_bucket_audio_files(bucket)
    if not available_files:
        return None

    matches = _match_audio_files(user_text, available_files, limit=1, min_score=min_score)
    if not matches:
        return None

    top_match = matches[0]
    filename = str(top_match.get("filename") or "").strip()
    if not filename:
        return None
    if _has_recent_service_audio_sent(chat_id, filename):
        return None
    media_url = await _build_bucket_audio_url(bucket, filename)
    if not media_url:
        return None
    await _send_voice(chat_id, media_url)
    _remember_service_audio_sent(chat_id, filename)
    display_name = _humanize_audio_display_name(str(top_match.get("display_name") or filename))
    return {
        "filename": filename,
        "display_name": display_name,
        "session_note": f"Enviei um audio de atendimento sobre {display_name}.",
    }


async def _maybe_send_profile_audio(
    chat_id: str,
    profile_id: Optional[str],
    user_text: str,
    assistant_text: str,
    active_turn: Optional[int] = None,
) -> Optional[str]:
    bucket = _get_audio_bucket_for_profile(profile_id)
    if not bucket or not chat_id:
        return None
    if _has_recent_audio_sent(chat_id):
        return None
    if not _is_chat_turn_current(str(chat_id), active_turn):
        return None

    available_files = await _list_bucket_audio_files(bucket)
    if not available_files:
        return None

    matches = _match_audio_files(user_text, available_files, limit=1, min_score=0.58)
    if not matches and _looks_like_booking_or_interest_intent(user_text):
        matches = _match_audio_files(assistant_text, available_files, limit=1, min_score=0.7)
    if not matches:
        return None

    filename = str(matches[0].get("filename") or "").strip()
    if not filename:
        return None
    if _has_recent_service_audio_sent(chat_id, filename):
        return None
    media_url = await _build_bucket_audio_url(bucket, filename)
    if not media_url:
        return None
    await _send_voice(chat_id, media_url)
    _remember_service_audio_sent(chat_id, filename)
    return filename


async def _hydrate_session_from_supabase(session: SQLiteSession, chat_id: str) -> None:
    phone = _normalize_phone(chat_id)
    rows = await _supabase_fetch_recent(phone, chat_id)
    if not rows:
        return

    items: list[Dict[str, str]] = []
    for row in reversed(rows):
        user_message = (row.get("user_message") or "").strip()
        if user_message:
            items.append({"role": "user", "content": user_message})
        bot_message = (row.get("bot_message") or "").strip()
        if bot_message:
            items.append({"role": "assistant", "content": bot_message})
    if not items:
        return
    try:
        await session.add_items(items)
    except Exception as exc:
        logger.warning("Failed to hydrate session from Supabase: %s", exc)


async def _trim_session(session: SQLiteSession, max_items: int) -> None:
    if max_items <= 0:
        return
    try:
        items = await session.get_items()
    except Exception:
        return
    if len(items) <= max_items:
        return
    keep = items[-max_items:]

    for method_name in ("set_items", "replace_items"):
        method = getattr(session, method_name, None)
        if method:
            try:
                await method(keep)
                return
            except Exception as exc:
                logger.warning("Failed to %s on session: %s", method_name, exc)
                return

    for method_name in ("clear", "clear_items", "delete_items"):
        method = getattr(session, method_name, None)
        if method:
            try:
                await method()
                await session.add_items(keep)
            except Exception as exc:
                logger.warning("Failed to %s session: %s", method_name, exc)
            return


async def _reset_session(session: SQLiteSession) -> None:
    for method_name in ("clear", "clear_items", "delete_items"):
        method = getattr(session, method_name, None)
        if method:
            try:
                await method()
                return
            except Exception:
                pass
    for method_name in ("set_items", "replace_items"):
        method = getattr(session, method_name, None)
        if method:
            try:
                await method([])
                return
            except Exception:
                pass


async def _log_conversation(
    chat_id: str,
    payload: Dict[str, Any],
    user_message: str,
    bot_message: str,
    message_type: str,
) -> None:
    if not user_message and not bot_message:
        return
    phone = _normalize_phone(chat_id)
    user_name = _name_from_payload(payload)
    row = {
        "user_id": phone or chat_id,
        "bot_message": bot_message or None,
        "phone": phone or None,
        "user_name": user_name or None,
        "user_message": user_message or None,
        "conversation_id": chat_id,
        "message_type": message_type or None,
        "active": True,
        "app": SUPABASE_APP or None,
    }
    await _supabase_insert(row)


def _guess_audio_filename(payload: Dict[str, Any], media_url: Optional[str] = None) -> str:
    mimetype = _extract_mimetype(payload)
    if mimetype == "audio/ogg" or mimetype == "audio/opus":
        return "audio.ogg"
    if mimetype == "audio/mpeg":
        return "audio.mp3"
    if mimetype in ("audio/mp4", "audio/m4a"):
        return "audio.m4a"
    if media_url:
        path = urlparse(media_url).path.lower()
        if path.endswith(".oga"):
            return "audio.oga"
        if path.endswith(".ogg"):
            return "audio.ogg"
        if path.endswith(".mp3"):
            return "audio.mp3"
        if path.endswith(".m4a"):
            return "audio.m4a"
        if path.endswith(".wav"):
            return "audio.wav"
    return "audio"


def _should_convert_to_wav(payload: Dict[str, Any], media_url: Optional[str]) -> bool:
    mimetype = _extract_mimetype(payload)
    if mimetype in ("audio/ogg", "audio/opus"):
        return True
    if media_url:
        path = urlparse(media_url).path.lower()
        if path.endswith(".oga") or path.endswith(".ogg"):
            return True
    return False


def _convert_ogg_to_wav_bytes(audio_bytes: bytes, input_name: str) -> Optional[bytes]:
    if not audio_bytes:
        return None
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, input_name)
            output_path = os.path.join(tmpdir, "output.wav")
            with open(input_path, "wb") as handle:
                handle.write(audio_bytes)
            subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    input_path,
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    output_path,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            with open(output_path, "rb") as handle:
                return handle.read()
    except Exception as exc:
        logger.exception("Audio conversion failed: %s", exc)
        return None


async def _download_media(url: str) -> Optional[bytes]:
    if not url:
        return None
    headers = {}
    if WAHA_API_KEY and url.startswith(WAHA_BASE_URL):
        headers["X-Api-Key"] = WAHA_API_KEY
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code >= 400:
            logger.warning("Media download failed: %s %s", resp.status_code, resp.text)
            return None
        return resp.content


async def _transcribe_audio(url: str, payload: Dict[str, Any]) -> Optional[str]:
    audio_bytes = await _download_media(url)
    if not audio_bytes:
        return None

    filename = _guess_audio_filename(payload, url)
    if _should_convert_to_wav(payload, url):
        wav_bytes = await anyio.to_thread.run_sync(_convert_ogg_to_wav_bytes, audio_bytes, filename)
        if not wav_bytes:
            return None
        audio_bytes = wav_bytes
        filename = "audio.wav"

    def _call_openai() -> Optional[str]:
        kwargs: Dict[str, Any] = {
            "model": OPENAI_TRANSCRIBE_MODEL,
            "file": (filename, audio_bytes),
        }
        if OPENAI_TRANSCRIBE_LANGUAGE:
            kwargs["language"] = OPENAI_TRANSCRIBE_LANGUAGE
        result = OPENAI_CLIENT.audio.transcriptions.create(**kwargs)
        text = (getattr(result, "text", "") or "").strip()
        return text or None

    try:
        return await anyio.to_thread.run_sync(_call_openai)
    except Exception as exc:
        logger.exception("Audio transcription failed: %s", exc)
        return None


async def _get_contact_name(chat_id: str) -> Optional[str]:
    if not chat_id:
        return None
    headers = {"Content-Type": "application/json"}
    if WAHA_API_KEY:
        headers["X-Api-Key"] = WAHA_API_KEY

    params = {"contactId": chat_id, "session": WAHA_SESSION}
    url = f"{WAHA_BASE_URL}/api/contacts"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params, headers=headers)
        if resp.status_code >= 400:
            logger.warning("WAHA contacts failed: %s %s", resp.status_code, resp.text)
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        for key in ("pushname", "name", "shortName"):
            value = (data.get(key) or "").strip()
            if value:
                return value
    return None


def _waha_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if WAHA_API_KEY:
        headers["X-Api-Key"] = WAHA_API_KEY
    return headers


def _compact_http_error_text(text: str, limit: int = 280) -> str:
    cleaned = " ".join(str(text or "").split()).strip()
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit].rstrip()}..."


def _guess_waha_file_mimetype(filename: str) -> str:
    lowered = str(filename or "").strip().lower()
    if lowered.endswith(".ogg") or lowered.endswith(".oga") or lowered.endswith(".opus"):
        return "audio/ogg"
    if lowered.endswith(".mp3"):
        return "audio/mpeg"
    if lowered.endswith(".m4a") or lowered.endswith(".mp4"):
        return "audio/mp4"
    if lowered.endswith(".wav"):
        return "audio/wav"
    return "application/octet-stream"


def _extract_waha_message_id(data: Dict[str, Any]) -> str:
    message_id = (
        data.get("id")
        or (data.get("message") or {}).get("id")
        or (data.get("data") or {}).get("id")
        or ((data.get("message") or {}).get("key") or {}).get("id")
    )
    if message_id:
        _remember_recent_key(
            _RECENT_OUTBOUND_MESSAGE_IDS,
            str(message_id),
            OUTBOUND_ECHO_TTL_SECONDS,
        )
    return str(message_id or "")


async def _set_presence(chat_id: str, presence: str) -> None:
    if not chat_id or not presence or not WAHA_SESSION:
        return
    url = f"{WAHA_BASE_URL}/api/{quote(WAHA_SESSION, safe='')}/presence"
    payload = {"chatId": chat_id, "presence": presence}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload, headers=_waha_headers())
        if resp.status_code >= 400:
            logger.warning("WAHA presence failed: %s %s", resp.status_code, resp.text)
    except Exception as exc:
        logger.warning("WAHA presence request failed: %s", exc)


async def _show_recording_preview(chat_id: str) -> None:
    preview_seconds = _delay_seconds_from_ms(
        WAHA_RECORDING_PREVIEW_MS,
        default_ms=1400,
        min_ms=0,
        max_ms=8000,
    )
    if preview_seconds <= 0:
        return
    await _set_presence(chat_id, "recording")
    await anyio.sleep(preview_seconds)
    await _set_presence(chat_id, "paused")


async def _send_text(chat_id: str, text: str) -> str:
    if not chat_id:
        raise ValueError("chat_id is required")

    payload = {
        "chatId": chat_id,
        "text": text,
        "session": WAHA_SESSION,
    }

    url = f"{WAHA_BASE_URL}/api/sendText"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, json=payload, headers=_waha_headers())
        if resp.status_code >= 400:
            logger.error("WAHA sendText failed: %s %s", resp.status_code, resp.text)
            raise HTTPException(status_code=502, detail="WAHA sendText failed")
        try:
            data = resp.json()
        except Exception:
            data = {}
    message_id = _extract_waha_message_id(data)
    if message_id:
        _log_webhook_debug(
            "remember_outbound_message",
            {"chat_id": str(chat_id), "message_id": str(message_id)},
        )
    return message_id


async def _send_voice(chat_id: str, media_url: str) -> str:
    if not chat_id:
        raise ValueError("chat_id is required")
    if not media_url:
        raise ValueError("media_url is required")
    parsed_url = urlparse(media_url)
    filename = os.path.basename(parsed_url.path) or "audio.ogg"
    mimetype = _guess_waha_file_mimetype(filename)

    payload = {
        "chatId": chat_id,
        "session": WAHA_SESSION,
        "file": {
            "filename": filename,
            "mimetype": mimetype,
            "url": media_url,
        },
        "convert": WAHA_SENDVOICE_CONVERT,
    }

    await _show_recording_preview(chat_id)
    url = f"{WAHA_BASE_URL}/api/sendVoice"
    async with httpx.AsyncClient(timeout=40) as client:
        resp = await client.post(url, json=payload, headers=_waha_headers())
        if resp.status_code >= 400:
            logger.error("WAHA sendVoice failed: %s %s", resp.status_code, resp.text)
            error_text = _compact_http_error_text(resp.text)
            detail = f"WAHA sendVoice failed ({resp.status_code})"
            if error_text:
                detail = f"{detail}: {error_text}"
            raise RuntimeError(detail)
        try:
            data = resp.json()
        except Exception:
            data = {}
    message_id = _extract_waha_message_id(data)
    if message_id:
        _log_webhook_debug(
            "remember_outbound_voice_message",
            {"chat_id": str(chat_id), "message_id": str(message_id)},
        )
    _remember_recent_audio_sent(chat_id)
    return message_id


async def _send_poll(chat_id: str, question: str, options: list[str]) -> Optional[str]:
    if not chat_id:
        raise ValueError("chat_id is required")
    if not options:
        raise ValueError("poll options are required")

    payload = {
        "chatId": chat_id,
        "session": WAHA_SESSION,
        "poll": {
            "name": question,
            "options": options,
            "multipleAnswers": False,
        },
    }

    url = f"{WAHA_BASE_URL}/api/sendPoll"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, json=payload, headers=_waha_headers())
        if resp.status_code >= 400:
            logger.error("WAHA sendPoll failed: %s %s", resp.status_code, resp.text)
            return None
        try:
            data = resp.json()
        except Exception:
            return None
    return (
        data.get("id")
        or (data.get("poll") or {}).get("id")
        or (data.get("message") or {}).get("id")
        or (data.get("data") or {}).get("id")
    )


async def _send_profile_poll(chat_id: str) -> Optional[str]:
    if not PROFILE_OPTIONS:
        logger.warning("Profile poll requested but PROFILE_OPTIONS is empty.")
        return None
    return await _send_poll(chat_id, PROFILE_POLL_NAME, PROFILE_OPTIONS)


def _clamp_int(raw_value: str, *, default: int, min_value: int, max_value: int) -> int:
    parsed = _parse_int(raw_value)
    value = parsed if parsed is not None else default
    return max(min_value, min(value, max_value))


def _delay_seconds_from_ms(
    raw_value: str,
    *,
    default_ms: int,
    min_ms: int = 0,
    max_ms: int = 5000,
) -> float:
    ms = _clamp_int(raw_value, default=default_ms, min_value=min_ms, max_value=max_ms)
    return ms / 1000.0


def _split_long_chunk(text: str, max_chars: int) -> list[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    if len(cleaned) <= max_chars:
        return [cleaned]

    chunks: list[str] = []
    remaining = cleaned
    while len(remaining) > max_chars:
        window = remaining[:max_chars]
        line_break_at = window.rfind("\n")
        split_at = line_break_at
        if split_at < int(max_chars * 0.45):
            split_at = window.rfind(" ")
        if split_at <= 0:
            split_at = max_chars

        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


def _normalize_whatsapp_part(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""
    # Keep punctuation inside the message, but avoid forced final period style.
    if cleaned.endswith(".") and not cleaned.endswith("..."):
        last_token = cleaned.split()[-1].lower() if cleaned.split() else ""
        abbreviations = {"dr.", "dra.", "sr.", "sra.", "etc.", "obs.", "prof.", "profa."}
        if last_token not in abbreviations:
            cleaned = cleaned[:-1].rstrip()
    return cleaned


def _looks_like_presentation_message(text: str, profile_id: Optional[str] = None) -> bool:
    cleaned = (text or "").strip()
    if not cleaned:
        return False
    lowered = _normalize_text(cleaned)
    if cleaned.count("\n✅ ") >= 2:
        return True
    if not _is_ariane_profile(profile_id):
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


def _is_emoji_or_punctuation_only(text: str) -> bool:
    cleaned = (text or "").strip()
    if not cleaned:
        return False
    if any(ch.isalnum() for ch in cleaned):
        return False
    return len(cleaned) <= 8


def _merge_short_whatsapp_parts(parts: list[str], target_chars: int) -> list[str]:
    if not parts:
        return []
    merged: list[str] = []
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
        emoji_or_punct_only = _is_emoji_or_punctuation_only(text)
        same_message_budget = len(prev) + len(text) + 2 <= target_chars
        is_question = text.endswith("?")

        if same_message_budget and (is_short or emoji_or_punct_only) and not is_question:
            joiner = " " if emoji_or_punct_only else "\n"
            merged[-1] = f"{prev}{joiner}{text}".strip()
        else:
            merged.append(text)

    return merged


def _split_messages(text: str, profile_id: Optional[str] = None) -> list[str]:
    if not text:
        return []
    cleaned = text.replace("\r\n", "\n").strip()
    if not cleaned:
        return []

    target_chars = _clamp_int(
        MESSAGE_SPLIT_TARGET_CHARS,
        default=420,
        min_value=120,
        max_value=1800,
    )
    max_chars = _clamp_int(
        MESSAGE_SPLIT_MAX_CHARS,
        default=720,
        min_value=target_chars,
        max_value=3000,
    )
    presentation_max_chars = _clamp_int(
        MESSAGE_SPLIT_PRESENTATION_MAX_CHARS,
        default=max_chars,
        min_value=max_chars,
        max_value=4000,
    )
    if _looks_like_presentation_message(cleaned, profile_id):
        max_chars = presentation_max_chars
        target_chars = max(target_chars, presentation_max_chars)
    # New message only for a new paragraph (blank line) or for oversized text blocks.
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", cleaned) if part and part.strip()]
    if not paragraphs:
        paragraphs = [cleaned]

    parts: list[str] = []
    for paragraph in paragraphs:
        if len(paragraph) > max_chars:
            parts.extend(_split_long_chunk(paragraph, max_chars))
        else:
            parts.append(paragraph)

    normalized = [_normalize_whatsapp_part(part) for part in parts]
    filtered = [part for part in normalized if part]
    merged = _merge_short_whatsapp_parts(filtered, target_chars)
    return [part for part in merged if part]


def _message_delay_seconds(profile_id: Optional[str] = None) -> float:
    raw = ARIANE_MESSAGE_DELAY_MS if _is_ariane_profile(profile_id) else MESSAGE_DELAY_MS
    return _delay_seconds_from_ms(raw, default_ms=350, min_ms=0, max_ms=60000)


def _schedule_delay_seconds(profile_id: Optional[str] = None) -> float:
    raw = ARIANE_SCHEDULE_DELAY_MS if _is_ariane_profile(profile_id) else SCHEDULE_DELAY_MS
    return _delay_seconds_from_ms(raw, default_ms=350, min_ms=0, max_ms=60000)


def _first_message_delay_seconds(profile_id: Optional[str] = None) -> float:
    raw = (
        ARIANE_FIRST_MESSAGE_DELAY_MS
        if _is_ariane_profile(profile_id)
        else FIRST_MESSAGE_DELAY_MS
    )
    return _delay_seconds_from_ms(raw, default_ms=180, min_ms=0, max_ms=60000)


def _coalesce_delay_seconds() -> float:
    return _delay_seconds_from_ms(
        USER_MESSAGE_COALESCE_MS,
        default_ms=800,
        min_ms=0,
        max_ms=5000,
    )


def _coalesce_max_wait_seconds() -> float:
    window = _coalesce_delay_seconds()
    max_wait = _delay_seconds_from_ms(
        USER_MESSAGE_COALESCE_MAX_MS,
        default_ms=2500,
        min_ms=0,
        max_ms=12000,
    )
    if window > 0 and max_wait > 0 and max_wait < window:
        return window
    return max_wait


def _get_pending_lock(chat_id: str) -> Any:
    lock = _PENDING_USER_LOCKS.get(chat_id)
    if lock is None:
        lock = anyio.Lock()
        _PENDING_USER_LOCKS[chat_id] = lock
    return lock


async def _coalesce_user_message(
    chat_id: str, text: str, is_audio: bool
) -> Optional[tuple[str, bool]]:
    window = _coalesce_delay_seconds()
    if window <= 0 or not chat_id:
        return (text, is_audio)
    max_wait = _coalesce_max_wait_seconds()
    if max_wait <= 0:
        max_wait = window

    lock = _get_pending_lock(chat_id)
    now = time.time()
    async with lock:
        state = _PENDING_USER_MESSAGES.get(chat_id)
        if not state:
            state = {
                "messages": [],
                "last": now,
                "first": now,
                "collector": False,
                "has_audio": False,
            }
            _PENDING_USER_MESSAGES[chat_id] = state
        state["messages"].append(text)
        state["last"] = now
        if is_audio:
            state["has_audio"] = True
        if state.get("collector"):
            return None
        state["collector"] = True

    while True:
        await anyio.sleep(window)
        async with lock:
            state = _PENDING_USER_MESSAGES.get(chat_id)
            if not state:
                return (text, is_audio)
            elapsed = time.time() - state.get("last", now)
            total = time.time() - state.get("first", now)
            if elapsed >= window or total >= max_wait:
                messages = state.get("messages") or []
                has_audio = bool(state.get("has_audio"))
                _PENDING_USER_MESSAGES.pop(chat_id, None)
                combined = "\n".join(
                    message.strip() for message in messages if message and message.strip()
                )
                return (combined or text, has_audio)


def _coerce_session_item_content(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            text = _coerce_session_item_content(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    if isinstance(value, dict):
        for key in ("text", "content", "value"):
            text = _coerce_session_item_content(value.get(key))
            if text:
                return text
        return ""
    try:
        return str(value)
    except Exception:
        return ""


def _normalize_text(text: Any) -> str:
    text = _coerce_session_item_content(text)
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text.lower())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _normalize_service_text(text: str) -> str:
    normalized = _normalize_text(text)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _contains_normalized_term(text: str, term: str) -> bool:
    haystack = _normalize_service_text(text)
    needle = _normalize_service_text(term)
    if not haystack or not needle:
        return False
    if haystack == needle:
        return True
    return re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack) is not None


def _short_hash(value: str) -> str:
    if not value:
        return ""
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def _log_webhook_debug(stage: str, data: Dict[str, Any]) -> None:
    if not LOG_WEBHOOK_DEBUG:
        return
    logger.info("WebhookDebug %s: %s", stage, data)


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "y", "sim", "s"):
            return True
        if lowered in ("false", "0", "no", "n", "nao", "não"):
            return False
    return None


def _extract_message_id_value(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        for key in ("_serialized", "serialized", "id", "messageId", "msgId", "_id"):
            value = raw.get(key)
            extracted = _extract_message_id_value(value)
            if extracted:
                return extracted
        return None
    value = str(raw).strip()
    return value or None


def _is_from_me_payload(payload: Dict[str, Any]) -> bool:
    if not payload:
        return False
    for container in (
        payload,
        payload.get("message"),
        payload.get("msg"),
        payload.get("key"),
        payload.get("_data"),
        payload.get("data"),
    ):
        if not isinstance(container, dict):
            continue
        for key in ("fromMe", "from_me", "isFromMe", "is_from_me"):
            value = _coerce_bool(container.get(key))
            if value is not None:
                return value
        key_obj = container.get("key")
        if isinstance(key_obj, dict):
            for key in ("fromMe", "from_me", "isFromMe", "is_from_me"):
                value = _coerce_bool(key_obj.get(key))
                if value is not None:
                    return value
    return False


def _extract_message_id(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    def _from_obj(obj: Any) -> Optional[str]:
        if not isinstance(obj, dict):
            return None
        for key in ("id", "messageId", "msgId", "message_id", "msg_id"):
            value = _extract_message_id_value(obj.get(key))
            if value:
                return value
        key_obj = obj.get("key")
        if isinstance(key_obj, dict):
            for key in ("id", "messageId", "msgId", "message_id", "msg_id"):
                value = _extract_message_id_value(key_obj.get(key))
                if value:
                    return value
        return None

    for container in (
        payload,
        payload.get("message"),
        payload.get("msg"),
        payload.get("_data"),
        payload.get("data"),
    ):
        value = _from_obj(container)
        if value:
            return value
    return None


def _extract_event_id(data: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    for key in ("id", "eventId", "event_id", "event.id"):
        value = data.get(key)
        if value:
            return str(value)
    if isinstance(payload, dict):
        value = payload.get("id")
        if value:
            return str(value)
    return None


def _extract_timestamp(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    for key in ("timestamp", "ts", "t"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    data_obj = payload.get("_data") or payload.get("data") or {}
    if isinstance(data_obj, dict):
        for key in ("timestamp", "ts", "t"):
            value = data_obj.get(key)
            if value is not None:
                return str(value)
    return None


def _message_fingerprint(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    chat_id = payload.get("from") or payload.get("chatId") or payload.get("to")
    msg_id = _extract_message_id(payload)
    if chat_id and msg_id:
        return f"{chat_id}:{msg_id}"
    timestamp = _extract_timestamp(payload)
    body = (payload.get("body") or payload.get("text") or "").strip()
    if chat_id and timestamp and body:
        return f"{chat_id}:{timestamp}:{body}"
    if chat_id and body:
        digest = hashlib.sha1(body.encode("utf-8")).hexdigest()
        return f"{chat_id}:{digest}"
    return None


def _is_greeting_only(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text)
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


def _store_schedule_options(chat_id: str, options: list[str]) -> None:
    if not chat_id or not options:
        return
    _LAST_SCHEDULE_OPTIONS[chat_id] = {"options": options, "ts": time.time()}


def _get_schedule_options(chat_id: str) -> list[str]:
    entry = _LAST_SCHEDULE_OPTIONS.get(chat_id)
    if not entry:
        return []
    if time.time() - entry.get("ts", 0) > 6 * 60 * 60:
        _LAST_SCHEDULE_OPTIONS.pop(chat_id, None)
        return []
    return list(entry.get("options") or [])


def _set_pending_signal_booking(chat_id: str, option: str, profile_id: str) -> None:
    if not chat_id:
        return
    _update_profile_state(
        chat_id,
        flow_state="awaiting_deposit_proof",
        flow_data={
            "pending_slot": option,
            "pending_since": int(time.time()),
            "profile_id": profile_id,
        },
    )


def _peek_pending_signal_booking(chat_id: str) -> Optional[Dict[str, str]]:
    if not chat_id:
        return None
    state = _get_profile_state(chat_id)
    if (state.get("flow_state") or "") != "awaiting_deposit_proof":
        return None
    flow_data = state.get("flow_data") if isinstance(state.get("flow_data"), dict) else {}
    option = str(flow_data.get("pending_slot") or "").strip()
    if not option:
        _update_profile_state(chat_id, flow_state=None, flow_data={})
        return None
    pending_since = int(flow_data.get("pending_since") or 0)
    if pending_since and (time.time() - pending_since > PENDING_SIGNAL_TTL_SECONDS):
        _update_profile_state(chat_id, flow_state=None, flow_data={})
        return None
    profile_id = str(flow_data.get("profile_id") or state.get("profile_id") or "").strip()
    return {"option": option, "profile_id": profile_id}


def _consume_pending_signal_booking(chat_id: str) -> Optional[Dict[str, str]]:
    entry = _peek_pending_signal_booking(chat_id)
    if entry is None:
        return None
    _update_profile_state(chat_id, flow_state=None, flow_data={})
    return entry


def _looks_like_payment_confirmation(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text)
    return any(
        token in lowered
        for token in (
            "comprovante",
            "pix pago",
            "pix feito",
            "pix enviado",
            "paguei",
            "pagamento",
            "transferi",
            "ja enviei",
            "ja mandei",
            "acabei de enviar",
        )
    )


def _is_ariane_profile(profile_id: Optional[str], chat_id: str = "") -> bool:
    def _matches_ariane_alias(value: str) -> bool:
        normalized = _normalize_text(value)
        if not normalized:
            return False
        if normalized == "ariane" or "ariane" in normalized:
            return True
        return normalized in ("estetica capilar", "estetica e terapia capilar")

    if _matches_ariane_alias(profile_id or ""):
        return True
    if profile_id:
        return False
    if chat_id:
        resolved = _resolve_profile_for_chat(chat_id) or ""
        if _matches_ariane_alias(resolved):
            return True
        if resolved:
            return False
    if _matches_ariane_alias(PROMPT_PROFILE or ""):
        return True
    return False


def _is_ariane_context_from_items(items: list[dict[str, Any]], user_text: str = "") -> bool:
    if not items and not user_text:
        return False
    chunks: list[str] = []
    for item in items[-16:]:
        if item.get("role") not in ("assistant", "user"):
            continue
        content = _coerce_session_item_content(item.get("content")).strip()
        if content:
            chunks.append(content)
    if user_text:
        chunks.append(user_text)
    corpus = _normalize_text("\n".join(chunks))
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


def _extract_day_time(text: str) -> Optional[str]:
    if not text:
        return None
    lowered = _normalize_text(text)
    day_tokens = {
        "segunda": "Segunda",
        "terca": "Terca",
        "quarta": "Quarta",
        "quinta": "Quinta",
        "sexta": "Sexta",
        "sabado": "Sabado",
        "domingo": "Domingo",
    }
    day = None
    for token, label in day_tokens.items():
        if token in lowered:
            day = label
            break
    if not day:
        return None
    # Match time like 10, 10h, 10:00, 10h30
    time_match = None
    for candidate in lowered.split():
        cleaned = candidate.strip(".,;!?")
        if cleaned.endswith("h") and cleaned[:-1].isdigit():
            time_match = f"{cleaned[:-1]}:00"
            break
        if cleaned.count(":") == 1 and cleaned.replace(":", "").isdigit():
            hours, minutes = cleaned.split(":")
            if len(minutes) == 1:
                minutes = f"{minutes}0"
            time_match = f"{hours}:{minutes}"
            break
        if "h" in cleaned and cleaned.replace("h", "").isdigit():
            hours, minutes = cleaned.split("h", 1)
            if not minutes:
                minutes = "00"
            elif len(minutes) == 1:
                minutes = f"{minutes}0"
            time_match = f"{hours}:{minutes}"
            break
    if not time_match:
        for token in lowered.split():
            cleaned = token.strip(".,;!?")
            if cleaned.isdigit():
                time_match = f"{cleaned}:00"
                break
    if not time_match:
        return None
    return f"{day} {time_match}"


def _try_match_schedule_option(chat_id: str, text: str) -> Optional[str]:
    options = _get_schedule_options(chat_id)
    if not options:
        return None
    normalized = _normalize_text(text)
    for option in options:
        if _normalize_text(option) in normalized:
            return option
    extracted = _extract_day_time(text)
    if extracted:
        for option in options:
            if _normalize_text(option) == _normalize_text(extracted):
                return option
    for weekday in ("segunda", "terca", "quarta", "quinta", "sexta", "sabado", "domingo"):
        if weekday not in normalized:
            continue
        matches = [option for option in options if weekday in _normalize_text(option)]
        if len(matches) == 1:
            return matches[0]
    return None


def _build_schedule_confirmation(
    option: str,
    user_text: str,
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> str:
    flow = _get_booking_flow(profile_id, chat_id, force_ariane=force_ariane)
    if flow is not None:
        return build_prebooking_message(flow, option)

    response = f"Perfeito! Vou reservar para {option}."
    lowered = _normalize_text(user_text)
    if any(token in lowered for token in ("demora", "quanto tempo", "tempo leva", "demor", "duracao")):
        response += " Sobre o tempo, isso pode variar conforme o caso e explicamos melhor na avaliação."
    return response


def _build_signal_received_confirmation(
    option: Optional[str],
    profile_id: Optional[str],
    chat_id: str = "",
    force_ariane: bool = False,
) -> str:
    flow = _get_booking_flow(profile_id, chat_id, force_ariane=force_ariane)
    if flow is not None and flow.requires_deposit:
        return build_proof_received_message(flow, option or "")
    if option:
        return f"Perfeito! Horario confirmado na agenda ({option})."
    return "Perfeito! Horario confirmado na agenda."


async def _run_agent(
    agent: Agent,
    input_text: str,
    session: SQLiteSession,
    chat_id: str,
    profile_id: Optional[str] = None,
) -> Any:
    chat_token = _CURRENT_CHAT_ID.set(chat_id)
    profile_token = _CURRENT_PROFILE_ID.set(profile_id or "")
    try:
        return await Runner.run(agent, input=input_text, session=session)
    finally:
        _CURRENT_PROFILE_ID.reset(profile_token)
        _CURRENT_CHAT_ID.reset(chat_token)


def _wants_profile_switch(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text)
    if any(token in lowered for token in ("trocar", "mudar", "alterar", "testar", "outro", "outra")):
        if any(token in lowered for token in ("assistente", "clinica", "perfil", "setor", "atendimento")):
            return True
    return False


def _resolve_profile_id_from_option(option: str) -> Optional[str]:
    if not option:
        return None
    raw = option.strip()
    if not raw:
        return None
    normalized = _normalize_text(raw)
    for profile in _PROFILE_LIST:
        profile_id = (profile.get("id") or "").strip()
        if profile_id and _normalize_text(profile_id) == normalized:
            return profile_id
        label = _normalize_text(profile.get("label") or "")
        if label and label == normalized:
            return profile.get("id")
        if label and normalized and label in normalized:
            return profile.get("id")
    lowered = raw.lower()
    if lowered in PROFILE_LABEL_TO_ID:
        return PROFILE_LABEL_TO_ID.get(lowered)
    return None


def _normalize_selected_options(raw: Any) -> list[str]:
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


def _resolve_profile_id_from_vote(selected_options: Any) -> Optional[str]:
    options = _normalize_selected_options(selected_options)
    if not options:
        return None
    option = options[0].strip()
    resolved_option = _resolve_profile_id_from_option(option)
    if resolved_option:
        return resolved_option
    if option.isdigit():
        idx = int(option)
        if 1 <= idx <= len(PROFILE_OPTIONS):
            return _resolve_profile_id_from_option(PROFILE_OPTIONS[idx - 1])
        if idx == 0 and PROFILE_OPTIONS:
            return _resolve_profile_id_from_option(PROFILE_OPTIONS[0])
    if option in PROFILES:
        return option
    return None


def _is_schedule_check_message(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text)
    return any(
        phrase in lowered
        for phrase in (
            "vou verificar os horários",
            "vou ver os horários",
            "vou consultar a agenda",
            "consultar a agenda",
            "um momento, por favor",
            "so um instante",
        )
    )


def _parse_schedule_preference(text: str) -> Optional[str]:
    if not text:
        return None
    lowered = _normalize_text(text)
    if "manha" in lowered:
        return "morning"
    if "tarde" in lowered:
        return "afternoon"
    if "noite" in lowered:
        return "evening"
    return None


def _weekday_pt_br(value: date) -> str:
    names = [
        "Segunda",
        "Terca",
        "Quarta",
        "Quinta",
        "Sexta",
        "Sabado",
        "Domingo",
    ]
    return names[value.weekday()]


def _next_business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    cursor = start
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _fake_schedule_options(preference: Optional[str]) -> list[str]:
    start = date.today() + timedelta(days=1)
    days = _next_business_days(start, 3)
    if preference == "morning":
        times = ["09:30", "10:30", "11:30"]
    elif preference == "afternoon":
        times = ["14:00", "15:30", "17:00"]
    elif preference == "evening":
        times = ["18:00", "18:30", "19:00"]
    else:
        times = ["10:00", "15:00", "17:30"]
    return [f"{_weekday_pt_br(day)} {time}" for day, time in zip(days, times)]


def _should_inject_fake_schedule(reply: str) -> bool:
    if not reply:
        return False
    lowered = _normalize_text(reply)
    if any(
        phrase in lowered
        for phrase in (
            "preciso de algumas informacoes",
            "qual e o seu nome",
            "qual o seu nome",
            "nome completo",
            "motivo principal",
            "preferencia de horario",
            "assim que eu tiver",
            "assim que tiver",
            "poderei verificar os horarios",
            "posso verificar os horarios",
            "depois que eu tiver",
        )
    ):
        return False
    if any(
        token in lowered
        for token in (
            "tenho estes horarios",
            "tenho esses horarios",
            "tenho os horarios",
        )
    ):
        return False
    if _reply_contains_schedule_options(reply):
        return False
    if any(
        token in lowered
        for token in (
            "horarios disponiveis",
            "consultar a agenda",
            "vou verificar os horarios",
            "vou ver os horarios",
            "um momento, por favor",
            "so um instante",
        )
    ):
        return True
    return False


def _reply_contains_schedule_options(reply: str) -> bool:
    if not reply:
        return False
    lowered = _normalize_text(reply)
    # Day names in Portuguese
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
        # Only consider it schedule-like if there's also a time-ish pattern
        if re.search(r"\b\d{1,2}(?::\d{2}|h\d{0,2})\b", lowered):
            return True
    # Generic time patterns (10h, 10:00)
    if re.search(r"\b\d{1,2}h(\d{2})?\b", lowered):
        return True
    if re.search(r"\b\d{1,2}:\d{2}\b", lowered):
        return True
    return False


def _inject_fake_schedule(chat_id: str, body: str, reply: str) -> str:
    if _SCHEDULING_TOOL is not None:
        return reply
    if not _should_inject_fake_schedule(reply):
        return reply
    preference = _parse_schedule_preference(body)
    options = _fake_schedule_options(preference)
    _store_schedule_options(chat_id, options)
    horarios = ", ".join(options)
    suggestion = f"Tenho estes horarios disponiveis nesta semana: {horarios}. Qual prefere?"
    return f"{reply}\n\n{suggestion}"


def _storage_list_prefix_sync(bucket: str, prefix: str = "") -> tuple[list[Dict[str, Any]], Optional[str]]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket:
        return ([], "supabase_not_configured")
    url = f"{SUPABASE_URL}/storage/v1/object/list/{quote(bucket, safe='')}"
    payload = {
        "prefix": prefix,
        "limit": 100,
        "offset": 0,
        "sortBy": {"column": "name", "order": "asc"},
    }
    try:
        with httpx.Client(timeout=20) as client:
            resp = client.post(url, json=payload, headers=_supabase_storage_headers())
        resp.raise_for_status()
        raw_items = resp.json()
    except Exception as exc:
        return ([], str(exc))
    if not isinstance(raw_items, list):
        return ([], "invalid_storage_list_response")
    return (raw_items, None)


def _build_audio_file_item(name: str) -> Optional[Dict[str, str]]:
    clean_name = str(name or "").strip().strip("/")
    if not clean_name or not clean_name.lower().endswith(".ogg"):
        return None
    stem = clean_name.rsplit(".", 1)[0].strip()
    if not stem:
        return None
    return {
        "name": clean_name,
        "stem": stem,
        "normalized_stem": _normalize_service_text(stem),
    }


def _list_bucket_audio_files_sync_detailed(bucket: str) -> Dict[str, Any]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket:
        return {"files": [], "error": "supabase_not_configured"}

    now = time.time()
    cache_key = f"bucket:{bucket}"
    cached_bucket = _AUDIO_FILE_CACHE.get("bucket")
    cached_files = _AUDIO_FILE_CACHE.get("files") or []
    if (
        cached_bucket == cache_key
        and now < float(_AUDIO_FILE_CACHE.get("expires_at") or 0)
        and isinstance(cached_files, list)
    ):
        return {"files": list(cached_files), "error": None}

    items: list[Dict[str, str]] = []
    seen_files: set[str] = set()
    visited_prefixes: set[str] = set()
    pending_prefixes: list[str] = [""]
    last_error: Optional[str] = None

    while pending_prefixes:
        prefix = pending_prefixes.pop(0)
        if prefix in visited_prefixes:
            continue
        visited_prefixes.add(prefix)
        raw_items, error = _storage_list_prefix_sync(bucket, prefix)
        if error:
            last_error = error
            continue
        for raw in raw_items or []:
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("name") or "").strip().strip("/")
            if not name:
                continue
            full_name = f"{prefix}/{name}".strip("/") if prefix else name
            built_item = _build_audio_file_item(full_name)
            if built_item is not None:
                file_name = built_item["name"]
                if file_name not in seen_files:
                    items.append(built_item)
                    seen_files.add(file_name)
                continue
            is_folder = not raw.get("id") and not raw.get("metadata")
            if is_folder:
                child_prefix = full_name
                if child_prefix and child_prefix not in visited_prefixes:
                    pending_prefixes.append(child_prefix)

    _AUDIO_FILE_CACHE["bucket"] = cache_key
    _AUDIO_FILE_CACHE["files"] = list(items)
    _AUDIO_FILE_CACHE["expires_at"] = now + max(CRIOLASER_AUDIO_CACHE_TTL_SECONDS, 30)
    return {"files": items, "error": last_error}


def _list_bucket_audio_files_sync(bucket: str) -> list[Dict[str, str]]:
    result = _list_bucket_audio_files_sync_detailed(bucket)
    if result.get("error"):
        logger.warning("Supabase storage list sync failed bucket=%s: %s", bucket, result["error"])
    return list(result.get("files") or [])


def _build_bucket_audio_url_sync(bucket: str, file_name: str) -> Optional[str]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket or not file_name:
        return None
    encoded_path = quote(file_name, safe="/")
    if CRIOLASER_AUDIO_PUBLIC_BUCKET:
        return f"{SUPABASE_URL}/storage/v1/object/public/{quote(bucket, safe='')}/{encoded_path}"

    url = f"{SUPABASE_URL}/storage/v1/object/sign/{quote(bucket, safe='')}/{encoded_path}"
    payload = {"expiresIn": max(CRIOLASER_AUDIO_SIGN_TTL, 60)}
    try:
        with httpx.Client(timeout=20) as client:
            resp = client.post(url, json=payload, headers=_supabase_storage_headers())
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Supabase signed URL sync failed: %s", exc)
        return None

    signed_url = str(data.get("signedURL") or data.get("signedUrl") or "").strip()
    if not signed_url:
        return None
    if signed_url.startswith("http://") or signed_url.startswith("https://"):
        return signed_url
    if signed_url.startswith("/storage/v1/"):
        return f"{SUPABASE_URL}{signed_url}"
    if signed_url.startswith("/"):
        return f"{SUPABASE_URL}/storage/v1{signed_url}"
    return f"{SUPABASE_URL}/storage/v1/{signed_url.lstrip('/')}"


def _set_presence_sync(chat_id: str, presence: str) -> None:
    if not chat_id or not presence or not WAHA_SESSION:
        return
    url = f"{WAHA_BASE_URL}/api/{quote(WAHA_SESSION, safe='')}/presence"
    payload = {"chatId": chat_id, "presence": presence}
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json=payload, headers=_waha_headers())
        if resp.status_code >= 400:
            logger.warning("WAHA presence sync failed: %s %s", resp.status_code, resp.text)
    except Exception as exc:
        logger.warning("WAHA presence sync request failed: %s", exc)


def _show_recording_preview_sync(chat_id: str) -> None:
    preview_seconds = _delay_seconds_from_ms(
        WAHA_RECORDING_PREVIEW_MS,
        default_ms=1400,
        min_ms=0,
        max_ms=8000,
    )
    if preview_seconds <= 0:
        return
    _set_presence_sync(chat_id, "recording")
    time.sleep(preview_seconds)
    _set_presence_sync(chat_id, "paused")


def _send_voice_sync(chat_id: str, media_url: str) -> str:
    if not chat_id:
        raise ValueError("chat_id is required")
    if not media_url:
        raise ValueError("media_url is required")
    parsed_url = urlparse(media_url)
    filename = os.path.basename(parsed_url.path) or "audio.ogg"
    mimetype = _guess_waha_file_mimetype(filename)
    payload = {
        "chatId": chat_id,
        "session": WAHA_SESSION,
        "file": {
            "filename": filename,
            "mimetype": mimetype,
            "url": media_url,
        },
        "convert": WAHA_SENDVOICE_CONVERT,
    }
    _show_recording_preview_sync(chat_id)
    url = f"{WAHA_BASE_URL}/api/sendVoice"
    with httpx.Client(timeout=40) as client:
        resp = client.post(url, json=payload, headers=_waha_headers())
    if resp.status_code >= 400:
        logger.error("WAHA sendVoice sync failed: %s %s", resp.status_code, resp.text)
        error_text = _compact_http_error_text(resp.text)
        detail = f"WAHA sendVoice failed ({resp.status_code})"
        if error_text:
            detail = f"{detail}: {error_text}"
        raise RuntimeError(detail)
    try:
        data = resp.json()
    except Exception:
        data = {}
    message_id = _extract_waha_message_id(data)
    _remember_recent_audio_sent(chat_id)
    return message_id


def _match_profile_audio_files(
    profile_id: str,
    query: str,
    *,
    top_k: int = 3,
) -> Dict[str, Any]:
    bucket = _get_audio_bucket_for_profile(profile_id)
    if not bucket:
        return {
            "status": "unavailable",
            "profile_id": profile_id,
            "query": query,
            "message": "audio_bucket_not_configured",
        }
    detailed = _list_bucket_audio_files_sync_detailed(bucket)
    files = list(detailed.get("files") or [])
    if detailed.get("error"):
        return {
            "status": "error",
            "profile_id": profile_id,
            "bucket": bucket,
            "query": query,
            "message": "audio_bucket_list_failed",
            "error": detailed.get("error"),
            "available_files": files,
            "matches": [],
        }
    matches = _match_audio_files(query, files, limit=max(1, min(top_k, 5)))
    return {
        "status": "ok",
        "profile_id": profile_id,
        "bucket": bucket,
        "query": query,
        "available_files": [str(item.get("name") or "") for item in files],
        "matches": matches,
    }


def _build_scheduling_tool() -> Optional[Any]:
    if function_tool is None:
        return None

    @function_tool
    def tool_agente_scheduling(
        task: str,
        preference: str = "",
        day: str = "",
        timezone: str = "",
        phone: str = "",
        notes: str = "",
        option: str = "",
    ) -> Dict[str, Any]:
        """
        Tool para sugerir horarios e confirmar o horario escolhido.
        task: get_horarios | make_call_meeting
        """
        normalized_task = (task or "").strip().lower()
        if normalized_task in ("get_horarios", "get_slots", "get_availability"):
            preference_text = preference or day or notes
            pref = _parse_schedule_preference(preference_text or "")
            options = _fake_schedule_options(pref)
            chat_id = _CURRENT_CHAT_ID.get("")
            if chat_id:
                _store_schedule_options(chat_id, options)
            return {
                "status": "ok",
                "task": "get_horarios",
                "options": options,
                "timezone": timezone or "",
            }
        if normalized_task in ("make_call_meeting", "create_booking", "book", "reserve"):
            chosen = option or day
            return {
                "status": "confirmed",
                "task": "make_call_meeting",
                "option": chosen,
                "phone": phone,
                "notes": notes,
            }
        return {"status": "error", "message": f"Unknown task: {task}"}

    return tool_agente_scheduling


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        try:
            data = dump()
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            data = to_dict()
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    raw = getattr(value, "__dict__", None)
    if isinstance(raw, dict):
        return dict(raw)
    return {}


def _extract_vector_search_items(payload: Any) -> list[Any]:
    if isinstance(payload, dict):
        data = payload.get("data")
        return data if isinstance(data, list) else []
    data = getattr(payload, "data", None)
    if isinstance(data, list):
        return data
    dumped = _as_dict(payload)
    data = dumped.get("data")
    return data if isinstance(data, list) else []


def _extract_vector_result_text(result: Any) -> str:
    result_dict = _as_dict(result)
    content = result_dict.get("content")
    parts: list[str] = []
    if isinstance(content, list):
        for entry in content:
            if isinstance(entry, str):
                text = entry.strip()
            else:
                entry_dict = _as_dict(entry)
                text = str(entry_dict.get("text") or entry_dict.get("content") or entry_dict.get("value") or "").strip()
            if text:
                parts.append(text)
    elif isinstance(content, str):
        text = content.strip()
        if text:
            parts.append(text)

    if not parts:
        for key in ("text", "snippet", "chunk"):
            value = result_dict.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
                break
    return "\n".join(parts).strip()


def _vector_result_source(result: Dict[str, Any], vector_store_id: str) -> str:
    filename = str(result.get("filename") or result.get("file_name") or "").strip()
    file_id = str(result.get("file_id") or "").strip()
    if filename and file_id:
        return f"{filename} ({file_id})"
    if filename:
        return filename
    if file_id:
        return file_id
    return f"vector_store:{vector_store_id}"


def _search_vector_store_sdk(vector_store_id: str, query: str, max_num_results: int) -> Any:
    search_functions: list[Any] = []
    vector_stores_api = getattr(OPENAI_CLIENT, "vector_stores", None)
    if vector_stores_api is not None and hasattr(vector_stores_api, "search"):
        search_functions.append(getattr(vector_stores_api, "search"))
    beta_api = getattr(OPENAI_CLIENT, "beta", None)
    beta_vector_stores_api = getattr(beta_api, "vector_stores", None) if beta_api is not None else None
    if beta_vector_stores_api is not None and hasattr(beta_vector_stores_api, "search"):
        search_functions.append(getattr(beta_vector_stores_api, "search"))
    if not search_functions:
        raise RuntimeError("vector_store_search_not_supported_by_sdk")

    last_error: Optional[Exception] = None
    for search_fn in search_functions:
        try:
            return search_fn(
                vector_store_id=vector_store_id,
                query=query,
                max_num_results=max_num_results,
            )
        except TypeError:
            try:
                return search_fn(
                    vector_store_id,
                    query=query,
                    max_num_results=max_num_results,
                )
            except Exception as exc:
                last_error = exc
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("vector_store_search_failed")


_KNOWLEDGE_QUERY_STOPWORDS = {
    "a",
    "ao",
    "aos",
    "as",
    "biovita",
    "com",
    "como",
    "da",
    "das",
    "de",
    "do",
    "dos",
    "e",
    "em",
    "na",
    "nas",
    "no",
    "nos",
    "o",
    "os",
    "ou",
    "para",
    "por",
    "qual",
    "quais",
    "que",
    "se",
    "tem",
    "tenho",
    "voces",
    "voce",
}
_KNOWLEDGE_DOMAIN_HINTS = {
    "aceita",
    "aceitam",
    "agendar",
    "agendamento",
    "atende",
    "atendem",
    "cidade",
    "cidades",
    "clinica",
    "clinicas",
    "convenio",
    "convenios",
    "endereco",
    "enderecos",
    "exame",
    "exames",
    "faz",
    "funciona",
    "funcionamento",
    "horario",
    "horarios",
    "laboratorio",
    "plano",
    "planos",
    "preparo",
    "realiza",
    "resultado",
    "resultados",
    "unidade",
    "unidades",
}
_KNOWLEDGE_TOKEN_EQUIVALENTS = {
    "especialista": {"especialidade", "profissional", "medico", "doutor", "doutora"},
    "especialistas": {"especialidades", "profissionais", "medicos", "doutores", "doutoras"},
    "gestacao": {"obstetra", "obstetricia", "pre", "natal"},
    "medico": {"profissional", "doutor", "doutora"},
    "medicos": {"profissionais", "doutores", "doutoras"},
    "obstetra": {"obstetricia", "gestacao", "pre", "natal"},
    "obstetricia": {"obstetra", "gestacao", "pre", "natal"},
    "prenatal": {"pre", "natal", "obstetra", "obstetricia", "gestacao"},
    "profissional": {"medico", "doutor", "doutora"},
    "profissionais": {"medicos", "doutores", "doutoras"},
}


def _search_vector_store_http(vector_store_id: str, query: str, max_num_results: int) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("openai_api_key_not_set")
    url = f"https://api.openai.com/v1/vector_stores/{vector_store_id}/search"
    payload = {
        "query": query,
        "max_num_results": max_num_results,
    }
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    timeout = httpx.Timeout(20.0, connect=10.0)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
    return data if isinstance(data, dict) else {"data": []}


def _knowledge_query_tokens(text: str, *, drop_domain_hints: bool = False) -> list[str]:
    tokens: list[str] = []
    for token in _normalize_service_text(text).split():
        if len(token) < 2 or token in _KNOWLEDGE_QUERY_STOPWORDS:
            continue
        if drop_domain_hints and token in _KNOWLEDGE_DOMAIN_HINTS:
            continue
        tokens.append(token)
    return tokens


def _knowledge_canonical_token(token: str) -> str:
    normalized = _normalize_service_text(token)
    if not normalized or " " in normalized:
        return normalized
    if len(normalized) <= 3:
        return normalized
    if normalized.endswith(("oes", "aes")) and len(normalized) > 4:
        return f"{normalized[:-3]}ao"
    if normalized.endswith("ais") and len(normalized) > 4:
        return f"{normalized[:-3]}al"
    if normalized.endswith("eis") and len(normalized) > 4:
        return f"{normalized[:-3]}el"
    if normalized.endswith("is") and len(normalized) > 4:
        return f"{normalized[:-2]}il"
    if normalized.endswith("s") and len(normalized) > 4:
        return normalized[:-1]
    return normalized


def _knowledge_expand_token(token: str) -> set[str]:
    normalized = _normalize_service_text(token)
    if not normalized:
        return set()
    expanded: set[str] = {normalized}
    canonical = _knowledge_canonical_token(normalized)
    if canonical:
        expanded.add(canonical)

    pending = list(expanded)
    seen = set(pending)
    while pending:
        current = pending.pop()
        for alias in _KNOWLEDGE_TOKEN_EQUIVALENTS.get(current, set()):
            alias_normalized = _normalize_service_text(alias)
            if not alias_normalized:
                continue
            alias_tokens = alias_normalized.split()
            for alias_token in alias_tokens or [alias_normalized]:
                canonical_alias = _knowledge_canonical_token(alias_token)
                for candidate in (alias_token, canonical_alias):
                    if candidate and candidate not in seen:
                        seen.add(candidate)
                        expanded.add(candidate)
                        pending.append(candidate)
    return expanded


def _knowledge_expand_token_set(tokens: list[str]) -> set[str]:
    expanded: set[str] = set()
    for token in tokens:
        expanded.update(_knowledge_expand_token(token))
    return expanded


def _split_markdown_sections(content: str) -> list[str]:
    normalized = (content or "").replace("\r\n", "\n").strip()
    if not normalized:
        return []
    sections: list[str] = []
    for block in re.split(r"\n(?=#{1,6}\s)", normalized):
        block = block.strip()
        if not block:
            continue
        for chunk in re.split(r"\n-{3,}\n", block):
            piece = chunk.strip()
            if piece:
                sections.append(piece)
    return sections or [normalized]


def _compact_knowledge_text(text: str, limit: int = 900) -> str:
    cleaned = re.sub(r"\n{3,}", "\n\n", (text or "").strip())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit].rstrip()}..."


def _score_local_knowledge_chunk(query: str, chunk: str, source_name: str) -> float:
    normalized_query = _normalize_service_text(query)
    normalized_chunk = _normalize_service_text(chunk)
    if not normalized_query or not normalized_chunk:
        return 0.0

    query_tokens = _knowledge_query_tokens(query)
    if not query_tokens:
        return 0.0
    specific_tokens = _knowledge_query_tokens(query, drop_domain_hints=True)
    source_tokens = _normalize_service_text(source_name.replace(".md", ""))
    searchable = f"{source_tokens} {normalized_chunk}".strip()
    searchable_token_set = _knowledge_expand_token_set(searchable.split())
    matched_tokens = [
        token for token in query_tokens if _knowledge_expand_token(token).intersection(searchable_token_set)
    ]
    matched_specific = [
        token for token in specific_tokens if _knowledge_expand_token(token).intersection(searchable_token_set)
    ]
    if specific_tokens and not matched_specific:
        return 0.0

    heading = _normalize_service_text((chunk.splitlines()[0] if chunk.splitlines() else ""))
    heading_token_set = _knowledge_expand_token_set(heading.split())
    source_token_set = _knowledge_expand_token_set(source_tokens.split())
    asks_for_unit_details = any(
        token in {"cidade", "cidades", "endereco", "enderecos", "horario", "horarios", "unidade", "unidades"}
        for token in query_tokens
    )
    asks_for_convenio = any(
        token in {"convenio", "convenios", "plano", "planos"}
        for token in query_tokens
    )
    score = 0.0
    if normalized_query in searchable:
        score += 8.0
    score += len(matched_tokens) * 1.6
    score += len(matched_specific) * 1.8
    if specific_tokens and len(matched_specific) == len(specific_tokens):
        score += 4.0
    elif len(matched_tokens) == len(query_tokens):
        score += 2.5
    if heading and matched_specific and all(
        _knowledge_expand_token(token).intersection(heading_token_set) for token in matched_specific
    ):
        score += 2.2
    elif heading and any(
        _knowledge_expand_token(token).intersection(heading_token_set) for token in matched_tokens
    ):
        score += 1.0
    if source_tokens and any(
        _knowledge_expand_token(token).intersection(source_token_set) for token in matched_tokens
    ):
        score += 0.8
    if asks_for_unit_details and source_token_set.intersection({"unidade", "unidades"}):
        score += 1.4
    if asks_for_convenio and source_token_set.intersection({"convenio", "convenios", "plano", "planos"}):
        score += 1.4
    return round(score, 4) if score >= 2.0 else 0.0


def _search_profile_local_docs_knowledge(profile_id: str, query: str, top_k: int) -> Dict[str, Any]:
    docs_dir = _get_docs_dir_for_profile(profile_id)
    if not docs_dir:
        return {"status": "unavailable", "profile_id": profile_id, "results": []}
    if not os.path.isdir(docs_dir):
        return {
            "status": "error",
            "profile_id": profile_id,
            "docs_dir": docs_dir,
            "message": "docs_dir_not_found",
            "results": [],
        }

    ranked: list[Dict[str, Any]] = []
    for root, _, filenames in os.walk(docs_dir):
        for filename in sorted(filenames):
            if not filename.lower().endswith(".md"):
                continue
            path = os.path.join(root, filename)
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    content = handle.read()
            except OSError as exc:
                logger.warning("Failed to read docs file %s: %s", path, exc)
                continue
            relative_name = os.path.relpath(path, docs_dir)
            for section in _split_markdown_sections(content):
                score = _score_local_knowledge_chunk(query, section, relative_name)
                if score <= 0:
                    continue
                ranked.append(
                    {
                        "source": relative_name,
                        "content": _compact_knowledge_text(section),
                        "score": score,
                    }
                )

    ranked.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    deduped: list[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in ranked:
        key = (str(item.get("source") or ""), str(item.get("content") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if len(deduped) >= top_k:
            break

    return {
        "status": "ok",
        "profile_id": profile_id,
        "docs_dir": docs_dir,
        "results": deduped,
    }


def _search_profile_vector_knowledge(profile_id: str, query: str, top_k: int) -> Dict[str, Any]:
    vector_store_ids = _get_vector_store_ids(profile_id)
    normalized_query = (query or "").strip() or "informacoes gerais da clinica"
    per_store_limit = max(top_k, 3)
    collected: list[Dict[str, Any]] = []
    errors: list[Dict[str, str]] = []
    local_docs_payload = _search_profile_local_docs_knowledge(
        profile_id=profile_id,
        query=normalized_query,
        top_k=per_store_limit,
    )

    if not vector_store_ids:
        errors.append(
            {
                "vector_store_id": "",
                "error": "vector_store_not_configured",
            }
        )

    for vector_store_id in vector_store_ids:
        payload: Any = None
        try:
            payload = _search_vector_store_sdk(vector_store_id, normalized_query, per_store_limit)
        except Exception as sdk_exc:
            logger.warning(
                "Vector store SDK search failed profile=%s vector_store=%s: %s",
                profile_id,
                vector_store_id,
                sdk_exc,
            )
            try:
                payload = _search_vector_store_http(vector_store_id, normalized_query, per_store_limit)
            except Exception as http_exc:
                errors.append(
                    {
                        "vector_store_id": vector_store_id,
                        "error": str(http_exc),
                    }
                )
                continue

        for raw_item in _extract_vector_search_items(payload):
            item = _as_dict(raw_item)
            content = _extract_vector_result_text(raw_item)
            if not content:
                continue
            if len(content) > 1800:
                content = content[:1800]
            score_raw = item.get("score")
            try:
                score = float(score_raw)
            except Exception:
                score = 0.0
            collected.append(
                {
                    "source": _vector_result_source(item, vector_store_id),
                    "content": content,
                    "vector_store_id": vector_store_id,
                    "source_type": "vector_store",
                    "_score": score,
                    "_rank": 6.0 + max(min(score, 1.0), 0.0) * 4.0,
                }
            )

    for item in list(local_docs_payload.get("results") or []):
        score_raw = item.get("score")
        try:
            score = float(score_raw)
        except Exception:
            score = 0.0
        collected.append(
            {
                "source": f"docs:{str(item.get('source') or '').strip()}",
                "content": str(item.get("content") or "").strip(),
                "vector_store_id": "",
                "source_type": "local_docs",
                "_score": score,
                "_rank": score,
            }
        )

    collected.sort(
        key=lambda entry: (entry.get("_rank", 0.0), entry.get("_score", 0.0)),
        reverse=True,
    )
    deduped: list[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for entry in collected:
        key = (str(entry.get("source") or ""), str(entry.get("content") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
        if len(deduped) >= top_k:
            break

    results: list[Dict[str, Any]] = []
    for entry in deduped:
        item: Dict[str, Any] = {
            "source": entry["source"],
            "content": entry["content"],
            "source_type": entry.get("source_type") or "vector_store",
        }
        vector_store_id = str(entry.get("vector_store_id") or "").strip()
        if vector_store_id:
            item["vector_store_id"] = vector_store_id
        score = entry.get("_score")
        if isinstance(score, float) and score > 0:
            item["score"] = round(score, 4)
        results.append(item)

    payload: Dict[str, Any] = {
        "status": "ok",
        "profile_id": profile_id,
        "query": normalized_query,
        "vector_store_ids": vector_store_ids,
        "results": results,
        "confirmation_status": "confirmed" if results else "unconfirmed",
        "search_sources": {
            "vector_store": len([item for item in results if item.get("source_type") == "vector_store"]),
            "local_docs": len([item for item in results if item.get("source_type") == "local_docs"]),
        },
        "answering_guidance": (
            "Se a busca vier sem resultados, trate como informacao nao confirmada no momento. "
            "Nao negue a existencia de unidade, convenio, exame ou servico apenas porque a busca veio vazia."
        ),
    }
    if errors:
        payload["warnings"] = errors
    if local_docs_payload.get("status") == "error":
        payload.setdefault("warnings", []).append(
            {
                "docs_dir": str(local_docs_payload.get("docs_dir") or ""),
                "error": str(local_docs_payload.get("message") or "local_docs_search_failed"),
            }
        )
    if not results:
        payload["message"] = "no_results"
    return payload


def _build_knowledge_tool() -> Optional[Any]:
    if function_tool is None:
        return None

    @function_tool
    def buscar_info_clinica(query: str = "", top_k: int = 3) -> Dict[str, Any]:
        """
        Tool para buscar informacoes da clinica no OpenAI Vector Store do perfil atual.
        """
        chat_id = _CURRENT_CHAT_ID.get("")
        profile_id = _CURRENT_PROFILE_ID.get("") or _resolve_profile_for_chat(chat_id)
        if not profile_id:
            profile_id = PROFILE_DEFAULT_ID or ""
        try:
            requested_top_k = int(top_k)
        except Exception:
            requested_top_k = 3
        requested_top_k = max(1, min(requested_top_k, 6))
        return _search_profile_vector_knowledge(
            profile_id=profile_id,
            query=query or "",
            top_k=requested_top_k,
        )

    return buscar_info_clinica


def _build_audio_match_tool() -> Optional[Any]:
    if function_tool is None:
        return None

    @function_tool
    def buscar_audio_atendimento(query: str, top_k: int = 3) -> Dict[str, Any]:
        """
        Tool para localizar audios disponiveis no bucket do perfil atual.
        Retorna arquivos existentes e os melhores matches para o nome informado.
        """
        chat_id = _CURRENT_CHAT_ID.get("")
        profile_id = _CURRENT_PROFILE_ID.get("") or _resolve_profile_for_chat(chat_id)
        if not profile_id:
            profile_id = PROFILE_DEFAULT_ID or ""
        try:
            requested_top_k = int(top_k)
        except Exception:
            requested_top_k = 3
        requested_top_k = max(1, min(requested_top_k, 5))
        return _match_profile_audio_files(profile_id=profile_id, query=query or "", top_k=requested_top_k)

    return buscar_audio_atendimento


def _build_audio_send_tool() -> Optional[Any]:
    if function_tool is None:
        return None

    @function_tool
    def enviar_audio_atendimento(filename: str) -> Dict[str, Any]:
        """
        Tool para enviar um audio existente do bucket do perfil atual para a conversa atual.
        Use somente com filename retornado por buscar_audio_atendimento.
        """
        chat_id = _CURRENT_CHAT_ID.get("")
        profile_id = _CURRENT_PROFILE_ID.get("") or _resolve_profile_for_chat(chat_id)
        if not profile_id:
            profile_id = PROFILE_DEFAULT_ID or ""
        bucket = _get_audio_bucket_for_profile(profile_id)
        normalized_filename = str(filename or "").strip()
        if not chat_id:
            return {"status": "error", "message": "chat_id_not_available", "filename": normalized_filename}
        if not bucket:
            return {"status": "error", "message": "audio_bucket_not_configured", "profile_id": profile_id}
        available_files = _list_bucket_audio_files_sync(bucket)
        valid_names = {str(item.get("name") or "").strip(): item for item in available_files}
        if normalized_filename not in valid_names:
            match_payload = _match_profile_audio_files(profile_id=profile_id, query=normalized_filename, top_k=3)
            return {
                "status": "error",
                "message": "filename_not_found_in_bucket",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
                "matches": match_payload.get("matches") or [],
                "available_files": match_payload.get("available_files") or [],
            }
        if _has_recent_service_audio_sent(chat_id, normalized_filename):
            return {
                "status": "skipped",
                "message": "audio_already_sent_recently",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
            }
        media_url = _build_bucket_audio_url_sync(bucket, normalized_filename)
        if not media_url:
            return {
                "status": "error",
                "message": "audio_url_not_available",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
            }
        try:
            message_id = _send_voice_sync(chat_id, media_url)
        except Exception as exc:
            return {
                "status": "error",
                "message": "audio_send_failed",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
                "error": str(exc),
            }
        _remember_service_audio_sent(chat_id, normalized_filename)
        return {
            "status": "sent",
            "profile_id": profile_id,
            "bucket": bucket,
            "filename": normalized_filename,
            "message_id": message_id,
        }

    return enviar_audio_atendimento


_SCHEDULING_TOOL = _build_scheduling_tool()
_KNOWLEDGE_TOOL = _build_knowledge_tool()
_AUDIO_MATCH_TOOL = _build_audio_match_tool()
_AUDIO_SEND_TOOL = _build_audio_send_tool()


AGENT = _build_agent()
_AGENT_CACHE: Dict[str, Agent] = {}
OPENAI_CLIENT = OpenAI()
_SUPABASE_CLIENT: Optional["SupabaseClient"] = None


def _get_agent(profile_id: Optional[str]) -> Agent:
    if not profile_id:
        return AGENT
    agent = _AGENT_CACHE.get(profile_id)
    if agent is None:
        agent = _build_agent_for_profile(profile_id)
        _AGENT_CACHE[profile_id] = agent
    return agent


async def _send_text_parts(chat_id: str, text: str, active_turn: Optional[int] = None) -> bool:
    profile_id = _resolve_profile_for_chat(str(chat_id))
    parts = _split_messages(text, profile_id)
    if not parts:
        return True
    chatwoot_service = get_chatwoot_service()
    _log_webhook_debug(
        "send_text",
        {
            "chat_id": str(chat_id),
            "parts": len(parts),
            "text_len": len(text or ""),
            "text_hash": _short_hash(text or ""),
            "turn": active_turn,
        },
    )
    delay = _message_delay_seconds(profile_id)
    first_delay = _first_message_delay_seconds(profile_id)
    if _reply_contains_schedule_options(text):
        delay = min(delay, _schedule_delay_seconds(profile_id))
    total_parts = len(parts)
    for idx, part in enumerate(parts):
        if not _is_chat_turn_current(str(chat_id), active_turn):
            _log_webhook_debug(
                "send_text_aborted_stale_turn",
                {
                    "chat_id": str(chat_id),
                    "turn": active_turn,
                    "idx": idx,
                    "phase": "before_wait",
                },
            )
            return False
        if idx == 0:
            wait = first_delay
        else:
            wait = delay
            if len(part) <= 120:
                wait = min(wait, 0.2)
        if wait:
            await anyio.sleep(wait)
        if not _is_chat_turn_current(str(chat_id), active_turn):
            _log_webhook_debug(
                "send_text_aborted_stale_turn",
                {
                    "chat_id": str(chat_id),
                    "turn": active_turn,
                    "idx": idx,
                    "phase": "before_send",
                },
            )
            return False
        sent_message_id = await _send_text(chat_id, part)
        await chatwoot_service.sync_outgoing_whatsapp_message(
            chat_id=str(chat_id),
            phone=_normalize_phone(str(chat_id)),
            contact_name="",
            content=part,
            message_id=sent_message_id,
        )
    return True


async def _send_reply(
    chat_id: str,
    text: str,
    *,
    user_text: str = "",
    profile_id: Optional[str] = None,
    active_turn: Optional[int] = None,
) -> bool:
    sent = await _send_text_parts(chat_id, text, active_turn=active_turn)
    if not sent:
        return False
    if not user_text.strip():
        return True
    if not _get_audio_bucket_for_profile(profile_id):
        return True
    if not _is_chat_turn_current(str(chat_id), active_turn):
        return False
    try:
        await _maybe_send_profile_audio(
            chat_id=chat_id,
            profile_id=profile_id,
            user_text=user_text,
            assistant_text=text,
            active_turn=active_turn,
        )
    except Exception as exc:
        logger.warning("Automatic audio fallback failed chat=%s profile=%s: %s", chat_id, profile_id, exc)
    return True


app.include_router(build_chatwoot_router(_send_text_parts))


async def _handle_poll_vote(data: Dict[str, Any]) -> Dict[str, Any]:
    payload = data.get("payload") or {}
    message = payload.get("message") or {}
    vote = payload.get("vote") or payload.get("pollVote") or message.get("vote") or {}
    poll = payload.get("poll") or message.get("poll") or payload.get("pollUpdate") or {}
    if poll.get("fromMe") is False:
        return {"ok": True, "ignored": "not_our_poll"}

    chat_id = (
        payload.get("chatId")
        or payload.get("from")
        or payload.get("to")
        or vote.get("from")
        or poll.get("to")
        or vote.get("to")
        or vote.get("chatId")
        or message.get("chatId")
    )
    if not chat_id:
        return {"ok": False, "error": "missing_chat_id"}

    vote_id = vote.get("id") or payload.get("voteId")
    if vote_id and _is_duplicate_key_global(
        _RECENT_EVENT_IDS, f"poll:{vote_id}", RECENT_EVENT_TTL_SECONDS
    ):
        return {"ok": True, "ignored": "duplicate_poll_vote"}

    state = _get_profile_state(str(chat_id))
    poll_id = poll.get("id") or payload.get("pollId") or payload.get("poll_id") or message.get("pollId")
    if state.get("poll_id") and poll_id and state["poll_id"] != poll_id:
        # Permite trocar de perfil mesmo votando em uma enquete anterior ou
        # quando o provedor nao marca corretamente o poll como fromMe.
        logger.info(
            "Accepting poll vote despite poll_id mismatch chat_id=%s state_poll_id=%s incoming_poll_id=%s",
            chat_id,
            state.get("poll_id"),
            poll_id,
        )

    selected_options = (
        vote.get("selectedOptions")
        or payload.get("selectedOptions")
        or vote.get("options")
        or vote.get("selectedOption")
        or vote.get("selectedOptionIds")
        or payload.get("selectedOptionIds")
        or payload.get("selectedOptionsIds")
        or []
    )
    normalized_options = _normalize_selected_options(selected_options)
    if not vote_id:
        poll_id = poll.get("id") or payload.get("pollId") or payload.get("poll_id")
        vote_key = {
            "poll": poll_id,
            "from": vote.get("from") or payload.get("from"),
            "opts": normalized_options,
        }
        vote_digest = hashlib.sha1(json.dumps(vote_key, sort_keys=True).encode("utf-8")).hexdigest()
        if _is_duplicate_key_global(
            _RECENT_EVENT_IDS, f"pollh:{vote_digest}", RECENT_EVENT_TTL_SECONDS
        ):
            return {"ok": True, "ignored": "duplicate_poll_vote_hash"}
    active_turn = _next_chat_turn(str(chat_id))
    if not normalized_options:
        await _send_text_parts(
            str(chat_id),
            "Desculpe, nao consegui entender sua escolha. Vou reenviar a enquete, por favor escolha novamente. 🙂",
            active_turn=active_turn,
        )
        new_poll_id = await _send_profile_poll(str(chat_id))
        _update_profile_state(str(chat_id), poll_id=new_poll_id)
        return {"ok": True, "poll_vote_failed": True}
    profile_id = _resolve_profile_id_from_vote(selected_options)
    if not profile_id or profile_id not in PROFILES:
        await _send_text_parts(
            str(chat_id),
            "Nao consegui identificar o perfil selecionado. Vou reenviar a enquete.",
            active_turn=active_turn,
        )
        new_poll_id = await _send_profile_poll(str(chat_id))
        _update_profile_state(str(chat_id), poll_id=new_poll_id)
        return {"ok": True, "profile_missing": True}

    previous_profile = state.get("profile_id")
    if previous_profile and previous_profile != profile_id:
        await _reset_session(_get_session(str(chat_id)))
        _LAST_SCHEDULE_OPTIONS.pop(str(chat_id), None)

    pending_message = (state.get("pending_message") or "").strip()
    _update_profile_state(
        str(chat_id),
        profile_id=profile_id,
        poll_id=None,
        pending_message=None,
        flow_state=None,
        flow_data={},
    )

    contact_name = await _get_contact_name(str(chat_id))
    first_name = _first_name(contact_name or "")
    greeting = _build_greeting(first_name, profile_id)

    session = _get_session(str(chat_id))
    agent = _get_agent(profile_id)

    if pending_message:
        if _is_greeting_only(pending_message):
            await _send_text_parts(str(chat_id), greeting, active_turn=active_turn)
            try:
                await session.add_items([{"role": "assistant", "content": greeting}])
            except Exception as exc:
                logger.warning("Failed to persist greeting item: %s", exc)
            await _trim_session(session, SESSION_MAX_ITEMS)
            return {"ok": True, "profile_selected": profile_id, "handled_pending": True, "greeting_only": True}
        try:
            await session.add_items([{"role": "assistant", "content": greeting}])
        except Exception as exc:
            logger.warning("Failed to persist greeting item: %s", exc)
        await _send_text_parts(str(chat_id), greeting, active_turn=active_turn)
        service_audio = await _try_send_service_audio_for_message(
            str(chat_id),
            profile_id,
            pending_message,
            active_turn=active_turn,
        )
        if service_audio:
            try:
                await session.add_items(
                    [
                        {"role": "user", "content": pending_message},
                        {"role": "assistant", "content": service_audio["session_note"]},
                    ]
                )
            except Exception as exc:
                logger.warning("Failed to persist service audio note: %s", exc)
            await _log_conversation(
                str(chat_id),
                {},
                pending_message,
                f"{greeting}\n\n[{service_audio['session_note']}]",
                "audio_auto",
            )
            await _trim_session(session, SESSION_MAX_ITEMS)
            return {
                "ok": True,
                "profile_selected": profile_id,
                "handled_pending": True,
                "service_audio_sent": service_audio["filename"],
            }
        result = None
        try:
            result = await _run_agent(agent, pending_message, session, str(chat_id), profile_id)
            reply = _truncate(_sanitize_plain_text(_extract_text_from_result(result), profile_id))
        except Exception as exc:
            logger.exception("Agent run failed: %s", exc)
            reply = ""
        if not reply:
            _log_empty_output_diagnostics(result, "pending_message")
            reply = "Desculpe, nao consegui responder agora."
            reply = _inject_fake_schedule(str(chat_id), pending_message, reply)
        combined = greeting if not reply else f"{greeting}\n\n{reply}"
        await _send_reply(
            str(chat_id),
            combined,
            user_text=pending_message,
            profile_id=profile_id,
            active_turn=active_turn,
        )
        await _trim_session(session, SESSION_MAX_ITEMS)
        return {"ok": True, "profile_selected": profile_id, "handled_pending": True}

    await _send_text_parts(str(chat_id), greeting, active_turn=active_turn)
    try:
        await session.add_items([{"role": "assistant", "content": greeting}])
    except Exception as exc:
        logger.warning("Failed to persist greeting item: %s", exc)
    await _trim_session(session, SESSION_MAX_ITEMS)
    return {"ok": True, "profile_selected": profile_id, "handled_pending": False}

@app.post("/webhook/waha")
async def waha_webhook(request: Request) -> Dict[str, Any]:
    data = await request.json()
    event = data.get("event")
    if LOG_WEBHOOK_PAYLOADS:
        logger.info("Webhook payload (%s): %s", event, data)
    if event == "poll.vote":
        return await _handle_poll_vote(data)
    if event == "poll.vote.failed":
        return await _handle_poll_vote(data)
    if event not in ("message", "message.any", "message.new"):
        return {"ok": True, "ignored": "event"}

    payload = data.get("payload") or {}
    event_id = _extract_event_id(data, payload)
    msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
    message_id = _extract_message_id(payload)
    chat_id = payload.get("from") or payload.get("chatId") or payload.get("to")
    from_me = _is_from_me_payload(payload)
    raw_body = (payload.get("body") or payload.get("text") or "").strip()
    fingerprint = _message_fingerprint(payload)
    _log_webhook_debug(
        "received",
        {
            "event": event,
            "event_id": event_id,
            "msg_type": msg_type,
            "chat_id": str(chat_id) if chat_id else None,
            "message_id": message_id,
            "fingerprint": fingerprint,
            "from_me": from_me,
            "timestamp": _extract_timestamp(payload),
            "body_len": len(raw_body),
            "body_hash": _short_hash(raw_body),
        },
    )
    if _is_duplicate_key_global(_RECENT_EVENT_IDS, event_id, RECENT_EVENT_TTL_SECONDS):
        _log_webhook_debug(
            "duplicate_event",
            {"event": event, "event_id": event_id, "chat_id": str(chat_id) if chat_id else None},
        )
        return {"ok": True, "ignored": "duplicate_event"}
    if msg_type in ("poll_vote", "pollvote", "poll_vote_event"):
        return await _handle_poll_vote(data)
    if payload.get("poll") and (payload.get("vote") or payload.get("pollVote")):
        return await _handle_poll_vote(data)
    if from_me:
        _log_webhook_debug(
            "from_me",
            {
                "event": event,
                "event_id": event_id,
                "chat_id": str(chat_id) if chat_id else None,
                "message_id": message_id,
            },
        )
        return {"ok": True, "ignored": "fromMe"}
    if _has_recent_key(
        _RECENT_OUTBOUND_MESSAGE_IDS,
        message_id,
        OUTBOUND_ECHO_TTL_SECONDS,
    ):
        _log_webhook_debug(
            "outbound_echo_message_id",
            {
                "event": event,
                "event_id": event_id,
                "chat_id": str(chat_id) if chat_id else None,
                "message_id": message_id,
            },
        )
        return {"ok": True, "ignored": "outbound_echo_message_id"}

    if not chat_id:
        return {"ok": False, "error": "missing chat_id"}

    if message_id:
        message_key = f"{chat_id}:{message_id}"
        if _is_duplicate_key_global(
            _RECENT_MESSAGE_KEYS, message_key, RECENT_EVENT_TTL_SECONDS
        ):
            _log_webhook_debug(
                "duplicate_message",
                {
                    "message_key": message_key,
                    "event": event,
                    "event_id": event_id,
                    "chat_id": str(chat_id),
                },
            )
            return {"ok": True, "ignored": "duplicate_message"}
    else:
        if _is_duplicate_key_global(
            _RECENT_MESSAGE_KEYS, fingerprint, RECENT_EVENT_TTL_SECONDS
        ):
            _log_webhook_debug(
                "duplicate_message_fallback",
                {
                    "fingerprint": fingerprint,
                    "event": event,
                    "event_id": event_id,
                    "chat_id": str(chat_id),
                },
            )
            return {"ok": True, "ignored": "duplicate_message_fallback"}

    if (not ALLOW_GROUPS) and str(chat_id).endswith("@g.us"):
        return {"ok": True, "ignored": "group"}

    if _is_non_text_media(payload):
        active_turn = _next_chat_turn(str(chat_id))
        pending_booking = _consume_pending_signal_booking(str(chat_id))
        if pending_booking:
            reply = _build_signal_received_confirmation(
                pending_booking.get("option"),
                pending_booking.get("profile_id"),
                str(chat_id),
            )
            await _send_text_parts(chat_id, reply, active_turn=active_turn)
            await _log_conversation(
                str(chat_id),
                payload,
                "[comprovante_pix_midia]",
                reply,
                msg_type or "media",
            )
            return {"ok": True, "signal_confirmed": True}
        reply = "Consigo acessar apenas mensagens de texto e audio. Pode enviar em texto ou audio, por favor?"
        await _send_text_parts(chat_id, reply, active_turn=active_turn)
        return {"ok": True, "ignored": "non_text_media"}

    is_audio = _is_audio_payload(payload)
    body = (payload.get("body") or "").strip()
    if is_audio:
        media_url = _extract_media_url(payload)
        if not media_url:
            active_turn = _next_chat_turn(str(chat_id))
            reply = "Consigo ouvir audios, mas nao consegui acessar esse. Pode reenviar, por favor?"
            await _send_text_parts(chat_id, reply, active_turn=active_turn)
            return {"ok": True, "ignored": "missing_audio_url"}
        transcription = await _transcribe_audio(media_url, payload)
        if not transcription:
            active_turn = _next_chat_turn(str(chat_id))
            reply = "Nao consegui transcrever o audio. Pode reenviar ou mandar em texto?"
            await _send_text_parts(chat_id, reply, active_turn=active_turn)
            return {"ok": True, "ignored": "transcription_failed"}
        body = transcription

    if not body:
        return {"ok": True, "ignored": "empty"}

    coalesced = await _coalesce_user_message(str(chat_id), body, is_audio)
    if coalesced is None:
        return {"ok": True, "queued": True}
    body, is_audio = coalesced
    await get_chatwoot_service().sync_incoming_whatsapp_message(
        chat_id=str(chat_id),
        phone=_normalize_phone(str(chat_id)),
        contact_name=_name_from_payload(payload) or "",
        content=body,
        message_id=message_id or fingerprint or "",
    )
    active_turn = _next_chat_turn(str(chat_id))
    _log_webhook_debug(
        "coalesced",
        {
            "chat_id": str(chat_id),
            "body_len": len(body or ""),
            "body_hash": _short_hash(body or ""),
            "is_audio": is_audio,
        },
    )

    profile_id: Optional[str] = None
    if PROFILE_ROUTING_ENABLED:
        if _wants_profile_switch(body):
            _clear_profile_state(str(chat_id))
            await _reset_session(_get_session(str(chat_id)))
            poll_id = await _send_profile_poll(str(chat_id))
            _update_profile_state(str(chat_id), poll_id=poll_id, pending_message=None)
            return {"ok": True, "profile_switch": True}

        state = _get_profile_state(str(chat_id))
        profile_id = state.get("profile_id")
        if not profile_id:
            if state.get("poll_id"):
                _update_profile_state(str(chat_id), pending_message=body)
                await _send_text_parts(
                    str(chat_id),
                    "Para continuar, escolha um perfil na enquete acima, por favor.",
                    active_turn=active_turn,
                )
                return {"ok": True, "awaiting_poll": True}
            if _is_duplicate_key(_RECENT_POLL_SENT, str(chat_id), POLL_THROTTLE_SECONDS):
                _update_profile_state(str(chat_id), pending_message=body)
                await _send_text_parts(
                    str(chat_id),
                    "Ja enviei a enquete acima. Pode escolher um perfil para continuarmos, por favor?",
                    active_turn=active_turn,
                )
                return {"ok": True, "poll_throttled": True}
            poll_id = await _send_profile_poll(str(chat_id))
            _update_profile_state(str(chat_id), poll_id=poll_id, pending_message=body)
            if poll_id:
                return {"ok": True, "poll_sent": True}
            profile_id = PROFILE_DEFAULT_ID or PROMPT_PROFILE or None
            _update_profile_state(str(chat_id), profile_id=profile_id, poll_id=None, pending_message=None)

    if not PROFILE_ROUTING_ENABLED:
        profile_id = PROMPT_PROFILE or PROFILE_DEFAULT_ID or None

    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

    session = _get_session(str(chat_id))
    try:
        items = await session.get_items()
    except Exception as exc:
        logger.warning("Failed to load session items: %s", exc)
        items = []

    if not items:
        await _hydrate_session_from_supabase(session, str(chat_id))
        try:
            items = await session.get_items()
        except Exception as exc:
            logger.warning("Failed to reload session items: %s", exc)
            items = []

    if SESSION_MAX_ITEMS > 0 and len(items) > SESSION_MAX_ITEMS:
        await _trim_session(session, SESSION_MAX_ITEMS)
        try:
            items = await session.get_items()
        except Exception as exc:
            logger.warning("Failed to reload trimmed session items: %s", exc)
            items = items[-SESSION_MAX_ITEMS:]

    if not _has_profile_greeting(items, profile_id):
        payload_name = _name_from_payload(payload)
        contact_name = payload_name or await _get_contact_name(str(chat_id))
        first_name = _first_name(contact_name or "")
        greeting = _build_greeting(first_name, profile_id)
        if _is_greeting_only(body):
            try:
                await session.add_items([{"role": "assistant", "content": greeting}])
            except Exception as exc:
                logger.warning("Failed to persist greeting item: %s", exc)
            await _send_text_parts(chat_id, greeting, active_turn=active_turn)
            await _log_conversation(
                str(chat_id),
                payload,
                body,
                greeting,
                "audio" if is_audio else "text",
            )
            await _trim_session(session, SESSION_MAX_ITEMS)
            return {"ok": True, "greeted": True, "greeting_only": True}

        try:
            await session.add_items([{"role": "assistant", "content": greeting}])
        except Exception as exc:
            logger.warning("Failed to persist greeting item: %s", exc)
        await _send_text_parts(chat_id, greeting, active_turn=active_turn)
        service_audio = await _try_send_service_audio_for_message(
            chat_id,
            profile_id,
            body,
            active_turn=active_turn,
        )
        if service_audio:
            try:
                await session.add_items(
                    [
                        {"role": "user", "content": body},
                        {"role": "assistant", "content": service_audio["session_note"]},
                    ]
                )
            except Exception as exc:
                logger.warning("Failed to persist service audio note: %s", exc)
            await _log_conversation(
                str(chat_id),
                payload,
                body,
                f"{greeting}\n\n[{service_audio['session_note']}]",
                "audio_auto",
            )
            await _trim_session(session, SESSION_MAX_ITEMS)
            return {
                "ok": True,
                "greeted": True,
                "service_audio_sent": service_audio["filename"],
            }
        result = None
        try:
            agent = _get_agent(profile_id)
            result = await _run_agent(agent, body, session, str(chat_id), profile_id)
            reply = _truncate(_sanitize_plain_text(_extract_text_from_result(result), profile_id))
        except Exception as exc:
            logger.exception("Agent run failed: %s", exc)
            reply = ""
        if not reply:
            _log_empty_output_diagnostics(result, "first_turn_after_greeting")
            reply = "Desculpe, nao consegui responder agora."
        reply = _inject_fake_schedule(str(chat_id), body, reply)
        combined = f"{greeting}\n\n{reply}"
        await _send_reply(
            chat_id,
            combined,
            user_text=body,
            profile_id=profile_id,
            active_turn=active_turn,
        )
        await _log_conversation(
            str(chat_id),
            payload,
            body,
            combined,
            "audio" if is_audio else "text",
        )
        await _trim_session(session, SESSION_MAX_ITEMS)
        return {"ok": True, "greeted": True, "answered": True}

    pending_booking = _peek_pending_signal_booking(str(chat_id))
    if pending_booking and _looks_like_payment_confirmation(body):
        confirmed_booking = _consume_pending_signal_booking(str(chat_id)) or pending_booking
        reply = _build_signal_received_confirmation(
            confirmed_booking.get("option"),
            confirmed_booking.get("profile_id"),
            str(chat_id),
        )
        try:
            await session.add_items(
                [
                    {"role": "user", "content": body},
                    {"role": "assistant", "content": reply},
                ]
            )
        except Exception as exc:
            logger.warning("Failed to persist signal confirmation: %s", exc)
        await _send_text_parts(chat_id, reply, active_turn=active_turn)
        await _log_conversation(
            str(chat_id),
            payload,
            body,
            reply,
            "audio" if is_audio else "text",
        )
        await _trim_session(session, SESSION_MAX_ITEMS)
        return {"ok": True, "signal_confirmed_text": True}

    schedule_choice = _try_match_schedule_option(str(chat_id), body)
    if schedule_choice:
        _LAST_SCHEDULE_OPTIONS.pop(str(chat_id), None)
        is_ariane_flow = _is_ariane_profile(profile_id, str(chat_id))
        if not is_ariane_flow and not profile_id:
            is_ariane_flow = _is_ariane_context_from_items(items, body)
        flow_profile_id = _resolve_flow_profile_id(
            profile_id,
            str(chat_id),
            force_ariane=is_ariane_flow,
        )
        flow_config = _get_booking_flow(
            profile_id,
            str(chat_id),
            force_ariane=is_ariane_flow,
        )
        reply = _build_schedule_confirmation(
            schedule_choice,
            body,
            profile_id,
            str(chat_id),
            force_ariane=is_ariane_flow,
        )
        if flow_config is not None and flow_config.requires_deposit:
            _set_pending_signal_booking(str(chat_id), schedule_choice, flow_profile_id)
        try:
            await session.add_items(
                [
                    {"role": "user", "content": body},
                    {"role": "assistant", "content": reply},
                ]
            )
        except Exception as exc:
            logger.warning("Failed to persist schedule confirmation: %s", exc)
        await _send_text_parts(chat_id, reply, active_turn=active_turn)
        await _log_conversation(
            str(chat_id),
            payload,
            body,
            reply,
            "audio" if is_audio else "text",
        )
        await _trim_session(session, SESSION_MAX_ITEMS)
        return {"ok": True, "schedule_confirmed": True}

    service_audio = await _try_send_service_audio_for_message(
        chat_id,
        profile_id,
        body,
        active_turn=active_turn,
    )
    if service_audio:
        try:
            await session.add_items(
                [
                    {"role": "user", "content": body},
                    {"role": "assistant", "content": service_audio["session_note"]},
                ]
            )
        except Exception as exc:
            logger.warning("Failed to persist service audio note: %s", exc)
        await _log_conversation(
            str(chat_id),
            payload,
            body,
            f"[{service_audio['session_note']}]",
            "audio_auto",
        )
        await _trim_session(session, SESSION_MAX_ITEMS)
        return {"ok": True, "service_audio_sent": service_audio["filename"]}

    try:
        agent = _get_agent(profile_id)
        result = await _run_agent(agent, body, session, str(chat_id), profile_id)
    except Exception as exc:
        logger.exception("Agent run failed: %s", exc)
        raise HTTPException(status_code=502, detail="Agent run failed") from exc

    reply = _truncate(_sanitize_plain_text(_extract_text_from_result(result), profile_id))
    if not reply:
        _log_empty_output_diagnostics(result, "regular_turn")
        reply = "Desculpe, não consegui responder agora."
    reply = _inject_fake_schedule(str(chat_id), body, reply)

    await _send_reply(
        chat_id,
        reply,
        user_text=body,
        profile_id=profile_id,
        active_turn=active_turn,
    )
    await _log_conversation(
        str(chat_id),
        payload,
        body,
        reply,
        "audio" if is_audio else "text",
    )
    await _trim_session(session, SESSION_MAX_ITEMS)
    return {"ok": True}
