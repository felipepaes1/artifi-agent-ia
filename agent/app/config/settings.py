import logging
import os
from typing import Optional


logger = logging.getLogger("agent")


def env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return default


def parse_float(value: str) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        logger.warning("Invalid float value: %s", value)
        return None


def parse_int(value: str) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid int value: %s", value)
        return None


OPENAI_API_KEY = env_first("OPENAI_API_KEY", "API_OPENAI_KEY")
OPENAI_MODEL = env_first("OPENAI_MODEL", "OPENAI_DEFAULT_MODEL")
OPENAI_MAX_TOKENS = env_first("OPENAI_MAX_TOKENS")
OPENAI_TRANSCRIBE_MODEL = env_first("OPENAI_TRANSCRIBE_MODEL", default="gpt-4o-mini-transcribe")
OPENAI_TRANSCRIBE_LANGUAGE = env_first("OPENAI_TRANSCRIBE_LANGUAGE", default="pt")
OPENAI_TTS_MODEL = env_first("OPENAI_TTS_MODEL", default="gpt-4o-mini-tts")
OPENAI_TTS_VOICE = env_first("OPENAI_TTS_VOICE", default="marin")
OPENAI_TTS_FORMAT = env_first("OPENAI_TTS_FORMAT", default="mp3")
OPENAI_TTS_BUCKET = env_first("OPENAI_TTS_BUCKET", default="agent-tts-audio")
OPENAI_TTS_INSTRUCTIONS = os.getenv("OPENAI_TTS_INSTRUCTIONS", "").strip()
SYSTEM_PROMPT = os.getenv("SYSTEM_PROMPT", "").strip()
PROMPT_PROFILE = os.getenv("AGENT_PROMPT_PROFILE", "").strip()
PROMPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts")
INSTRUCTIONS_PATH = os.getenv("AGENT_INSTRUCTIONS_PATH", "").strip()
if not INSTRUCTIONS_PATH:
    if PROMPT_PROFILE:
        INSTRUCTIONS_PATH = os.path.join(PROMPTS_DIR, f"{PROMPT_PROFILE}.txt")
    else:
        INSTRUCTIONS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assistant_instructions.txt")
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
MESSAGE_SPLIT_SENTENCE_THRESHOLD = os.getenv("MESSAGE_SPLIT_SENTENCE_THRESHOLD", "180").strip()
PROFILES_PATH = os.getenv(
    "AGENT_PROFILES_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "profiles.json"),
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
WAHA_TYPING_PREVIEW_MS = os.getenv("WAHA_TYPING_PREVIEW_MS", "1200").strip()
WAHA_RECORDING_PREVIEW_MS = os.getenv("WAHA_RECORDING_PREVIEW_MS", "1400").strip()
WAHA_SENDVOICE_CONVERT = os.getenv("WAHA_SENDVOICE_CONVERT", "true").lower() == "true"

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = env_first("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE_KEY")
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
