import os
from dataclasses import dataclass
from typing import Optional


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return default


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _env_float(name: str, default: Optional[float]) -> Optional[float]:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip())
    except ValueError:
        return default


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@dataclass(frozen=True)
class Settings:
    openai_api_key: str
    openai_model: str
    openai_temperature: Optional[float]
    openai_max_tokens: Optional[int]
    openai_transcribe_model: str
    openai_transcribe_language: str
    system_prompt: str
    prompts_dir: str
    instructions_path: str
    profiles_path: str
    profile_routing_enabled: bool
    profile_state_db: str
    session_db: str
    allow_groups: bool
    max_reply_chars: int
    message_delay_ms: int
    user_message_coalesce_ms: int
    user_message_coalesce_max_ms: int
    waha_base_url: str
    waha_api_key: str
    waha_session: str
    log_webhook_payloads: bool
    recent_event_ttl_seconds: int
    poll_throttle_seconds: int
    agent_verbose: bool
    calendar_api_url: str
    calendar_api_key: str
    open_api_base_url: str
    open_api_key: str
    docs_dir: str
    examples_dir: str


def load_settings() -> Settings:
    prompts_dir = os.path.join(BASE_DIR, "prompts")
    instructions_path = os.path.join(BASE_DIR, "assistant_instructions.txt")
    profiles_path = os.path.join(BASE_DIR, "profiles.json")
    session_db = os.getenv("LC_SESSION_DB", "sessions_lc.db").strip() or "sessions_lc.db"
    profile_state_db = os.getenv("LC_PROFILE_STATE_DB", "profile_state_lc.db").strip() or "profile_state_lc.db"

    openai_api_key = _env_first("OPENAI_API_KEY", "API_OPENAI_KEY")
    if openai_api_key and not os.getenv("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = openai_api_key

    return Settings(
        openai_api_key=openai_api_key,
        openai_model=_env_first("OPENAI_MODEL", "OPENAI_DEFAULT_MODEL", default="gpt-4o-mini"),
        openai_temperature=_env_float("OPENAI_TEMPERATURE", None),
        openai_max_tokens=_env_int("OPENAI_MAX_TOKENS", 0) or None,
        openai_transcribe_model=_env_first(
            "OPENAI_TRANSCRIBE_MODEL",
            default="gpt-4o-mini-transcribe",
        ),
        openai_transcribe_language=_env_first("OPENAI_TRANSCRIBE_LANGUAGE", default="pt"),
        system_prompt=os.getenv("SYSTEM_PROMPT", "").strip(),
        prompts_dir=os.getenv("AGENT_PROMPTS_DIR", prompts_dir).strip() or prompts_dir,
        instructions_path=os.getenv("AGENT_INSTRUCTIONS_PATH", instructions_path).strip() or instructions_path,
        profiles_path=os.getenv("AGENT_PROFILES_PATH", profiles_path).strip() or profiles_path,
        profile_routing_enabled=_env_bool("AGENT_PROFILE_ROUTING", True),
        profile_state_db=os.getenv("AGENT_PROFILE_DB", profile_state_db).strip() or profile_state_db,
        session_db=session_db,
        allow_groups=_env_bool("ALLOW_GROUPS", False),
        max_reply_chars=_env_int("MAX_REPLY_CHARS", 1200),
        message_delay_ms=_env_int("MESSAGE_DELAY_MS", 900),
        user_message_coalesce_ms=_env_int("USER_MESSAGE_COALESCE_MS", 1200),
        user_message_coalesce_max_ms=_env_int("USER_MESSAGE_COALESCE_MAX_MS", 6000),
        waha_base_url=os.getenv("WAHA_BASE_URL", "http://waha:3000").rstrip("/"),
        waha_api_key=os.getenv("WAHA_API_KEY_PLAIN", os.getenv("WAHA_API_KEY", "")).strip(),
        waha_session=os.getenv("WAHA_SESSION", "default").strip(),
        log_webhook_payloads=_env_bool("LOG_WEBHOOK_PAYLOADS", False),
        recent_event_ttl_seconds=_env_int("RECENT_EVENT_TTL_SECONDS", 60),
        poll_throttle_seconds=_env_int("POLL_THROTTLE_SECONDS", 6),
        agent_verbose=_env_bool("LC_AGENT_VERBOSE", False),
        calendar_api_url=os.getenv("CALENDAR_API_URL", "").strip(),
        calendar_api_key=os.getenv("CALENDAR_API_KEY", "").strip(),
        open_api_base_url=os.getenv("OPEN_API_BASE_URL", "").strip(),
        open_api_key=os.getenv("OPEN_API_KEY", "").strip(),
        docs_dir=os.getenv("LC_DOCS_DIR", "").strip(),
        examples_dir=os.getenv("LC_EXAMPLES_DIR", "").strip(),
    )
