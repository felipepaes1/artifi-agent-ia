from __future__ import annotations

import logging
import os


logger = logging.getLogger("mcp")


def env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    return default


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid int value for setting: %s", value)
        return default


def parse_float(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning("Invalid float value for setting: %s", value)
        return default


def parse_csv(value: str | None, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    if value is None or not value.strip():
        return default
    return tuple(part.strip() for part in value.split(",") if part.strip())


MCP_NAME = env_first("MCP_NAME", default="n8n-waha")
MCP_PORT = parse_int(os.getenv("MCP_PORT"), 8001)
MCP_TRANSPORT = env_first("MCP_TRANSPORT", default="http")
HTTP_TIMEOUT = parse_float(os.getenv("HTTP_TIMEOUT"), 20.0)
LOG_LEVEL = env_first("LOG_LEVEL", default="INFO").upper()

WAHA_BASE_URL = env_first("WAHA_BASE_URL", default="http://waha:3000").rstrip("/")
WAHA_API_KEY = env_first("WAHA_API_KEY_PLAIN", "WAHA_API_KEY")
WAHA_SESSION = env_first("WAHA_SESSION", default="default")
N8N_WEBHOOK_BASE_URL = env_first("N8N_WEBHOOK_BASE_URL", default="http://n8n:5678/webhook").rstrip("/")

CALENDAR_PROVIDER = env_first("CALENDAR_PROVIDER", default="google_calendar")
CALENDAR_DEFAULT_TIMEZONE = env_first("CALENDAR_DEFAULT_TIMEZONE", "TZ", default="America/Sao_Paulo")
CALENDAR_DEFAULT_ID = env_first("CALENDAR_DEFAULT_ID", default="primary")
CALENDAR_DEFAULT_ACCOUNT_ID = env_first("CALENDAR_DEFAULT_ACCOUNT_ID", default="default")
CALENDAR_SLOT_INCREMENT_MINUTES = parse_int(os.getenv("CALENDAR_SLOT_INCREMENT_MINUTES"), 15)
CALENDAR_MAX_SUGGESTED_SLOTS = parse_int(os.getenv("CALENDAR_MAX_SUGGESTED_SLOTS"), 12)

GOOGLE_CLIENT_ID = env_first("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = env_first("GOOGLE_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = env_first(
    "GOOGLE_OAUTH_REDIRECT_URI",
    default="http://localhost:8765/oauth2/callback",
)
GOOGLE_OAUTH_AUTH_URI = env_first(
    "GOOGLE_OAUTH_AUTH_URI",
    default="https://accounts.google.com/o/oauth2/v2/auth",
)
GOOGLE_OAUTH_TOKEN_URI = env_first(
    "GOOGLE_OAUTH_TOKEN_URI",
    default="https://oauth2.googleapis.com/token",
)
GOOGLE_CALENDAR_API_BASE_URL = env_first(
    "GOOGLE_CALENDAR_API_BASE_URL",
    default="https://www.googleapis.com/calendar/v3",
).rstrip("/")
GOOGLE_TOKEN_STORE_PATH = env_first(
    "GOOGLE_TOKEN_STORE_PATH",
    default="/data/google_calendar_tokens.json",
)
GOOGLE_CALENDAR_SCOPES = parse_csv(
    os.getenv("GOOGLE_CALENDAR_SCOPES"),
    default=("https://www.googleapis.com/auth/calendar.events",),
)
GOOGLE_OAUTH_STATE_SECRET = env_first("GOOGLE_OAUTH_STATE_SECRET", default="")
GOOGLE_OAUTH_ACCESS_TYPE = env_first("GOOGLE_OAUTH_ACCESS_TYPE", default="offline")
GOOGLE_OAUTH_PROMPT = env_first("GOOGLE_OAUTH_PROMPT", default="consent")
GOOGLE_OAUTH_INCLUDE_GRANTED_SCOPES = parse_bool(
    os.getenv("GOOGLE_OAUTH_INCLUDE_GRANTED_SCOPES"),
    default=True,
)
