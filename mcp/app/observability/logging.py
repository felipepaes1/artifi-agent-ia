from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from ..settings import LOG_LEVEL


_REDACT_KEYS = {
    "access_token",
    "refresh_token",
    "client_secret",
    "authorization",
    "token",
    "secret",
    "id_token",
}


def configure_logging() -> None:
    logging.basicConfig(level=LOG_LEVEL, format="%(message)s")


def _sanitize(value: Any, key: str | None = None) -> Any:
    if key and key.lower() in _REDACT_KEYS:
        return "***"
    if isinstance(value, dict):
        return {child_key: _sanitize(child_value, child_key) for child_key, child_value in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize(item) for item in value]
    return value


def log_event(logger: logging.Logger, level: int, event: str, **fields: Any) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **_sanitize(fields),
    }
    logger.log(level, json.dumps(payload, ensure_ascii=True, default=str))
