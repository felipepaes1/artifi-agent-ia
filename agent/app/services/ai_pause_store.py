from __future__ import annotations

import logging
import os
import sqlite3
import time
from typing import Iterable, Optional


logger = logging.getLogger("agent")


HUMAN_PAUSE_TTL_SECONDS = int(os.getenv("HUMAN_PAUSE_TTL_SECONDS", "86400") or "86400")


def _db_path() -> str:
    from ..config.settings import PROFILE_STATE_DB
    return PROFILE_STATE_DB


def _normalize_chat_id(chat_id: object) -> str:
    return str(chat_id or "").strip()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=2)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_pause (
            chat_id TEXT PRIMARY KEY,
            paused_until INTEGER NOT NULL,
            reason TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    return conn


def pause_chat(
    chat_id: object,
    *,
    ttl_seconds: Optional[int] = None,
    reason: str = "",
) -> int:
    key = _normalize_chat_id(chat_id)
    if not key:
        return 0
    ttl = int(ttl_seconds) if ttl_seconds is not None else HUMAN_PAUSE_TTL_SECONDS
    if ttl <= 0:
        return 0
    now = int(time.time())
    paused_until = now + ttl
    try:
        conn = _connect()
        conn.execute(
            """
            INSERT INTO ai_pause (chat_id, paused_until, reason, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                paused_until=excluded.paused_until,
                reason=excluded.reason,
                created_at=excluded.created_at
            """,
            (key, paused_until, (reason or "")[:240], now),
        )
        conn.commit()
        return paused_until
    except Exception as exc:
        logger.warning("Failed to pause chat %s: %s", key, exc)
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def pause_chat_ids(
    chat_ids: Iterable[object],
    *,
    ttl_seconds: Optional[int] = None,
    reason: str = "",
) -> int:
    paused_until = 0
    seen: set[str] = set()
    for value in chat_ids:
        key = _normalize_chat_id(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result = pause_chat(key, ttl_seconds=ttl_seconds, reason=reason)
        if result > paused_until:
            paused_until = result
    return paused_until


def is_chat_paused(chat_id: object) -> bool:
    key = _normalize_chat_id(chat_id)
    if not key:
        return False
    now = int(time.time())
    try:
        conn = _connect()
        row = conn.execute(
            "SELECT paused_until FROM ai_pause WHERE chat_id = ?",
            (key,),
        ).fetchone()
        if not row:
            return False
        paused_until = int(row[0] or 0)
        if paused_until <= now:
            conn.execute("DELETE FROM ai_pause WHERE chat_id = ?", (key,))
            conn.commit()
            return False
        return True
    except Exception as exc:
        logger.warning("Failed to read pause state for %s: %s", key, exc)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def any_chat_paused(chat_ids: Iterable[object]) -> bool:
    seen: set[str] = set()
    for value in chat_ids:
        key = _normalize_chat_id(value)
        if not key or key in seen:
            continue
        seen.add(key)
        if is_chat_paused(key):
            return True
    return False


def get_pause_expiry(chat_id: object) -> Optional[int]:
    key = _normalize_chat_id(chat_id)
    if not key:
        return None
    now = int(time.time())
    try:
        conn = _connect()
        row = conn.execute(
            "SELECT paused_until FROM ai_pause WHERE chat_id = ?",
            (key,),
        ).fetchone()
        if not row:
            return None
        paused_until = int(row[0] or 0)
        if paused_until <= now:
            conn.execute("DELETE FROM ai_pause WHERE chat_id = ?", (key,))
            conn.commit()
            return None
        return paused_until
    except Exception as exc:
        logger.warning("Failed to read pause expiry for %s: %s", key, exc)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def clear_pause(chat_id: object) -> bool:
    key = _normalize_chat_id(chat_id)
    if not key:
        return False
    try:
        conn = _connect()
        cur = conn.execute("DELETE FROM ai_pause WHERE chat_id = ?", (key,))
        conn.commit()
        return cur.rowcount > 0
    except Exception as exc:
        logger.warning("Failed to clear pause for %s: %s", key, exc)
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def clear_pause_ids(chat_ids: Iterable[object]) -> int:
    cleared = 0
    seen: set[str] = set()
    for value in chat_ids:
        key = _normalize_chat_id(value)
        if not key or key in seen:
            continue
        seen.add(key)
        if clear_pause(key):
            cleared += 1
    return cleared
