import json
import logging
import sqlite3
import time
from typing import Any, Dict, Optional

import anyio
from agents import SQLiteSession

from ..config.settings import (
    DEDUP_DB_TTL_SECONDS,
    PENDING_SIGNAL_TTL_SECONDS,
    PROFILE_STATE_DB,
    RECENT_EVENT_TTL_SECONDS,
    SERVICE_AUDIO_REPEAT_TTL_SECONDS,
    SESSION_DB_PATH,
    USER_MESSAGE_COALESCE_MAX_MS,
    USER_MESSAGE_COALESCE_MS,
)
from ..formatters.message_formatter import delay_seconds_from_ms


logger = logging.getLogger("agent")


RECENT_EVENT_IDS: Dict[str, float] = {}
RECENT_MESSAGE_KEYS: Dict[str, float] = {}
RECENT_POLL_SENT: Dict[str, float] = {}
RECENT_OUTBOUND_MESSAGE_IDS: Dict[str, float] = {}
RECENT_AUDIO_SENT_CHATS: Dict[str, float] = {}
RECENT_SERVICE_AUDIO_KEYS: Dict[str, float] = {}
LAST_SCHEDULE_OPTIONS: Dict[str, Dict[str, Any]] = {}
PENDING_USER_MESSAGES: Dict[str, Dict[str, Any]] = {}
PENDING_USER_LOCKS: Dict[str, Any] = {}
CHAT_ACTIVE_TURN: Dict[str, int] = {}
UNSET = object()


def is_duplicate_key(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> bool:
    if not key:
        return False
    now = time.time()
    if len(store) > 5000:
        store.clear()
    expired = [k for k, ts in store.items() if now - ts > ttl_seconds]
    for item in expired:
        store.pop(item, None)
    if key in store:
        return True
    store[key] = now
    return False


def has_recent_key(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> bool:
    if not key:
        return False
    now = time.time()
    if len(store) > 5000:
        store.clear()
    expired = [k for k, ts in store.items() if now - ts > ttl_seconds]
    for item in expired:
        store.pop(item, None)
    return key in store


def remember_recent_key(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> None:
    if not key:
        return
    is_duplicate_key(store, key, ttl_seconds)


def remember_recent_audio_sent(chat_id: str) -> None:
    if not chat_id:
        return
    remember_recent_key(RECENT_AUDIO_SENT_CHATS, str(chat_id), 45)


def has_recent_audio_sent(chat_id: str) -> bool:
    if not chat_id:
        return False
    return has_recent_key(RECENT_AUDIO_SENT_CHATS, str(chat_id), 45)


def service_audio_key(chat_id: str, filename: str) -> str:
    return f"{str(chat_id or '').strip()}::{str(filename or '').strip()}"


def remember_service_audio_sent(chat_id: str, filename: str) -> None:
    key = service_audio_key(chat_id, filename)
    if not key.strip(":"):
        return
    remember_recent_key(
        RECENT_SERVICE_AUDIO_KEYS,
        key,
        max(SERVICE_AUDIO_REPEAT_TTL_SECONDS, 60),
    )


def has_recent_service_audio_sent(chat_id: str, filename: str) -> bool:
    key = service_audio_key(chat_id, filename)
    if not key.strip(":"):
        return False
    return has_recent_key(
        RECENT_SERVICE_AUDIO_KEYS,
        key,
        max(SERVICE_AUDIO_REPEAT_TTL_SECONDS, 60),
    )


def next_chat_turn(chat_id: str) -> int:
    key = str(chat_id or "").strip()
    if not key:
        return 0
    if len(CHAT_ACTIVE_TURN) > 5000:
        CHAT_ACTIVE_TURN.clear()
    turn = (CHAT_ACTIVE_TURN.get(key) or 0) + 1
    CHAT_ACTIVE_TURN[key] = turn
    return turn


def is_chat_turn_current(chat_id: str, turn: Optional[int]) -> bool:
    if not turn:
        return True
    key = str(chat_id or "").strip()
    if not key:
        return True
    return (CHAT_ACTIVE_TURN.get(key) or 0) == turn


def is_duplicate_key_db(key: Optional[str], ttl_seconds: int) -> bool:
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


def is_duplicate_key_global(store: Dict[str, float], key: Optional[str], ttl_seconds: int) -> bool:
    memory_dup = is_duplicate_key(store, key, ttl_seconds)
    db_dup = is_duplicate_key_db(key, DEDUP_DB_TTL_SECONDS)
    return memory_dup or db_dup


def get_session(session_id: str) -> SQLiteSession:
    return SQLiteSession(session_id, SESSION_DB_PATH)


def init_profile_state_db() -> None:
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


def get_profile_state(chat_id: str) -> Dict[str, Any]:
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


def update_profile_state(
    chat_id: str,
    profile_id: Any = UNSET,
    poll_id: Any = UNSET,
    pending_message: Any = UNSET,
    flow_state: Any = UNSET,
    flow_data: Any = UNSET,
) -> None:
    if not chat_id:
        return
    state = get_profile_state(chat_id)

    def coerce_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    def coerce_json_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text or None
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return json.dumps({"value": str(value)}, ensure_ascii=False)

    if profile_id is not UNSET:
        state["profile_id"] = coerce_text(profile_id)
    if poll_id is not UNSET:
        state["poll_id"] = coerce_text(poll_id)
    if pending_message is not UNSET:
        state["pending_message"] = coerce_text(pending_message)
    if flow_state is not UNSET:
        state["flow_state"] = coerce_text(flow_state)
    if flow_data is not UNSET:
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
                coerce_json_text(state.get("flow_data")),
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


def clear_profile_state(chat_id: str) -> None:
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


def store_schedule_options(
    chat_id: str,
    options: list[str],
    details: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    if not chat_id or not options:
        return
    LAST_SCHEDULE_OPTIONS[chat_id] = {
        "options": options,
        "details": details or {},
        "ts": time.time(),
    }


def get_schedule_options(chat_id: str) -> list[str]:
    entry = LAST_SCHEDULE_OPTIONS.get(chat_id)
    if not entry:
        return []
    if time.time() - entry.get("ts", 0) > 6 * 60 * 60:
        LAST_SCHEDULE_OPTIONS.pop(chat_id, None)
        return []
    return list(entry.get("options") or [])


def get_schedule_option_details(chat_id: str, option: str) -> Dict[str, Any]:
    entry = LAST_SCHEDULE_OPTIONS.get(chat_id)
    if not entry:
        return {}
    if time.time() - entry.get("ts", 0) > 6 * 60 * 60:
        LAST_SCHEDULE_OPTIONS.pop(chat_id, None)
        return {}
    details = entry.get("details")
    if not isinstance(details, dict):
        return {}
    payload = details.get(option)
    return dict(payload) if isinstance(payload, dict) else {}


def set_pending_signal_booking(chat_id: str, option: str, profile_id: str) -> None:
    if not chat_id:
        return
    update_profile_state(
        chat_id,
        flow_state="awaiting_deposit_proof",
        flow_data={
            "pending_slot": option,
            "pending_since": int(time.time()),
            "profile_id": profile_id,
        },
    )


def peek_pending_signal_booking(chat_id: str) -> Optional[Dict[str, str]]:
    if not chat_id:
        return None
    state = get_profile_state(chat_id)
    if (state.get("flow_state") or "") != "awaiting_deposit_proof":
        return None
    flow_data = state.get("flow_data") if isinstance(state.get("flow_data"), dict) else {}
    option = str(flow_data.get("pending_slot") or "").strip()
    if not option:
        update_profile_state(chat_id, flow_state=None, flow_data={})
        return None
    pending_since = int(flow_data.get("pending_since") or 0)
    if pending_since and (time.time() - pending_since > PENDING_SIGNAL_TTL_SECONDS):
        update_profile_state(chat_id, flow_state=None, flow_data={})
        return None
    profile_id = str(flow_data.get("profile_id") or state.get("profile_id") or "").strip()
    return {"option": option, "profile_id": profile_id}


def consume_pending_signal_booking(chat_id: str) -> Optional[Dict[str, str]]:
    entry = peek_pending_signal_booking(chat_id)
    if entry is None:
        return None
    update_profile_state(chat_id, flow_state=None, flow_data={})
    return entry


def get_pending_lock(chat_id: str) -> Any:
    lock = PENDING_USER_LOCKS.get(chat_id)
    if lock is None:
        lock = anyio.Lock()
        PENDING_USER_LOCKS[chat_id] = lock
    return lock


def coalesce_delay_seconds() -> float:
    return delay_seconds_from_ms(
        USER_MESSAGE_COALESCE_MS,
        default_ms=800,
        min_ms=0,
        max_ms=5000,
    )


def coalesce_max_wait_seconds() -> float:
    window = coalesce_delay_seconds()
    max_wait = delay_seconds_from_ms(
        USER_MESSAGE_COALESCE_MAX_MS,
        default_ms=2500,
        min_ms=0,
        max_ms=12000,
    )
    if window > 0 and max_wait > 0 and max_wait < window:
        return window
    return max_wait


async def coalesce_user_message(chat_id: str, text: str, is_audio: bool) -> Optional[tuple[str, bool]]:
    window = coalesce_delay_seconds()
    if window <= 0 or not chat_id:
        return (text, is_audio)
    max_wait = coalesce_max_wait_seconds()
    if max_wait <= 0:
        max_wait = window

    lock = get_pending_lock(chat_id)
    now = time.time()
    async with lock:
        state = PENDING_USER_MESSAGES.get(chat_id)
        if not state:
            state = {
                "messages": [],
                "last": now,
                "first": now,
                "collector": False,
                "has_audio": False,
            }
            PENDING_USER_MESSAGES[chat_id] = state
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
            state = PENDING_USER_MESSAGES.get(chat_id)
            if not state:
                return (text, is_audio)
            elapsed = time.time() - state.get("last", now)
            total = time.time() - state.get("first", now)
            if elapsed >= window or total >= max_wait:
                messages = state.get("messages") or []
                has_audio = bool(state.get("has_audio"))
                PENDING_USER_MESSAGES.pop(chat_id, None)
                combined = "\n".join(
                    message.strip() for message in messages if message and message.strip()
                )
                return (combined or text, has_audio)


init_profile_state_db()
