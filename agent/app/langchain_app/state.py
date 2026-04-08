import sqlite3
import threading
import time
from typing import Any, Dict, Optional


class ProfileStateStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path or "profile_state_lc.db"
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS profile_state (
                    chat_id TEXT PRIMARY KEY,
                    profile_id TEXT,
                    poll_id TEXT,
                    pending_message TEXT,
                    updated_at REAL
                )
                """
            )
            conn.commit()

    def get_state(self, chat_id: str) -> Dict[str, Any]:
        if not chat_id:
            return {}
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT profile_id, poll_id, pending_message FROM profile_state WHERE chat_id = ?",
                    (chat_id,),
                )
                row = cur.fetchone()
        if not row:
            return {}
        return {"profile_id": row[0], "poll_id": row[1], "pending_message": row[2]}

    def update_state(
        self,
        chat_id: str,
        profile_id: Optional[str] = None,
        poll_id: Optional[str] = None,
        pending_message: Optional[str] = None,
    ) -> None:
        if not chat_id:
            return
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO profile_state (chat_id, profile_id, poll_id, pending_message, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        profile_id=excluded.profile_id,
                        poll_id=excluded.poll_id,
                        pending_message=excluded.pending_message,
                        updated_at=excluded.updated_at
                    """,
                    (chat_id, profile_id, poll_id, pending_message, time.time()),
                )
                conn.commit()

    def clear_state(self, chat_id: str) -> None:
        if not chat_id:
            return
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM profile_state WHERE chat_id = ?", (chat_id,))
                conn.commit()
