import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional


logger = logging.getLogger("agent.chatwoot")


@dataclass
class ChatwootMapping:
    whatsapp_chat_id: str
    phone: str = ""
    contact_name: str = ""
    contact_id: Optional[int] = None
    contact_source_id: str = ""
    conversation_id: Optional[int] = None
    identifier: str = ""
    updated_at: int = 0


class ChatwootStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path or "chatwoot_state.db"
        self._ensure_tables()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_tables(self) -> None:
        directory = os.path.dirname(self.db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chatwoot_mappings (
                    whatsapp_chat_id TEXT PRIMARY KEY,
                    phone TEXT NOT NULL DEFAULT '',
                    contact_name TEXT NOT NULL DEFAULT '',
                    contact_id INTEGER,
                    contact_source_id TEXT NOT NULL DEFAULT '',
                    conversation_id INTEGER UNIQUE,
                    identifier TEXT NOT NULL DEFAULT '',
                    updated_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chatwoot_processed_messages (
                    message_id TEXT PRIMARY KEY,
                    processed_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_chatwoot_mappings_conversation_id
                ON chatwoot_mappings(conversation_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_chatwoot_processed_messages_processed_at
                ON chatwoot_processed_messages(processed_at)
                """
            )

    def get_by_chat_id(self, chat_id: str) -> Optional[ChatwootMapping]:
        if not chat_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    whatsapp_chat_id,
                    phone,
                    contact_name,
                    contact_id,
                    contact_source_id,
                    conversation_id,
                    identifier,
                    updated_at
                FROM chatwoot_mappings
                WHERE whatsapp_chat_id = ?
                """,
                (chat_id,),
            ).fetchone()
        return self._row_to_mapping(row)

    def get_by_conversation_id(self, conversation_id: int) -> Optional[ChatwootMapping]:
        if not conversation_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    whatsapp_chat_id,
                    phone,
                    contact_name,
                    contact_id,
                    contact_source_id,
                    conversation_id,
                    identifier,
                    updated_at
                FROM chatwoot_mappings
                WHERE conversation_id = ?
                """,
                (conversation_id,),
            ).fetchone()
        return self._row_to_mapping(row)

    def upsert_mapping(self, mapping: ChatwootMapping) -> ChatwootMapping:
        if not mapping.whatsapp_chat_id:
            raise ValueError("whatsapp_chat_id is required")
        now = int(time.time())
        mapping.updated_at = now
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO chatwoot_mappings (
                    whatsapp_chat_id,
                    phone,
                    contact_name,
                    contact_id,
                    contact_source_id,
                    conversation_id,
                    identifier,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(whatsapp_chat_id) DO UPDATE SET
                    phone = excluded.phone,
                    contact_name = excluded.contact_name,
                    contact_id = excluded.contact_id,
                    contact_source_id = excluded.contact_source_id,
                    conversation_id = excluded.conversation_id,
                    identifier = excluded.identifier,
                    updated_at = excluded.updated_at
                """,
                (
                    mapping.whatsapp_chat_id,
                    mapping.phone or "",
                    mapping.contact_name or "",
                    mapping.contact_id,
                    mapping.contact_source_id or "",
                    mapping.conversation_id,
                    mapping.identifier or "",
                    now,
                ),
            )
        return mapping

    def clear_mapping(self, chat_id: str) -> None:
        if not chat_id:
            return
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM chatwoot_mappings WHERE whatsapp_chat_id = ?",
                (chat_id,),
            )

    def is_processed_message(self, message_id: str) -> bool:
        if not message_id:
            return False
        self.prune_processed_messages()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM chatwoot_processed_messages WHERE message_id = ?",
                (message_id,),
            ).fetchone()
        return row is not None

    def mark_processed_message(self, message_id: str) -> None:
        if not message_id:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO chatwoot_processed_messages (message_id, processed_at)
                VALUES (?, ?)
                ON CONFLICT(message_id) DO UPDATE SET
                    processed_at = excluded.processed_at
                """,
                (message_id, int(time.time())),
            )

    def prune_processed_messages(self, ttl_seconds: int = 7 * 24 * 60 * 60) -> None:
        cutoff = int(time.time()) - max(ttl_seconds, 0)
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM chatwoot_processed_messages WHERE processed_at < ?",
                (cutoff,),
            )

    @staticmethod
    def _row_to_mapping(row: Optional[sqlite3.Row]) -> Optional[ChatwootMapping]:
        if row is None:
            return None
        return ChatwootMapping(
            whatsapp_chat_id=str(row["whatsapp_chat_id"] or ""),
            phone=str(row["phone"] or ""),
            contact_name=str(row["contact_name"] or ""),
            contact_id=row["contact_id"],
            contact_source_id=str(row["contact_source_id"] or ""),
            conversation_id=row["conversation_id"],
            identifier=str(row["identifier"] or ""),
            updated_at=int(row["updated_at"] or 0),
        )
