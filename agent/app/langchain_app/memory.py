import os
from typing import Callable

from .config import Settings


def _sqlite_connection_string(path: str) -> str:
    if not path:
        path = "sessions_lc.db"
    abs_path = os.path.abspath(path)
    return f"sqlite:///{abs_path}"


def build_message_history_factory(settings: Settings) -> Callable[[str], object]:
    redis_url = os.getenv("REDIS_URL", "").strip()

    if redis_url:
        try:
            from langchain_community.chat_message_histories import RedisChatMessageHistory
        except Exception as exc:  # pragma: no cover
            raise RuntimeError("Redis chat history requested but langchain_community is missing") from exc

        def _get_history(session_id: str) -> object:
            return RedisChatMessageHistory(session_id=session_id, url=redis_url)

        return _get_history

    try:
        from langchain_community.chat_message_histories import SQLChatMessageHistory
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("SQL chat history requires langchain_community") from exc

    conn_string = _sqlite_connection_string(settings.session_db)

    def _get_history(session_id: str) -> object:
        return SQLChatMessageHistory(session_id=session_id, connection_string=conn_string)

    return _get_history
