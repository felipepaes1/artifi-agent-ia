import logging
from typing import Any, Dict, Optional

from ..config.settings import SESSION_MAX_ITEMS, SUPABASE_APP
from ..integrations.supabase import supabase_fetch_recent, supabase_insert
from ..integrations.supabase_agent import record_turn
from ..integrations.waha import name_from_payload, normalize_phone


logger = logging.getLogger("agent")


async def hydrate_session_from_supabase(session, chat_id: str) -> None:
    phone = normalize_phone(chat_id)
    rows = await supabase_fetch_recent(phone, chat_id)
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


async def trim_session(session, max_items: int = SESSION_MAX_ITEMS) -> None:
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


async def reset_session(session) -> None:
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


async def log_conversation(
    chat_id: str,
    payload: Dict[str, Any],
    user_message: str,
    bot_message: str,
    message_type: str,
    profile_id: Optional[str] = None,
) -> None:
    if not user_message and not bot_message:
        return
    phone = normalize_phone(chat_id)
    user_name = name_from_payload(payload)
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
    await supabase_insert(row)

    if profile_id:
        try:
            await record_turn(
                tenant_slug=profile_id,
                phone=phone or chat_id,
                wa_chat_id=chat_id,
                display_name=user_name,
                user_message=user_message or None,
                bot_message=bot_message or None,
                message_type=message_type,
            )
        except Exception as exc:
            logger.warning("Dual-write to agent schema failed: %s", exc)

