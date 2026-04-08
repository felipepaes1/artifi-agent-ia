import logging
from typing import Optional

import anyio

from ..chatwoot_integration import get_chatwoot_service
from ..formatters.message_formatter import (
    first_message_delay_seconds,
    message_delay_seconds,
    schedule_delay_seconds,
    split_messages,
)
from ..integrations.waha import normalize_phone, send_text, typing_preview_seconds_for_text
from .audio_service import maybe_send_profile_audio
from .routing_service import resolve_profile_for_chat
from .scheduling_service import reply_contains_schedule_options


logger = logging.getLogger("agent")


def log_webhook_debug(logger_enabled: bool, logger_obj, stage: str, data: dict) -> None:
    if not logger_enabled:
        return
    logger_obj.info("WebhookDebug %s: %s", stage, data)


async def send_text_parts(
    chat_id: str,
    text: str,
    *,
    active_turn: Optional[int] = None,
    is_chat_turn_current,
    log_debug,
) -> bool:
    profile_id = resolve_profile_for_chat(str(chat_id))
    parts = split_messages(text, profile_id)
    if not parts:
        return True
    chatwoot_service = get_chatwoot_service()
    log_debug(
        "send_text",
        {
            "chat_id": str(chat_id),
            "parts": len(parts),
            "text_len": len(text or ""),
            "turn": active_turn,
        },
    )
    delay = message_delay_seconds(profile_id)
    first_delay = first_message_delay_seconds(profile_id)
    if reply_contains_schedule_options(text):
        delay = min(delay, schedule_delay_seconds(profile_id))
    for idx, part in enumerate(parts):
        if not is_chat_turn_current(str(chat_id), active_turn):
            log_debug(
                "send_text_aborted_stale_turn",
                {
                    "chat_id": str(chat_id),
                    "turn": active_turn,
                    "idx": idx,
                    "phase": "before_wait",
                },
            )
            return False
        if idx == 0:
            wait = first_delay
        else:
            wait = delay
            if len(part) <= 120:
                wait = min(wait, 0.2)
        preview_seconds = typing_preview_seconds_for_text(part)
        wait_before_typing = max(wait - preview_seconds, 0)
        if wait_before_typing:
            await anyio.sleep(wait_before_typing)
        if not is_chat_turn_current(str(chat_id), active_turn):
            log_debug(
                "send_text_aborted_stale_turn",
                {
                    "chat_id": str(chat_id),
                    "turn": active_turn,
                    "idx": idx,
                    "phase": "before_send",
                },
            )
            return False
        sent_message_id = await send_text(chat_id, part, preview_seconds=preview_seconds)
        await chatwoot_service.sync_outgoing_whatsapp_message(
            chat_id=str(chat_id),
            phone=normalize_phone(str(chat_id)),
            contact_name="",
            content=part,
            message_id=sent_message_id,
        )
    return True


async def send_reply(
    chat_id: str,
    text: str,
    *,
    user_text: str = "",
    profile_id: Optional[str] = None,
    active_turn: Optional[int] = None,
    send_text_parts_fn,
    get_audio_bucket_for_profile,
    is_chat_turn_current,
) -> bool:
    sent = await send_text_parts_fn(chat_id, text, active_turn=active_turn)
    if not sent:
        return False
    if not user_text.strip():
        return True
    if not get_audio_bucket_for_profile(profile_id):
        return True
    if not is_chat_turn_current(str(chat_id), active_turn):
        return False
    try:
        await maybe_send_profile_audio(
            chat_id=chat_id,
            profile_id=profile_id,
            user_text=user_text,
            assistant_text=text,
            active_turn=active_turn,
        )
    except Exception as exc:
        logger.warning("Automatic audio fallback failed chat=%s profile=%s: %s", chat_id, profile_id, exc)
    return True
