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
from ..core.profiles import profile_tts_fallback_to_text, profile_uses_tts_audio_reply
from .audio_service import maybe_send_profile_audio
from .routing_service import resolve_profile_for_chat
from .scheduling_service import reply_contains_schedule_options
from .tts_audio_service import send_tts_audio_reply


logger = logging.getLogger("agent")

# WhatsApp aceita textos longos, mas mantemos uma margem de seguranca para nao
# estourar limites de UI/cliente. Mensagens de atendente humano sao entregues
# verbatim e so quebram se ultrapassarem este teto.
_HUMAN_MESSAGE_MAX_CHARS = 4000


def _chunk_human_message(text: str, *, limit: int = _HUMAN_MESSAGE_MAX_CHARS) -> list[str]:
    text = str(text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        window = remaining[:limit]
        split_at = window.rfind("\n")
        if split_at < limit // 2:
            split_at = window.rfind(" ")
        if split_at < limit // 2:
            split_at = limit
        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].strip()
    if remaining:
        chunks.append(remaining)
    return chunks


async def send_human_agent_message(chat_id: str, text: str) -> bool:
    """Entrega a resposta de um atendente humano (vinda do Chatwoot) no WhatsApp.

    Diferente do caminho da IA, NAO reaplicamos as regras de fatiamento por
    perfil nem os delays de digitacao: o atendente digitou exatamente o que quer
    enviar. So quebramos se a mensagem ultrapassar o teto de seguranca, e nao
    espelhamos de volta no Chatwoot (a mensagem ja existe la).
    """
    parts = _chunk_human_message(text)
    if not parts:
        return True
    for part in parts:
        try:
            await send_text(chat_id, part, preview_seconds=0)
        except Exception as exc:
            logger.warning(
                "Human agent message delivery failed chat=%s: %s", chat_id, exc
            )
            return False
    return True


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
    if profile_uses_tts_audio_reply(profile_id):
        try:
            sent_audio = await send_tts_audio_reply(
                chat_id=chat_id,
                text=text,
                profile_id=profile_id,
                active_turn=active_turn,
            )
        except Exception as exc:
            logger.warning("TTS audio reply failed chat=%s profile=%s: %s", chat_id, profile_id, exc)
            sent_audio = False
        if sent_audio:
            return True
        if not profile_tts_fallback_to_text(profile_id):
            return False

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
