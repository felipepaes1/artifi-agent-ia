import hashlib
import json
import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Request

from ..chatwoot_integration import get_chatwoot_service
from ..config.settings import (
    ALLOW_GROUPS,
    LOG_WEBHOOK_DEBUG,
    LOG_WEBHOOK_PAYLOADS,
    OPENAI_API_KEY,
    OUTBOUND_ECHO_TTL_SECONDS,
    POLL_THROTTLE_SECONDS,
    PROFILE_ROUTING_ENABLED,
    PROMPT_PROFILE,
    RECENT_EVENT_TTL_SECONDS,
    SESSION_MAX_ITEMS,
)
from ..core.profiles import (
    PROFILE_DEFAULT_ID,
    PROFILES,
    build_greeting,
    get_audio_bucket_for_profile,
    has_profile_greeting,
    resolve_profile_id_from_vote,
)
from ..core.state import (
    RECENT_EVENT_IDS,
    RECENT_MESSAGE_KEYS,
    RECENT_POLL_SENT,
    RECENT_OUTBOUND_MESSAGE_IDS,
    LAST_SCHEDULE_OPTIONS,
    clear_profile_state,
    coalesce_user_message,
    consume_pending_signal_booking,
    get_profile_state,
    get_session,
    has_recent_key,
    is_chat_turn_current,
    is_duplicate_key,
    is_duplicate_key_global,
    next_chat_turn,
    peek_pending_signal_booking,
    set_pending_signal_booking,
    update_profile_state,
)
from ..formatters.sanitizer import sanitize_plain_text, truncate
from ..integrations.waha import (
    extract_event_id,
    extract_media_url,
    extract_message_id,
    extract_timestamp,
    get_contact_name,
    is_audio_payload,
    is_from_me_payload,
    is_non_text_media,
    message_fingerprint,
    name_from_payload,
    normalize_phone,
    send_profile_poll,
    transcribe_audio,
)
from ..profiles.ariane.rules import is_ariane_context_from_items
from ..services.agent_service import (
    SCHEDULING_TOOL,
    extract_text_from_result,
    get_agent,
    log_empty_output_diagnostics,
    run_agent,
)
from ..services.audio_service import try_send_service_audio_for_message
from ..services.conversation_service import (
    hydrate_session_from_supabase,
    log_conversation,
    reset_session,
    trim_session,
)
from ..services.guardrail_service import enforce_scheduling_entity_guardrail
from ..services.urgency_guardrail import maybe_handle_urgency
from ..services.message_service import send_reply as send_reply_impl
from ..services.message_service import send_text_parts as send_text_parts_impl
from ..services.routing_service import is_ariane_profile, is_greeting_only, resolve_profile_for_chat, wants_profile_switch
from ..services.scheduling_service import (
    build_schedule_confirmation,
    build_signal_received_confirmation,
    confirm_mcp_schedule_option,
    get_booking_flow,
    inject_fake_schedule,
    looks_like_payment_confirmation,
    resolve_flow_profile_id,
    try_match_schedule_option,
    uses_mcp_scheduling,
)
from ..utils.text import short_hash


logger = logging.getLogger("agent")


def log_webhook_debug(stage: str, data: Dict[str, Any]) -> None:
    if not LOG_WEBHOOK_DEBUG:
        return
    logger.info("WebhookDebug %s: %s", stage, data)


async def send_text_parts(chat_id: str, text: str, active_turn: Optional[int] = None) -> bool:
    return await send_text_parts_impl(
        chat_id,
        text,
        active_turn=active_turn,
        is_chat_turn_current=is_chat_turn_current,
        log_debug=log_webhook_debug,
    )


async def send_reply(
    chat_id: str,
    text: str,
    *,
    user_text: str = "",
    profile_id: Optional[str] = None,
    active_turn: Optional[int] = None,
) -> bool:
    return await send_reply_impl(
        chat_id,
        text,
        user_text=user_text,
        profile_id=profile_id,
        active_turn=active_turn,
        send_text_parts_fn=send_text_parts,
        get_audio_bucket_for_profile=get_audio_bucket_for_profile,
        is_chat_turn_current=is_chat_turn_current,
    )


async def handle_poll_vote(data: Dict[str, Any]) -> Dict[str, Any]:
    payload = data.get("payload") or {}
    message = payload.get("message") or {}
    vote = payload.get("vote") or payload.get("pollVote") or message.get("vote") or {}
    poll = payload.get("poll") or message.get("poll") or payload.get("pollUpdate") or {}
    if poll.get("fromMe") is False:
        return {"ok": True, "ignored": "not_our_poll"}

    chat_id = (
        payload.get("chatId")
        or payload.get("from")
        or payload.get("to")
        or vote.get("from")
        or poll.get("to")
        or vote.get("to")
        or vote.get("chatId")
        or message.get("chatId")
    )
    if not chat_id:
        return {"ok": False, "error": "missing_chat_id"}

    vote_id = vote.get("id") or payload.get("voteId")
    if vote_id and is_duplicate_key_global(
        RECENT_EVENT_IDS, f"poll:{vote_id}", RECENT_EVENT_TTL_SECONDS
    ):
        return {"ok": True, "ignored": "duplicate_poll_vote"}

    state = get_profile_state(str(chat_id))
    poll_id = poll.get("id") or payload.get("pollId") or payload.get("poll_id") or message.get("pollId")
    if state.get("poll_id") and poll_id and state["poll_id"] != poll_id:
        if poll.get("fromMe") is not True:
            return {"ok": True, "ignored": "poll_id_mismatch"}

    selected_options = (
        vote.get("selectedOptions")
        or payload.get("selectedOptions")
        or vote.get("options")
        or vote.get("selectedOption")
        or vote.get("selectedOptionIds")
        or payload.get("selectedOptionIds")
        or payload.get("selectedOptionsIds")
        or []
    )
    normalized_options = []
    if isinstance(selected_options, list):
        for entry in selected_options:
            if isinstance(entry, dict):
                value = (
                    entry.get("name")
                    or entry.get("label")
                    or entry.get("title")
                    or entry.get("option")
                    or entry.get("value")
                    or entry.get("id")
                )
                if value is not None:
                    normalized_options.append(str(value))
            else:
                normalized_options.append(str(entry))
    elif selected_options:
        normalized_options.append(str(selected_options))
    if not vote_id:
        poll_id = poll.get("id") or payload.get("pollId") or payload.get("poll_id")
        vote_key = {
            "poll": poll_id,
            "from": vote.get("from") or payload.get("from"),
            "opts": normalized_options,
        }
        vote_digest = hashlib.sha1(json.dumps(vote_key, sort_keys=True).encode("utf-8")).hexdigest()
        if is_duplicate_key_global(
            RECENT_EVENT_IDS, f"pollh:{vote_digest}", RECENT_EVENT_TTL_SECONDS
        ):
            return {"ok": True, "ignored": "duplicate_poll_vote_hash"}
    active_turn = next_chat_turn(str(chat_id))
    if not normalized_options:
        await send_text_parts(
            str(chat_id),
            "Desculpe, nao consegui entender sua escolha. Vou reenviar a enquete, por favor escolha novamente. 🙂",
            active_turn=active_turn,
        )
        new_poll_id = await send_profile_poll(str(chat_id))
        update_profile_state(str(chat_id), poll_id=new_poll_id)
        return {"ok": True, "poll_vote_failed": True}
    profile_id = resolve_profile_id_from_vote(selected_options)
    if not profile_id or profile_id not in PROFILES:
        await send_text_parts(
            str(chat_id),
            "Nao consegui identificar o perfil selecionado. Vou reenviar a enquete.",
            active_turn=active_turn,
        )
        new_poll_id = await send_profile_poll(str(chat_id))
        update_profile_state(str(chat_id), poll_id=new_poll_id)
        return {"ok": True, "profile_missing": True}

    previous_profile = state.get("profile_id")
    if previous_profile and previous_profile != profile_id:
        await reset_session(get_session(str(chat_id)))

    pending_message = (state.get("pending_message") or "").strip()
    update_profile_state(str(chat_id), profile_id=profile_id, poll_id=None, pending_message=None)

    contact_name = await get_contact_name(str(chat_id))
    first_name = (contact_name or "").strip().split()[0] if contact_name else None
    greeting = build_greeting(first_name, profile_id)

    session = get_session(str(chat_id))
    agent = get_agent(profile_id)

    if pending_message:
        if is_greeting_only(pending_message):
            await send_reply(
                str(chat_id),
                greeting,
                profile_id=profile_id,
                active_turn=active_turn,
            )
            try:
                await session.add_items([{"role": "assistant", "content": greeting}])
            except Exception as exc:
                logger.warning("Failed to persist greeting item: %s", exc)
            await trim_session(session, SESSION_MAX_ITEMS)
            return {"ok": True, "profile_selected": profile_id, "handled_pending": True, "greeting_only": True}
        try:
            await session.add_items([{"role": "assistant", "content": greeting}])
        except Exception as exc:
            logger.warning("Failed to persist greeting item: %s", exc)
        await send_reply(
            str(chat_id),
            greeting,
            profile_id=profile_id,
            active_turn=active_turn,
        )
        service_audio = await try_send_service_audio_for_message(
            str(chat_id),
            profile_id,
            pending_message,
            active_turn=active_turn,
        )
        if service_audio:
            try:
                await session.add_items(
                    [
                        {"role": "user", "content": pending_message},
                        {"role": "assistant", "content": service_audio["session_note"]},
                    ]
                )
            except Exception as exc:
                logger.warning("Failed to persist service audio note: %s", exc)
            await log_conversation(
                str(chat_id),
                {},
                pending_message,
                f"{greeting}\n\n[{service_audio['session_note']}]",
                "audio_auto",
                profile_id=profile_id,
            )
            await trim_session(session, SESSION_MAX_ITEMS)
            return {
                "ok": True,
                "profile_selected": profile_id,
                "handled_pending": True,
                "service_audio_sent": service_audio["filename"],
            }
        urgency_reply = await maybe_handle_urgency(profile_id, pending_message, session)
        if urgency_reply is not None:
            reply = urgency_reply
        else:
            result = None
            try:
                result = await run_agent(agent, pending_message, session, str(chat_id), profile_id)
                reply = truncate(
                    sanitize_plain_text(extract_text_from_result(result), profile_id),
                    profile_id,
                )
            except Exception as exc:
                logger.exception("Agent run failed: %s", exc)
                reply = ""
            if not reply:
                log_empty_output_diagnostics(result, "pending_message")
                reply = "Desculpe, nao consegui responder agora."
                reply = inject_fake_schedule(str(chat_id), pending_message, reply, has_scheduling_tool=SCHEDULING_TOOL is not None)
            reply = enforce_scheduling_entity_guardrail(profile_id, pending_message, reply)
        combined = greeting if not reply else f"{greeting}\n\n{reply}"
        await send_reply(
            str(chat_id),
            combined,
            user_text=pending_message,
            profile_id=profile_id,
            active_turn=active_turn,
        )
        await trim_session(session, SESSION_MAX_ITEMS)
        return {"ok": True, "profile_selected": profile_id, "handled_pending": True}

    await send_reply(
        str(chat_id),
        greeting,
        profile_id=profile_id,
        active_turn=active_turn,
    )
    try:
        await session.add_items([{"role": "assistant", "content": greeting}])
    except Exception as exc:
        logger.warning("Failed to persist greeting item: %s", exc)
    await trim_session(session, SESSION_MAX_ITEMS)
    return {"ok": True, "profile_selected": profile_id, "handled_pending": False}


def build_waha_router() -> APIRouter:
    router = APIRouter()

    @router.post("/webhook/waha")
    async def waha_webhook(request: Request) -> Dict[str, Any]:
        data = await request.json()
        event = data.get("event")
        if LOG_WEBHOOK_PAYLOADS:
            logger.info("Webhook payload (%s): %s", event, data)
        if event == "poll.vote":
            return await handle_poll_vote(data)
        if event == "poll.vote.failed":
            return await handle_poll_vote(data)
        if event not in ("message", "message.any", "message.new"):
            return {"ok": True, "ignored": "event"}

        payload = data.get("payload") or {}
        event_id = extract_event_id(data, payload)
        msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
        message_id = extract_message_id(payload)
        chat_id = payload.get("from") or payload.get("chatId") or payload.get("to")
        from_me = is_from_me_payload(payload)
        raw_body = (payload.get("body") or payload.get("text") or "").strip()
        fingerprint = message_fingerprint(payload)
        log_webhook_debug(
            "received",
            {
                "event": event,
                "event_id": event_id,
                "msg_type": msg_type,
                "chat_id": str(chat_id) if chat_id else None,
                "message_id": message_id,
                "fingerprint": fingerprint,
                "from_me": from_me,
                "timestamp": extract_timestamp(payload),
                "body_len": len(raw_body),
                "body_hash": short_hash(raw_body),
            },
        )
        if is_duplicate_key_global(RECENT_EVENT_IDS, event_id, RECENT_EVENT_TTL_SECONDS):
            log_webhook_debug(
                "duplicate_event",
                {"event": event, "event_id": event_id, "chat_id": str(chat_id) if chat_id else None},
            )
            return {"ok": True, "ignored": "duplicate_event"}
        if msg_type in ("poll_vote", "pollvote", "poll_vote_event"):
            return await handle_poll_vote(data)
        if payload.get("poll") and (payload.get("vote") or payload.get("pollVote")):
            return await handle_poll_vote(data)
        if from_me:
            log_webhook_debug(
                "from_me",
                {
                    "event": event,
                    "event_id": event_id,
                    "chat_id": str(chat_id) if chat_id else None,
                    "message_id": message_id,
                },
            )
            return {"ok": True, "ignored": "fromMe"}
        if has_recent_key(
            RECENT_OUTBOUND_MESSAGE_IDS,
            message_id,
            OUTBOUND_ECHO_TTL_SECONDS,
        ):
            log_webhook_debug(
                "outbound_echo_message_id",
                {
                    "event": event,
                    "event_id": event_id,
                    "chat_id": str(chat_id) if chat_id else None,
                    "message_id": message_id,
                },
            )
            return {"ok": True, "ignored": "outbound_echo_message_id"}

        if not chat_id:
            return {"ok": False, "error": "missing chat_id"}

        if message_id:
            message_key = f"{chat_id}:{message_id}"
            if is_duplicate_key_global(
                RECENT_MESSAGE_KEYS, message_key, RECENT_EVENT_TTL_SECONDS
            ):
                log_webhook_debug(
                    "duplicate_message",
                    {
                        "message_key": message_key,
                        "event": event,
                        "event_id": event_id,
                        "chat_id": str(chat_id),
                    },
                )
                return {"ok": True, "ignored": "duplicate_message"}
        else:
            if is_duplicate_key_global(
                RECENT_MESSAGE_KEYS, fingerprint, RECENT_EVENT_TTL_SECONDS
            ):
                log_webhook_debug(
                    "duplicate_message_fallback",
                    {
                        "fingerprint": fingerprint,
                        "event": event,
                        "event_id": event_id,
                        "chat_id": str(chat_id),
                    },
                )
                return {"ok": True, "ignored": "duplicate_message_fallback"}

        if (not ALLOW_GROUPS) and str(chat_id).endswith("@g.us"):
            return {"ok": True, "ignored": "group"}

        if is_non_text_media(payload):
            active_turn = next_chat_turn(str(chat_id))
            pending_booking = consume_pending_signal_booking(str(chat_id))
            if pending_booking:
                reply = build_signal_received_confirmation(
                    pending_booking.get("option"),
                    pending_booking.get("profile_id"),
                    str(chat_id),
                )
                await send_reply(
                    chat_id,
                    reply,
                    profile_id=pending_booking.get("profile_id"),
                    active_turn=active_turn,
                )
                await log_conversation(
                    str(chat_id),
                    payload,
                    "[comprovante_pix_midia]",
                    reply,
                    msg_type or "media",
                    profile_id=pending_booking.get("profile_id"),
                )
                return {"ok": True, "signal_confirmed": True}
            reply = "Consigo acessar apenas mensagens de texto e audio. Pode enviar em texto ou audio, por favor?"
            await send_text_parts(chat_id, reply, active_turn=active_turn)
            return {"ok": True, "ignored": "non_text_media"}

        is_audio = is_audio_payload(payload)
        body = (payload.get("body") or "").strip()
        if is_audio:
            media_url = extract_media_url(payload)
            if not media_url:
                active_turn = next_chat_turn(str(chat_id))
                reply = "Consigo ouvir audios, mas nao consegui acessar esse. Pode reenviar, por favor?"
                await send_text_parts(chat_id, reply, active_turn=active_turn)
                return {"ok": True, "ignored": "missing_audio_url"}
            transcription = await transcribe_audio(media_url, payload)
            if not transcription:
                active_turn = next_chat_turn(str(chat_id))
                reply = "Nao consegui transcrever o audio. Pode reenviar ou mandar em texto?"
                await send_text_parts(chat_id, reply, active_turn=active_turn)
                return {"ok": True, "ignored": "transcription_failed"}
            body = transcription

        if not body:
            return {"ok": True, "ignored": "empty"}

        pre_coalesce_profile_id: Optional[str] = None
        if PROFILE_ROUTING_ENABLED:
            pre_coalesce_profile_id = (get_profile_state(str(chat_id)).get("profile_id") or "").strip() or None
        else:
            pre_coalesce_profile_id = PROMPT_PROFILE or PROFILE_DEFAULT_ID or None
        coalesced = await coalesce_user_message(
            str(chat_id),
            body,
            is_audio,
            profile_id=pre_coalesce_profile_id,
        )
        if coalesced is None:
            return {"ok": True, "queued": True}
        body, is_audio = coalesced
        await get_chatwoot_service().sync_incoming_whatsapp_message(
            chat_id=str(chat_id),
            phone=normalize_phone(str(chat_id)),
            contact_name=name_from_payload(payload) or "",
            content=body,
            message_id=message_id or fingerprint or "",
        )
        active_turn = next_chat_turn(str(chat_id))
        log_webhook_debug(
            "coalesced",
            {
                "chat_id": str(chat_id),
                "body_len": len(body or ""),
                "body_hash": short_hash(body or ""),
                "is_audio": is_audio,
            },
        )

        profile_id: Optional[str] = None
        if PROFILE_ROUTING_ENABLED:
            if wants_profile_switch(body):
                clear_profile_state(str(chat_id))
                await reset_session(get_session(str(chat_id)))
                poll_id = await send_profile_poll(str(chat_id))
                update_profile_state(str(chat_id), poll_id=poll_id, pending_message=None)
                return {"ok": True, "profile_switch": True}

            state = get_profile_state(str(chat_id))
            profile_id = state.get("profile_id")
            if not profile_id:
                if state.get("poll_id"):
                    update_profile_state(str(chat_id), pending_message=body)
                    await send_text_parts(
                        str(chat_id),
                        "Para continuar, escolha um perfil na enquete acima, por favor.",
                        active_turn=active_turn,
                    )
                    return {"ok": True, "awaiting_poll": True}
                if is_duplicate_key(RECENT_POLL_SENT, str(chat_id), POLL_THROTTLE_SECONDS):
                    update_profile_state(str(chat_id), pending_message=body)
                    await send_text_parts(
                        str(chat_id),
                        "Ja enviei a enquete acima. Pode escolher um perfil para continuarmos, por favor?",
                        active_turn=active_turn,
                    )
                    return {"ok": True, "poll_throttled": True}
                poll_id = await send_profile_poll(str(chat_id))
                update_profile_state(str(chat_id), poll_id=poll_id, pending_message=body)
                if poll_id:
                    return {"ok": True, "poll_sent": True}
                profile_id = PROFILE_DEFAULT_ID or PROMPT_PROFILE or None
                update_profile_state(str(chat_id), profile_id=profile_id, poll_id=None, pending_message=None)

        if not PROFILE_ROUTING_ENABLED:
            profile_id = PROMPT_PROFILE or PROFILE_DEFAULT_ID or None

        if not OPENAI_API_KEY:
            raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

        session = get_session(str(chat_id))
        try:
            items = await session.get_items()
        except Exception as exc:
            logger.warning("Failed to load session items: %s", exc)
            items = []

        if not items:
            await hydrate_session_from_supabase(session, str(chat_id))
            try:
                items = await session.get_items()
            except Exception as exc:
                logger.warning("Failed to reload session items: %s", exc)
                items = []

        if SESSION_MAX_ITEMS > 0 and len(items) > SESSION_MAX_ITEMS:
            await trim_session(session, SESSION_MAX_ITEMS)
            try:
                items = await session.get_items()
            except Exception as exc:
                logger.warning("Failed to reload trimmed session items: %s", exc)
                items = items[-SESSION_MAX_ITEMS:]

        if not has_profile_greeting(items, profile_id):
            payload_name = name_from_payload(payload)
            contact_name = payload_name or await get_contact_name(str(chat_id))
            first_name = (contact_name or "").strip().split()[0] if contact_name else None
            greeting = build_greeting(first_name, profile_id)
            if is_greeting_only(body):
                try:
                    await session.add_items([{"role": "assistant", "content": greeting}])
                except Exception as exc:
                    logger.warning("Failed to persist greeting item: %s", exc)
                await send_reply(
                    chat_id,
                    greeting,
                    profile_id=profile_id,
                    active_turn=active_turn,
                )
                await log_conversation(
                    str(chat_id),
                    payload,
                    body,
                    greeting,
                    "audio" if is_audio else "text",
                    profile_id=profile_id,
                )
                await trim_session(session, SESSION_MAX_ITEMS)
                return {"ok": True, "greeted": True, "greeting_only": True}

            try:
                await session.add_items([{"role": "assistant", "content": greeting}])
            except Exception as exc:
                logger.warning("Failed to persist greeting item: %s", exc)
            await send_reply(
                chat_id,
                greeting,
                profile_id=profile_id,
                active_turn=active_turn,
            )
            service_audio = await try_send_service_audio_for_message(
                chat_id,
                profile_id,
                body,
                active_turn=active_turn,
            )
            if service_audio:
                try:
                    await session.add_items(
                        [
                            {"role": "user", "content": body},
                            {"role": "assistant", "content": service_audio["session_note"]},
                        ]
                    )
                except Exception as exc:
                    logger.warning("Failed to persist service audio note: %s", exc)
                await log_conversation(
                    str(chat_id),
                    payload,
                    body,
                    f"{greeting}\n\n[{service_audio['session_note']}]",
                    "audio_auto",
                    profile_id=profile_id,
                )
                await trim_session(session, SESSION_MAX_ITEMS)
                return {
                    "ok": True,
                    "greeted": True,
                    "service_audio_sent": service_audio["filename"],
                }
            urgency_reply = await maybe_handle_urgency(profile_id, body, session)
            if urgency_reply is not None:
                reply = urgency_reply
            else:
                result = None
                try:
                    agent = get_agent(profile_id)
                    result = await run_agent(agent, body, session, str(chat_id), profile_id)
                    reply = truncate(
                        sanitize_plain_text(extract_text_from_result(result), profile_id),
                        profile_id,
                    )
                except Exception as exc:
                    logger.exception("Agent run failed: %s", exc)
                    reply = ""
                if not reply:
                    log_empty_output_diagnostics(result, "first_turn_after_greeting")
                    reply = "Desculpe, nao consegui responder agora."
                reply = enforce_scheduling_entity_guardrail(profile_id, body, reply)
                reply = inject_fake_schedule(str(chat_id), body, reply, has_scheduling_tool=SCHEDULING_TOOL is not None)
            combined = f"{greeting}\n\n{reply}"
            await send_reply(
                chat_id,
                combined,
                user_text=body,
                profile_id=profile_id,
                active_turn=active_turn,
            )
            await log_conversation(
                str(chat_id),
                payload,
                body,
                combined,
                "audio" if is_audio else "text",
                profile_id=profile_id,
            )
            await trim_session(session, SESSION_MAX_ITEMS)
            return {"ok": True, "greeted": True, "answered": True}

        pending_booking = peek_pending_signal_booking(str(chat_id))
        if pending_booking and looks_like_payment_confirmation(body):
            confirmed_booking = consume_pending_signal_booking(str(chat_id)) or pending_booking
            reply = build_signal_received_confirmation(
                confirmed_booking.get("option"),
                confirmed_booking.get("profile_id"),
                str(chat_id),
            )
            try:
                await session.add_items(
                    [
                        {"role": "user", "content": body},
                        {"role": "assistant", "content": reply},
                    ]
                )
            except Exception as exc:
                logger.warning("Failed to persist signal confirmation: %s", exc)
            await send_reply(
                chat_id,
                reply,
                profile_id=profile_id,
                active_turn=active_turn,
            )
            await log_conversation(
                str(chat_id),
                payload,
                body,
                reply,
                "audio" if is_audio else "text",
                profile_id=profile_id,
            )
            await trim_session(session, SESSION_MAX_ITEMS)
            return {"ok": True, "signal_confirmed_text": True}

        schedule_choice = try_match_schedule_option(str(chat_id), body)
        if schedule_choice:
            is_ariane_flow = is_ariane_profile(profile_id, str(chat_id))
            if not is_ariane_flow and not profile_id:
                is_ariane_flow = is_ariane_context_from_items(items, body)
            flow_profile_id = resolve_flow_profile_id(
                profile_id,
                str(chat_id),
                force_ariane=is_ariane_flow,
            )
            flow_config = get_booking_flow(
                profile_id,
                str(chat_id),
                force_ariane=is_ariane_flow,
            )
            if uses_mcp_scheduling(
                profile_id,
                str(chat_id),
                force_ariane=is_ariane_flow,
            ):
                confirmation = await confirm_mcp_schedule_option(
                    schedule_choice,
                    profile_id=profile_id,
                    chat_id=str(chat_id),
                    phone=normalize_phone(str(chat_id)),
                    force_ariane=is_ariane_flow,
                )
                LAST_SCHEDULE_OPTIONS.pop(str(chat_id), None)
                if confirmation.get("status") == "confirmed":
                    reply = build_signal_received_confirmation(
                        schedule_choice,
                        profile_id,
                        str(chat_id),
                        force_ariane=is_ariane_flow,
                    )
                else:
                    logger.warning(
                        "Failed to confirm MCP schedule option chat_id=%s option=%s details=%s",
                        chat_id,
                        schedule_choice,
                        confirmation,
                    )
                    reply = (
                        "Nao consegui confirmar esse horario agora porque ele pode ter acabado de ser ocupado. "
                        "Posso consultar outras opcoes para voce?"
                    )
            else:
                LAST_SCHEDULE_OPTIONS.pop(str(chat_id), None)
                reply = build_schedule_confirmation(
                    schedule_choice,
                    body,
                    profile_id,
                    str(chat_id),
                    force_ariane=is_ariane_flow,
                )
                if flow_config is not None and flow_config.requires_deposit:
                    set_pending_signal_booking(str(chat_id), schedule_choice, flow_profile_id)
            try:
                await session.add_items(
                    [
                        {"role": "user", "content": body},
                        {"role": "assistant", "content": reply},
                    ]
                )
            except Exception as exc:
                logger.warning("Failed to persist schedule confirmation: %s", exc)
            await send_reply(
                chat_id,
                reply,
                profile_id=profile_id,
                active_turn=active_turn,
            )
            await log_conversation(
                str(chat_id),
                payload,
                body,
                reply,
                "audio" if is_audio else "text",
                profile_id=profile_id,
            )
            await trim_session(session, SESSION_MAX_ITEMS)
            return {"ok": True, "schedule_confirmed": True}

        service_audio = await try_send_service_audio_for_message(
            chat_id,
            profile_id,
            body,
            active_turn=active_turn,
        )
        if service_audio:
            try:
                await session.add_items(
                    [
                        {"role": "user", "content": body},
                        {"role": "assistant", "content": service_audio["session_note"]},
                    ]
                )
            except Exception as exc:
                logger.warning("Failed to persist service audio note: %s", exc)
            await log_conversation(
                str(chat_id),
                payload,
                body,
                f"[{service_audio['session_note']}]",
                "audio_auto",
                profile_id=profile_id,
            )
            await trim_session(session, SESSION_MAX_ITEMS)
            return {"ok": True, "service_audio_sent": service_audio["filename"]}

        urgency_reply = await maybe_handle_urgency(profile_id, body, session)
        if urgency_reply is not None:
            reply = urgency_reply
        else:
            try:
                agent = get_agent(profile_id)
                result = await run_agent(agent, body, session, str(chat_id), profile_id)
            except Exception as exc:
                logger.exception("Agent run failed: %s", exc)
                raise HTTPException(status_code=502, detail="Agent run failed") from exc

            reply = truncate(
                sanitize_plain_text(extract_text_from_result(result), profile_id),
                profile_id,
            )
            if not reply:
                log_empty_output_diagnostics(result, "regular_turn")
                reply = "Desculpe, não consegui responder agora."
            reply = enforce_scheduling_entity_guardrail(profile_id, body, reply)
            reply = inject_fake_schedule(str(chat_id), body, reply, has_scheduling_tool=SCHEDULING_TOOL is not None)

        await send_reply(
            chat_id,
            reply,
            user_text=body,
            profile_id=profile_id,
            active_turn=active_turn,
        )
        await log_conversation(
            str(chat_id),
            payload,
            body,
            reply,
            "audio" if is_audio else "text",
            profile_id=profile_id,
        )
        await trim_session(session, SESSION_MAX_ITEMS)
        return {"ok": True}

    return router
