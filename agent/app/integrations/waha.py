import hashlib
import logging
import mimetypes
import os
import re
import subprocess
import tempfile
import time
from typing import Any, Dict, Optional
from urllib.parse import quote, urlparse, urlunparse

import anyio
import httpx
from fastapi import HTTPException
from openai import OpenAI

from ..config.settings import (
    LOG_WEBHOOK_DEBUG,
    OPENAI_TRANSCRIBE_LANGUAGE,
    OPENAI_TRANSCRIBE_MODEL,
    OUTBOUND_ECHO_TTL_SECONDS,
    WAHA_API_KEY,
    WAHA_BASE_URL,
    WAHA_RECORDING_PREVIEW_MS,
    WAHA_SENDVOICE_CONVERT,
    WAHA_SESSION,
    WAHA_TYPING_PREVIEW_MS,
    parse_int,
)
from ..core.profiles import PROFILE_OPTIONS, PROFILE_POLL_NAME
from ..core.state import (
    RECENT_OUTBOUND_MESSAGE_IDS,
    RECENT_OUTBOUND_TEXT_KEYS,
    remember_recent_audio_sent,
    remember_recent_key,
)


logger = logging.getLogger("agent")
OPENAI_CLIENT = OpenAI()


def log_webhook_debug(stage: str, data: Dict[str, Any]) -> None:
    if not LOG_WEBHOOK_DEBUG:
        return
    logger.info("WebhookDebug %s: %s", stage, data)


def name_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    for key in ("pushName", "pushname", "notifyName", "name", "senderName", "contactName"):
        value = (payload.get(key) or "").strip()
        if value and not _looks_like_technical_whatsapp_name(value):
            return value
    return None


def is_audio_payload(payload: Dict[str, Any]) -> bool:
    if not payload:
        return False
    msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
    if msg_type in ("audio", "voice", "ptt", "voicenote", "voice_note"):
        return True
    mimetype = (payload.get("mimetype") or payload.get("mimeType") or "").lower()
    if mimetype.startswith("audio/"):
        return True
    media = payload.get("media")
    if isinstance(media, dict):
        media_type = (media.get("type") or "").lower()
        if media_type in ("audio", "voice", "ptt", "voicenote", "voice_note"):
            return True
        media_mime = (media.get("mimetype") or media.get("mimeType") or "").lower()
        if media_mime.startswith("audio/"):
            return True
    return False


def is_non_text_media(payload: Dict[str, Any]) -> bool:
    if not payload:
        return False
    if is_audio_payload(payload):
        return False
    msg_type = (payload.get("type") or payload.get("messageType") or "").lower()
    if msg_type in ("image", "video", "document", "file", "sticker", "ptv", "media"):
        return True
    has_media = payload.get("hasMedia")
    if has_media is True or str(has_media).strip().lower() in ("1", "true", "yes", "sim"):
        return True
    media = payload.get("media")
    if isinstance(media, dict):
        media_type = (media.get("type") or "").lower()
        if media_type in ("image", "video", "document", "file", "sticker", "ptv", "media"):
            return True
        if (
            media.get("mimetype")
            or media.get("mimeType")
            or media.get("mediaUrl")
            or media.get("fileUrl")
            or media.get("downloadUrl")
            or media.get("url")
            or media.get("fileName")
            or media.get("filename")
        ):
            return True
    if media:
        return True
    if payload.get("mimetype") or payload.get("mimeType"):
        return True
    if payload.get("mediaUrl") or payload.get("fileUrl") or payload.get("downloadUrl"):
        return True
    return False


def extract_media_url(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    for key in ("mediaUrl", "fileUrl", "downloadUrl", "url"):
        value = (payload.get(key) or "").strip()
        if value:
            return normalize_media_url(value)
    media = payload.get("media")
    if isinstance(media, dict):
        for key in ("mediaUrl", "fileUrl", "downloadUrl", "url"):
            value = (media.get(key) or "").strip()
            if value:
                return normalize_media_url(value)
    return None


def normalize_media_url(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if not parsed.scheme:
        base = urlparse(WAHA_BASE_URL)
        path = url if url.startswith("/") else f"/{url}"
        return urlunparse((base.scheme, base.netloc, path, "", "", ""))
    host = parsed.hostname or ""
    if host in ("localhost", "127.0.0.1"):
        base = urlparse(WAHA_BASE_URL)
        return urlunparse(
            (base.scheme, base.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
        )
    return url


def normalize_mimetype(value: str) -> str:
    if not value:
        return ""
    return value.split(";", 1)[0].strip().lower()


def extract_mimetype(payload: Dict[str, Any]) -> str:
    mimetype = normalize_mimetype((payload.get("mimetype") or payload.get("mimeType") or ""))
    if not mimetype and isinstance(payload.get("media"), dict):
        mimetype = normalize_mimetype(
            (payload["media"].get("mimetype") or payload["media"].get("mimeType") or "")
        )
    return mimetype


def infer_chatwoot_file_type(payload: Dict[str, Any]) -> str:
    mimetype = extract_mimetype(payload)
    if mimetype.startswith("image/"):
        return "image"
    if mimetype.startswith("audio/"):
        return "audio"
    if mimetype.startswith("video/"):
        return "video"
    msg_type = (payload.get("type") or payload.get("messageType") or "").strip().lower()
    if msg_type in ("image", "audio", "video"):
        return msg_type
    media = payload.get("media")
    if isinstance(media, dict):
        media_type = (media.get("type") or "").strip().lower()
        if media_type in ("image", "audio", "video"):
            return media_type
    return "file"


def extract_media_filename(
    payload: Dict[str, Any],
    media_url: Optional[str] = None,
    *,
    default_prefix: str = "attachment",
) -> str:
    for container in (
        payload,
        payload.get("media") if isinstance(payload.get("media"), dict) else None,
        payload.get("_data") if isinstance(payload.get("_data"), dict) else None,
        payload.get("data") if isinstance(payload.get("data"), dict) else None,
    ):
        if not isinstance(container, dict):
            continue
        for key in ("filename", "fileName", "name", "file_name"):
            value = str(container.get(key) or "").strip()
            if value:
                return _safe_filename(value)

    if media_url:
        parsed_name = os.path.basename(urlparse(media_url).path)
        if parsed_name:
            return _safe_filename(parsed_name)

    mimetype = extract_mimetype(payload)
    extension = mimetypes.guess_extension(mimetype or "") or ""
    if mimetype == "audio/ogg":
        extension = ".ogg"
    if not extension and is_audio_payload(payload):
        return guess_audio_filename(payload, media_url)
    return f"{default_prefix}{extension or '.bin'}"


def _safe_filename(value: str) -> str:
    filename = os.path.basename(str(value or "").strip())
    filename = re.sub(r"[^A-Za-z0-9._-]+", "_", filename).strip("._")
    return filename or "attachment"


def _payload_dicts(payload: Dict[str, Any]) -> list[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    items: list[Dict[str, Any]] = [payload]
    for key in ("message", "msg", "_data", "data", "key"):
        value = payload.get(key)
        if isinstance(value, dict):
            items.append(value)
    return items


def _looks_like_whatsapp_phone_id(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text.endswith("@c.us") or text.endswith("@s.whatsapp.net")


def is_lid_or_technical_chat_id(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if text.endswith("@lid"):
        return True
    if "@" in text:
        return False
    digits = "".join(ch for ch in text if ch.isdigit())
    return bool(digits and digits == text and not digits.startswith("55"))


def _extract_brazil_phone_digits(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if _looks_like_whatsapp_phone_id(text):
        return normalize_phone(text)
    if text.lower().endswith("@lid"):
        return ""
    digits = "".join(ch for ch in text.split("@", 1)[0] if ch.isdigit())
    if digits.startswith("55") and len(digits) in (12, 13):
        return digits
    return ""


def _looks_like_technical_whatsapp_name(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "@lid" in text or "@c.us" in text or "@s.whatsapp.net" in text:
        return True
    return text.startswith("whatsapp chat id") or text.startswith("whatsapp lid")


def _extract_first_whatsapp_id(payload: Dict[str, Any], keys: tuple[str, ...]) -> str:
    first_candidate = ""
    for item in _payload_dicts(payload):
        for key in keys:
            value = str(item.get(key) or "").strip()
            if not value:
                continue
            if _looks_like_whatsapp_phone_id(value):
                return value
            if not first_candidate:
                first_candidate = value
    return first_candidate


def extract_chat_id(payload: Dict[str, Any]) -> Optional[str]:
    if is_from_me_payload(payload):
        # In outgoing WAHA webhooks, "from" can be the connected WhatsApp
        # session. The conversation target is usually chatId/to/remoteJid.
        outgoing_target = _extract_first_whatsapp_id(
            payload,
            (
                "chatId",
                "chat_id",
                "to",
                "jid",
                "whatsappJid",
                "whatsapp_jid",
                "remoteJid",
                "remote_jid",
            ),
        )
        if outgoing_target:
            return outgoing_target

    first_candidate = ""
    for item in _payload_dicts(payload):
        for key in (
            "from",
            "chatId",
            "chat_id",
            "jid",
            "whatsappJid",
            "whatsapp_jid",
            "remoteJid",
            "remote_jid",
        ):
            value = str(item.get(key) or "").strip()
            if not value:
                continue
            if _looks_like_whatsapp_phone_id(value):
                return value
            if not first_candidate:
                first_candidate = value
        key_obj = item.get("key")
        if isinstance(key_obj, dict):
            for key in ("remoteJid", "remote_jid", "from"):
                value = str(key_obj.get(key) or "").strip()
                if not value:
                    continue
                if _looks_like_whatsapp_phone_id(value):
                    return value
                if not first_candidate:
                    first_candidate = value
        id_obj = item.get("id")
        if isinstance(id_obj, dict):
            for key in ("remote", "remoteJid", "remote_jid", "participant", "jid"):
                value = str(id_obj.get(key) or "").strip()
                if not value:
                    continue
                if _looks_like_whatsapp_phone_id(value):
                    return value
                if not first_candidate:
                    first_candidate = value
        for nested_key in ("sender", "contact", "chat"):
            nested = item.get(nested_key)
            if not isinstance(nested, dict):
                continue
            for key in ("jid", "whatsappJid", "whatsapp_jid", "id"):
                value = str(nested.get(key) or "").strip()
                if not value:
                    continue
                if _looks_like_whatsapp_phone_id(value):
                    return value
                if not first_candidate:
                    first_candidate = value
        if not first_candidate:
            value = str(item.get("to") or "").strip()
            if value:
                first_candidate = value
    return first_candidate or None


def normalize_phone(chat_id: str) -> str:
    if not chat_id:
        return ""
    text = str(chat_id or "").strip()
    if text.endswith("@lid"):
        return ""
    base = text.split("@", 1)[0]
    digits = "".join(ch for ch in base if ch.isdigit())
    return digits or base


def extract_phone_from_payload(payload: Dict[str, Any], fallback_chat_id: str = "") -> str:
    if is_from_me_payload(payload):
        outgoing_target = _extract_first_whatsapp_id(
            payload,
            (
                "chatId",
                "chat_id",
                "to",
                "jid",
                "whatsappJid",
                "whatsapp_jid",
                "remoteJid",
                "remote_jid",
            ),
        )
        digits = _extract_brazil_phone_digits(outgoing_target)
        if digits:
            return digits

    for item in _payload_dicts(payload):
        for key in (
            "from",
            "chatId",
            "chat_id",
            "jid",
            "whatsappJid",
            "whatsapp_jid",
            "author",
            "participant",
            "remoteJid",
            "remote_jid",
        ):
            value = item.get(key)
            if _looks_like_whatsapp_phone_id(value):
                return normalize_phone(str(value))
            digits = _extract_brazil_phone_digits(value)
            if digits:
                return digits

        key_obj = item.get("key")
        if isinstance(key_obj, dict):
            for key in ("remoteJid", "remote_jid", "participant", "from"):
                value = key_obj.get(key)
                if _looks_like_whatsapp_phone_id(value):
                    return normalize_phone(str(value))
                digits = _extract_brazil_phone_digits(value)
                if digits:
                    return digits

        for nested_key in ("sender", "contact"):
            nested = item.get(nested_key)
            if not isinstance(nested, dict):
                continue
            for key in ("phone", "phoneNumber", "phone_number", "jid", "whatsappJid", "whatsapp_jid"):
                value = nested.get(key)
                if _looks_like_whatsapp_phone_id(value):
                    return normalize_phone(str(value))
                digits = _extract_brazil_phone_digits(value)
                if digits:
                    return digits
            nested_id = nested.get("id")
            if _looks_like_whatsapp_phone_id(nested_id):
                return normalize_phone(str(nested_id))

    return _extract_brazil_phone_digits(fallback_chat_id)


def extract_phone_from_contact_info(data: Dict[str, Any]) -> str:
    if not isinstance(data, dict):
        return ""
    for key in (
        "phone",
        "phoneNumber",
        "phone_number",
        "number",
        "jid",
        "whatsappJid",
        "whatsapp_jid",
        "id",
    ):
        digits = _extract_brazil_phone_digits(data.get(key))
        if digits:
            return digits
    for key in ("contact", "sender", "profile"):
        nested = data.get(key)
        if isinstance(nested, dict):
            digits = extract_phone_from_contact_info(nested)
            if digits:
                return digits
    return ""


def guess_audio_filename(payload: Dict[str, Any], media_url: Optional[str] = None) -> str:
    mimetype = extract_mimetype(payload)
    if mimetype == "audio/ogg" or mimetype == "audio/opus":
        return "audio.ogg"
    if mimetype == "audio/mpeg":
        return "audio.mp3"
    if mimetype in ("audio/mp4", "audio/m4a"):
        return "audio.m4a"
    if media_url:
        path = urlparse(media_url).path.lower()
        if path.endswith(".oga"):
            return "audio.oga"
        if path.endswith(".ogg"):
            return "audio.ogg"
        if path.endswith(".mp3"):
            return "audio.mp3"
        if path.endswith(".m4a"):
            return "audio.m4a"
        if path.endswith(".wav"):
            return "audio.wav"
    return "audio"


def should_convert_to_wav(payload: Dict[str, Any], media_url: Optional[str]) -> bool:
    mimetype = extract_mimetype(payload)
    if mimetype in ("audio/ogg", "audio/opus"):
        return True
    if media_url:
        path = urlparse(media_url).path.lower()
        if path.endswith(".oga") or path.endswith(".ogg"):
            return True
    return False


def convert_ogg_to_wav_bytes(audio_bytes: bytes, input_name: str) -> Optional[bytes]:
    if not audio_bytes:
        return None
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, input_name)
            output_path = os.path.join(tmpdir, "output.wav")
            with open(input_path, "wb") as handle:
                handle.write(audio_bytes)
            subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    input_path,
                    "-ac",
                    "1",
                    "-ar",
                    "16000",
                    output_path,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            with open(output_path, "rb") as handle:
                return handle.read()
    except Exception as exc:
        logger.exception("Audio conversion failed: %s", exc)
        return None


async def download_media(url: str) -> Optional[bytes]:
    if not url:
        return None
    headers = {}
    if WAHA_API_KEY and url.startswith(WAHA_BASE_URL):
        headers["X-Api-Key"] = WAHA_API_KEY
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(url, headers=headers)
        if resp.status_code >= 400:
            logger.warning("Media download failed: %s %s", resp.status_code, resp.text)
            return None
        return resp.content


async def transcribe_audio(url: str, payload: Dict[str, Any]) -> Optional[str]:
    audio_bytes = await download_media(url)
    if not audio_bytes:
        return None

    filename = guess_audio_filename(payload, url)
    if should_convert_to_wav(payload, url):
        wav_bytes = await anyio.to_thread.run_sync(convert_ogg_to_wav_bytes, audio_bytes, filename)
        if not wav_bytes:
            return None
        audio_bytes = wav_bytes
        filename = "audio.wav"

    def call_openai() -> Optional[str]:
        kwargs: Dict[str, Any] = {
            "model": OPENAI_TRANSCRIBE_MODEL,
            "file": (filename, audio_bytes),
        }
        if OPENAI_TRANSCRIBE_LANGUAGE:
            kwargs["language"] = OPENAI_TRANSCRIBE_LANGUAGE
        result = OPENAI_CLIENT.audio.transcriptions.create(**kwargs)
        text = (getattr(result, "text", "") or "").strip()
        return text or None

    try:
        return await anyio.to_thread.run_sync(call_openai)
    except Exception as exc:
        logger.exception("Audio transcription failed: %s", exc)
        return None


async def get_contact_info(chat_id: str) -> Optional[Dict[str, Any]]:
    if not chat_id:
        return None
    params = {"contactId": chat_id, "session": WAHA_SESSION}
    url = f"{WAHA_BASE_URL}/api/contacts"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, params=params, headers=waha_headers())
        if resp.status_code >= 400:
            logger.warning("WAHA contacts failed: %s %s", resp.status_code, resp.text)
            return None
        try:
            data = resp.json()
        except ValueError as exc:
            logger.warning(
                "WAHA contacts returned non-JSON for chat_id=%s: %s body=%s",
                chat_id,
                exc,
                resp.text[:200],
            )
            return None
        if not isinstance(data, dict):
            return None
        return data


async def get_contact_name(chat_id: str) -> Optional[str]:
    data = await get_contact_info(chat_id)
    if not data:
        return None
    for key in ("pushname", "pushName", "name", "shortName", "notifyName"):
        value = (data.get(key) or "").strip()
        if value and not _looks_like_technical_whatsapp_name(value):
            return value
    return None


async def get_contact_phone(chat_id: str) -> str:
    data = await get_contact_info(chat_id)
    if not data:
        return ""
    return extract_phone_from_contact_info(data)


def waha_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if WAHA_API_KEY:
        headers["X-Api-Key"] = WAHA_API_KEY
    return headers


def compact_http_error_text(text: str, limit: int = 280) -> str:
    cleaned = " ".join(str(text or "").split()).strip()
    if not cleaned:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit].rstrip()}..."


def guess_waha_file_mimetype(filename: str) -> str:
    lowered = str(filename or "").strip().lower()
    if lowered.endswith(".ogg") or lowered.endswith(".oga") or lowered.endswith(".opus"):
        return "audio/ogg"
    if lowered.endswith(".mp3"):
        return "audio/mpeg"
    if lowered.endswith(".m4a") or lowered.endswith(".mp4"):
        return "audio/mp4"
    if lowered.endswith(".wav"):
        return "audio/wav"
    return "application/octet-stream"


def extract_waha_message_id(data: Dict[str, Any]) -> str:
    message_id = (
        data.get("id")
        or (data.get("message") or {}).get("id")
        or (data.get("data") or {}).get("id")
        or ((data.get("message") or {}).get("key") or {}).get("id")
    )
    if message_id:
        remember_recent_key(
            RECENT_OUTBOUND_MESSAGE_IDS,
            str(message_id),
            OUTBOUND_ECHO_TTL_SECONDS,
        )
    return str(message_id or "")


def outbound_text_key(chat_id: str, text: str) -> str:
    compact_chat_id = str(chat_id or "").strip()
    compact_text = str(text or "").strip()
    if not compact_chat_id or not compact_text:
        return ""
    digest = hashlib.sha1(compact_text.encode("utf-8")).hexdigest()
    return f"text:{compact_chat_id}:{digest}"


async def set_presence(chat_id: str, presence: str) -> None:
    if not chat_id or not presence or not WAHA_SESSION:
        return
    url = f"{WAHA_BASE_URL}/api/{quote(WAHA_SESSION, safe='')}/presence"
    payload = {"chatId": chat_id, "presence": presence}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload, headers=waha_headers())
        if resp.status_code >= 400:
            logger.warning("WAHA presence failed: %s %s", resp.status_code, resp.text)
    except Exception as exc:
        logger.warning("WAHA presence request failed: %s", exc)


async def show_recording_preview(chat_id: str, delay_seconds_from_ms) -> None:
    preview_seconds = delay_seconds_from_ms(
        WAHA_RECORDING_PREVIEW_MS,
        default_ms=1400,
        min_ms=0,
        max_ms=8000,
    )
    if preview_seconds <= 0:
        return
    await set_presence(chat_id, "recording")
    await anyio.sleep(preview_seconds)
    await set_presence(chat_id, "paused")


def typing_preview_seconds() -> float:
    preview_ms = parse_int(WAHA_TYPING_PREVIEW_MS)
    if preview_ms is None:
        preview_ms = 1200
    preview_ms = max(0, min(preview_ms, 8000))
    return preview_ms / 1000.0


def clamp_preview_seconds(value: float) -> float:
    return max(0.0, min(float(value), 8.0))


def typing_preview_seconds_for_text(text: str, *, max_seconds: float | None = None) -> float:
    cap_seconds = typing_preview_seconds() if max_seconds is None else clamp_preview_seconds(max_seconds)
    if cap_seconds <= 0:
        return 0.0
    compact_text = re.sub(r"\s+", " ", str(text or "").strip())
    if not compact_text:
        return min(0.25, cap_seconds)
    estimated_seconds = 0.35 + min(len(compact_text), 160) * 0.007
    return min(cap_seconds, estimated_seconds)


async def show_typing_preview(chat_id: str, text: str = "", preview_seconds: float | None = None) -> None:
    if preview_seconds is None:
        preview_seconds = typing_preview_seconds_for_text(text)
    else:
        preview_seconds = clamp_preview_seconds(preview_seconds)
    if preview_seconds <= 0:
        return
    await set_presence(chat_id, "typing")
    await anyio.sleep(preview_seconds)
    await set_presence(chat_id, "paused")


async def send_text(chat_id: str, text: str, *, preview_seconds: float | None = None) -> str:
    if not chat_id:
        raise ValueError("chat_id is required")

    payload = {
        "chatId": chat_id,
        "text": text,
        "session": WAHA_SESSION,
    }

    remember_recent_key(
        RECENT_OUTBOUND_TEXT_KEYS,
        outbound_text_key(chat_id, text),
        OUTBOUND_ECHO_TTL_SECONDS,
    )
    await show_typing_preview(chat_id, text, preview_seconds)
    url = f"{WAHA_BASE_URL}/api/sendText"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, json=payload, headers=waha_headers())
        if resp.status_code >= 400:
            logger.error("WAHA sendText failed: %s %s", resp.status_code, resp.text)
            raise HTTPException(status_code=502, detail="WAHA sendText failed")
        try:
            data = resp.json()
        except Exception:
            data = {}
    message_id = extract_waha_message_id(data)
    if message_id:
        log_webhook_debug(
            "remember_outbound_message",
            {"chat_id": str(chat_id), "message_id": str(message_id)},
        )
    return message_id


async def send_voice(chat_id: str, media_url: str, delay_seconds_from_ms) -> str:
    if not chat_id:
        raise ValueError("chat_id is required")
    if not media_url:
        raise ValueError("media_url is required")
    parsed_url = urlparse(media_url)
    filename = os.path.basename(parsed_url.path) or "audio.ogg"
    mimetype = guess_waha_file_mimetype(filename)

    payload = {
        "chatId": chat_id,
        "session": WAHA_SESSION,
        "file": {
            "filename": filename,
            "mimetype": mimetype,
            "url": media_url,
        },
        "convert": WAHA_SENDVOICE_CONVERT,
    }

    await show_recording_preview(chat_id, delay_seconds_from_ms)
    url = f"{WAHA_BASE_URL}/api/sendVoice"
    async with httpx.AsyncClient(timeout=40) as client:
        resp = await client.post(url, json=payload, headers=waha_headers())
        if resp.status_code >= 400:
            logger.error("WAHA sendVoice failed: %s %s", resp.status_code, resp.text)
            error_text = compact_http_error_text(resp.text)
            detail = f"WAHA sendVoice failed ({resp.status_code})"
            if error_text:
                detail = f"{detail}: {error_text}"
            raise RuntimeError(detail)
        try:
            data = resp.json()
        except Exception:
            data = {}
    message_id = extract_waha_message_id(data)
    if message_id:
        log_webhook_debug(
            "remember_outbound_voice_message",
            {"chat_id": str(chat_id), "message_id": str(message_id)},
        )
    remember_recent_audio_sent(chat_id)
    return message_id


async def send_poll(chat_id: str, question: str, options: list[str]) -> Optional[str]:
    if not chat_id:
        raise ValueError("chat_id is required")
    if not options:
        raise ValueError("poll options are required")

    payload = {
        "chatId": chat_id,
        "session": WAHA_SESSION,
        "poll": {
            "name": question,
            "options": options,
            "multipleAnswers": False,
        },
    }

    url = f"{WAHA_BASE_URL}/api/sendPoll"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, json=payload, headers=waha_headers())
        if resp.status_code >= 400:
            logger.error("WAHA sendPoll failed: %s %s", resp.status_code, resp.text)
            return None
        try:
            data = resp.json()
        except Exception:
            return None
    poll_id = (
        data.get("id")
        or (data.get("poll") or {}).get("id")
        or (data.get("message") or {}).get("id")
        or (data.get("data") or {}).get("id")
    )
    if poll_id:
        remember_recent_key(
            RECENT_OUTBOUND_MESSAGE_IDS,
            str(poll_id),
            OUTBOUND_ECHO_TTL_SECONDS,
        )
    return poll_id


async def send_profile_poll(chat_id: str) -> Optional[str]:
    if not PROFILE_OPTIONS:
        logger.warning("Profile poll requested but PROFILE_OPTIONS is empty.")
        return None
    return await send_poll(chat_id, PROFILE_POLL_NAME, PROFILE_OPTIONS)


def coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "y", "sim", "s"):
            return True
        if lowered in ("false", "0", "no", "n", "nao", "não"):
            return False
    return None


def extract_message_id_value(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        for key in ("_serialized", "serialized", "id", "messageId", "msgId", "_id"):
            value = raw.get(key)
            extracted = extract_message_id_value(value)
            if extracted:
                return extracted
        return None
    value = str(raw).strip()
    return value or None


def is_from_me_payload(payload: Dict[str, Any]) -> bool:
    if not payload:
        return False
    for container in (
        payload,
        payload.get("message"),
        payload.get("msg"),
        payload.get("key"),
        payload.get("_data"),
        payload.get("data"),
    ):
        if not isinstance(container, dict):
            continue
        for key in ("fromMe", "from_me", "isFromMe", "is_from_me"):
            value = coerce_bool(container.get(key))
            if value is not None:
                return value
        key_obj = container.get("key")
        if isinstance(key_obj, dict):
            for key in ("fromMe", "from_me", "isFromMe", "is_from_me"):
                value = coerce_bool(key_obj.get(key))
                if value is not None:
                    return value
    return False


def extract_message_id(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None

    def from_obj(obj: Any) -> Optional[str]:
        if not isinstance(obj, dict):
            return None
        for key in ("id", "messageId", "msgId", "message_id", "msg_id"):
            value = extract_message_id_value(obj.get(key))
            if value:
                return value
        key_obj = obj.get("key")
        if isinstance(key_obj, dict):
            for key in ("id", "messageId", "msgId", "message_id", "msg_id"):
                value = extract_message_id_value(key_obj.get(key))
                if value:
                    return value
        return None

    for container in (
        payload,
        payload.get("message"),
        payload.get("msg"),
        payload.get("_data"),
        payload.get("data"),
    ):
        value = from_obj(container)
        if value:
            return value
    return None


def extract_event_id(data: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    for key in ("id", "eventId", "event_id", "event.id"):
        value = data.get(key)
        if value:
            return str(value)
    if isinstance(payload, dict):
        value = payload.get("id")
        if value:
            return str(value)
    return None


def extract_timestamp(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    for key in ("timestamp", "ts", "t"):
        value = payload.get(key)
        if value is not None:
            return str(value)
    data_obj = payload.get("_data") or payload.get("data") or {}
    if isinstance(data_obj, dict):
        for key in ("timestamp", "ts", "t"):
            value = data_obj.get(key)
            if value is not None:
                return str(value)
    return None


def message_fingerprint(payload: Dict[str, Any]) -> Optional[str]:
    if not payload:
        return None
    chat_id = extract_chat_id(payload)
    msg_id = extract_message_id(payload)
    if chat_id and msg_id:
        return f"{chat_id}:{msg_id}"
    timestamp = extract_timestamp(payload)
    body = (payload.get("body") or payload.get("text") or "").strip()
    if chat_id and timestamp and body:
        return f"{chat_id}:{timestamp}:{body}"
    if chat_id and body:
        digest = hashlib.sha1(body.encode("utf-8")).hexdigest()
        return f"{chat_id}:{digest}"
    return None


def set_presence_sync(chat_id: str, presence: str) -> None:
    if not chat_id or not presence or not WAHA_SESSION:
        return
    url = f"{WAHA_BASE_URL}/api/{quote(WAHA_SESSION, safe='')}/presence"
    payload = {"chatId": chat_id, "presence": presence}
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json=payload, headers=waha_headers())
        if resp.status_code >= 400:
            logger.warning("WAHA presence sync failed: %s %s", resp.status_code, resp.text)
    except Exception as exc:
        logger.warning("WAHA presence sync request failed: %s", exc)


def show_recording_preview_sync(chat_id: str, delay_seconds_from_ms) -> None:
    preview_seconds = delay_seconds_from_ms(
        WAHA_RECORDING_PREVIEW_MS,
        default_ms=1400,
        min_ms=0,
        max_ms=8000,
    )
    if preview_seconds <= 0:
        return
    set_presence_sync(chat_id, "recording")
    time.sleep(preview_seconds)
    set_presence_sync(chat_id, "paused")


def send_voice_sync(chat_id: str, media_url: str, delay_seconds_from_ms) -> str:
    if not chat_id:
        raise ValueError("chat_id is required")
    if not media_url:
        raise ValueError("media_url is required")
    parsed_url = urlparse(media_url)
    filename = os.path.basename(parsed_url.path) or "audio.ogg"
    mimetype = guess_waha_file_mimetype(filename)
    payload = {
        "chatId": chat_id,
        "session": WAHA_SESSION,
        "file": {
            "filename": filename,
            "mimetype": mimetype,
            "url": media_url,
        },
        "convert": WAHA_SENDVOICE_CONVERT,
    }
    show_recording_preview_sync(chat_id, delay_seconds_from_ms)
    url = f"{WAHA_BASE_URL}/api/sendVoice"
    with httpx.Client(timeout=40) as client:
        resp = client.post(url, json=payload, headers=waha_headers())
    if resp.status_code >= 400:
        logger.error("WAHA sendVoice sync failed: %s %s", resp.status_code, resp.text)
        error_text = compact_http_error_text(resp.text)
        detail = f"WAHA sendVoice failed ({resp.status_code})"
        if error_text:
            detail = f"{detail}: {error_text}"
        raise RuntimeError(detail)
    try:
        data = resp.json()
    except Exception:
        data = {}
    message_id = extract_waha_message_id(data)
    remember_recent_audio_sent(chat_id)
    return message_id
