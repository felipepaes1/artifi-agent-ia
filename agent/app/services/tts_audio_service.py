import logging
import time
from typing import Optional

import anyio
from openai import OpenAI

from ..core.profiles import get_profile_tts_config
from ..core.state import is_chat_turn_current
from ..formatters.message_formatter import delay_seconds_from_ms
from ..integrations import supabase as supabase_integration
from ..integrations import waha as waha_integration
from ..utils.text import short_hash


logger = logging.getLogger("agent")

OPENAI_CLIENT = OpenAI()

TTS_CONTENT_TYPES = {
    "mp3": "audio/mpeg",
    "opus": "audio/ogg",
    "aac": "audio/aac",
    "flac": "audio/flac",
    "wav": "audio/wav",
    "pcm": "application/octet-stream",
}

TTS_FILE_EXTENSIONS = {
    "mp3": "mp3",
    "opus": "ogg",
    "aac": "aac",
    "flac": "flac",
    "wav": "wav",
    "pcm": "pcm",
}


def _sanitize_storage_part(value: str, default: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in str(value or "").strip())
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return cleaned[:80] or default


def build_tts_storage_path(profile_id: Optional[str], chat_id: str, response_format: str, text: str) -> str:
    profile_part = _sanitize_storage_part(profile_id or "default", "default")
    chat_part = short_hash(chat_id) or "chat"
    text_part = short_hash(text) or "reply"
    timestamp = int(time.time() * 1000)
    extension = TTS_FILE_EXTENSIONS.get(response_format, response_format or "mp3")
    return f"{profile_part}/{chat_part}/{timestamp}-{text_part}.{extension}"


async def synthesize_speech_bytes(text: str, *, profile_id: Optional[str]) -> tuple[bytes, str, str]:
    config = get_profile_tts_config(profile_id)
    response_format = config["response_format"]

    def call_openai() -> bytes:
        kwargs = {
            "model": config["model"],
            "voice": config["voice"],
            "input": text,
            "response_format": response_format,
        }
        instructions = (config.get("instructions") or "").strip()
        if instructions:
            kwargs["instructions"] = instructions
        response = OPENAI_CLIENT.audio.speech.create(**kwargs)
        return response.read()

    audio_bytes = await anyio.to_thread.run_sync(call_openai)
    content_type = TTS_CONTENT_TYPES.get(response_format, "application/octet-stream")
    return audio_bytes, response_format, content_type


async def send_tts_audio_reply(
    *,
    chat_id: str,
    text: str,
    profile_id: Optional[str],
    active_turn: Optional[int] = None,
) -> bool:
    clean_text = (text or "").strip()
    if not clean_text:
        return True
    if not chat_id:
        return False
    if not is_chat_turn_current(str(chat_id), active_turn):
        return False

    config = get_profile_tts_config(profile_id)
    bucket = (config.get("bucket") or "").strip()
    if not bucket:
        logger.warning("TTS audio reply skipped: no TTS bucket configured for profile=%s", profile_id)
        return False

    audio_bytes, response_format, content_type = await synthesize_speech_bytes(clean_text, profile_id=profile_id)
    if not audio_bytes:
        logger.warning("TTS audio reply skipped: empty audio bytes for profile=%s", profile_id)
        return False
    if not is_chat_turn_current(str(chat_id), active_turn):
        return False

    file_name = build_tts_storage_path(profile_id, chat_id, response_format, clean_text)
    uploaded = await supabase_integration.upload_storage_bytes(
        bucket,
        file_name,
        audio_bytes,
        content_type=content_type,
        upsert=True,
    )
    if not uploaded:
        return False

    media_url = await supabase_integration.build_bucket_audio_url(bucket, file_name)
    if not media_url:
        logger.warning("TTS audio reply failed: signed URL unavailable bucket=%s file=%s", bucket, file_name)
        return False
    if not is_chat_turn_current(str(chat_id), active_turn):
        return False

    await waha_integration.send_voice(chat_id, media_url, delay_seconds_from_ms)
    return True
