import difflib
import logging
import re
from typing import Any, Dict, Optional

try:
    from agents import function_tool
except Exception:
    function_tool = None

from ..core.profiles import PROFILE_DEFAULT_ID, get_audio_bucket_for_profile
from ..core.state import (
    has_recent_audio_sent,
    has_recent_service_audio_sent,
    is_chat_turn_current,
    remember_service_audio_sent,
)
from ..formatters.message_formatter import delay_seconds_from_ms
from ..integrations import supabase as supabase_integration
from ..integrations import waha as waha_integration
from ..utils.text import normalize_text, normalize_service_text


logger = logging.getLogger("agent")

CURRENT_CHAT_ID = None
CURRENT_PROFILE_ID = None
resolve_profile_for_chat = None


def configure_runtime(*, chat_context, profile_context, profile_resolver) -> None:
    global CURRENT_CHAT_ID, CURRENT_PROFILE_ID, resolve_profile_for_chat
    CURRENT_CHAT_ID = chat_context
    CURRENT_PROFILE_ID = profile_context
    resolve_profile_for_chat = profile_resolver


def audio_file_variants(file_info: Dict[str, str]) -> list[str]:
    stem = normalize_service_text(file_info.get("stem") or file_info.get("normalized_stem") or "")
    if not stem:
        return []
    tokens = [token for token in stem.split() if token]
    variants: set[str] = {stem}
    max_ngram = min(len(tokens), 3)
    for size in range(1, max_ngram + 1):
        for idx in range(0, len(tokens) - size + 1):
            chunk = " ".join(tokens[idx : idx + size]).strip()
            if len(chunk) >= 4:
                variants.add(chunk)
    return sorted(variants, key=len, reverse=True)


def score_audio_match(query: str, file_info: Dict[str, str]) -> float:
    normalized_query = normalize_service_text(query)
    if not normalized_query:
        return 0.0
    query_tokens = {token for token in normalized_query.split() if token}
    best_score = 0.0
    for variant in audio_file_variants(file_info):
        variant_tokens = {token for token in variant.split() if token}
        if normalized_query == variant:
            return 1.0
        if normalized_query in variant or variant in normalized_query:
            best_score = max(best_score, 0.93)
        token_overlap = 0.0
        if query_tokens and variant_tokens:
            token_overlap = len(query_tokens & variant_tokens) / max(len(query_tokens), len(variant_tokens))
            if query_tokens.issubset(variant_tokens):
                best_score = max(best_score, 0.9)
        ratio = difflib.SequenceMatcher(None, normalized_query, variant).ratio()
        candidate_score = (ratio * 0.72) + (token_overlap * 0.28)
        best_score = max(best_score, candidate_score)
    return round(best_score, 4)


def match_audio_files(
    query: str,
    available_files: list[Dict[str, str]],
    *,
    limit: int = 3,
    min_score: float = 0.55,
) -> list[Dict[str, Any]]:
    normalized_query = normalize_service_text(query)
    if not normalized_query:
        return []
    ranked: list[Dict[str, Any]] = []
    for file_info in available_files:
        score = score_audio_match(normalized_query, file_info)
        if score < min_score:
            continue
        ranked.append(
            {
                "filename": str(file_info.get("name") or "").strip(),
                "display_name": str(file_info.get("stem") or "").strip(),
                "score": score,
            }
        )
    ranked.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    return ranked[: max(limit, 1)]


def looks_like_booking_or_interest_intent(text: str) -> bool:
    lowered = normalize_text(text or "")
    if not lowered:
        return False
    triggers = (
        "quero agendar",
        "quero marcar",
        "quero fazer",
        "tenho interesse",
        "tenho interesse em",
        "quero esse",
        "quero este",
        "gostei desse",
        "gostei desse procedimento",
        "vamos agendar",
        "como agenda",
        "como agendar",
        "podemos agendar",
        "quero saber desse",
        "me interessei",
    )
    return any(trigger in lowered for trigger in triggers)


def humanize_audio_display_name(name: str) -> str:
    cleaned = str(name or "").strip().replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or "o procedimento"


async def try_send_service_audio_for_message(
    chat_id: str,
    profile_id: Optional[str],
    user_text: str,
    *,
    active_turn: Optional[int] = None,
    min_score: float = 0.72,
) -> Optional[Dict[str, str]]:
    bucket = get_audio_bucket_for_profile(profile_id)
    if not bucket or not chat_id:
        return None
    if has_recent_audio_sent(chat_id):
        return None
    if not is_chat_turn_current(str(chat_id), active_turn):
        return None

    available_files = await supabase_integration.list_bucket_audio_files(bucket)
    if not available_files:
        return None

    matches = match_audio_files(user_text, available_files, limit=1, min_score=min_score)
    if not matches:
        return None

    top_match = matches[0]
    filename = str(top_match.get("filename") or "").strip()
    if not filename:
        return None
    if has_recent_service_audio_sent(chat_id, filename):
        return None
    media_url = await supabase_integration.build_bucket_audio_url(bucket, filename)
    if not media_url:
        return None
    await waha_integration.send_voice(chat_id, media_url, delay_seconds_from_ms)
    remember_service_audio_sent(chat_id, filename)
    display_name = humanize_audio_display_name(str(top_match.get("display_name") or filename))
    return {
        "filename": filename,
        "display_name": display_name,
        "session_note": f"Enviei um audio de atendimento sobre {display_name}.",
    }


async def maybe_send_profile_audio(
    chat_id: str,
    profile_id: Optional[str],
    user_text: str,
    assistant_text: str,
    active_turn: Optional[int] = None,
) -> Optional[str]:
    bucket = get_audio_bucket_for_profile(profile_id)
    if not bucket or not chat_id:
        return None
    if has_recent_audio_sent(chat_id):
        return None
    if not is_chat_turn_current(str(chat_id), active_turn):
        return None

    available_files = await supabase_integration.list_bucket_audio_files(bucket)
    if not available_files:
        return None

    matches = match_audio_files(user_text, available_files, limit=1, min_score=0.58)
    if not matches and looks_like_booking_or_interest_intent(user_text):
        matches = match_audio_files(assistant_text, available_files, limit=1, min_score=0.7)
    if not matches:
        return None

    filename = str(matches[0].get("filename") or "").strip()
    if not filename:
        return None
    if has_recent_service_audio_sent(chat_id, filename):
        return None
    media_url = await supabase_integration.build_bucket_audio_url(bucket, filename)
    if not media_url:
        return None
    await waha_integration.send_voice(chat_id, media_url, delay_seconds_from_ms)
    remember_service_audio_sent(chat_id, filename)
    return filename


def match_profile_audio_files(
    profile_id: str,
    query: str,
    *,
    top_k: int = 3,
) -> Dict[str, Any]:
    bucket = get_audio_bucket_for_profile(profile_id)
    if not bucket:
        return {
            "status": "unavailable",
            "profile_id": profile_id,
            "query": query,
            "message": "audio_bucket_not_configured",
        }
    detailed = supabase_integration.list_bucket_audio_files_sync_detailed(bucket)
    files = list(detailed.get("files") or [])
    if detailed.get("error"):
        return {
            "status": "error",
            "profile_id": profile_id,
            "bucket": bucket,
            "query": query,
            "message": "audio_bucket_list_failed",
            "error": detailed.get("error"),
            "available_files": files,
            "matches": [],
        }
    matches = match_audio_files(query, files, limit=max(1, min(top_k, 5)))
    return {
        "status": "ok",
        "profile_id": profile_id,
        "bucket": bucket,
        "query": query,
        "available_files": [str(item.get("name") or "") for item in files],
        "matches": matches,
    }


def build_audio_match_tool():
    if function_tool is None:
        return None

    @function_tool
    def buscar_audio_atendimento(query: str, top_k: int = 3) -> Dict[str, Any]:
        chat_id = CURRENT_CHAT_ID.get("") if CURRENT_CHAT_ID is not None else ""
        profile_id = (CURRENT_PROFILE_ID.get("") if CURRENT_PROFILE_ID is not None else "") or (
            resolve_profile_for_chat(chat_id) if resolve_profile_for_chat is not None else ""
        )
        if not profile_id:
            profile_id = PROFILE_DEFAULT_ID or ""
        try:
            requested_top_k = int(top_k)
        except Exception:
            requested_top_k = 3
        requested_top_k = max(1, min(requested_top_k, 5))
        return match_profile_audio_files(profile_id=profile_id, query=query or "", top_k=requested_top_k)

    return buscar_audio_atendimento


def build_audio_send_tool():
    if function_tool is None:
        return None

    @function_tool
    def enviar_audio_atendimento(filename: str) -> Dict[str, Any]:
        chat_id = CURRENT_CHAT_ID.get("") if CURRENT_CHAT_ID is not None else ""
        profile_id = (CURRENT_PROFILE_ID.get("") if CURRENT_PROFILE_ID is not None else "") or (
            resolve_profile_for_chat(chat_id) if resolve_profile_for_chat is not None else ""
        )
        if not profile_id:
            profile_id = PROFILE_DEFAULT_ID or ""
        bucket = get_audio_bucket_for_profile(profile_id)
        normalized_filename = str(filename or "").strip()
        if not chat_id:
            return {"status": "error", "message": "chat_id_not_available", "filename": normalized_filename}
        if not bucket:
            return {"status": "error", "message": "audio_bucket_not_configured", "profile_id": profile_id}
        available_files = supabase_integration.list_bucket_audio_files_sync(bucket)
        valid_names = {str(item.get("name") or "").strip(): item for item in available_files}
        if normalized_filename not in valid_names:
            match_payload = match_profile_audio_files(profile_id=profile_id, query=normalized_filename, top_k=3)
            return {
                "status": "error",
                "message": "filename_not_found_in_bucket",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
                "matches": match_payload.get("matches") or [],
                "available_files": match_payload.get("available_files") or [],
            }
        if has_recent_service_audio_sent(chat_id, normalized_filename):
            return {
                "status": "skipped",
                "message": "audio_already_sent_recently",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
            }
        media_url = supabase_integration.build_bucket_audio_url_sync(bucket, normalized_filename)
        if not media_url:
            return {
                "status": "error",
                "message": "audio_url_not_available",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
            }
        try:
            message_id = waha_integration.send_voice_sync(chat_id, media_url, delay_seconds_from_ms)
        except Exception as exc:
            return {
                "status": "error",
                "message": "audio_send_failed",
                "profile_id": profile_id,
                "bucket": bucket,
                "filename": normalized_filename,
                "error": str(exc),
            }
        remember_service_audio_sent(chat_id, normalized_filename)
        return {
            "status": "sent",
            "profile_id": profile_id,
            "bucket": bucket,
            "filename": normalized_filename,
            "message_id": message_id,
        }

    return enviar_audio_atendimento
