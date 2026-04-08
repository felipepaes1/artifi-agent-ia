import logging
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import quote

import anyio
import httpx

from ..config.settings import (
    CRIOLASER_AUDIO_CACHE_TTL_SECONDS,
    CRIOLASER_AUDIO_PUBLIC_BUCKET,
    CRIOLASER_AUDIO_SIGN_TTL,
    SUPABASE_APP,
    SUPABASE_ENABLED,
    SUPABASE_KEY,
    SUPABASE_SESSION_LIMIT,
    SUPABASE_TABLE,
    SUPABASE_URL,
)
from ..utils.text import normalize_service_text

try:
    from supabase import Client as SupabaseClient
    from supabase import create_client as supabase_create_client
except Exception:
    SupabaseClient = None
    supabase_create_client = None


logger = logging.getLogger("agent")

SUPABASE_CLIENT: Optional["SupabaseClient"] = None
AUDIO_FILE_CACHE: Dict[str, Any] = {"expires_at": 0.0, "files": []}


def get_supabase_client() -> Optional["SupabaseClient"]:
    if not SUPABASE_ENABLED:
        return None
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    if supabase_create_client is None:
        return None
    global SUPABASE_CLIENT
    if SUPABASE_CLIENT is None:
        SUPABASE_CLIENT = supabase_create_client(SUPABASE_URL, SUPABASE_KEY)
    return SUPABASE_CLIENT


async def supabase_insert(row: Dict[str, Any]) -> None:
    client = get_supabase_client()
    if not client or not SUPABASE_TABLE:
        return

    def insert() -> None:
        client.table(SUPABASE_TABLE).insert(row).execute()

    try:
        await anyio.to_thread.run_sync(insert)
    except Exception as exc:
        logger.warning("Supabase insert failed: %s", exc)


async def supabase_fetch_recent(phone: str, chat_id: Optional[str] = None) -> list[Dict[str, Any]]:
    client = get_supabase_client()
    if not client or not SUPABASE_TABLE or not phone or SUPABASE_SESSION_LIMIT <= 0:
        return []

    def fetch() -> list[Dict[str, Any]]:
        query = client.table(SUPABASE_TABLE).select("user_message, bot_message, created_at").eq(
            "phone", phone
        )
        if SUPABASE_APP:
            query = query.eq("app", SUPABASE_APP)
        if chat_id:
            query = query.eq("conversation_id", chat_id)
        resp = query.order("created_at", desc=True).limit(SUPABASE_SESSION_LIMIT).execute()
        data = list(resp.data or [])
        if data or not chat_id:
            return data
        fallback = client.table(SUPABASE_TABLE).select("user_message, bot_message, created_at").eq(
            "phone", phone
        )
        if SUPABASE_APP:
            fallback = fallback.eq("app", SUPABASE_APP)
        resp = fallback.order("created_at", desc=True).limit(SUPABASE_SESSION_LIMIT).execute()
        return list(resp.data or [])

    try:
        return await anyio.to_thread.run_sync(fetch)
    except Exception as exc:
        logger.warning("Supabase fetch failed: %s", exc)
        return []


def supabase_storage_headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if SUPABASE_KEY:
        headers["Authorization"] = f"Bearer {SUPABASE_KEY}"
        headers["apikey"] = SUPABASE_KEY
    return headers


async def list_bucket_audio_files(bucket: str) -> list[Dict[str, str]]:
    result = await anyio.to_thread.run_sync(list_bucket_audio_files_sync_detailed, bucket)
    if result.get("error"):
        logger.warning("Supabase storage list failed bucket=%s: %s", bucket, result["error"])
    return list(result.get("files") or [])


async def build_bucket_audio_url(bucket: str, file_name: str) -> Optional[str]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket or not file_name:
        return None
    encoded_path = quote(file_name, safe="/")
    if CRIOLASER_AUDIO_PUBLIC_BUCKET:
        return f"{SUPABASE_URL}/storage/v1/object/public/{quote(bucket, safe='')}/{encoded_path}"

    url = f"{SUPABASE_URL}/storage/v1/object/sign/{quote(bucket, safe='')}/{encoded_path}"
    payload = {"expiresIn": max(CRIOLASER_AUDIO_SIGN_TTL, 60)}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload, headers=supabase_storage_headers())
        if resp.status_code >= 400:
            logger.warning("Supabase signed URL failed: %s %s", resp.status_code, resp.text)
            return None
        data = resp.json()
    except Exception as exc:
        logger.warning("Supabase signed URL request failed: %s", exc)
        return None

    signed_url = str(data.get("signedURL") or data.get("signedUrl") or "").strip()
    if not signed_url:
        return None
    if signed_url.startswith("http://") or signed_url.startswith("https://"):
        return signed_url
    if signed_url.startswith("/storage/v1/"):
        return f"{SUPABASE_URL}{signed_url}"
    if signed_url.startswith("/"):
        return f"{SUPABASE_URL}/storage/v1{signed_url}"
    return f"{SUPABASE_URL}/storage/v1/{signed_url.lstrip('/')}"


def storage_list_prefix_sync(bucket: str, prefix: str = "") -> tuple[list[Dict[str, Any]], Optional[str]]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket:
        return ([], "supabase_not_configured")
    url = f"{SUPABASE_URL}/storage/v1/object/list/{quote(bucket, safe='')}"
    payload = {
        "prefix": prefix,
        "limit": 100,
        "offset": 0,
        "sortBy": {"column": "name", "order": "asc"},
    }
    try:
        with httpx.Client(timeout=20) as client:
            resp = client.post(url, json=payload, headers=supabase_storage_headers())
        resp.raise_for_status()
        raw_items = resp.json()
    except Exception as exc:
        return ([], str(exc))
    if not isinstance(raw_items, list):
        return ([], "invalid_storage_list_response")
    return (raw_items, None)


def build_audio_file_item(name: str) -> Optional[Dict[str, str]]:
    clean_name = str(name or "").strip().strip("/")
    if not clean_name or not clean_name.lower().endswith(".ogg"):
        return None
    stem = clean_name.rsplit(".", 1)[0].strip()
    if not stem:
        return None
    return {
        "name": clean_name,
        "stem": stem,
        "normalized_stem": normalize_service_text(stem),
    }


def list_bucket_audio_files_sync_detailed(bucket: str) -> Dict[str, Any]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket:
        return {"files": [], "error": "supabase_not_configured"}

    now = time.time()
    cache_key = f"bucket:{bucket}"
    cached_bucket = AUDIO_FILE_CACHE.get("bucket")
    cached_files = AUDIO_FILE_CACHE.get("files") or []
    if (
        cached_bucket == cache_key
        and now < float(AUDIO_FILE_CACHE.get("expires_at") or 0)
        and isinstance(cached_files, list)
    ):
        return {"files": list(cached_files), "error": None}

    items: list[Dict[str, str]] = []
    seen_files: set[str] = set()
    visited_prefixes: set[str] = set()
    pending_prefixes: list[str] = [""]
    last_error: Optional[str] = None

    while pending_prefixes:
        prefix = pending_prefixes.pop(0)
        if prefix in visited_prefixes:
            continue
        visited_prefixes.add(prefix)
        raw_items, error = storage_list_prefix_sync(bucket, prefix)
        if error:
            last_error = error
            continue
        for raw in raw_items or []:
            if not isinstance(raw, dict):
                continue
            name = str(raw.get("name") or "").strip().strip("/")
            if not name:
                continue
            full_name = f"{prefix}/{name}".strip("/") if prefix else name
            built_item = build_audio_file_item(full_name)
            if built_item is not None:
                file_name = built_item["name"]
                if file_name not in seen_files:
                    items.append(built_item)
                    seen_files.add(file_name)
                continue
            is_folder = not raw.get("id") and not raw.get("metadata")
            if is_folder:
                child_prefix = full_name
                if child_prefix and child_prefix not in visited_prefixes:
                    pending_prefixes.append(child_prefix)

    AUDIO_FILE_CACHE["bucket"] = cache_key
    AUDIO_FILE_CACHE["files"] = list(items)
    AUDIO_FILE_CACHE["expires_at"] = now + max(CRIOLASER_AUDIO_CACHE_TTL_SECONDS, 30)
    return {"files": items, "error": last_error}


def list_bucket_audio_files_sync(bucket: str) -> list[Dict[str, str]]:
    result = list_bucket_audio_files_sync_detailed(bucket)
    if result.get("error"):
        logger.warning("Supabase storage list sync failed bucket=%s: %s", bucket, result["error"])
    return list(result.get("files") or [])


def build_bucket_audio_url_sync(bucket: str, file_name: str) -> Optional[str]:
    if not SUPABASE_URL or not SUPABASE_KEY or not bucket or not file_name:
        return None
    encoded_path = quote(file_name, safe="/")
    if CRIOLASER_AUDIO_PUBLIC_BUCKET:
        return f"{SUPABASE_URL}/storage/v1/object/public/{quote(bucket, safe='')}/{encoded_path}"

    url = f"{SUPABASE_URL}/storage/v1/object/sign/{quote(bucket, safe='')}/{encoded_path}"
    payload = {"expiresIn": max(CRIOLASER_AUDIO_SIGN_TTL, 60)}
    try:
        with httpx.Client(timeout=20) as client:
            resp = client.post(url, json=payload, headers=supabase_storage_headers())
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("Supabase signed URL sync failed: %s", exc)
        return None

    signed_url = str(data.get("signedURL") or data.get("signedUrl") or "").strip()
    if not signed_url:
        return None
    if signed_url.startswith("http://") or signed_url.startswith("https://"):
        return signed_url
    if signed_url.startswith("/storage/v1/"):
        return f"{SUPABASE_URL}{signed_url}"
    if signed_url.startswith("/"):
        return f"{SUPABASE_URL}/storage/v1{signed_url}"
    return f"{SUPABASE_URL}/storage/v1/{signed_url.lstrip('/')}"

