import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import anyio

from ..config.settings import SUPABASE_ENABLED
from .supabase import get_supabase_client


logger = logging.getLogger("agent")

SCHEMA = "agent"
_TENANT_CACHE: Dict[str, str] = {}


def _db():
    client = get_supabase_client()
    if client is None:
        return None
    try:
        return client.schema(SCHEMA)
    except Exception as exc:
        logger.warning("supabase schema(%s) unavailable: %s", SCHEMA, exc)
        return None


async def _run(fn):
    return await anyio.to_thread.run_sync(fn)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def resolve_tenant_id(slug: Optional[str]) -> Optional[str]:
    if not slug:
        return None
    cached = _TENANT_CACHE.get(slug)
    if cached:
        return cached
    db = _db()
    if db is None:
        return None

    def fetch():
        resp = db.table("tenants").select("id").eq("slug", slug).limit(1).execute()
        rows = list(resp.data or [])
        return rows[0]["id"] if rows else None

    try:
        tenant_id = await _run(fetch)
    except Exception as exc:
        logger.warning("resolve_tenant_id failed slug=%s: %s", slug, exc)
        return None
    if tenant_id:
        _TENANT_CACHE[slug] = tenant_id
    return tenant_id


async def upsert_contact(
    tenant_id: str,
    phone: str,
    wa_chat_id: Optional[str] = None,
    display_name: Optional[str] = None,
) -> Optional[str]:
    if not tenant_id or not phone:
        return None
    db = _db()
    if db is None:
        return None
    row: Dict[str, Any] = {
        "tenant_id": tenant_id,
        "phone": phone,
        "last_seen_at": _now_iso(),
    }
    if wa_chat_id:
        row["wa_chat_id"] = wa_chat_id
    if display_name:
        row["display_name"] = display_name

    def upsert():
        resp = (
            db.table("contacts")
            .upsert(row, on_conflict="tenant_id,phone")
            .execute()
        )
        rows = list(resp.data or [])
        return rows[0]["id"] if rows else None

    try:
        return await _run(upsert)
    except Exception as exc:
        logger.warning(
            "upsert_contact failed tenant=%s phone=%s: %s", tenant_id, phone, exc
        )
        return None


async def upsert_conversation(
    tenant_id: str,
    contact_id: str,
    wa_chat_id: Optional[str] = None,
    chatwoot_conversation_id: Optional[int] = None,
) -> Optional[str]:
    if not tenant_id or not contact_id:
        return None
    db = _db()
    if db is None:
        return None

    def find():
        resp = (
            db.table("conversations")
            .select("id")
            .eq("tenant_id", tenant_id)
            .eq("contact_id", contact_id)
            .eq("status", "active")
            .order("last_message_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = list(resp.data or [])
        return rows[0]["id"] if rows else None

    try:
        existing = await _run(find)
    except Exception as exc:
        logger.warning("find conversation failed: %s", exc)
        existing = None

    if existing:
        update_row: Dict[str, Any] = {"last_message_at": _now_iso()}
        if wa_chat_id:
            update_row["wa_chat_id"] = wa_chat_id
        if chatwoot_conversation_id:
            update_row["chatwoot_conversation_id"] = chatwoot_conversation_id

        def touch():
            db.table("conversations").update(update_row).eq("id", existing).execute()

        try:
            await _run(touch)
        except Exception as exc:
            logger.warning("touch conversation failed: %s", exc)
        return existing

    row: Dict[str, Any] = {"tenant_id": tenant_id, "contact_id": contact_id}
    if wa_chat_id:
        row["wa_chat_id"] = wa_chat_id
    if chatwoot_conversation_id:
        row["chatwoot_conversation_id"] = chatwoot_conversation_id

    def insert():
        resp = db.table("conversations").insert(row).execute()
        rows = list(resp.data or [])
        return rows[0]["id"] if rows else None

    try:
        return await _run(insert)
    except Exception as exc:
        logger.warning("insert conversation failed: %s", exc)
        return None


async def insert_messages(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    db = _db()
    if db is None:
        return

    def insert():
        db.table("messages").insert(rows).execute()

    try:
        await _run(insert)
    except Exception as exc:
        logger.warning("insert_messages failed (%d rows): %s", len(rows), exc)


async def record_turn(
    tenant_slug: str,
    phone: str,
    wa_chat_id: Optional[str] = None,
    display_name: Optional[str] = None,
    user_message: Optional[str] = None,
    bot_message: Optional[str] = None,
    message_type: Optional[str] = None,
) -> None:
    if not SUPABASE_ENABLED or not tenant_slug or not phone:
        return
    if not user_message and not bot_message:
        return
    tenant_id = await resolve_tenant_id(tenant_slug)
    if not tenant_id:
        logger.warning("record_turn: tenant not found slug=%s", tenant_slug)
        return
    contact_id = await upsert_contact(tenant_id, phone, wa_chat_id, display_name)
    if not contact_id:
        return
    conversation_id = await upsert_conversation(tenant_id, contact_id, wa_chat_id)
    if not conversation_id:
        return

    messages: List[Dict[str, Any]] = []
    if user_message:
        messages.append(
            {
                "tenant_id": tenant_id,
                "conversation_id": conversation_id,
                "contact_id": contact_id,
                "role": "user",
                "content": user_message,
                "message_type": message_type,
            }
        )
    if bot_message:
        messages.append(
            {
                "tenant_id": tenant_id,
                "conversation_id": conversation_id,
                "contact_id": contact_id,
                "role": "assistant",
                "content": bot_message,
                "message_type": message_type,
            }
        )
    await insert_messages(messages)


async def get_patient_context(
    tenant_slug: str, phone: str, limit: int = 12
) -> Optional[Dict[str, Any]]:
    if not SUPABASE_ENABLED or not tenant_slug or not phone:
        return None
    db = _db()
    if db is None:
        return None

    def call():
        resp = db.rpc(
            "get_patient_context",
            {
                "p_tenant_slug": tenant_slug,
                "p_phone": phone,
                "p_msg_limit": limit,
            },
        ).execute()
        return resp.data

    try:
        return await _run(call)
    except Exception as exc:
        logger.warning(
            "get_patient_context failed slug=%s phone=%s: %s", tenant_slug, phone, exc
        )
        return None


async def upsert_fact(
    tenant_id: str,
    contact_id: str,
    key: str,
    value: Any,
    source: Optional[str] = None,
    confidence: float = 1.0,
) -> None:
    if not tenant_id or not contact_id or not key:
        return
    db = _db()
    if db is None:
        return
    row = {
        "tenant_id": tenant_id,
        "contact_id": contact_id,
        "key": key,
        "value": value,
        "confidence": confidence,
        "source": source,
    }

    def upsert():
        db.table("contact_facts").upsert(
            row, on_conflict="tenant_id,contact_id,key"
        ).execute()

    try:
        await _run(upsert)
    except Exception as exc:
        logger.warning("upsert_fact failed: %s", exc)


async def update_contact_summary(contact_id: str, summary: str) -> None:
    if not contact_id:
        return
    db = _db()
    if db is None:
        return

    def update():
        db.table("contacts").update({"summary": summary}).eq("id", contact_id).execute()

    try:
        await _run(update)
    except Exception as exc:
        logger.warning("update_contact_summary failed: %s", exc)


async def create_handoff(
    tenant_id: str,
    conversation_id: str,
    contact_id: str,
    reason: Optional[str] = None,
    to_agent: Optional[str] = None,
) -> Optional[str]:
    if not tenant_id or not conversation_id or not contact_id:
        return None
    db = _db()
    if db is None:
        return None
    row = {
        "tenant_id": tenant_id,
        "conversation_id": conversation_id,
        "contact_id": contact_id,
        "reason": reason,
        "to_agent": to_agent,
    }

    def insert():
        resp = db.table("handoffs").insert(row).execute()
        rows = list(resp.data or [])
        return rows[0]["id"] if rows else None

    try:
        return await _run(insert)
    except Exception as exc:
        logger.warning("create_handoff failed: %s", exc)
        return None


async def close_handoff(handoff_id: str, notes: Optional[str] = None) -> None:
    if not handoff_id:
        return
    db = _db()
    if db is None:
        return
    row: Dict[str, Any] = {"status": "closed", "resolved_at": _now_iso()}
    if notes:
        row["notes"] = notes

    def update():
        db.table("handoffs").update(row).eq("id", handoff_id).execute()

    try:
        await _run(update)
    except Exception as exc:
        logger.warning("close_handoff failed: %s", exc)


async def upsert_appointment(
    tenant_id: str,
    contact_id: str,
    scheduled_at: str,
    conversation_id: Optional[str] = None,
    procedure: Optional[str] = None,
    professional: Optional[str] = None,
    status: str = "scheduled",
    external_id: Optional[str] = None,
    notes: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if not tenant_id or not contact_id or not scheduled_at:
        return None
    db = _db()
    if db is None:
        return None
    row: Dict[str, Any] = {
        "tenant_id": tenant_id,
        "contact_id": contact_id,
        "scheduled_at": scheduled_at,
        "status": status,
    }
    if conversation_id:
        row["conversation_id"] = conversation_id
    if procedure:
        row["procedure"] = procedure
    if professional:
        row["professional"] = professional
    if external_id:
        row["external_id"] = external_id
    if notes:
        row["notes"] = notes
    if metadata:
        row["metadata"] = metadata

    def write():
        if external_id:
            resp = (
                db.table("appointments")
                .upsert(row, on_conflict="external_id")
                .execute()
            )
        else:
            resp = db.table("appointments").insert(row).execute()
        rows = list(resp.data or [])
        return rows[0]["id"] if rows else None

    try:
        return await _run(write)
    except Exception as exc:
        logger.warning("upsert_appointment failed: %s", exc)
        return None
