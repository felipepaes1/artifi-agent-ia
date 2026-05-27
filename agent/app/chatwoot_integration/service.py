import hashlib
import hmac
import logging
import os
import re
import threading
import time
import contextvars
import unicodedata
from typing import Any, Awaitable, Callable, Optional

from .client import ChatwootApiError, ChatwootClient, ChatwootConfig
from .store import ChatwootMapping, ChatwootStore


logger = logging.getLogger("agent.chatwoot")

SendWhatsappMessage = Callable[[str, str], Awaitable[bool]]

_SERVICE: Optional["ChatwootService"] = None
_BACKEND_ECHO_PREFIX = "backend:"
_SUPPRESS_OUTBOUND_SYNC: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "chatwoot_suppress_outbound_sync",
    default=False,
)

# Chatwoot strips echo_id from outbound webhook payloads and the message_created
# webhook can race ahead of our own create_outgoing_message HTTP response by
# ~200ms, so message_id-based dedupe is unreliable. Track recently-issued
# (conversation_id, content) tuples in-memory and use them as a fingerprint to
# break the loop when the webhook fires for our own outbound posts.
_RECENT_OUTBOUND_TTL_SECONDS = 60
_recent_outbound_lock = threading.Lock()
_recent_outbound: dict[tuple[int, str], float] = {}


def _outbound_fingerprint(conversation_id: int, content: str) -> Optional[tuple[int, str]]:
    if not conversation_id or not content:
        return None
    return (int(conversation_id), str(content).strip())


def _mark_recent_outbound(conversation_id: int, content: str) -> None:
    key = _outbound_fingerprint(conversation_id, content)
    if key is None:
        return
    now = time.time()
    cutoff = now - _RECENT_OUTBOUND_TTL_SECONDS
    with _recent_outbound_lock:
        _recent_outbound[key] = now
        for stale_key, ts in list(_recent_outbound.items()):
            if ts < cutoff:
                del _recent_outbound[stale_key]


def _consume_recent_outbound(conversation_id: int, content: str) -> bool:
    key = _outbound_fingerprint(conversation_id, content)
    if key is None:
        return False
    cutoff = time.time() - _RECENT_OUTBOUND_TTL_SECONDS
    with _recent_outbound_lock:
        ts = _recent_outbound.get(key)
        if ts is None:
            return False
        if ts < cutoff:
            del _recent_outbound[key]
            return False
        del _recent_outbound[key]
        return True


class ChatwootService:
    def __init__(self, config: ChatwootConfig) -> None:
        self.config = config
        self.client = ChatwootClient(config)
        self.store = ChatwootStore(config.state_db_path)

    def sync_enabled(self) -> bool:
        return self.config.sync_enabled

    def should_sync_outgoing_whatsapp_message(self) -> bool:
        return self.sync_enabled() and not _SUPPRESS_OUTBOUND_SYNC.get()

    def verify_signature(self, raw_body: bytes, headers: dict[str, str]) -> bool:
        secret = self.config.webhook_secret
        if not secret:
            return True
        signature = (
            headers.get("x-chatwoot-signature")
            or headers.get("X-Chatwoot-Signature")
            or ""
        ).strip()
        timestamp = (
            headers.get("x-chatwoot-timestamp")
            or headers.get("X-Chatwoot-Timestamp")
            or ""
        ).strip()
        if not signature or not timestamp:
            return False
        payload = timestamp.encode("utf-8") + b"." + raw_body
        digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
        normalized = signature.removeprefix("sha256=").strip()
        return hmac.compare_digest(normalized, digest)

    def is_human_handoff_active(self, chat_id: str) -> bool:
        mapping = self.store.get_by_chat_id(str(chat_id or "").strip())
        if not mapping:
            return False
        status = _normalize_chatwoot_status(mapping.conversation_status)
        if status in ("pending", "resolved"):
            return False
        return mapping.handoff_state == "human"

    def should_request_human_handoff(self, content: str) -> bool:
        text = _normalize_handoff_text(content)
        if not text:
            return False
        patterns = (
            r"\bfalar com (um |uma |o |a )?(atendente|humano|pessoa|recepcao|equipe)\b",
            r"\bquero (um |uma |o |a )?(atendente|humano|pessoa|recepcao)\b",
            r"\bpreciso (de |falar com )?(um |uma |o |a )?(atendente|humano|pessoa|recepcao|equipe)\b",
            r"\bme (passa|passe|transfer(e|ir)|encaminha|encaminhe) (para|pra) (um |uma |o |a )?(atendente|humano|pessoa|recepcao|equipe)\b",
            r"\bchamar (um |uma |o |a )?(atendente|humano|pessoa|recepcao|equipe)\b",
        )
        return any(re.search(pattern, text) for pattern in patterns)

    async def handoff_to_human(
        self,
        *,
        chat_id: str,
        phone: str,
        contact_name: str,
        reason: str = "",
    ) -> bool:
        if not self.config.account_mode:
            return False
        chat_id = str(chat_id or "").strip()
        if not chat_id:
            return False
        phone_digits = _normalize_phone(phone) or _normalize_phone(chat_id)
        mapping = await self._ensure_mapping(
            chat_id=chat_id,
            phone=phone_digits,
            contact_name=contact_name,
        )
        if not mapping or not mapping.conversation_id:
            return False

        status = self.config.human_handoff_status or "open"
        try:
            await self.client.update_conversation_status(
                conversation_id=int(mapping.conversation_id),
                status=status,
            )
        except ChatwootApiError as exc:
            logger.warning(
                "Chatwoot handoff status update failed: conversation_id=%s status=%s response=%s",
                mapping.conversation_id,
                exc.status_code,
                exc.response_text,
            )

        mapping.handoff_state = "human"
        mapping.conversation_status = status
        mapping.last_handoff_at = int(time.time())
        self.store.upsert_mapping(mapping)

        note = "Atendimento transferido para humano pela IA."
        compact_reason = " ".join(str(reason or "").split()).strip()
        if compact_reason:
            note = f"{note} Motivo: {compact_reason[:240]}"
        try:
            await self.client.create_private_note(
                conversation_id=int(mapping.conversation_id),
                content=note,
            )
        except ChatwootApiError as exc:
            logger.warning(
                "Chatwoot handoff private note failed: conversation_id=%s status=%s response=%s",
                mapping.conversation_id,
                exc.status_code,
                exc.response_text,
            )
        return True

    async def sync_incoming_whatsapp_message(
        self,
        *,
        chat_id: str,
        phone: str,
        contact_name: str,
        content: str,
        message_id: str = "",
    ) -> None:
        if not self.sync_enabled():
            return
        chat_id = str(chat_id or "").strip()
        content = str(content or "").strip()
        if not chat_id or not content:
            return

        phone_digits = _normalize_phone(phone) or _normalize_phone(chat_id)
        try:
            mapping = await self._ensure_mapping(
                chat_id=chat_id,
                phone=phone_digits,
                contact_name=contact_name,
            )
            if not mapping or not mapping.conversation_id or not mapping.contact_source_id:
                return
            try:
                await self.client.create_incoming_message(
                    conversation_id=int(mapping.conversation_id),
                    content=content,
                    contact_source_id=mapping.contact_source_id,
                    echo_id=message_id,
                )
            except ChatwootApiError as exc:
                if exc.status_code != 404:
                    raise
                logger.warning(
                    "Chatwoot conversation stale, recreating mapping for chat_id=%s status=%s",
                    chat_id,
                    exc.status_code,
                )
                self.store.clear_mapping(chat_id)
                mapping = await self._ensure_mapping(
                    chat_id=chat_id,
                    phone=phone_digits,
                    contact_name=contact_name,
                )
                if not mapping or not mapping.conversation_id or not mapping.contact_source_id:
                    return
                await self.client.create_incoming_message(
                    conversation_id=int(mapping.conversation_id),
                    content=content,
                    contact_source_id=mapping.contact_source_id,
                    echo_id=message_id,
                )
            logger.info(
                "Chatwoot inbound sync: conversation_id=%s phone=%s content=%s",
                mapping.conversation_id,
                mapping.phone or phone_digits,
                content,
            )
        except ChatwootApiError as exc:
            logger.warning(
                "Chatwoot sync failed: status=%s response=%s",
                exc.status_code,
                exc.response_text,
            )
        except Exception as exc:
            logger.exception("Chatwoot sync failed unexpectedly: %s", exc)

    async def sync_incoming_whatsapp_media(
        self,
        *,
        chat_id: str,
        phone: str,
        contact_name: str,
        content: str,
        file_bytes: bytes,
        filename: str,
        content_type: str,
        file_type: str,
        message_id: str = "",
    ) -> None:
        if not self.sync_enabled():
            return
        chat_id = str(chat_id or "").strip()
        content = str(content or "").strip() or filename or "Arquivo recebido"
        if not chat_id:
            return
        if not file_bytes or not self.config.account_mode:
            await self.sync_incoming_whatsapp_message(
                chat_id=chat_id,
                phone=phone,
                contact_name=contact_name,
                content=content,
                message_id=message_id,
            )
            return

        phone_digits = _normalize_phone(phone) or _normalize_phone(chat_id)
        try:
            mapping = await self._ensure_mapping(
                chat_id=chat_id,
                phone=phone_digits,
                contact_name=contact_name,
            )
            if not mapping or not mapping.conversation_id:
                return
            try:
                await self.client.create_incoming_attachment_message(
                    conversation_id=int(mapping.conversation_id),
                    content=content,
                    file_bytes=file_bytes,
                    filename=filename or "attachment",
                    content_type=content_type or "application/octet-stream",
                    file_type=file_type or "file",
                    echo_id=message_id,
                )
            except ChatwootApiError as exc:
                if exc.status_code != 404:
                    raise
                logger.warning(
                    "Chatwoot media conversation stale, recreating mapping for chat_id=%s status=%s",
                    chat_id,
                    exc.status_code,
                )
                self.store.clear_mapping(chat_id)
                mapping = await self._ensure_mapping(
                    chat_id=chat_id,
                    phone=phone_digits,
                    contact_name=contact_name,
                )
                if not mapping or not mapping.conversation_id:
                    return
                await self.client.create_incoming_attachment_message(
                    conversation_id=int(mapping.conversation_id),
                    content=content,
                    file_bytes=file_bytes,
                    filename=filename or "attachment",
                    content_type=content_type or "application/octet-stream",
                    file_type=file_type or "file",
                    echo_id=message_id,
                )
            logger.info(
                "Chatwoot inbound media sync: conversation_id=%s phone=%s filename=%s file_type=%s",
                mapping.conversation_id,
                mapping.phone or phone_digits,
                filename,
                file_type,
            )
        except ChatwootApiError as exc:
            logger.warning(
                "Chatwoot media sync failed: status=%s response=%s",
                exc.status_code,
                exc.response_text,
            )
        except Exception as exc:
            logger.exception("Chatwoot media sync failed unexpectedly: %s", exc)

    async def sync_outgoing_whatsapp_message(
        self,
        *,
        chat_id: str,
        phone: str,
        contact_name: str,
        content: str,
        message_id: str = "",
    ) -> None:
        if not self.should_sync_outgoing_whatsapp_message():
            return
        if not self.config.account_mode:
            return

        chat_id = str(chat_id or "").strip()
        content = str(content or "").strip()
        if not chat_id or not content:
            return

        phone_digits = _normalize_phone(phone) or _normalize_phone(chat_id)
        echo_id = self._build_backend_echo_id(message_id)
        try:
            mapping = await self._ensure_mapping(
                chat_id=chat_id,
                phone=phone_digits,
                contact_name=contact_name,
            )
            if not mapping or not mapping.conversation_id:
                return
            # Mark the fingerprint BEFORE the POST: Chatwoot's message_created
            # webhook frequently races ahead of the create_outgoing_message HTTP
            # response, so marking after the call is too late.
            _mark_recent_outbound(int(mapping.conversation_id), content)
            try:
                created = await self.client.create_outgoing_message(
                    conversation_id=int(mapping.conversation_id),
                    content=content,
                    echo_id=echo_id,
                )
            except ChatwootApiError as exc:
                if exc.status_code != 404:
                    raise
                logger.warning(
                    "Chatwoot outbound conversation stale, recreating mapping for chat_id=%s status=%s",
                    chat_id,
                    exc.status_code,
                )
                self.store.clear_mapping(chat_id)
                mapping = await self._ensure_mapping(
                    chat_id=chat_id,
                    phone=phone_digits,
                    contact_name=contact_name,
                )
                if not mapping or not mapping.conversation_id:
                    return
                _mark_recent_outbound(int(mapping.conversation_id), content)
                created = await self.client.create_outgoing_message(
                    conversation_id=int(mapping.conversation_id),
                    content=content,
                    echo_id=echo_id,
                )
            created_id = str((created or {}).get("id") or "").strip()
            if created_id:
                self.store.mark_processed_message(created_id)
            logger.info(
                "Chatwoot outbound sync: conversation_id=%s phone=%s content=%s",
                mapping.conversation_id,
                mapping.phone or phone_digits,
                content,
            )
        except ChatwootApiError as exc:
            logger.warning(
                "Chatwoot outbound sync failed: status=%s response=%s",
                exc.status_code,
                exc.response_text,
            )
        except Exception as exc:
            logger.exception("Chatwoot outbound sync failed unexpectedly: %s", exc)

    def extract_outbound_event(
        self, payload: dict[str, Any]
    ) -> "dict[str, Any] | str":
        """Validate and extract data from a Chatwoot webhook payload.

        Returns a string (ignored reason) or a dict with keys:
        message_id, conversation_id, content, chat_id, phone, contact_name.
        This is fast/sync so the HTTP handler can respond 200 immediately.
        """
        event = str(payload.get("event") or "").strip().lower()
        message_id = str(payload.get("id") or "").strip()
        conversation = payload.get("conversation") or {}
        conversation_id = _coerce_int(payload.get("conversation_id")) or _coerce_int(
            conversation.get("id")
        )
        if event in ("conversation_status_changed", "conversation_updated"):
            conversation_id = conversation_id or _coerce_int(payload.get("id"))

        logger.info(
            "Chatwoot webhook received: event=%s message_id=%s conversation_id=%s",
            event or None,
            message_id or None,
            conversation_id,
        )

        if event in ("conversation_status_changed", "conversation_updated"):
            return self._process_conversation_state_event(payload, conversation_id)
        if event != "message_created":
            return "event"
        if message_id and self.store.is_processed_message(message_id):
            return "duplicate_message"
        if self._is_backend_origin_message(payload):
            return "backend_origin"
        if not self._is_human_agent_message(payload):
            return "not_human_agent"

        content = str(payload.get("content") or "").strip()
        if not content:
            return "empty_content"

        if conversation_id and _consume_recent_outbound(conversation_id, content):
            if message_id:
                self.store.mark_processed_message(message_id)
            return "recent_outbound"

        mapping = self.store.get_by_conversation_id(conversation_id) if conversation_id else None
        chat_id = (mapping.whatsapp_chat_id if mapping else "") or self._extract_chat_id(payload)
        phone = (mapping.phone if mapping else "") or self._extract_phone(payload)
        if not chat_id:
            logger.warning(
                "Chatwoot event ignored because target chat was not resolved: conversation_id=%s message_id=%s",
                conversation_id,
                message_id or None,
            )
            return "missing_chat_id"

        return {
            "message_id": message_id,
            "conversation_id": conversation_id,
            "content": content,
            "chat_id": chat_id,
            "phone": phone,
            "contact_name": self._extract_contact_name(payload),
        }

    async def deliver_outbound_event(
        self,
        extracted: dict[str, Any],
        *,
        send_whatsapp_message: SendWhatsappMessage,
    ) -> None:
        """Deliver a human-agent message to WhatsApp. Runs in background."""
        chat_id = extracted["chat_id"]
        content = extracted["content"]
        message_id = extracted["message_id"]
        conversation_id = extracted["conversation_id"]
        phone = extracted["phone"]
        contact_name = extracted["contact_name"]

        logger.info(
            "Chatwoot event received: conversation_id=%s phone=%s content=%s",
            conversation_id,
            phone or _normalize_phone(chat_id),
            content,
        )
        token = _SUPPRESS_OUTBOUND_SYNC.set(True)
        try:
            delivered = await send_whatsapp_message(chat_id, content)
        finally:
            _SUPPRESS_OUTBOUND_SYNC.reset(token)
        if not delivered:
            logger.warning(
                "Chatwoot outbound delivery aborted by WhatsApp: conversation_id=%s chat_id=%s",
                conversation_id,
                chat_id,
            )
            return
        if message_id:
            self.store.mark_processed_message(message_id)
        if conversation_id:
            existing = self.store.get_by_chat_id(chat_id) or self.store.get_by_conversation_id(
                conversation_id
            )
            mapping = existing or ChatwootMapping(whatsapp_chat_id=chat_id)
            mapping.phone = phone or mapping.phone or _normalize_phone(chat_id)
            mapping.contact_name = contact_name or mapping.contact_name
            mapping.conversation_id = conversation_id
            mapping.identifier = mapping.identifier or _preferred_contact_identifier(chat_id, phone)
            mapping.handoff_state = "human"
            mapping.conversation_status = self.config.human_handoff_status or "open"
            mapping.last_handoff_at = int(time.time())
            self.store.upsert_mapping(mapping)
            try:
                await self.client.update_conversation_status(
                    conversation_id=int(conversation_id),
                    status=self.config.human_handoff_status or "open",
                )
            except ChatwootApiError as exc:
                logger.warning(
                    "Chatwoot human reply status update failed: conversation_id=%s status=%s response=%s",
                    conversation_id,
                    exc.status_code,
                    exc.response_text,
                )

    async def process_message_created_event(
        self,
        payload: dict[str, Any],
        *,
        send_whatsapp_message: SendWhatsappMessage,
    ) -> dict[str, Any]:
        """Legacy synchronous path — kept for backwards compatibility with tests."""
        result = self.extract_outbound_event(payload)
        if isinstance(result, str):
            return {"ok": True, "ignored": result}
        await self.deliver_outbound_event(result, send_whatsapp_message=send_whatsapp_message)
        return {
            "ok": True,
            "delivered": True,
            "conversation_id": result["conversation_id"],
            "phone": result["phone"] or _normalize_phone(result["chat_id"]),
        }

    async def _ensure_mapping(
        self,
        *,
        chat_id: str,
        phone: str,
        contact_name: str,
    ) -> Optional[ChatwootMapping]:
        identifier = _preferred_contact_identifier(chat_id, phone)
        contact_display_name = _contact_display_name(contact_name, phone, identifier)
        existing = self.store.get_by_chat_id(chat_id)
        if existing and _normalize_chatwoot_status(existing.conversation_status) == "resolved":
            existing.conversation_id = None
        if existing and existing.contact_source_id and existing.conversation_id:
            existing_name = existing.contact_name
            if (
                (contact_display_name and contact_display_name != existing.contact_name)
                or (phone and phone != existing.phone)
                or (identifier and identifier != existing.identifier)
            ):
                existing.contact_name = contact_display_name or existing.contact_name
                existing.phone = phone or existing.phone
                existing.identifier = identifier or existing.identifier
                self.store.upsert_mapping(existing)
            if (
                self.config.account_mode
                and existing.contact_id
                and _should_repair_contact_name(existing_name, contact_name, contact_display_name)
            ):
                await self._update_account_contact(
                    contact_id=int(existing.contact_id),
                    identifier=identifier,
                    display_name=contact_display_name,
                    phone_number=_format_phone_number(phone),
                )
            return existing

        if self.config.account_mode:
            mapping = await self._ensure_account_mapping(
                existing=existing,
                chat_id=chat_id,
                phone=phone,
                contact_name=contact_name,
                identifier=identifier,
            )
        else:
            mapping = await self._ensure_public_mapping(
                existing=existing,
                chat_id=chat_id,
                phone=phone,
                contact_name=contact_name,
                identifier=identifier,
            )
        if mapping:
            self.store.upsert_mapping(mapping)
        return mapping

    async def _ensure_account_mapping(
        self,
        *,
        existing: Optional[ChatwootMapping],
        chat_id: str,
        phone: str,
        contact_name: str,
        identifier: str,
    ) -> Optional[ChatwootMapping]:
        contact_id = existing.contact_id if existing else None
        contact_source_id = existing.contact_source_id if existing else ""
        phone_number = _format_phone_number(phone)
        display_name = _contact_display_name(contact_name, phone, identifier)
        picked_contact: dict[str, Any] = {}

        if not contact_id:
            contacts = await self.client.search_contacts(identifier)
            if phone and phone != identifier:
                contacts.extend(await self.client.search_contacts(phone))
            contact = self._pick_contact(contacts, identifier=identifier, phone=phone)
            if contact:
                picked_contact = contact
                contact_id = _coerce_int(contact.get("id"))
                contact_source_id = self._extract_contact_source_id(contact) or contact_source_id

        if not contact_id:
            created_contact = await self.client.create_contact(
                identifier=identifier,
                name=display_name,
                phone_number=phone_number,
            )
            picked_contact = created_contact
            contact_id = _coerce_int(created_contact.get("id"))
            contact_source_id = self._extract_contact_source_id(created_contact)
        elif _should_update_existing_contact(
            picked_contact=picked_contact,
            stored_name=existing.contact_name if existing else "",
            requested_name=contact_name,
            display_name=display_name,
            phone_number=phone_number,
            identifier=identifier,
        ):
            await self._update_account_contact(
                contact_id=int(contact_id),
                identifier=identifier,
                display_name=display_name,
                phone_number=phone_number,
            )

        if contact_id and not contact_source_id:
            contact_source_id = self._pick_contactable_source_id(
                await self.client.get_contactable_inboxes(int(contact_id))
            )

        if contact_id and not contact_source_id:
            created_contact_inbox = await self.client.create_contact_inbox(int(contact_id), identifier)
            contact_source_id = self._extract_contact_source_id(created_contact_inbox) or identifier

        if not contact_id or not contact_source_id:
            return None

        conversation_id = existing.conversation_id if existing else None
        conversation_status = existing.conversation_status if existing else ""
        if not conversation_id:
            conversations = await self.client.list_contact_conversations(int(contact_id))
            conversation = self._pick_conversation(conversations)
            if conversation:
                conversation_id = _coerce_int(conversation.get("id"))
                conversation_status = _normalize_chatwoot_status(conversation.get("status"))

        if not conversation_id:
            conversation = await self.client.create_conversation(
                contact_id=int(contact_id),
                contact_source_id=contact_source_id,
            )
            conversation_id = _coerce_int(conversation.get("id"))
            conversation_status = (
                _normalize_chatwoot_status(conversation.get("status"))
                or self.config.ai_conversation_status
                or "pending"
            )

        handoff_state = "human" if conversation_status in ("open", "snoozed") else "ai"

        return ChatwootMapping(
            whatsapp_chat_id=chat_id,
            phone=phone,
            contact_name=display_name or (existing.contact_name if existing else ""),
            contact_id=contact_id,
            contact_source_id=contact_source_id,
            conversation_id=conversation_id,
            identifier=identifier,
            handoff_state=handoff_state,
            conversation_status=conversation_status,
            last_handoff_at=(
                int(time.time())
                if handoff_state == "human"
                else (existing.last_handoff_at if existing else 0)
            ),
        )

    async def _ensure_public_mapping(
        self,
        *,
        existing: Optional[ChatwootMapping],
        chat_id: str,
        phone: str,
        contact_name: str,
        identifier: str,
    ) -> Optional[ChatwootMapping]:
        contact_id = existing.contact_id if existing else None
        contact_source_id = existing.contact_source_id if existing else ""
        phone_number = _format_phone_number(phone)
        display_name = _contact_display_name(contact_name, phone, identifier)

        if not contact_source_id:
            contact = await self.client.create_contact(
                identifier=identifier,
                name=display_name,
                phone_number=phone_number,
            )
            contact_id = _coerce_int(contact.get("id")) or contact_id
            contact_source_id = self._extract_contact_source_id(contact) or identifier

        conversation_id = existing.conversation_id if existing else None
        conversation_status = existing.conversation_status if existing else ""
        if not conversation_id:
            conversation = await self.client.create_conversation(
                contact_id=contact_id,
                contact_source_id=contact_source_id,
            )
            conversation_id = _coerce_int(conversation.get("id"))
            conversation_status = _normalize_chatwoot_status(conversation.get("status")) or "open"

        return ChatwootMapping(
            whatsapp_chat_id=chat_id,
            phone=phone,
            contact_name=display_name or (existing.contact_name if existing else ""),
            contact_id=contact_id,
            contact_source_id=contact_source_id,
            conversation_id=conversation_id,
            identifier=identifier,
            handoff_state="ai",
            conversation_status=conversation_status,
            last_handoff_at=existing.last_handoff_at if existing else 0,
        )

    def _pick_contact(
        self,
        contacts: list[dict[str, Any]],
        *,
        identifier: str,
        phone: str,
    ) -> Optional[dict[str, Any]]:
        exact_identifier = []
        exact_phone = []
        fallback = []
        for contact in contacts:
            if not isinstance(contact, dict):
                continue
            fallback.append(contact)
            contact_identifier = str(contact.get("identifier") or "").strip()
            if contact_identifier == identifier:
                exact_identifier.append(contact)
            contact_phone = _normalize_phone(str(contact.get("phone_number") or ""))
            if phone and contact_phone == phone:
                exact_phone.append(contact)
        if exact_identifier:
            return exact_identifier[0]
        if exact_phone:
            return exact_phone[0]
        return fallback[0] if fallback else None

    def _pick_conversation(self, conversations: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        target_inbox_id = _coerce_int(self.config.inbox_id)
        pending = []
        human = []
        unknown = []
        fallback = []
        for conversation in conversations:
            if not isinstance(conversation, dict):
                continue
            fallback.append(conversation)
            inbox_id = _coerce_int(conversation.get("inbox_id"))
            status = _normalize_chatwoot_status(conversation.get("status"))
            if target_inbox_id and inbox_id != target_inbox_id:
                continue
            if status == "pending":
                pending.append(conversation)
            elif status in ("open", "snoozed"):
                human.append(conversation)
            elif not status:
                unknown.append(conversation)
        if pending:
            return pending[0]
        if human:
            return human[0]
        if unknown:
            return unknown[0]
        if target_inbox_id:
            return None
        return fallback[0] if fallback else None

    def _pick_contactable_source_id(self, items: list[dict[str, Any]]) -> str:
        target_inbox_id = _coerce_int(self.config.inbox_id)
        fallback = ""
        for item in items:
            if not isinstance(item, dict):
                continue
            source_id = str(item.get("source_id") or item.get("sourceId") or "").strip()
            if not source_id:
                continue
            if not fallback:
                fallback = source_id
            inbox_id = _coerce_int(item.get("inbox_id") or item.get("id"))
            if target_inbox_id and inbox_id == target_inbox_id:
                return source_id
        return fallback

    def _extract_contact_source_id(self, payload: dict[str, Any]) -> str:
        direct = str(
            payload.get("source_id")
            or payload.get("contact_source_id")
            or ""
        ).strip()
        if direct:
            return direct

        contact_inboxes = payload.get("contact_inboxes") or payload.get("contactInboxes") or []
        if not isinstance(contact_inboxes, list):
            return ""
        target_inbox_id = _coerce_int(self.config.inbox_id)
        fallback = ""
        for item in contact_inboxes:
            if not isinstance(item, dict):
                continue
            source_id = str(item.get("source_id") or item.get("sourceId") or "").strip()
            if not source_id:
                continue
            if not fallback:
                fallback = source_id
            inbox = item.get("inbox") or {}
            inbox_id = _coerce_int(item.get("inbox_id") or inbox.get("id"))
            if target_inbox_id and inbox_id == target_inbox_id:
                return source_id
        return fallback

    async def _update_account_contact(
        self,
        *,
        contact_id: int,
        identifier: str,
        display_name: str,
        phone_number: str,
    ) -> None:
        try:
            await self.client.update_contact(
                int(contact_id),
                identifier=identifier,
                name=display_name,
                phone_number=phone_number,
            )
        except ChatwootApiError as exc:
            logger.warning(
                "Chatwoot contact update failed: contact_id=%s status=%s response=%s",
                contact_id,
                exc.status_code,
                exc.response_text,
            )

    def _process_conversation_state_event(
        self,
        payload: dict[str, Any],
        conversation_id: Optional[int],
    ) -> str:
        if not conversation_id:
            return "conversation_missing_id"
        status = _extract_status_from_payload(payload)
        if not status:
            return "conversation_status_missing"

        if status == "pending":
            self.store.update_handoff_state(
                int(conversation_id),
                handoff_state="ai",
                conversation_status=status,
            )
            return "conversation_pending_ai"
        if status == "resolved":
            self.store.release_conversation(
                int(conversation_id),
                conversation_status=status,
            )
            return "conversation_resolved_ai"

        handoff_state = "human" if status in ("open", "snoozed") else "ai"
        self.store.update_handoff_state(
            int(conversation_id),
            handoff_state=handoff_state,
            conversation_status=status,
        )
        return f"conversation_{status}_{handoff_state}"

    def _is_human_agent_message(self, payload: dict[str, Any]) -> bool:
        raw_message_type = payload.get("message_type")
        if raw_message_type in (1, "1"):
            message_type = "outgoing"
        elif raw_message_type in (0, "0"):
            message_type = "incoming"
        else:
            message_type = str(raw_message_type or "").strip().lower()
        if message_type != "outgoing":
            return False
        if _as_bool(payload.get("private")):
            return False

        sender = payload.get("sender") or {}
        sender_type = str(payload.get("sender_type") or sender.get("type") or "").strip().lower()
        if sender_type in ("contact", "agent_bot", "bot"):
            return False
        if sender_type in ("user", "agent"):
            return True
        return bool(sender.get("email") or sender.get("availability_status"))

    def _is_backend_origin_message(self, payload: dict[str, Any]) -> bool:
        echo_id = str(payload.get("echo_id") or "").strip().lower()
        return echo_id.startswith(_BACKEND_ECHO_PREFIX)

    def _extract_chat_id(self, payload: dict[str, Any]) -> str:
        contact = payload.get("contact") or {}
        conversation = payload.get("conversation") or {}
        meta = conversation.get("meta") or {}
        sender_meta = meta.get("sender") or {}

        for value in (
            contact.get("identifier"),
            sender_meta.get("identifier"),
            contact.get("phone_number"),
            sender_meta.get("phone_number"),
        ):
            chat_id = _to_whatsapp_chat_id(value)
            if chat_id:
                return chat_id
        return ""

    def _extract_phone(self, payload: dict[str, Any]) -> str:
        contact = payload.get("contact") or {}
        conversation = payload.get("conversation") or {}
        meta = conversation.get("meta") or {}
        sender_meta = meta.get("sender") or {}

        for value in (
            contact.get("phone_number"),
            sender_meta.get("phone_number"),
            contact.get("identifier"),
            sender_meta.get("identifier"),
        ):
            digits = _normalize_phone(str(value or ""))
            if digits:
                return digits
        return ""

    def _extract_contact_name(self, payload: dict[str, Any]) -> str:
        contact = payload.get("contact") or {}
        for value in (
            contact.get("name"),
            payload.get("source_name"),
            payload.get("sender", {}).get("name"),
        ):
            text = str(value or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _build_backend_echo_id(message_id: str) -> str:
        suffix = str(message_id or "").strip()
        if not suffix:
            suffix = str(int(time.time() * 1000))
        return f"{_BACKEND_ECHO_PREFIX}{suffix}"


def get_chatwoot_service() -> ChatwootService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = ChatwootService(
            ChatwootConfig(
                base_url=os.getenv("CHATWOOT_BASE_URL", "").rstrip("/"),
                account_id=os.getenv("CHATWOOT_ACCOUNT_ID", "").strip(),
                api_access_token=os.getenv("CHATWOOT_API_ACCESS_TOKEN", "").strip(),
                inbox_id=os.getenv("CHATWOOT_INBOX_ID", "").strip(),
                inbox_identifier=os.getenv("CHATWOOT_INBOX_IDENTIFIER", "").strip(),
                webhook_secret=os.getenv("CHATWOOT_WEBHOOK_SECRET", "").strip(),
                state_db_path=os.getenv("CHATWOOT_STATE_DB", "chatwoot_state.db").strip()
                or "chatwoot_state.db",
                ai_conversation_status=_normalize_chatwoot_status(
                    os.getenv("CHATWOOT_AI_STATUS", "pending")
                )
                or "pending",
                human_handoff_status=_normalize_chatwoot_status(
                    os.getenv("CHATWOOT_HUMAN_STATUS", "open")
                )
                or "open",
            )
        )
    return _SERVICE


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "yes", "sim")


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_chatwoot_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    if status in ("open", "resolved", "pending", "snoozed"):
        return status
    return ""


def _extract_status_from_payload(payload: dict[str, Any]) -> str:
    status = _normalize_chatwoot_status(payload.get("status"))
    if status:
        return status
    conversation = payload.get("conversation") or {}
    if isinstance(conversation, dict):
        status = _normalize_chatwoot_status(conversation.get("status"))
        if status:
            return status

    changed_attributes = payload.get("changed_attributes") or payload.get("changedAttributes") or []
    if isinstance(changed_attributes, list):
        for item in changed_attributes:
            if not isinstance(item, dict):
                continue
            raw = item.get("status")
            if isinstance(raw, dict):
                status = _normalize_chatwoot_status(
                    raw.get("current_value") or raw.get("currentValue")
                )
            else:
                status = _normalize_chatwoot_status(raw)
            if status:
                return status
    return ""


def _normalize_handoff_text(content: str) -> str:
    text = str(content or "").strip().lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.split())


def _normalize_phone(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith("@lid"):
        return ""
    base = text.split("@", 1)[0]
    digits = "".join(ch for ch in base if ch.isdigit())
    return digits or base


def _preferred_contact_identifier(chat_id: str, phone: str) -> str:
    whatsapp_chat_id = _to_whatsapp_chat_id(chat_id)
    if whatsapp_chat_id.endswith("@c.us"):
        return whatsapp_chat_id
    phone_chat_id = _to_whatsapp_chat_id(phone)
    if phone_chat_id:
        return phone_chat_id
    return whatsapp_chat_id or str(chat_id or "").strip()


def _is_technical_contact_name(value: str) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if "@lid" in text or "@c.us" in text or "@s.whatsapp.net" in text:
        return True
    if text.startswith("whatsapp chat id") or text.startswith("whatsapp lid"):
        return True
    return False


def _format_phone_display(phone: str) -> str:
    digits = _normalize_phone(phone)
    if not digits:
        return ""
    if digits.startswith("55") and len(digits) == 12:
        return f"+55 ({digits[2:4]}) {digits[4:8]}-{digits[8:12]}"
    if digits.startswith("55") and len(digits) == 13:
        return f"+55 ({digits[2:4]}) {digits[4:9]}-{digits[9:13]}"
    formatted = _format_phone_number(digits)
    return formatted or digits


def _contact_display_name(contact_name: str, phone: str, identifier: str) -> str:
    cleaned_name = " ".join(str(contact_name or "").split()).strip()
    if cleaned_name and not _is_technical_contact_name(cleaned_name):
        return cleaned_name
    phone_display = _format_phone_display(phone)
    if phone_display:
        return phone_display
    return _format_phone_number(phone) or identifier


def _should_repair_contact_name(
    existing_name: str,
    requested_name: str,
    display_name: str,
) -> bool:
    if not display_name:
        return False
    if requested_name and not _is_technical_contact_name(requested_name):
        return True
    return _is_technical_contact_name(existing_name) or not str(existing_name or "").strip()


def _should_update_existing_contact(
    *,
    picked_contact: dict[str, Any],
    stored_name: str,
    requested_name: str,
    display_name: str,
    phone_number: str,
    identifier: str,
) -> bool:
    current_name = str((picked_contact or {}).get("name") or stored_name or "").strip()
    current_phone = _format_phone_number(str((picked_contact or {}).get("phone_number") or ""))
    current_identifier = str((picked_contact or {}).get("identifier") or "").strip()
    if _should_repair_contact_name(current_name, requested_name, display_name):
        return True
    if phone_number and current_phone != phone_number:
        return True
    if identifier and current_identifier and _is_technical_contact_name(current_identifier):
        return True
    return False


def _format_phone_number(phone: str) -> str:
    digits = _normalize_phone(phone)
    if not digits:
        return ""
    if digits.startswith("+"):
        return digits
    return f"+{digits}"


def _to_whatsapp_chat_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "@" in text:
        return text
    digits = _normalize_phone(text)
    if digits:
        return f"{digits}@c.us"
    return ""
