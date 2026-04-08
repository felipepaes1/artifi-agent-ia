import hashlib
import hmac
import logging
import os
import time
import contextvars
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
            try:
                await self.client.create_outgoing_message(
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
                await self.client.create_outgoing_message(
                    conversation_id=int(mapping.conversation_id),
                    content=content,
                    echo_id=echo_id,
                )
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

    async def process_message_created_event(
        self,
        payload: dict[str, Any],
        *,
        send_whatsapp_message: SendWhatsappMessage,
    ) -> dict[str, Any]:
        event = str(payload.get("event") or "").strip().lower()
        message_id = str(payload.get("id") or "").strip()
        conversation = payload.get("conversation") or {}
        conversation_id = _coerce_int(payload.get("conversation_id")) or _coerce_int(
            conversation.get("id")
        )

        logger.info(
            "Chatwoot webhook received: event=%s message_id=%s conversation_id=%s",
            event or None,
            message_id or None,
            conversation_id,
        )

        if event != "message_created":
            return {"ok": True, "ignored": "event"}

        if message_id and self.store.is_processed_message(message_id):
            return {"ok": True, "ignored": "duplicate_message"}
        if self._is_backend_origin_message(payload):
            return {"ok": True, "ignored": "backend_origin"}

        if not self._is_human_agent_message(payload):
            return {"ok": True, "ignored": "not_human_agent"}

        content = str(payload.get("content") or "").strip()
        if not content:
            return {"ok": True, "ignored": "empty_content"}

        mapping = self.store.get_by_conversation_id(conversation_id) if conversation_id else None
        chat_id = (mapping.whatsapp_chat_id if mapping else "") or self._extract_chat_id(payload)
        phone = (mapping.phone if mapping else "") or self._extract_phone(payload)
        if not chat_id:
            logger.warning(
                "Chatwoot event ignored because target chat was not resolved: conversation_id=%s message_id=%s",
                conversation_id,
                message_id or None,
            )
            return {"ok": True, "ignored": "missing_chat_id"}

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
            raise RuntimeError("WhatsApp delivery aborted")
        if message_id:
            self.store.mark_processed_message(message_id)
        if conversation_id:
            self.store.upsert_mapping(
                ChatwootMapping(
                    whatsapp_chat_id=chat_id,
                    phone=phone or _normalize_phone(chat_id),
                    contact_name=self._extract_contact_name(payload),
                    conversation_id=conversation_id,
                    identifier=_preferred_contact_identifier(chat_id, phone),
                )
            )
        return {
            "ok": True,
            "delivered": True,
            "conversation_id": conversation_id,
            "phone": phone or _normalize_phone(chat_id),
        }

    async def _ensure_mapping(
        self,
        *,
        chat_id: str,
        phone: str,
        contact_name: str,
    ) -> Optional[ChatwootMapping]:
        identifier = _preferred_contact_identifier(chat_id, phone)
        existing = self.store.get_by_chat_id(chat_id)
        if existing and existing.contact_source_id and existing.conversation_id:
            if (
                (contact_name and contact_name != existing.contact_name)
                or (phone and phone != existing.phone)
                or (identifier and identifier != existing.identifier)
            ):
                existing.contact_name = contact_name or existing.contact_name
                existing.phone = phone or existing.phone
                existing.identifier = identifier or existing.identifier
                self.store.upsert_mapping(existing)
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

        if not contact_id:
            contacts = await self.client.search_contacts(identifier)
            if phone and phone != identifier:
                contacts.extend(await self.client.search_contacts(phone))
            contact = self._pick_contact(contacts, identifier=identifier, phone=phone)
            if contact:
                contact_id = _coerce_int(contact.get("id"))
                contact_source_id = self._extract_contact_source_id(contact) or contact_source_id

        if not contact_id:
            created_contact = await self.client.create_contact(
                identifier=identifier,
                name=contact_name,
                phone_number=phone_number,
            )
            contact_id = _coerce_int(created_contact.get("id"))
            contact_source_id = self._extract_contact_source_id(created_contact)

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
        if not conversation_id:
            conversations = await self.client.list_contact_conversations(int(contact_id))
            conversation = self._pick_conversation(conversations)
            if conversation:
                conversation_id = _coerce_int(conversation.get("id"))

        if not conversation_id:
            conversation = await self.client.create_conversation(
                contact_id=int(contact_id),
                contact_source_id=contact_source_id,
            )
            conversation_id = _coerce_int(conversation.get("id"))

        return ChatwootMapping(
            whatsapp_chat_id=chat_id,
            phone=phone,
            contact_name=contact_name or (existing.contact_name if existing else ""),
            contact_id=contact_id,
            contact_source_id=contact_source_id,
            conversation_id=conversation_id,
            identifier=identifier,
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

        if not contact_source_id:
            contact = await self.client.create_contact(
                identifier=identifier,
                name=contact_name,
                phone_number=phone_number,
            )
            contact_id = _coerce_int(contact.get("id")) or contact_id
            contact_source_id = self._extract_contact_source_id(contact) or identifier

        conversation_id = existing.conversation_id if existing else None
        if not conversation_id:
            conversation = await self.client.create_conversation(
                contact_id=contact_id,
                contact_source_id=contact_source_id,
            )
            conversation_id = _coerce_int(conversation.get("id"))

        return ChatwootMapping(
            whatsapp_chat_id=chat_id,
            phone=phone,
            contact_name=contact_name or (existing.contact_name if existing else ""),
            contact_id=contact_id,
            contact_source_id=contact_source_id,
            conversation_id=conversation_id,
            identifier=identifier,
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
        preferred = []
        same_inbox = []
        fallback = []
        for conversation in conversations:
            if not isinstance(conversation, dict):
                continue
            fallback.append(conversation)
            inbox_id = _coerce_int(conversation.get("inbox_id"))
            status = str(conversation.get("status") or "").strip().lower()
            if target_inbox_id and inbox_id != target_inbox_id:
                continue
            same_inbox.append(conversation)
            if status in ("open", "pending", ""):
                preferred.append(conversation)
        if preferred:
            return preferred[0]
        if same_inbox:
            return same_inbox[0]
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
