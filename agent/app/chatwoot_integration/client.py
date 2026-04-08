import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx


logger = logging.getLogger("agent.chatwoot")


class ChatwootApiError(RuntimeError):
    def __init__(self, message: str, status_code: int, response_text: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


@dataclass
class ChatwootConfig:
    base_url: str
    account_id: str = ""
    api_access_token: str = ""
    inbox_id: str = ""
    inbox_identifier: str = ""
    webhook_secret: str = ""
    state_db_path: str = "chatwoot_state.db"

    @property
    def account_mode(self) -> bool:
        return bool(self.base_url and self.account_id and self.api_access_token and self.inbox_id)

    @property
    def public_mode(self) -> bool:
        return bool(self.base_url and self.inbox_identifier)

    @property
    def sync_enabled(self) -> bool:
        return self.account_mode or self.public_mode


class ChatwootClient:
    def __init__(self, config: ChatwootConfig) -> None:
        self.config = config

    async def search_contacts(self, query: str) -> list[dict[str, Any]]:
        if not self.config.account_mode or not query:
            return []
        payload = await self._request_account("GET", "/contacts/search", params={"q": query})
        return self._unwrap_list(payload, "contacts")

    async def create_contact(self, *, identifier: str, name: str, phone_number: str) -> dict[str, Any]:
        if self.config.account_mode:
            body = {
                "inbox_id": int(self.config.inbox_id),
                "identifier": identifier,
                "name": name or phone_number or identifier,
            }
            if phone_number:
                body["phone_number"] = phone_number
            payload = await self._request_account("POST", "/contacts", json=body)
            return self._unwrap_dict(payload, "contact")

        if self.config.public_mode:
            body = {
                "identifier": identifier,
                "name": name or phone_number or identifier,
            }
            if phone_number:
                body["phone_number"] = phone_number
            payload = await self._request_public("POST", "/contacts", json=body)
            return self._unwrap_dict(payload, "contact")

        return {}

    async def create_contact_inbox(self, contact_id: int, source_id: str) -> dict[str, Any]:
        if not self.config.account_mode or not contact_id or not source_id:
            return {}
        payload = await self._request_account(
            "POST",
            f"/contacts/{contact_id}/contact_inboxes",
            json={
                "inbox_id": int(self.config.inbox_id),
                "source_id": source_id,
            },
        )
        return self._unwrap_dict(payload, "contact_inbox")

    async def get_contactable_inboxes(self, contact_id: int) -> list[dict[str, Any]]:
        if not self.config.account_mode or not contact_id:
            return []
        payload = await self._request_account("GET", f"/contacts/{contact_id}/contactable_inboxes")
        return self._unwrap_list(payload, "payload")

    async def list_contact_conversations(self, contact_id: int) -> list[dict[str, Any]]:
        if not self.config.account_mode or not contact_id:
            return []
        payload = await self._request_account("GET", f"/contacts/{contact_id}/conversations")
        return self._unwrap_list(payload, "conversations")

    async def create_conversation(
        self,
        *,
        contact_id: Optional[int],
        contact_source_id: str,
    ) -> dict[str, Any]:
        if self.config.account_mode:
            body = {
                "source_id": contact_source_id,
                "inbox_id": int(self.config.inbox_id),
                "status": "open",
            }
            if contact_id:
                body["contact_id"] = int(contact_id)
            payload = await self._request_account("POST", "/conversations", json=body)
            return self._unwrap_dict(payload, "conversation")

        if self.config.public_mode:
            if not contact_source_id:
                return {}
            payload = await self._request_public(
                "POST",
                f"/contacts/{contact_source_id}/conversations",
                json={},
            )
            return self._unwrap_dict(payload, "conversation")

        return {}

    async def create_incoming_message(
        self,
        *,
        conversation_id: int,
        content: str,
        contact_source_id: str,
        echo_id: str = "",
    ) -> dict[str, Any]:
        if self.config.public_mode:
            body: dict[str, Any] = {"content": content}
            if echo_id:
                body["echo_id"] = echo_id
            try:
                payload = await self._request_public(
                    "POST",
                    f"/contacts/{contact_source_id}/conversations/{conversation_id}/messages",
                    json=body,
                )
                return self._unwrap_dict(payload, "message")
            except ChatwootApiError as exc:
                if exc.status_code != 404 or not self.config.account_mode:
                    raise
                logger.warning(
                    "Chatwoot public incoming message failed for inbox_identifier=%s; falling back to account API",
                    self.config.inbox_identifier,
                )

        if self.config.account_mode:
            body = {
                "content": content,
                "message_type": "incoming",
                "private": False,
            }
            if echo_id:
                body["echo_id"] = echo_id
            payload = await self._request_account(
                "POST",
                f"/conversations/{conversation_id}/messages",
                json=body,
            )
            return self._unwrap_dict(payload, "message")

        return {}

    async def create_outgoing_message(
        self,
        *,
        conversation_id: int,
        content: str,
        echo_id: str = "",
    ) -> dict[str, Any]:
        if not self.config.account_mode:
            return {}

        body: dict[str, Any] = {
            "content": content,
            "message_type": "outgoing",
            "private": False,
            "content_type": "text",
        }
        if echo_id:
            body["echo_id"] = echo_id
        payload = await self._request_account(
            "POST",
            f"/conversations/{conversation_id}/messages",
            json=body,
        )
        return self._unwrap_dict(payload, "message")

    async def _request_account(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        url = f"{self.config.base_url}/api/v1/accounts/{self.config.account_id}{path}"
        headers = {
            "Content-Type": "application/json",
            "api_access_token": self.config.api_access_token,
        }
        return await self._request(method, url, headers=headers, json=json, params=params)

    async def _request_public(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        url = f"{self.config.base_url}/public/api/v1/inboxes/{self.config.inbox_identifier}{path}"
        headers = {"Content-Type": "application/json"}
        return await self._request(method, url, headers=headers, json=json, params=params)

    async def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.request(method, url, headers=headers, json=json, params=params)
        if response.status_code >= 400:
            raise ChatwootApiError(
                f"Chatwoot request failed: {method} {url}",
                response.status_code,
                response.text,
            )
        try:
            data = response.json()
        except ValueError:
            logger.warning("Chatwoot returned a non-JSON response for %s %s", method, url)
            return {}
        return data if isinstance(data, dict) else {"payload": data}

    @staticmethod
    def _unwrap(payload: dict[str, Any]) -> Any:
        for key in ("payload", "data"):
            value = payload.get(key)
            if value is not None:
                return value
        return payload

    @classmethod
    def _unwrap_dict(cls, payload: dict[str, Any], *candidate_keys: str) -> dict[str, Any]:
        data = cls._unwrap(payload)
        if isinstance(data, dict):
            for key in candidate_keys:
                nested = data.get(key)
                if isinstance(nested, dict):
                    return nested
            return data
        return {}

    @classmethod
    def _unwrap_list(cls, payload: dict[str, Any], *candidate_keys: str) -> list[dict[str, Any]]:
        data = cls._unwrap(payload)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            for key in candidate_keys:
                nested = data.get(key)
                if isinstance(nested, list):
                    return [item for item in nested if isinstance(item, dict)]
        return []
