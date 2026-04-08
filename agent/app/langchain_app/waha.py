import logging
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse

import httpx


logger = logging.getLogger("langchain_app")


class WahaClient:
    def __init__(self, base_url: str, api_key: str, session: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key.strip()
        self.session = session.strip() or "default"

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-Api-Key"] = self.api_key
        return headers

    def _normalize_media_url(self, url: str) -> str:
        if not url:
            return url
        parsed = urlparse(url)
        if not parsed.scheme:
            base = urlparse(self.base_url)
            path = url if url.startswith("/") else f"/{url}"
            return urlunparse((base.scheme, base.netloc, path, "", "", ""))
        host = parsed.hostname or ""
        if host in ("localhost", "127.0.0.1"):
            base = urlparse(self.base_url)
            return urlunparse(
                (base.scheme, base.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
            )
        return url

    async def get_contact_name(self, chat_id: str) -> Optional[str]:
        if not chat_id:
            return None
        params = {"contactId": chat_id, "session": self.session}
        url = f"{self.base_url}/api/contacts"
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url, params=params, headers=self._headers())
            if resp.status_code >= 400:
                logger.warning("WAHA contacts failed: %s %s", resp.status_code, resp.text)
                return None
            data = resp.json()
        if not isinstance(data, dict):
            return None
        for key in ("pushname", "name", "shortName"):
            value = (data.get(key) or "").strip()
            if value:
                return value
        return None

    async def send_text(self, chat_id: str, text: str) -> None:
        if not chat_id:
            raise ValueError("chat_id is required")
        payload = {"chatId": chat_id, "text": text, "session": self.session}
        url = f"{self.base_url}/api/sendText"
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload, headers=self._headers())
            if resp.status_code >= 400:
                logger.error("WAHA sendText failed: %s %s", resp.status_code, resp.text)
                raise RuntimeError("WAHA sendText failed")

    async def send_poll(self, chat_id: str, question: str, options: list[str]) -> Optional[str]:
        if not chat_id:
            raise ValueError("chat_id is required")
        if not options:
            raise ValueError("poll options are required")
        payload = {
            "chatId": chat_id,
            "session": self.session,
            "poll": {"name": question, "options": options, "multipleAnswers": False},
        }
        url = f"{self.base_url}/api/sendPoll"
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(url, json=payload, headers=self._headers())
            if resp.status_code >= 400:
                logger.error("WAHA sendPoll failed: %s %s", resp.status_code, resp.text)
                return None
            try:
                data = resp.json()
            except Exception:
                return None
        return (
            data.get("id")
            or (data.get("poll") or {}).get("id")
            or (data.get("message") or {}).get("id")
            or (data.get("data") or {}).get("id")
        )

    async def download_media(self, url: str) -> Optional[bytes]:
        if not url:
            return None
        url = self._normalize_media_url(url)
        headers = {}
        if self.api_key and url.startswith(self.base_url):
            headers["X-Api-Key"] = self.api_key
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code >= 400:
                logger.warning("Media download failed: %s %s", resp.status_code, resp.text)
                return None
            return resp.content
