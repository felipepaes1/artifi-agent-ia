from __future__ import annotations

import re
from typing import Any

import httpx
from fastmcp import FastMCP

from ..settings import HTTP_TIMEOUT, N8N_WEBHOOK_BASE_URL, WAHA_API_KEY, WAHA_BASE_URL, WAHA_SESSION


_SLUG_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _require_slug(value: str, name: str) -> str:
    if (not value) or (not _SLUG_RE.match(value)):
        raise ValueError(f"{name} must match {_SLUG_RE.pattern}")
    return value


def register_messaging_tools(mcp: FastMCP) -> None:
    @mcp.tool()
    async def waha_send_text(chat_id: str, text: str, session: str | None = None) -> dict[str, Any]:
        """Send a WhatsApp message via WAHA."""
        if not chat_id:
            raise ValueError("chat_id is required")
        if not text:
            raise ValueError("text is required")

        headers = {"Content-Type": "application/json"}
        if WAHA_API_KEY:
            headers["X-Api-Key"] = WAHA_API_KEY

        payload = {
            "chatId": chat_id,
            "text": text,
            "session": session or WAHA_SESSION,
        }

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.post(f"{WAHA_BASE_URL}/api/sendText", json=payload, headers=headers)

        body = (response.text or "")[:1000]
        if response.status_code >= 400:
            raise RuntimeError(f"WAHA sendText failed: {response.status_code} {body}")

        return {"ok": True, "status_code": response.status_code, "body": body}

    @mcp.tool()
    async def n8n_trigger_webhook(trigger: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Trigger an n8n webhook by name (slug-only)."""
        trigger = _require_slug(trigger, "trigger")
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            response = await client.post(f"{N8N_WEBHOOK_BASE_URL}/{trigger}", json=payload or {})

        body = (response.text or "")[:1000]
        if response.status_code >= 400:
            raise RuntimeError(f"n8n webhook failed: {response.status_code} {body}")

        return {"ok": True, "status_code": response.status_code, "body": body}

    @mcp.tool()
    async def ping() -> dict[str, Any]:
        """Simple health-check tool."""
        return {"ok": True}
