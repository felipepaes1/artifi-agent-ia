import json
import logging
from typing import Awaitable, Callable

from fastapi import APIRouter, HTTPException, Request

from .service import get_chatwoot_service


SendTextParts = Callable[[str, str], Awaitable[bool]]
logger = logging.getLogger("agent.chatwoot")


def build_chatwoot_router(send_text_parts: SendTextParts) -> APIRouter:
    router = APIRouter()
    service = get_chatwoot_service()

    @router.post("/webhook/chatwoot")
    async def chatwoot_webhook(request: Request) -> dict:
        raw_body = await request.body()
        headers = dict(request.headers)
        logger.info(
            "Chatwoot webhook hit: content_length=%s user_agent=%s",
            headers.get("content-length"),
            headers.get("user-agent"),
        )
        if not service.verify_signature(raw_body, headers):
            logger.warning("Chatwoot webhook signature rejected")
            raise HTTPException(status_code=401, detail="Invalid Chatwoot signature")

        try:
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid Chatwoot payload")

        try:
            return await service.process_message_created_event(
                payload,
                send_whatsapp_message=send_text_parts,
            )
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=502, detail="Failed to process Chatwoot webhook") from exc

    return router
