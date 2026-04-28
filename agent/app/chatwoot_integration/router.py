import json
import logging
from typing import Awaitable, Callable

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from .service import get_chatwoot_service


SendTextParts = Callable[[str, str], Awaitable[bool]]
logger = logging.getLogger("agent.chatwoot")


def build_chatwoot_router(send_text_parts: SendTextParts) -> APIRouter:
    router = APIRouter()
    service = get_chatwoot_service()

    @router.post("/webhook/chatwoot")
    async def chatwoot_webhook(request: Request, background_tasks: BackgroundTasks) -> dict:
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

        # Extract and validate synchronously (fast) so we can respond 200 immediately.
        # The actual WhatsApp delivery runs in background to avoid Chatwoot webhook timeout.
        try:
            extracted = service.extract_outbound_event(payload)
        except Exception as exc:
            raise HTTPException(status_code=502, detail="Failed to process Chatwoot webhook") from exc

        if isinstance(extracted, str):
            return {"ok": True, "ignored": extracted}

        background_tasks.add_task(
            service.deliver_outbound_event,
            extracted,
            send_whatsapp_message=send_text_parts,
        )
        return {"ok": True, "queued": True, "conversation_id": extracted["conversation_id"]}

    return router
