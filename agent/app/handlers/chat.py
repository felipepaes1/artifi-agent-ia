import logging
import os
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from ..config.settings import OPENAI_API_KEY, PROMPT_PROFILE
from ..core.profiles import PROFILE_DEFAULT_ID, PROFILE_LIST, PROFILE_POLL_NAME
from ..core.state import get_session
from ..formatters.sanitizer import sanitize_plain_text, truncate
from ..services.agent_service import (
    SCHEDULING_TOOL,
    extract_text_from_result,
    get_agent,
    log_empty_output_diagnostics,
    run_agent,
)
from ..services.guardrail_service import enforce_scheduling_entity_guardrail
from ..services.scheduling_service import inject_fake_schedule
from ..services.urgency_guardrail import maybe_handle_urgency


logger = logging.getLogger("agent")


def build_chat_router() -> APIRouter:
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> Dict[str, Any]:
        return {"ok": True}

    @router.get("/chat-ui", response_class=HTMLResponse)
    async def chat_ui() -> HTMLResponse:
        html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "chat_tester.html")
        try:
            with open(html_path, "r", encoding="utf-8") as handle:
                return HTMLResponse(content=handle.read())
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="chat_tester.html not found") from exc
        except OSError as exc:
            logger.exception("Failed to read chat tester UI: %s", exc)
            raise HTTPException(status_code=500, detail="Failed to load chat tester UI") from exc

    @router.get("/chat/profiles")
    async def chat_profiles() -> Dict[str, Any]:
        profiles = []
        for profile in PROFILE_LIST:
            profile_id = str(profile.get("id") or "").strip()
            if not profile_id:
                continue
            profiles.append(
                {
                    "id": profile_id,
                    "label": str(profile.get("label") or profile_id).strip(),
                }
            )
        return {
            "poll_name": PROFILE_POLL_NAME,
            "default_profile_id": PROFILE_DEFAULT_ID or None,
            "profiles": profiles,
        }

    @router.post("/chat")
    async def chat(request: Request) -> Dict[str, Any]:
        data = await request.json()
        message = (data.get("message") or "").strip()
        session_id = (data.get("session_id") or "local-test").strip()
        profile_id = (data.get("profile_id") or "").strip() or None
        if not message:
            raise HTTPException(status_code=400, detail="message is required")
        if not session_id:
            session_id = "local-test"

        if not profile_id:
            profile_id = PROMPT_PROFILE or PROFILE_DEFAULT_ID or None

        if not OPENAI_API_KEY:
            raise HTTPException(status_code=500, detail="OPENAI_API_KEY is not set")

        session = get_session(session_id)
        urgency_reply = await maybe_handle_urgency(profile_id, message, session)
        if urgency_reply is not None:
            return {
                "reply": urgency_reply,
                "session_id": session_id,
                "profile_id": profile_id,
            }

        agent = get_agent(profile_id)
        try:
            result = await run_agent(agent, message, session, session_id, profile_id)
            reply = truncate(
                sanitize_plain_text(extract_text_from_result(result), profile_id),
                profile_id,
            )
        except Exception as exc:
            logger.exception("Agent run failed: %s", exc)
            raise HTTPException(status_code=502, detail="Agent run failed") from exc

        if not reply:
            log_empty_output_diagnostics(result, "chat_endpoint")
            reply = "Desculpe, nao consegui responder agora."
        reply = enforce_scheduling_entity_guardrail(profile_id, message, reply)
        reply = inject_fake_schedule(
            session_id,
            message,
            reply,
            has_scheduling_tool=SCHEDULING_TOOL is not None,
        )

        return {
            "reply": reply,
            "session_id": session_id,
            "profile_id": profile_id,
        }

    return router
