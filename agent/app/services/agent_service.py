import contextvars
import logging
from typing import Any, Dict, Optional

from agents import Agent, ModelSettings, Runner

try:
    from agents import FileSearchTool
except Exception:
    FileSearchTool = None

from ..config.settings import OPENAI_MAX_TOKENS, OPENAI_MODEL, PROMPT_PROFILE, parse_int
from ..core.profiles import (
    PROFILE_DEFAULT_ID,
    PROFILES,
    append_profile_runtime_instructions,
    get_audio_bucket_for_profile,
    get_profile_max_tokens,
    get_profile_temperature,
    get_vector_store_ids,
    load_instructions,
    load_profile_instructions,
    make_dynamic_instructions,
    profile_uses_direct_response_style,
)
from . import audio_service, knowledge_service, scheduling_service
from .routing_service import resolve_profile_for_chat, is_ariane_profile


logger = logging.getLogger("agent")

CURRENT_CHAT_ID: contextvars.ContextVar[str] = contextvars.ContextVar("CURRENT_CHAT_ID", default="")
CURRENT_PROFILE_ID: contextvars.ContextVar[str] = contextvars.ContextVar("CURRENT_PROFILE_ID", default="")
CURRENT_USER_INPUT: contextvars.ContextVar[str] = contextvars.ContextVar("CURRENT_USER_INPUT", default="")

audio_service.configure_runtime(
    chat_context=CURRENT_CHAT_ID,
    profile_context=CURRENT_PROFILE_ID,
    profile_resolver=resolve_profile_for_chat,
)
knowledge_service.configure_runtime(
    chat_context=CURRENT_CHAT_ID,
    profile_context=CURRENT_PROFILE_ID,
    profile_resolver=resolve_profile_for_chat,
)
scheduling_service.configure_runtime(
    chat_context=CURRENT_CHAT_ID,
    profile_context=CURRENT_PROFILE_ID,
    user_input_context=CURRENT_USER_INPUT,
    profile_resolver=resolve_profile_for_chat,
    ariane_matcher=is_ariane_profile,
)

SCHEDULING_TOOL = scheduling_service.build_scheduling_tool()
KNOWLEDGE_TOOL = knowledge_service.build_knowledge_tool()
AUDIO_MATCH_TOOL = audio_service.build_audio_match_tool()
AUDIO_SEND_TOOL = audio_service.build_audio_send_tool()


def build_model_settings(profile_id: Optional[str] = None) -> Optional[ModelSettings]:
    max_tokens = parse_int(OPENAI_MAX_TOKENS)
    model_name = (OPENAI_MODEL or "").strip().lower()
    if model_name.startswith("gpt-5"):
        if max_tokens is None or max_tokens < 1024:
            if max_tokens is not None:
                logger.info(
                    "OPENAI_MAX_TOKENS=%s is low for %s; using 1024 to avoid empty responses.",
                    max_tokens,
                    OPENAI_MODEL,
                )
            max_tokens = 1024
        return ModelSettings(
            max_tokens=max_tokens,
            reasoning={"effort": "low"},
            verbosity="low",
        )
    if profile_uses_direct_response_style(profile_id):
        profile_max_tokens = get_profile_max_tokens(profile_id)
        profile_temperature = get_profile_temperature(profile_id)
        settings_kwargs: Dict[str, Any] = {}
        resolved_max_tokens = profile_max_tokens if profile_max_tokens is not None else max_tokens
        if resolved_max_tokens is not None:
            settings_kwargs["max_tokens"] = resolved_max_tokens
        if profile_temperature is not None:
            settings_kwargs["temperature"] = profile_temperature
        if settings_kwargs:
            return ModelSettings(**settings_kwargs)
        return None
    if max_tokens is None:
        return None
    return ModelSettings(max_tokens=max_tokens)


def extract_text_from_result(result: Any) -> str:
    final_output = getattr(result, "final_output", None)
    if isinstance(final_output, str):
        text = final_output.strip()
        if text:
            return text

    raw_responses = getattr(result, "raw_responses", None) or []
    for raw in reversed(raw_responses):
        output_items = getattr(raw, "output", None) or []
        for item in reversed(output_items):
            content = getattr(item, "content", None) or []
            parts: list[str] = []
            for part in content:
                part_type = getattr(part, "type", "")
                part_text = getattr(part, "text", None)
                if isinstance(part_text, str) and part_text.strip() and part_type in ("output_text", "text"):
                    parts.append(part_text.strip())
            if parts:
                return "\n".join(parts).strip()

            item_text = getattr(item, "text", None)
            if isinstance(item_text, str) and item_text.strip():
                return item_text.strip()

    return ""


def log_empty_output_diagnostics(result: Any, context: str) -> None:
    try:
        raw_responses = getattr(result, "raw_responses", None) or []
        if not raw_responses:
            logger.warning("Agent returned empty output (%s): no raw_responses", context)
            return
        last_response = raw_responses[-1]
        output_items = getattr(last_response, "output", None) or []
        output_types = [getattr(item, "type", item.__class__.__name__) for item in output_items]
        logger.warning(
            "Agent returned empty output (%s): response_id=%s output_types=%s output_items=%s",
            context,
            getattr(last_response, "response_id", None),
            output_types,
            len(output_items),
        )
    except Exception as exc:
        logger.warning("Failed to build empty output diagnostics (%s): %s", context, exc)


def build_tools_for_profile(profile_id: Optional[str]) -> list[Any]:
    tools: list[Any] = []
    ids = get_vector_store_ids(profile_id)
    if ids:
        if FileSearchTool is None:
            logger.warning("FileSearchTool not available. Update openai-agents to enable file search.")
        else:
            tools.append(
                FileSearchTool(
                    vector_store_ids=ids,
                    max_num_results=6,
                )
            )
    if SCHEDULING_TOOL is not None:
        tools.append(SCHEDULING_TOOL)
    if KNOWLEDGE_TOOL is not None:
        tools.append(KNOWLEDGE_TOOL)
    if get_audio_bucket_for_profile(profile_id):
        if AUDIO_MATCH_TOOL is not None:
            tools.append(AUDIO_MATCH_TOOL)
        if AUDIO_SEND_TOOL is not None:
            tools.append(AUDIO_SEND_TOOL)
    return tools


def build_agent() -> Agent:
    profile_hint = PROMPT_PROFILE or PROFILE_DEFAULT_ID or None
    static = append_profile_runtime_instructions(load_instructions(), profile_hint)
    kwargs: Dict[str, Any] = {
        "name": "Assistente",
        "instructions": make_dynamic_instructions(static),
    }
    tools = build_tools_for_profile(profile_hint)
    if tools:
        kwargs["tools"] = tools
    if OPENAI_MODEL:
        kwargs["model"] = OPENAI_MODEL
    model_settings = build_model_settings(profile_hint)
    if model_settings:
        kwargs["model_settings"] = model_settings
    return Agent(**kwargs)


def build_agent_for_profile(profile_id: str) -> Agent:
    static = load_profile_instructions(profile_id)
    if not static:
        static = append_profile_runtime_instructions(load_instructions(), profile_id)
    profile = PROFILES.get(profile_id) or {}
    kwargs: Dict[str, Any] = {
        "name": profile.get("label") or "Assistente",
        "instructions": make_dynamic_instructions(static),
    }
    tools = build_tools_for_profile(profile_id)
    if tools:
        kwargs["tools"] = tools
    if OPENAI_MODEL:
        kwargs["model"] = OPENAI_MODEL
    model_settings = build_model_settings(profile_id)
    if model_settings:
        kwargs["model_settings"] = model_settings
    return Agent(**kwargs)


AGENT = build_agent()
AGENT_CACHE: Dict[str, Agent] = {}


def get_agent(profile_id: Optional[str]) -> Agent:
    if not profile_id:
        return AGENT
    agent = AGENT_CACHE.get(profile_id)
    if agent is None:
        agent = build_agent_for_profile(profile_id)
        AGENT_CACHE[profile_id] = agent
    return agent


async def run_agent(
    agent: Agent,
    input_text: str,
    session,
    chat_id: str,
    profile_id: Optional[str] = None,
) -> Any:
    chat_token = CURRENT_CHAT_ID.set(chat_id)
    profile_token = CURRENT_PROFILE_ID.set(profile_id or "")
    input_token = CURRENT_USER_INPUT.set(input_text or "")
    try:
        return await Runner.run(agent, input=input_text, session=session)
    finally:
        CURRENT_USER_INPUT.reset(input_token)
        CURRENT_PROFILE_ID.reset(profile_token)
        CURRENT_CHAT_ID.reset(chat_token)
