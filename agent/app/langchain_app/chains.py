from typing import Any, Optional

from .config import Settings
from .examples import ExamplesText
from .profiles import Profile


def _import_langchain():
    try:
        from langchain_openai import ChatOpenAI
    except Exception:  # pragma: no cover
        try:
            from langchain.chat_models import ChatOpenAI  # type: ignore
        except Exception as exc:
            raise RuntimeError("ChatOpenAI not available. Install langchain-openai.") from exc

    try:
        from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
    except Exception:  # pragma: no cover
        from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder  # type: ignore

    try:
        from langchain_core.runnables import RunnableWithMessageHistory
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("RunnableWithMessageHistory not available. Install langchain-core.") from exc

    try:
        from langchain.agents import AgentExecutor, create_openai_tools_agent
    except Exception:  # pragma: no cover
        try:
            from langchain.agents import AgentExecutor, create_tool_calling_agent  # type: ignore
            create_openai_tools_agent = create_tool_calling_agent
        except Exception as exc:
            raise RuntimeError("LangChain agents not available.") from exc

    return {
        "ChatOpenAI": ChatOpenAI,
        "ChatPromptTemplate": ChatPromptTemplate,
        "MessagesPlaceholder": MessagesPlaceholder,
        "RunnableWithMessageHistory": RunnableWithMessageHistory,
        "AgentExecutor": AgentExecutor,
        "create_openai_tools_agent": create_openai_tools_agent,
    }


def _build_system_prompt(base_prompt: str, examples: ExamplesText, include_context: bool) -> str:
    parts = []
    if base_prompt:
        parts.append(base_prompt.strip())
    if examples.good:
        parts.append("GOOD EXAMPLES:\n" + examples.good.strip())
    if examples.bad:
        parts.append("BAD EXAMPLES (AVOID):\n" + examples.bad.strip())
    if include_context:
        parts.append("Use the CONTEXT section if it is relevant.")
    prompt = "\n\n".join(parts).strip()
    return prompt or "You are a helpful assistant."


def build_llm(settings: Settings, profile: Profile) -> Any:
    lc = _import_langchain()
    ChatOpenAI = lc["ChatOpenAI"]

    model = profile.model or settings.openai_model
    max_tokens = profile.max_tokens if profile.max_tokens is not None else settings.openai_max_tokens

    kwargs = {"model": model}
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    if settings.openai_api_key:
        kwargs["api_key"] = settings.openai_api_key

    return ChatOpenAI(**kwargs)


def build_runnable(
    settings: Settings,
    profile: Profile,
    system_prompt: str,
    tools: list,
    include_context: bool,
) -> Any:
    lc = _import_langchain()
    ChatPromptTemplate = lc["ChatPromptTemplate"]
    MessagesPlaceholder = lc["MessagesPlaceholder"]
    AgentExecutor = lc["AgentExecutor"]
    create_openai_tools_agent = lc["create_openai_tools_agent"]

    messages = [("system", system_prompt)]
    if include_context:
        messages.append(("system", "CONTEXT:\n{context}"))
    messages.append(MessagesPlaceholder("history"))
    messages.append(("human", "{input}"))

    if tools:
        messages.append(MessagesPlaceholder("agent_scratchpad"))

    prompt = ChatPromptTemplate.from_messages(messages)
    llm = build_llm(settings, profile)

    if tools:
        agent = create_openai_tools_agent(llm, tools, prompt)
        return AgentExecutor(agent=agent, tools=tools, verbose=settings.agent_verbose)

    return prompt | llm


def build_chain_with_history(
    runnable: Any,
    history_factory: Any,
) -> Any:
    lc = _import_langchain()
    RunnableWithMessageHistory = lc["RunnableWithMessageHistory"]

    return RunnableWithMessageHistory(
        runnable,
        history_factory,
        input_messages_key="input",
        history_messages_key="history",
    )


def build_system_prompt(
    settings: Settings,
    profile: Profile,
    examples: ExamplesText,
    include_context: bool,
) -> str:
    base_prompt = settings.system_prompt
    if not base_prompt:
        try:
            with open(profile.instructions_path, "r", encoding="utf-8") as handle:
                base_prompt = handle.read().strip()
        except Exception:
            base_prompt = ""
    return _build_system_prompt(base_prompt, examples, include_context)
