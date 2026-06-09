"""
Valida a auto-pausa por intervencao humana sem chamar APIs externas.

Como rodar:
    cd agent
    python3 ../scripts/test_ai_pause_flow.py
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import os
import sys
import tempfile
import types
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))
os.environ.setdefault("OPENAI_API_KEY", "test-key")


def install_missing_dependency_stubs() -> None:
    if importlib.util.find_spec("anyio") is None:
        anyio_stub = types.ModuleType("anyio")

        async def _sleep(_seconds: float) -> None:
            return None

        class _Lock:
            async def __aenter__(self) -> "_Lock":
                return self

            async def __aexit__(self, *_args: Any) -> None:
                return None

        anyio_stub.sleep = _sleep
        anyio_stub.Lock = _Lock
        sys.modules["anyio"] = anyio_stub

    if importlib.util.find_spec("httpx") is None:
        httpx_stub = types.ModuleType("httpx")

        class AsyncClient:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                pass

        httpx_stub.AsyncClient = AsyncClient
        sys.modules["httpx"] = httpx_stub

    if importlib.util.find_spec("fastapi") is None:
        fastapi_stub = types.ModuleType("fastapi")

        class APIRouter:
            def __init__(self) -> None:
                self.routes: List[Any] = []

            def post(self, path: str, *_args: Any, **_kwargs: Any):
                router = self

                def decorator(func):
                    route = types.SimpleNamespace(path=path, endpoint=func)
                    router.routes.append(route)
                    return func

                return decorator

        class BackgroundTasks:
            pass

        class HTTPException(Exception):
            def __init__(self, status_code: int, detail: str = "") -> None:
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        class Request:
            def __init__(self, payload: Dict[str, Any]) -> None:
                self._payload = payload

            async def json(self) -> Dict[str, Any]:
                return self._payload

        fastapi_stub.APIRouter = APIRouter
        fastapi_stub.BackgroundTasks = BackgroundTasks
        fastapi_stub.HTTPException = HTTPException
        fastapi_stub.Request = Request
        sys.modules["fastapi"] = fastapi_stub

    if importlib.util.find_spec("openai") is None:
        openai_stub = types.ModuleType("openai")

        class OpenAI:
            pass

        openai_stub.OpenAI = OpenAI
        sys.modules["openai"] = openai_stub

    if importlib.util.find_spec("agents") is None:
        agents_stub = types.ModuleType("agents")

        class SQLiteSession:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                self.items: List[Dict[str, Any]] = []

            async def get_items(self) -> List[Dict[str, Any]]:
                return list(self.items)

            async def add_items(self, items: List[Dict[str, Any]]) -> None:
                self.items.extend(items)

        class Agent:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                pass

        class ModelSettings:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                pass

        class Runner:
            @staticmethod
            async def run(*_args: Any, **_kwargs: Any) -> Any:
                return {"output": []}

        class FileSearchTool:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                pass

        def function_tool(*_args: Any, **_kwargs: Any):
            def decorator(func):
                return func

            if _args and callable(_args[0]):
                return _args[0]
            return decorator

        agents_stub.SQLiteSession = SQLiteSession
        agents_stub.Agent = Agent
        agents_stub.ModelSettings = ModelSettings
        agents_stub.Runner = Runner
        agents_stub.FileSearchTool = FileSearchTool
        agents_stub.function_tool = function_tool
        sys.modules["agents"] = agents_stub


install_missing_dependency_stubs()


def assert_equal(actual: Any, expected: Any, label: str, failures: List[str]) -> None:
    if actual != expected:
        failures.append(f"{label}: esperado={expected!r}, got={actual!r}")


def assert_true(condition: bool, label: str, failures: List[str]) -> None:
    if not condition:
        failures.append(label)


def isolated_state(tmpdir: str) -> None:
    os.environ["AGENT_PROFILE_DB"] = os.path.join(tmpdir, "profile_state.db")
    os.environ["AGENT_SESSION_DB"] = os.path.join(tmpdir, "sessions.db")
    os.environ["CHATWOOT_STATE_DB"] = os.path.join(tmpdir, "chatwoot_state.db")

    for module_name in list(sys.modules.keys()):
        if module_name == "app" or module_name.startswith("app."):
            sys.modules.pop(module_name, None)


def test_pause_basic_set_and_check(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        isolated_state(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        chat = "5511999999999@c.us"
        assert_equal(store.is_chat_paused(chat), False, "chat livre antes de pausar", failures)
        paused_until = store.pause_chat(chat, ttl_seconds=60, reason="manual")
        assert_true(paused_until > 0, "pause_chat retorna timestamp futuro", failures)
        assert_equal(store.is_chat_paused(chat), True, "chat fica pausado", failures)
        expiry = store.get_pause_expiry(chat)
        assert_equal(expiry, paused_until, "expiry coincide", failures)


def test_pause_expires_after_ttl(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        isolated_state(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        chat = "5511999999999@c.us"
        store.pause_chat(chat, ttl_seconds=-5, reason="ttl negativo")
        assert_equal(store.is_chat_paused(chat), False, "ttl <=0 nao pausa", failures)

        store.pause_chat(chat, ttl_seconds=1, reason="curto")
        assert_equal(store.is_chat_paused(chat), True, "pausa ativa com ttl curto", failures)

        import sqlite3
        import time

        past = int(time.time()) - 10
        with sqlite3.connect(os.environ["AGENT_PROFILE_DB"]) as conn:
            conn.execute("UPDATE ai_pause SET paused_until = ? WHERE chat_id = ?", (past, chat))
        assert_equal(
            store.is_chat_paused(chat),
            False,
            "pausa expira quando paused_until passa do agora",
            failures,
        )
        assert_equal(store.get_pause_expiry(chat), None, "expiry None apos limpeza", failures)


def test_clear_pause(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        isolated_state(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        chat = "5511999999999@c.us"
        store.pause_chat(chat, ttl_seconds=3600)
        assert_equal(store.clear_pause(chat), True, "clear_pause remove pausa", failures)
        assert_equal(store.is_chat_paused(chat), False, "chat livre apos clear", failures)
        assert_equal(store.clear_pause(chat), False, "clear sem nada retorna False", failures)


def test_pause_multiple_ids_lid_and_resolved(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        isolated_state(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        lid = "71330884006023@lid"
        resolved = "555599069114@c.us"
        store.pause_chat_ids([lid, resolved], ttl_seconds=3600, reason="lid+resolved")
        assert_equal(store.is_chat_paused(lid), True, "lid pausado", failures)
        assert_equal(store.is_chat_paused(resolved), True, "resolved pausado", failures)
        assert_equal(store.any_chat_paused([lid]), True, "any com lid", failures)
        assert_equal(store.any_chat_paused([resolved]), True, "any com resolved", failures)
        assert_equal(
            store.any_chat_paused([" outro@c.us "]), False, "outro chat livre", failures
        )
        cleared = store.clear_pause_ids([lid, resolved])
        assert_equal(cleared, 2, "clear_pause_ids retorna contagem", failures)


def test_pause_dedup_and_invalid_ids(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        isolated_state(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        assert_equal(store.pause_chat("", ttl_seconds=60), 0, "vazio nao pausa", failures)
        assert_equal(store.is_chat_paused(""), False, "vazio nunca pausado", failures)
        assert_equal(store.pause_chat(None, ttl_seconds=60), 0, "None nao pausa", failures)
        assert_equal(
            store.pause_chat_ids(["chat@c.us", "chat@c.us", ""], ttl_seconds=60) > 0,
            True,
            "pause_chat_ids ignora vazio e dedup",
            failures,
        )


def build_message_payload(
    *,
    chat_id: str,
    body: str = "oi",
    from_me: bool = False,
    message_id: str = "msg-1",
    timestamp: float | None = None,
) -> Dict[str, Any]:
    import time

    return {
        "event": "message",
        "payload": {
            "id": message_id,
            "from": chat_id,
            "fromMe": from_me,
            "body": body,
            "type": "chat",
            "timestamp": timestamp if timestamp is not None else time.time(),
        },
    }


async def run_webhook(handler, payload: Dict[str, Any]) -> Dict[str, Any]:
    from fastapi import Request

    request = Request(payload)
    return await handler(request)


def patch_webhook_module(tmpdir: str):
    os.environ["AGENT_PROMPT_PROFILE"] = "criolaser"
    os.environ["AGENT_PROFILE_ROUTING"] = "false"
    os.environ["CHATWOOT_BASE_URL"] = ""
    os.environ["MAX_MESSAGE_AGE_SECONDS"] = "9999"
    os.environ["WARMUP_PERIOD_SECONDS"] = "0"
    os.environ["USER_MESSAGE_COALESCE_MS"] = "0"

    isolated_state(tmpdir)

    from app.handlers import waha_webhook as webhook_module

    sent: List[Dict[str, Any]] = []
    agent_runs: List[Dict[str, Any]] = []

    async def fake_send_reply(chat_id, text, **kwargs):
        sent.append({"kind": "reply", "chat_id": chat_id, "text": text, **kwargs})
        return True

    async def fake_send_text_parts(chat_id, text, active_turn=None):
        sent.append({"kind": "text", "chat_id": chat_id, "text": text, "active_turn": active_turn})
        return True

    async def fake_run_agent(agent, body, session, chat_id, profile_id):
        agent_runs.append({"chat_id": chat_id, "body": body, "profile_id": profile_id})
        return {"output": [{"type": "message", "content": [{"type": "output_text", "text": "resposta da IA"}]}]}

    def fake_extract_text(result):
        return "resposta da IA"

    def fake_get_agent(profile_id):
        return object()

    async def fake_hydrate(*_args, **_kwargs):
        return None

    async def fake_trim(*_args, **_kwargs):
        return None

    async def fake_log_conversation(*_args, **_kwargs):
        return None

    async def fake_reset_session(*_args, **_kwargs):
        return None

    async def fake_audio_check(*_args, **_kwargs):
        return None

    async def fake_urgency(*_args, **_kwargs):
        return None

    async def fake_coalesce(chat_id, body, is_audio, profile_id=None):
        return (body, is_audio)

    async def fake_get_contact_name(_chat_id):
        return ""

    async def fake_get_contact_phone(_chat_id):
        return ""

    async def fake_transcribe(_url, _payload):
        return ""

    async def fake_download_media(_url):
        return b""

    async def fake_set_presence(_chat_id, _presence):
        return None

    async def fake_send_profile_poll(_chat_id):
        return ""

    webhook_module.send_reply = fake_send_reply
    webhook_module.send_text_parts = fake_send_text_parts
    webhook_module.run_agent = fake_run_agent
    webhook_module.extract_text_from_result = fake_extract_text
    webhook_module.get_agent = fake_get_agent
    webhook_module.hydrate_session_from_supabase = fake_hydrate
    webhook_module.trim_session = fake_trim
    webhook_module.log_conversation = fake_log_conversation
    webhook_module.reset_session = fake_reset_session
    webhook_module.try_send_service_audio_for_message = fake_audio_check
    webhook_module.maybe_handle_urgency = fake_urgency
    webhook_module.coalesce_user_message = fake_coalesce
    webhook_module.get_contact_name = fake_get_contact_name
    webhook_module.get_contact_phone = fake_get_contact_phone
    webhook_module.transcribe_audio = fake_transcribe
    webhook_module.download_media = fake_download_media
    webhook_module.send_profile_poll = fake_send_profile_poll

    class FakeChatwootService:
        def __init__(self) -> None:
            self.handoff_active: Dict[str, bool] = {}

        def is_human_handoff_active(self, chat_id: str) -> bool:
            return self.handoff_active.get(str(chat_id), False)

        def should_request_human_handoff(self, _content: str) -> bool:
            return False

        async def handoff_to_human(self, **_kwargs) -> bool:
            return False

        async def activate_human_handoff(self, **kwargs) -> bool:
            self.handoff_active[str(kwargs.get("chat_id"))] = True
            return True

        async def sync_incoming_whatsapp_message(self, **_kwargs) -> None:
            return None

        async def sync_outgoing_whatsapp_message(self, **_kwargs) -> None:
            return None

        async def sync_incoming_whatsapp_media(self, **_kwargs) -> None:
            return None

        def sync_enabled(self) -> bool:
            return False

        def should_sync_outgoing_whatsapp_message(self) -> bool:
            return False

    fake_chatwoot = FakeChatwootService()
    webhook_module.get_chatwoot_service = lambda: fake_chatwoot

    return webhook_module, sent, agent_runs, fake_chatwoot


def resolve_webhook_handler(webhook_module):
    router = webhook_module.build_waha_router()
    for route in getattr(router, "routes", []):
        if getattr(route, "path", "") == "/webhook/waha":
            return route.endpoint
    raise RuntimeError("waha_webhook handler not found")


async def test_webhook_human_message_triggers_pause(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        webhook_module, sent, agent_runs, _ = patch_webhook_module(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        handler = resolve_webhook_handler(webhook_module)

        chat = "5511988887777@c.us"
        payload_in = build_message_payload(
            chat_id=chat, body="Oi, quero agendar", message_id="patient-1"
        )
        result = await run_webhook(handler, payload_in)
        assert_true(
            result.get("ok") is True and len(agent_runs) == 1,
            f"primeira msg do paciente deveria rodar agent, result={result} runs={agent_runs}",
            failures,
        )

        payload_human = build_message_payload(
            chat_id=chat,
            body="Aqui e a Maria, vou continuar",
            from_me=True,
            message_id="owner-1",
        )
        result2 = await run_webhook(handler, payload_human)
        assert_equal(
            result2.get("handoff"),
            "manual_whatsapp_from_me",
            "fromMe humano dispara handoff",
            failures,
        )
        assert_true(
            store.is_chat_paused(chat),
            "chat deveria ficar pausado apos fromMe humano",
            failures,
        )

        payload_after = build_message_payload(
            chat_id=chat,
            body="Tem horario amanha?",
            message_id="patient-2",
        )
        runs_before = len(agent_runs)
        result3 = await run_webhook(handler, payload_after)
        assert_equal(
            result3.get("ignored"),
            "ai_paused",
            "mensagem do paciente apos pausa deve ser ignorada",
            failures,
        )
        assert_equal(
            len(agent_runs),
            runs_before,
            "agent NAO deveria rodar enquanto pausado",
            failures,
        )


async def test_webhook_bot_echo_does_not_pause(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        webhook_module, sent, agent_runs, _ = patch_webhook_module(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        state_module = importlib.import_module("app.core.state")
        router = webhook_module.build_waha_router()

        handler = None
        for route in getattr(router, "routes", []):
            if getattr(route, "path", "") == "/webhook/waha":
                handler = route.endpoint
                break
        if handler is None:
            handler = next(
                attr
                for attr in router.__dict__.values()
                if callable(attr) and getattr(attr, "__name__", "") == "waha_webhook"
            )

        chat = "5511966665555@c.us"
        bot_message_id = "BOT-MID-123"
        bot_text = "Ola! Como posso ajudar?"

        from app.integrations.waha import outbound_text_key

        state_module.remember_recent_key(
            state_module.RECENT_OUTBOUND_MESSAGE_IDS,
            bot_message_id,
            300,
        )
        state_module.remember_recent_key(
            state_module.RECENT_OUTBOUND_TEXT_KEYS,
            outbound_text_key(chat, bot_text),
            300,
        )

        payload_echo = build_message_payload(
            chat_id=chat,
            body=bot_text,
            from_me=True,
            message_id=bot_message_id,
        )
        result = await run_webhook(handler, payload_echo)
        assert_true(
            result.get("ignored") in ("outbound_echo_message_id", "outbound_echo_text"),
            f"echo do bot deveria ser ignorado, got={result}",
            failures,
        )
        assert_equal(
            store.is_chat_paused(chat),
            False,
            "echo do bot NAO pode disparar pausa",
            failures,
        )


async def test_webhook_clear_pause_resumes_ai(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        webhook_module, sent, agent_runs, _ = patch_webhook_module(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        router = webhook_module.build_waha_router()

        handler = None
        for route in getattr(router, "routes", []):
            if getattr(route, "path", "") == "/webhook/waha":
                handler = route.endpoint
                break
        if handler is None:
            handler = next(
                attr
                for attr in router.__dict__.values()
                if callable(attr) and getattr(attr, "__name__", "") == "waha_webhook"
            )

        chat = "5511955554444@c.us"
        store.pause_chat(chat, ttl_seconds=3600, reason="manual")
        assert_equal(store.is_chat_paused(chat), True, "chat pausado", failures)

        payload = build_message_payload(chat_id=chat, body="Oi", message_id="p-x")
        result = await run_webhook(handler, payload)
        assert_equal(result.get("ignored"), "ai_paused", "ignorado por pausa", failures)
        assert_equal(len(agent_runs), 0, "agent NAO roda durante pausa", failures)

        store.clear_pause(chat)
        payload2 = build_message_payload(chat_id=chat, body="Oi de novo", message_id="p-y")
        result2 = await run_webhook(handler, payload2)
        assert_true(
            result2.get("ok") is True and len(agent_runs) == 1,
            f"agent deveria rodar apos clear_pause, result={result2} runs={agent_runs}",
            failures,
        )


async def test_pause_default_ttl_is_one_day(failures: List[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        isolated_state(tmpdir)
        store = importlib.import_module("app.services.ai_pause_store")
        assert_equal(
            store.HUMAN_PAUSE_TTL_SECONDS, 86400, "default TTL eh 24h", failures
        )

        chat = "5511944443333@c.us"
        import time

        before = int(time.time())
        paused_until = store.pause_chat(chat)
        after = int(time.time())
        assert_true(
            before + 86400 - 5 <= paused_until <= after + 86400 + 5,
            f"paused_until aproximadamente +24h (got={paused_until}, before={before}, after={after})",
            failures,
        )


async def main() -> int:
    failures: List[str] = []

    test_pause_basic_set_and_check(failures)
    test_pause_expires_after_ttl(failures)
    test_clear_pause(failures)
    test_pause_multiple_ids_lid_and_resolved(failures)
    test_pause_dedup_and_invalid_ids(failures)

    await test_webhook_human_message_triggers_pause(failures)
    await test_webhook_bot_echo_does_not_pause(failures)
    await test_webhook_clear_pause_resumes_ai(failures)
    await test_pause_default_ttl_is_one_day(failures)

    if failures:
        print("FAIL")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("OK: auto-pausa por intervencao humana validada localmente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
