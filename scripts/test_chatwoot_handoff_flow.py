"""
Valida o fluxo local de handoff Chatwoot sem chamar APIs externas.

Como rodar:
    cd agent
    python3 ../scripts/test_chatwoot_handoff_flow.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sqlite3
import sys
import tempfile
import types
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))
os.environ.setdefault("OPENAI_API_KEY", "test-key")


def install_missing_dependency_stubs() -> None:
    if importlib.util.find_spec("anyio") is None:
        anyio_stub = types.ModuleType("anyio")
        anyio_stub.sleep = asyncio.sleep
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
            def post(self, *_args: Any, **_kwargs: Any):
                def decorator(func):
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
            pass

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
                pass

        agents_stub.SQLiteSession = SQLiteSession
        sys.modules["agents"] = agents_stub


install_missing_dependency_stubs()

from app.chatwoot_integration.client import ChatwootConfig  # noqa: E402
from app.chatwoot_integration.service import ChatwootService  # noqa: E402
from app.chatwoot_integration.store import ChatwootMapping, ChatwootStore  # noqa: E402
from app.integrations.waha import (  # noqa: E402
    extract_media_filename,
    extract_phone_from_payload,
    infer_chatwoot_file_type,
    normalize_phone,
)


class FakeChatwootClient:
    def __init__(self) -> None:
        self.status_updates: list[dict[str, Any]] = []

    async def update_conversation_status(self, *, conversation_id: int, status: str) -> dict[str, Any]:
        self.status_updates.append({"conversation_id": conversation_id, "status": status})
        return {"success": True, "current_status": status}


class FakeMappingChatwootClient:
    def __init__(self) -> None:
        self.created_contacts: list[dict[str, Any]] = []
        self.incoming_messages: list[dict[str, Any]] = []

    async def search_contacts(self, _query: str) -> list[dict[str, Any]]:
        return []

    async def create_contact(self, *, identifier: str, name: str, phone_number: str) -> dict[str, Any]:
        contact = {
            "id": 15,
            "identifier": identifier,
            "name": name,
            "phone_number": phone_number,
            "contact_inboxes": [
                {
                    "source_id": identifier,
                    "inbox": {"id": 6},
                }
            ],
        }
        self.created_contacts.append(contact)
        return contact

    async def get_contactable_inboxes(self, _contact_id: int) -> list[dict[str, Any]]:
        return []

    async def create_contact_inbox(self, contact_id: int, source_id: str) -> dict[str, Any]:
        return {"contact_id": contact_id, "source_id": source_id, "inbox": {"id": 6}}

    async def list_contact_conversations(self, _contact_id: int) -> list[dict[str, Any]]:
        return []

    async def create_conversation(self, *, contact_id: int, contact_source_id: str) -> dict[str, Any]:
        return {
            "id": 66,
            "contact_id": contact_id,
            "source_id": contact_source_id,
            "status": "pending",
        }

    async def create_incoming_message(
        self,
        *,
        conversation_id: int,
        content: str,
        contact_source_id: str,
        echo_id: str = "",
    ) -> dict[str, Any]:
        message = {
            "conversation_id": conversation_id,
            "content": content,
            "contact_source_id": contact_source_id,
            "echo_id": echo_id,
        }
        self.incoming_messages.append(message)
        return {"id": 901, **message}


def assert_equal(actual: Any, expected: Any, label: str, failures: list[str]) -> None:
    if actual != expected:
        failures.append(f"{label}: esperado={expected!r}, got={actual!r}")


def test_store_migrates_old_schema(failures: list[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "chatwoot_state.db")
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE chatwoot_mappings (
                    whatsapp_chat_id TEXT PRIMARY KEY,
                    phone TEXT NOT NULL DEFAULT '',
                    contact_name TEXT NOT NULL DEFAULT '',
                    contact_id INTEGER,
                    contact_source_id TEXT NOT NULL DEFAULT '',
                    conversation_id INTEGER UNIQUE,
                    identifier TEXT NOT NULL DEFAULT '',
                    updated_at INTEGER NOT NULL
                )
                """
            )
        store = ChatwootStore(db_path)
        store.upsert_mapping(
            ChatwootMapping(
                whatsapp_chat_id="5511999999999@c.us",
                conversation_id=10,
                handoff_state="human",
                conversation_status="open",
            )
        )
        mapping = store.get_by_chat_id("5511999999999@c.us")
        assert_equal(mapping.handoff_state if mapping else None, "human", "handoff migrado", failures)
        assert_equal(mapping.conversation_status if mapping else None, "open", "status migrado", failures)


def test_status_events_control_handoff(failures: list[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        service = ChatwootService(ChatwootConfig(base_url="", state_db_path=os.path.join(tmpdir, "cw.db")))
        service.store.upsert_mapping(
            ChatwootMapping(
                whatsapp_chat_id="5511999999999@c.us",
                conversation_id=42,
                handoff_state="human",
                conversation_status="open",
            )
        )
        if not service.is_human_handoff_active("5511999999999@c.us"):
            failures.append("handoff humano deveria estar ativo com status open")

        ignored = service.extract_outbound_event(
            {"event": "conversation_status_changed", "id": 42, "status": "pending"}
        )
        assert_equal(ignored, "conversation_pending_ai", "evento pending", failures)
        if service.is_human_handoff_active("5511999999999@c.us"):
            failures.append("handoff humano deveria desligar quando status vira pending")

        ignored = service.extract_outbound_event(
            {"event": "conversation_status_changed", "id": 42, "status": "resolved"}
        )
        assert_equal(ignored, "conversation_resolved_ai", "evento resolved", failures)
        mapping = service.store.get_by_chat_id("5511999999999@c.us")
        assert_equal(mapping.conversation_id if mapping else "missing", None, "resolved libera conversa", failures)


async def test_human_message_sets_handoff(failures: list[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        service = ChatwootService(
            ChatwootConfig(
                base_url="https://chatwoot.example",
                account_id="1",
                api_access_token="token",
                inbox_id="6",
                state_db_path=os.path.join(tmpdir, "cw.db"),
            )
        )
        fake_client = FakeChatwootClient()
        service.client = fake_client  # type: ignore[assignment]
        service.store.upsert_mapping(
            ChatwootMapping(
                whatsapp_chat_id="5511999999999@c.us",
                phone="5511999999999",
                conversation_id=77,
                contact_source_id="5511999999999@c.us",
                conversation_status="pending",
            )
        )
        extracted = service.extract_outbound_event(
            {
                "event": "message_created",
                "id": 900,
                "conversation_id": 77,
                "content": "Ola, vou continuar seu atendimento.",
                "message_type": "outgoing",
                "private": False,
                "sender": {"type": "user", "email": "agent@example.com"},
            }
        )
        if isinstance(extracted, str):
            failures.append(f"mensagem humana foi ignorada: {extracted}")
            return

        delivered_to: list[tuple[str, str]] = []

        async def fake_send(chat_id: str, content: str) -> bool:
            delivered_to.append((chat_id, content))
            return True

        await service.deliver_outbound_event(extracted, send_whatsapp_message=fake_send)
        assert_equal(
            delivered_to,
            [("5511999999999@c.us", "Ola, vou continuar seu atendimento.")],
            "entrega whatsapp",
            failures,
        )
        if not service.is_human_handoff_active("5511999999999@c.us"):
            failures.append("mensagem humana deveria ativar handoff humano")
        assert_equal(
            fake_client.status_updates,
            [{"conversation_id": 77, "status": "open"}],
            "status open apos humano",
            failures,
        )


async def test_whatsapp_phone_format_for_chatwoot_contact(failures: list[str]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        service = ChatwootService(
            ChatwootConfig(
                base_url="https://chatwoot.example",
                account_id="1",
                api_access_token="token",
                inbox_id="6",
                state_db_path=os.path.join(tmpdir, "cw.db"),
            )
        )
        fake_client = FakeMappingChatwootClient()
        service.client = fake_client  # type: ignore[assignment]

        await service.sync_incoming_whatsapp_message(
            chat_id="5511999999999@c.us",
            phone="5511999999999",
            contact_name="Maria",
            content="Oi",
            message_id="wa-1",
        )

        assert_equal(
            fake_client.created_contacts[0]["phone_number"] if fake_client.created_contacts else None,
            "+5511999999999",
            "telefone Chatwoot em formato E.164",
            failures,
        )
        mapping = service.store.get_by_chat_id("5511999999999@c.us")
        assert_equal(mapping.phone if mapping else None, "5511999999999", "telefone persistido", failures)

    lid_payload = {"from": "123456789012345@lid", "chatId": "551188887777@c.us"}
    assert_equal(
        extract_phone_from_payload(lid_payload, "123456789012345@lid"),
        "551188887777",
        "telefone preferido quando from vem como lid",
        failures,
    )
    assert_equal(normalize_phone("123456789012345@lid"), "", "lid nao vira telefone", failures)


def test_media_helpers(failures: list[str]) -> None:
    audio_payload = {"type": "audio", "mimetype": "audio/ogg"}
    assert_equal(infer_chatwoot_file_type(audio_payload), "audio", "file_type audio", failures)
    assert_equal(
        extract_media_filename(audio_payload, "http://waha/api/files/audio.ogg"),
        "audio.ogg",
        "filename audio",
        failures,
    )

    doc_payload = {"type": "document", "media": {"fileName": "exame sangue.pdf", "mimetype": "application/pdf"}}
    assert_equal(infer_chatwoot_file_type(doc_payload), "file", "file_type doc", failures)
    assert_equal(extract_media_filename(doc_payload), "exame_sangue.pdf", "filename doc", failures)


async def main() -> int:
    failures: list[str] = []
    test_store_migrates_old_schema(failures)
    test_status_events_control_handoff(failures)
    await test_human_message_sets_handoff(failures)
    await test_whatsapp_phone_format_for_chatwoot_contact(failures)
    test_media_helpers(failures)

    if failures:
        print("FAIL")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("OK: fluxo Chatwoot de anexos, status e handoff validado localmente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
