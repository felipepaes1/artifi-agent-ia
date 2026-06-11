"""
Valida a estabilidade de identidade LID e a idempotencia duravel da saudacao.

Historico coberto:
- Em 2026-06-11, contatos @lid sem mapeamento no WAHA nunca resolviam telefone
  (o payload trazia remoteJidAlt/senderPn, mas os extratores ignoravam) e a
  saudacao re-disparava no meio da conversa quando o trim de SESSION_MAX_ITEMS
  descartava o item de saudacao da sessao.

Como rodar:
    cd agent
    python3 ../scripts/test_lid_greeting_stability.py
"""

from __future__ import annotations

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
        sys.modules["anyio"] = anyio_stub

    if importlib.util.find_spec("httpx") is None:
        httpx_stub = types.ModuleType("httpx")

        class AsyncClient:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                pass

        class Client:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                pass

        httpx_stub.AsyncClient = AsyncClient
        httpx_stub.Client = Client
        sys.modules["httpx"] = httpx_stub

    if importlib.util.find_spec("fastapi") is None:
        fastapi_stub = types.ModuleType("fastapi")

        class HTTPException(Exception):
            def __init__(self, status_code: int, detail: str = "") -> None:
                super().__init__(detail)
                self.status_code = status_code
                self.detail = detail

        fastapi_stub.HTTPException = HTTPException
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

TMPDIR = tempfile.TemporaryDirectory()
os.environ["AGENT_PROFILE_DB"] = str(Path(TMPDIR.name) / "profile_state.db")

from app.core import state  # noqa: E402
from app.integrations.waha import extract_phone_from_payload  # noqa: E402
from app.services.routing_service import wants_profile_switch  # noqa: E402


def assert_equal(actual: Any, expected: Any, label: str, failures: list[str]) -> None:
    if actual != expected:
        failures.append(f"{label}: esperado={expected!r}, got={actual!r}")


def test_lid_payload_phone_extraction(failures: list[str]) -> None:
    lid_with_alt = {
        "from": "131924232282128@lid",
        "body": "Oi",
        "_data": {
            "key": {
                "remoteJid": "131924232282128@lid",
                "fromMe": False,
                "remoteJidAlt": "554791112233@s.whatsapp.net",
            }
        },
    }
    assert_equal(
        extract_phone_from_payload(lid_with_alt, "131924232282128@lid"),
        "554791112233",
        "telefone via remoteJidAlt",
        failures,
    )

    lid_with_sender_pn = {
        "from": "131924232282128@lid",
        "_data": {"key": {"remoteJid": "131924232282128@lid", "senderPn": "554791112233@s.whatsapp.net"}},
    }
    assert_equal(
        extract_phone_from_payload(lid_with_sender_pn, "131924232282128@lid"),
        "554791112233",
        "telefone via senderPn",
        failures,
    )

    plain_cus = {"from": "554791112233@c.us", "body": "Oi"}
    assert_equal(
        extract_phone_from_payload(plain_cus, "554791112233@c.us"),
        "554791112233",
        "telefone @c.us inalterado",
        failures,
    )

    lid_without_alt = {
        "from": "131924232282128@lid",
        "_data": {"key": {"remoteJid": "131924232282128@lid", "fromMe": False}},
    }
    assert_equal(
        extract_phone_from_payload(lid_without_alt, "131924232282128@lid"),
        "",
        "lid sem alt nao inventa telefone",
        failures,
    )


def test_lid_phone_map_persistence(failures: list[str]) -> None:
    state.store_lid_phone("131924232282128@lid", "554791112233")
    assert_equal(
        state.get_lid_phone("131924232282128@lid"),
        "554791112233",
        "lid map roundtrip",
        failures,
    )
    state.store_lid_phone("131924232282128@lid", "554799998877")
    assert_equal(
        state.get_lid_phone("131924232282128@lid"),
        "554799998877",
        "lid map upsert",
        failures,
    )
    assert_equal(state.get_lid_phone("999@lid"), "", "lid map ausente", failures)


def test_greeting_durable_state(failures: list[str]) -> None:
    chat_id = "554791112233@c.us"
    assert_equal(
        state.was_chat_greeted_recently(chat_id, "odena", 86400),
        False,
        "saudacao inexistente",
        failures,
    )
    state.mark_chat_greeted(chat_id, "odena")
    assert_equal(
        state.was_chat_greeted_recently(chat_id, "odena", 86400),
        True,
        "saudacao registrada",
        failures,
    )
    assert_equal(
        state.was_chat_greeted_recently(chat_id, "biovita", 86400),
        False,
        "saudacao por perfil isolada",
        failures,
    )

    conn = sqlite3.connect(os.environ["AGENT_PROFILE_DB"])
    conn.execute(
        "UPDATE greeting_state SET greeted_at = greeted_at - 90000 WHERE chat_id = ?",
        (chat_id,),
    )
    conn.commit()
    conn.close()
    assert_equal(
        state.was_chat_greeted_recently(chat_id, "odena", 86400),
        False,
        "saudacao expira pelo TTL",
        failures,
    )


def test_greeting_state_copy_on_identity_flip(failures: list[str]) -> None:
    state.mark_chat_greeted("131924232282128@lid", "odena")
    state.copy_greeting_state("131924232282128@lid", "554791112233@c.us")
    assert_equal(
        state.was_chat_greeted_recently("554791112233@c.us", "odena", 86400),
        True,
        "saudacao migrada no flip lid->c.us",
        failures,
    )


def test_profile_switch_requires_short_command(failures: list[str]) -> None:
    assert_equal(wants_profile_switch("Trocar Atendimento"), True, "comando curto troca", failures)
    assert_equal(wants_profile_switch("mudar de perfil"), True, "comando curto perfil", failures)
    assert_equal(
        wants_profile_switch("quero mudar meu atendimento para outro dia da semana"),
        False,
        "frase longa de remarcacao nao reseta",
        failures,
    )
    assert_equal(
        wants_profile_switch("voces tem outro horario de atendimento amanha de manha?"),
        False,
        "pergunta longa sobre horario nao reseta",
        failures,
    )


def main() -> int:
    failures: list[str] = []
    test_lid_payload_phone_extraction(failures)
    test_lid_phone_map_persistence(failures)
    test_greeting_durable_state(failures)
    test_greeting_state_copy_on_identity_flip(failures)
    test_profile_switch_requires_short_command(failures)

    if failures:
        print("FAIL")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("OK: identidade LID, mapa persistente e saudacao duravel verificados.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
