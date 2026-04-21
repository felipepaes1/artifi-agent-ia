"""
Teste ponta-a-ponta do novo schema `agent.*` no Supabase.

Como rodar (local ou em qualquer máquina com as envs de prod carregadas):

    cd agent
    SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... SUPABASE_ENABLED=true \\
        python3 ../scripts/test_supabase_agent.py

O script não toca na tabela legada `conversations_agent_sessions`.
Ele cria / toca linhas em: agent.contacts, agent.conversations, agent.messages.
Use um telefone de teste (ex.: +5500000000000) para poder deletar depois.

Antes de rodar: garanta que o schema `agent` está exposto na API
(Supabase Dashboard -> Project Settings -> API -> Exposed schemas).
"""

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))

from app.integrations.supabase_agent import (  # noqa: E402
    get_patient_context,
    insert_messages,
    record_turn,
    resolve_tenant_id,
    upsert_contact,
    upsert_conversation,
)


TEST_TENANT = os.getenv("TEST_TENANT_SLUG", "mariano")
TEST_PHONE = os.getenv("TEST_PHONE", "5500000000000")
TEST_CHAT_ID = os.getenv("TEST_CHAT_ID", f"{TEST_PHONE}@c.us")


async def main() -> int:
    print(f"[1] resolve_tenant_id({TEST_TENANT!r})")
    tenant_id = await resolve_tenant_id(TEST_TENANT)
    assert tenant_id, f"tenant não encontrado. Seed rodou? Schema exposto na API?"
    print(f"    -> {tenant_id}")

    print(f"[2] upsert_contact(tenant, {TEST_PHONE!r})")
    contact_id = await upsert_contact(
        tenant_id,
        TEST_PHONE,
        wa_chat_id=TEST_CHAT_ID,
        display_name="Paciente Teste",
    )
    assert contact_id, "upsert_contact retornou None"
    print(f"    -> {contact_id}")

    print("[3] upsert_conversation")
    conversation_id = await upsert_conversation(
        tenant_id,
        contact_id,
        wa_chat_id=TEST_CHAT_ID,
    )
    assert conversation_id, "upsert_conversation retornou None"
    print(f"    -> {conversation_id}")

    print("[4] insert_messages (user + assistant)")
    await insert_messages(
        [
            {
                "tenant_id": tenant_id,
                "conversation_id": conversation_id,
                "contact_id": contact_id,
                "role": "user",
                "content": "Olá, gostaria de agendar uma limpeza.",
                "message_type": "text",
            },
            {
                "tenant_id": tenant_id,
                "conversation_id": conversation_id,
                "contact_id": contact_id,
                "role": "assistant",
                "content": "Claro! Para qual dia você gostaria?",
                "message_type": "text",
            },
        ]
    )
    print("    -> OK")

    print("[5] record_turn (entry point do dual-write)")
    await record_turn(
        tenant_slug=TEST_TENANT,
        phone=TEST_PHONE,
        wa_chat_id=TEST_CHAT_ID,
        display_name="Paciente Teste",
        user_message="Tem vaga terça de manhã?",
        bot_message="Sim, tenho 9h e 10h. Qual prefere?",
        message_type="text",
    )
    print("    -> OK")

    print(f"[6] get_patient_context({TEST_TENANT!r}, {TEST_PHONE!r})")
    ctx = await get_patient_context(TEST_TENANT, TEST_PHONE, limit=10)
    assert ctx and ctx.get("exists"), f"contexto não retornado: {ctx!r}"
    msgs = ctx.get("recent_messages") or []
    print(f"    -> exists=True, recent_messages={len(msgs)}")
    assert len(msgs) >= 4, "esperava ao menos 4 mensagens registradas"
    for m in msgs[-4:]:
        print(f"       [{m['role']:<9}] {m['content'][:60]}")

    print("\nTudo ok. Pra limpar o paciente de teste rode no SQL Editor:")
    print(
        f"  DELETE FROM agent.contacts WHERE tenant_id = '{tenant_id}' "
        f"AND phone = '{TEST_PHONE}';"
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
