# Infra overview

## Objetivo
Este projeto entrega um agente em Python que recebe mensagens do WhatsApp via WAHA, chama um LLM e responde de volta no WhatsApp. Ele tambem expoe um servidor MCP (FastMCP) para integracao com a OpenAI Platform.

## Fluxo
1. WAHA recebe a mensagem do WhatsApp.
2. WAHA envia webhook para o agente em `POST /webhook/waha`.
3. O agente gera a resposta e envia para o WAHA via `POST /api/sendText`.
4. WAHA entrega a resposta no WhatsApp.
5. (Opcional) OpenAI Platform -> MCP -> ferramentas (WAHA / n8n).

## Servicos (docker-compose)
- `waha`: WhatsApp HTTP API e webhooks.
- `agent`: FastAPI + LangChain + Groq/OpenAI.
- `mcp`: servidor FastMCP para ferramentas (WhatsApp e n8n).
- `n8n`: automacoes/workflows (opcional).
- `postgres`: banco do n8n (somente producao).
- `traefik`: proxy TLS/HTTPS (somente producao).

## Portas (local)
- `3000`: WAHA
- `8000`: Agent
- `8001`: MCP
- `5678`: n8n

## Variaveis principais (.env)
- `GROQ_API_KEY`: chave da Groq.
- `GROQ_MODEL`: modelo usado pelo agente.
- `GROQ_TEMPERATURE` e `GROQ_MAX_TOKENS`: parametros do modelo.
- `WAHA_API_KEY` / `WAHA_API_KEY_PLAIN`: chave de API do WAHA.
- `WHATSAPP_HOOK_URL`: URL do webhook (normalmente `http://agent:8000/webhook/waha`).
- `WHATSAPP_HOOK_EVENTS`: eventos enviados (ex.: `message`).
- `WAHA_SESSION`: nome da sessao do WAHA.
- `N8N_WEBHOOK_BASE_URL`: base dos webhooks do n8n (ex.: `http://n8n:5678/webhook`).
- `MCP_NAME`, `MCP_PORT`, `MCP_TRANSPORT`, `HTTP_TIMEOUT`: configuracoes do MCP.

## Deploy (Portainer/VPS)
- Coloque o agente e o MCP atras de um proxy com HTTPS.
- Evite expor a API do WAHA publicamente sem protecao.
- Para OpenAI Platform, exponha o MCP com TLS e controle de acesso.
