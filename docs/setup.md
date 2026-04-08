# Setup local

## 1) Configurar .env
Edite o arquivo `.env` e preencha:
- `GROQ_API_KEY`
- `WAHA_API_KEY` e `WAHA_API_KEY_PLAIN`
- `WAHA_DASHBOARD_USERNAME` e `WAHA_DASHBOARD_PASSWORD`
- `GROQ_MODEL` (opcional)
- `N8N_WEBHOOK_BASE_URL` (opcional, default `http://n8n:5678/webhook`)
- `MCP_NAME`, `MCP_PORT`, `MCP_TRANSPORT` (opcional)

## 2) Subir os servicos
```bash
docker compose up -d --build
```

## 3) Criar sessao no WAHA
- Acesse o dashboard do WAHA em `http://localhost:3000/`.
- Crie a sessao `default` (ou o nome definido em `WAHA_SESSION`).
- Escaneie o QR code do WhatsApp.

## 4) Testar
Envie uma mensagem para o numero conectado. O agente deve responder.

## 5) Testar MCP (opcional)
- O MCP sobe em `http://localhost:8001/`.
- Use um cliente MCP para chamar a ferramenta `ping` ou `waha_send_text`.

## Troubleshooting rapido
- Se o agente nao responder: verifique logs do container `agent`.
- Se nao receber webhook: confirme `WHATSAPP_HOOK_URL` e `WHATSAPP_HOOK_EVENTS`.
- Se o WAHA recusar a chamada: confirme a `WAHA_API_KEY_PLAIN` no agent.
