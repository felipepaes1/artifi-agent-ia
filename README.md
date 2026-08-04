# artifi-agent-ia

Agente de IA multi-perfil para atendimento via WhatsApp. O projeto recebe eventos do WhatsApp pelo WAHA, processa a conversa em um agent FastAPI com ferramentas de conhecimento, agendamento e áudio, responde pelo WhatsApp e sincroniza o atendimento com Chatwoot e Supabase. Também inclui um dashboard web para acompanhar conversas e indicadores por tenant.

## Sumário

- [Visão geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Como funciona na prática](#como-funciona-na-prática)
- [Pré-requisitos](#pré-requisitos)
- [Configuração local](#configuração-local)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Produção](#produção)
- [Operação diária](#operação-diária)
- [Testes e validação](#testes-e-validação)
- [Troubleshooting](#troubleshooting)

## Visão geral

O sistema foi desenhado para operar atendimentos comerciais e clínicos via WhatsApp, com separação por perfis como `mariano`, `ariane`, `mais_vision`, `criolaser`, `biovita`, `odena` e `bella_vita`.

Principais capacidades:

- Receber mensagens, áudios, mídias e votos de enquete do WhatsApp via WAHA.
- Roteamento por perfil/tenant, com instruções específicas por clínica ou segmento.
- Geração de respostas com `openai-agents`.
- Busca de conhecimento local e, opcionalmente, vector stores da OpenAI por perfil.
- Transcrição de áudio, resposta em texto e suporte a TTS para perfis configurados.
- Fluxos de agendamento com provider falso, handoff ou MCP com Google Calendar.
- Sincronização com Chatwoot para atendimento humano.
- Persistência de histórico e métricas em Supabase.
- Dashboard React/Vite/Express para operação e indicadores.
- Disparador isolado para aniversariantes Biovita.

## Arquitetura

Fluxo principal:

```text
WhatsApp
  -> WAHA (:3000)
  -> POST /webhook/waha
  -> agent FastAPI (:8000)
  -> OpenAI Agents + ferramentas internas
  -> WAHA /api/sendText ou /api/sendVoice
  -> WhatsApp

agent
  -> Chatwoot API/Webhook para handoff humano
  -> Supabase para histórico, tenants, conversas e dashboard
  -> MCP opcional (:8001) para ferramentas de agenda e mensageria
```

Serviços principais:

| Serviço | Pasta/Imagem | Porta local | Função |
| --- | --- | ---: | --- |
| `agent` | `agent/` | `8000` | API FastAPI, webhooks e execução do agente |
| `waha` | `devlikeapro/waha` | `3000` | Gateway WhatsApp, sessões e webhooks |
| `mcp` | `mcp/` | `8001` | FastMCP com ferramentas WAHA, n8n e Google Calendar |
| `n8n` | `n8nio/n8n` | `5678` | Automações locais no compose base |
| `chatwoot` | `chatwoot/chatwoot` | `3000` interno | Atendimento humano em produção/AWS |
| `dashboard` | `whatsapp-agent-dashboard/` | `3000` | Painel web com BFF Express e frontend Vite |
| `traefik` | `traefik:v3` | `80/443` | HTTPS e roteamento em produção |

Endpoints do agent:

- `GET /healthz`: health check.
- `GET /chat-ui`: tela simples de teste local.
- `GET /chat/profiles`: lista perfis disponíveis.
- `POST /chat`: teste direto do agente sem WhatsApp.
- `POST /webhook/waha`: entrada de eventos do WAHA.
- `POST /webhook/chatwoot`: eventos outbound do Chatwoot para entregar mensagens humanas no WhatsApp.

Endpoints do dashboard:

- `GET /api/tenants`
- `GET /api/overview?tenant=<slug>&range=<dias>`
- `GET /api/timeseries?tenant=<slug>&range=<dias>`
- `GET /api/conversations?tenant=<slug>&limit=<n>`
- `GET /api/messages?conversationId=<uuid>`

## Estrutura do projeto

```text
.
├── agent/                         # FastAPI + agente de IA
│   ├── app/main.py                # Entrypoint da API
│   ├── app/handlers/              # Webhook WAHA e teste de chat
│   ├── app/chatwoot_integration/  # Cliente, webhook e estado Chatwoot
│   ├── app/services/              # IA, mensagens, áudio, agenda, guardrails
│   ├── app/core/                  # Perfis, estado de sessão e roteamento
│   ├── app/prompts/               # Prompts por perfil
│   ├── app/profiles*.json         # Configuração multi-perfil
│   └── Dockerfile
├── mcp/                           # Servidor FastMCP
│   ├── app/tools/                 # Ferramentas de calendário e mensageria
│   ├── app/integrations/calendar/ # Google Calendar OAuth/API
│   └── Dockerfile
├── whatsapp-agent-dashboard/      # Dashboard React/Vite + Express
│   ├── client/
│   ├── server/
│   ├── shared/
│   └── Dockerfile
├── disparos_biovita/              # Disparos isolados de aniversário
├── docs/                          # Documentação operacional complementar
├── docker-compose.yml             # Stack local base: n8n, WAHA, agent, MCP
├── docker-compose.prod.yml        # Stack produção leve: Traefik, Redis, WAHA, agent, MCP opcional
├── docker-compose.aws.yml         # Stack AWS completa: Traefik, Chatwoot, WAHA, agent, dashboard
├── docker-compose.chatwoot.yml    # Chatwoot separado, legado/auxiliar
├── docker-compose.waha.yml        # Override para expor WAHA diretamente
└── env.aws.example                # Modelo de variáveis para AWS
```

## Como funciona na prática

### Entrada pelo WhatsApp

1. O usuário envia uma mensagem para o número conectado.
2. O WAHA recebe a mensagem na sessão configurada, por exemplo `default`.
3. O WAHA chama `POST http://agent:8000/webhook/waha`.
4. O agent normaliza `chat_id`, telefone, nome, texto, áudio ou mídia.
5. O sistema descarta duplicados, eventos antigos, ecos do próprio bot e mensagens de grupo quando `ALLOW_GROUPS=false`.
6. O perfil é resolvido por configuração fixa, enquete ou estado salvo.
7. O agent carrega histórico da sessão e contexto recente do Supabase, quando habilitado.
8. A IA roda com as instruções do perfil e ferramentas disponíveis.
9. A resposta é sanitizada, truncada conforme o perfil e dividida em partes para WhatsApp.
10. O agent envia as partes pelo WAHA e registra a conversa no Supabase.

### Handoff com Chatwoot

O Chatwoot é usado para atendimento humano e acompanhamento operacional.

- Mensagens recebidas do WhatsApp podem ser sincronizadas como conversa no Chatwoot.
- Mensagens humanas enviadas pelo Chatwoot disparam `POST /webhook/chatwoot`.
- O agent entrega a mensagem humana para o WhatsApp via WAHA.
- Conversas abertas/atribuídas podem pausar a IA.
- Conversas resolvidas ou pendentes limpam a pausa da IA.

Para o webhook outbound do Chatwoot, use URL interna quando os serviços estiverem na mesma rede Docker:

```text
http://agent:8000/webhook/chatwoot
```

Evite usar o domínio público do agent para tráfego interno entre containers.

### Supabase e dashboard

O agent grava dois tipos de histórico:

- Tabela legada configurável por `SUPABASE_TABLE`, padrão `conversations_agent_sessions`.
- Schema `agent`, usado pelo dashboard, com tabelas esperadas como `tenants`, `contacts`, `conversations` e `messages`.

O dashboard lê o Supabase somente pelo backend Express. A `SUPABASE_SERVICE_ROLE_KEY` não deve ser exposta no navegador e não deve receber prefixo `VITE_`.

O repositório inclui apenas índices opcionais para performance do dashboard:

```bash
whatsapp-agent-dashboard/server/sql/optional-index.sql
```

O SQL completo de criação do schema `agent` não está versionado neste repositório. Em um ambiente novo, crie/provisione esse schema antes de depender do dashboard.

### Base de conhecimento por perfil

Cada perfil pode usar:

- Arquivos locais em `storage/<perfil>/`, apontados por `docs_dir`.
- Vector stores remotos da OpenAI, configurados por `AGENT_VECTOR_STORE_<PERFIL>`.

Importante: `storage/` é dado operacional e fica fora do Git. Alterar arquivos locais de conhecimento no servidor não atualiza automaticamente os vector stores remotos. Quando a base muda, atualize as duas fontes se ambas estiverem em uso.

### MCP e Google Calendar

O MCP é opcional. Ele registra ferramentas para:

- `waha_send_text`
- `n8n_trigger_webhook`
- `ping`
- `check_availability`
- `list_events`
- `suggest_slots`
- `create_event`
- `update_event`
- `cancel_event`

Para agenda real via Google Calendar, configure `CALENDAR_PROVIDER=google_calendar`, credenciais OAuth e `GOOGLE_TOKEN_STORE_PATH`. O compose de produção leve deixa o `mcp` em profile `disabled`; suba explicitamente se algum perfil usa `schedule.provider=mcp_google_calendar`.

## Pré-requisitos

Para desenvolvimento local:

- Docker e Docker Compose.
- Chave de modelo compatível com `openai-agents`, normalmente `OPENAI_API_KEY`.
- Chave de API do WAHA.
- WhatsApp disponível para escanear o QR Code da sessão WAHA.

Para produção:

- Servidor Linux com Docker e Docker Compose.
- DNS apontando para o servidor.
- Portas `80` e `443` abertas.
- Domínios para `CHATWOOT_HOST`, `WAHA_HOST`, `AGENT_HOST` e `DASHBOARD_HOST`, quando usar `docker-compose.aws.yml`.
- Supabase preparado se histórico/dashboard forem usados.
- Backups definidos para `volumes/` e bancos.

## Configuração local

### 1. Criar arquivo `.env`

Crie um `.env` na raiz do projeto. Valores mínimos para subir a stack base:

```bash
WEBHOOK_URL=http://localhost:5678

WAHA_API_KEY=dev-waha-key
WAHA_API_KEY_PLAIN=dev-waha-key
WAHA_DASHBOARD_USERNAME=admin
WAHA_DASHBOARD_PASSWORD=admin
WAHA_BASE_URL=http://waha:3000
WAHA_SESSION=default
WHATSAPP_HOOK_URL=http://agent:8000/webhook/waha
WHATSAPP_HOOK_EVENTS=message

OPENAI_API_KEY=sk-...
OPENAI_MODEL=
OPENAI_MAX_TOKENS=

AGENT_PROMPT_PROFILE=mariano
AGENT_BASE_INSTRUCTIONS_ENABLED=false
ALLOW_GROUPS=false
MAX_REPLY_CHARS=1200

SUPABASE_ENABLED=false
SUPABASE_URL=
SUPABASE_SERVICE_ROLE_KEY=
SUPABASE_TABLE=conversations_agent_sessions
SUPABASE_APP=delivery
```

Se for testar Chatwoot localmente, preencha também as variáveis `CHATWOOT_*` e suba o compose adequado.

### 2. Subir serviços

```bash
docker compose --env-file .env up -d --build
```

Serviços esperados:

```bash
docker compose ps
```

### 3. Criar sessão no WAHA

1. Acesse `http://localhost:3000`.
2. Entre com as credenciais do dashboard WAHA.
3. Crie a sessão `default` ou o valor de `WAHA_SESSION`.
4. Escaneie o QR Code.
5. Confirme que a sessão está conectada.

### 4. Testar health check e chat direto

```bash
curl http://localhost:8000/healthz
```

```bash
curl -X POST http://localhost:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"local-test","profile_id":"mariano","message":"Olá, gostaria de agendar uma avaliação."}'
```

Também existe uma interface simples:

```text
http://localhost:8000/chat-ui
```

### 5. Testar pelo WhatsApp

Envie uma mensagem para o número conectado no WAHA. O fluxo esperado é:

- WAHA recebe a mensagem.
- Container `agent` registra o webhook.
- Agent responde pelo WAHA.
- Conversa é registrada no Supabase se `SUPABASE_ENABLED=true`.

Logs úteis:

```bash
docker compose logs -f agent
docker compose logs -f waha
```

## Dashboard local

O dashboard roda separado da stack base.

```bash
cd whatsapp-agent-dashboard
cp .env.example .env
pnpm install
pnpm dev
```

Em desenvolvimento, o Vite serve frontend e API. Configure no arquivo `whatsapp-agent-dashboard/.env`:

```bash
SUPABASE_URL=https://...
SUPABASE_SERVICE_ROLE_KEY=...
DASHBOARD_DB_SCHEMA=agent
DASHBOARD_CACHE_TTL_MS=60000
```

Com build local:

```bash
cd whatsapp-agent-dashboard
pnpm build
pnpm start
```

## Variáveis de ambiente

### Agent

| Variável | Obrigatória | Função |
| --- | --- | --- |
| `OPENAI_API_KEY` | Sim | Chave usada pelo `openai-agents` e APIs OpenAI |
| `OPENAI_MODEL` | Depende | Modelo principal; se vazio, depende do comportamento/default da SDK |
| `OPENAI_MAX_TOKENS` | Não | Limite de tokens; GPT-5 recebe mínimo interno de 1024 quando configurado abaixo disso |
| `OPENAI_TRANSCRIBE_MODEL` | Não | Modelo de transcrição, padrão `gpt-4o-mini-transcribe` |
| `OPENAI_TTS_MODEL` | Não | Modelo TTS, padrão `gpt-4o-mini-tts` |
| `OPENAI_TTS_VOICE` | Não | Voz TTS, padrão `marin` |
| `AGENT_PROMPT_PROFILE` | Recomendado | Perfil fixo quando não há roteamento por enquete |
| `AGENT_PROFILE_ROUTING` | Não | Habilita roteamento multi-perfil |
| `AGENT_PROFILES_PATH` | Não | Caminho do JSON de perfis |
| `AGENT_BASE_INSTRUCTIONS_ENABLED` | Não | Prefixa instruções globais do agent |
| `ALLOW_GROUPS` | Não | Permite responder grupos quando `true` |
| `MAX_REPLY_CHARS` | Não | Limite global de resposta antes da divisão em mensagens |
| `SESSION_MAX_ITEMS` | Não | Limite de itens em memória por sessão; `0` desativa trim |

### WAHA

| Variável | Obrigatória | Função |
| --- | --- | --- |
| `WAHA_BASE_URL` | Sim | URL interna do WAHA para o agent, normalmente `http://waha:3000` |
| `WAHA_API_KEY_PLAIN` | Sim | Chave enviada em chamadas do agent para o WAHA |
| `WAHA_API_KEY` | Sim | Chave usada pelo WAHA e MCP; pode ser igual à plain |
| `WAHA_SESSION` | Sim | Nome da sessão WhatsApp |
| `WHATSAPP_HOOK_URL` | Sim | Webhook do agent recebido pelo WAHA |
| `WHATSAPP_HOOK_EVENTS` | Sim | Eventos assinados, normalmente `message` |
| `WAHA_DASHBOARD_USERNAME` | Produção | Login do dashboard WAHA |
| `WAHA_DASHBOARD_PASSWORD` | Produção | Senha do dashboard WAHA |

### Chatwoot

| Variável | Obrigatória | Função |
| --- | --- | --- |
| `CHATWOOT_SYNC_ENABLED` | Não | Liga/desliga sincronização; padrão do código é `true` |
| `CHATWOOT_BASE_URL` | Sim para sync | URL interna, normalmente `http://chatwoot:3000` |
| `CHATWOOT_ACCOUNT_ID` | Sim para sync | Conta do Chatwoot |
| `CHATWOOT_API_ACCESS_TOKEN` | Sim para sync | Token da API do Chatwoot |
| `CHATWOOT_INBOX_ID` | Sim para sync | Inbox onde conversas serão criadas |
| `CHATWOOT_INBOX_IDENTIFIER` | Depende | Identificador público do inbox |
| `CHATWOOT_WEBHOOK_SECRET` | Recomendado | Segredo para validar webhook Chatwoot |
| `CHATWOOT_AI_STATUS` | Não | Status usado para conversas com IA, padrão `pending` |
| `CHATWOOT_HUMAN_STATUS` | Não | Status usado no handoff humano, padrão `open` |

### Supabase

| Variável | Obrigatória | Função |
| --- | --- | --- |
| `SUPABASE_ENABLED` | Não | Liga/desliga Supabase, padrão `true` |
| `SUPABASE_URL` | Sim se habilitado | URL do projeto Supabase |
| `SUPABASE_SERVICE_ROLE_KEY` | Recomendado | Chave de serviço para agent/dashboard |
| `SUPABASE_TABLE` | Não | Tabela legada de sessões, padrão `conversations_agent_sessions` |
| `SUPABASE_APP` | Não | Nome lógico da aplicação/tenant legado |
| `SUPABASE_SESSION_LIMIT` | Não | Quantidade de turnos recentes para hidratar contexto |
| `DASHBOARD_DB_SCHEMA` | Dashboard | Schema lido pelo dashboard, padrão `agent` |

### MCP e Google Calendar

| Variável | Obrigatória | Função |
| --- | --- | --- |
| `AGENT_SCHEDULING_MCP_URL` | Se usar agenda MCP | URL do MCP para o agent, padrão `http://mcp:8001/mcp/` |
| `MCP_NAME` | Não | Nome do servidor MCP |
| `MCP_PORT` | Não | Porta, padrão `8001` |
| `MCP_TRANSPORT` | Não | Transporte, padrão `http` |
| `CALENDAR_PROVIDER` | Se usar calendário | Apenas `google_calendar` está implementado |
| `CALENDAR_DEFAULT_TIMEZONE` | Não | Padrão `America/Sao_Paulo` |
| `CALENDAR_DEFAULT_ID` | Não | Calendário padrão, padrão `primary` |
| `GOOGLE_CLIENT_ID` | Sim para Google | Client ID OAuth |
| `GOOGLE_CLIENT_SECRET` | Sim para Google | Client secret OAuth |
| `GOOGLE_TOKEN_STORE_PATH` | Sim para persistência | Arquivo de tokens, padrão `/data/google_calendar_tokens.json` |

## Produção

Há dois caminhos principais de produção.

### Opção A: AWS completa

Use `docker-compose.aws.yml` quando quiser subir a stack completa com:

- Traefik
- Chatwoot
- Postgres do Chatwoot
- Redis do Chatwoot
- WAHA
- Agent
- Dashboard

Passos sugeridos:

```bash
cp env.aws.example .env.aws
nano .env.aws
```

Preencha no mínimo:

- Domínios e TLS: `ACME_EMAIL`, `CHATWOOT_HOST`, `AGENT_HOST`, `DASHBOARD_HOST`, `WAHA_HOST`, `FRONTEND_URL`.
- Chatwoot: `SECRET_KEY_BASE`, `POSTGRES_*`, `REDIS_PASSWORD`.
- WAHA: `WAHA_API_KEY_PLAIN`, `WAHA_DASHBOARD_USERNAME`, `WAHA_DASHBOARD_PASSWORD`, `WAHA_SESSION`.
- Agent: `OPENAI_API_KEY`, `OPENAI_MODEL`, `AGENT_PROMPT_PROFILE` ou `AGENT_PROFILE_ROUTING`.
- Supabase: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_TABLE`, `SUPABASE_APP`.
- Chatwoot API: `CHATWOOT_ACCOUNT_ID`, `CHATWOOT_API_ACCESS_TOKEN`, `CHATWOOT_INBOX_ID`, `CHATWOOT_INBOX_IDENTIFIER`.

Gerar `SECRET_KEY_BASE`:

```bash
openssl rand -hex 64
```

Preparar o diretório de volumes:

```bash
mkdir -p volumes
```

Preparar banco do Chatwoot em instalação nova:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml run --rm chatwoot \
  bundle exec rails db:chatwoot_prepare
```

Subir a stack:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml up -d --build
```

Validar:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml ps
curl https://$AGENT_HOST/healthz
```

Acessos esperados:

- Chatwoot: `https://$CHATWOOT_HOST`
- WAHA: `https://$WAHA_HOST`
- Agent health: `https://$AGENT_HOST/healthz`
- Dashboard: `https://$DASHBOARD_HOST`

Configurações pós-subida:

1. Entrar no WAHA e conectar a sessão `WAHA_SESSION`.
2. Criar/configurar conta, usuário e inbox no Chatwoot.
3. Configurar webhook outbound do Chatwoot para `http://agent:8000/webhook/chatwoot`.
4. Preencher `CHATWOOT_ACCOUNT_ID`, `CHATWOOT_API_ACCESS_TOKEN`, `CHATWOOT_INBOX_ID` e `CHATWOOT_INBOX_IDENTIFIER`.
5. Reiniciar o agent após mudar variáveis:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml up -d --build agent
```

### Opção B: produção leve

Use `docker-compose.prod.yml` quando o ambiente não precisa subir Chatwoot e Dashboard na mesma stack. Ele contém:

- Traefik
- Redis
- WAHA
- Agent
- MCP opcional em profile desabilitado
- `biovita-sender` sob demanda

Subida:

```bash
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d --build
```

Subir MCP quando necessário:

```bash
docker compose --env-file .env.prod -f docker-compose.prod.yml --profile disabled up -d --build mcp
```

Rodar disparador Biovita sob demanda:

```bash
docker compose --env-file .env.prod -f docker-compose.prod.yml --profile manual run --rm biovita-sender \
  node biovita_birthday_sender.js --input input.manual.json --message-file message.txt --limit 20
```

Leia os detalhes em `disparos_biovita/README.md` antes de usar `--send`.

### Deploy de código

Deploy não é automático. Depois de merge/push, entre no servidor e rode:

```bash
git pull
docker compose --env-file .env.aws -f docker-compose.aws.yml up -d --build agent dashboard
```

Para stack leve:

```bash
git pull
docker compose --env-file .env.prod -f docker-compose.prod.yml up -d --build agent
```

Evite `--remove-orphans` em servidores compartilhados com outras stacks, pois pode derrubar serviços que não fazem parte do compose atual.

### Backup

Inclua no backup:

- `volumes/waha_sessions`: sessão WhatsApp e credenciais da sessão WAHA.
- `volumes/agent_state` ou `storage`: SQLite de sessões, pausas e estado do agent.
- `volumes/chatwoot_postgres`: banco do Chatwoot.
- `volumes/chatwoot_redis`: filas/cache do Chatwoot.
- `volumes/chatwoot_storage`: anexos do Chatwoot.
- `volumes/traefik`: certificados ACME.
- `volumes/mcp_data`: tokens OAuth do Google Calendar, se MCP estiver ativo.

Não recrie WAHA sem preservar o volume de sessões, a menos que seja aceitável escanear QR novamente.

## Operação diária

Comandos úteis:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml ps
docker compose --env-file .env.aws -f docker-compose.aws.yml logs -f agent
docker compose --env-file .env.aws -f docker-compose.aws.yml logs -f waha
docker compose --env-file .env.aws -f docker-compose.aws.yml logs -f chatwoot_worker
```

Rebuild apenas do agent:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml up -d --build agent
```

Rebuild apenas do dashboard:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml up -d --build dashboard
```

Ver health do agent dentro da rede:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml exec agent \
  python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/healthz').read().decode())"
```

Verificar logs de webhook:

```bash
docker compose --env-file .env.aws -f docker-compose.aws.yml logs agent | grep webhook
```

## Testes e validação

### Testes Python do agent

Os testes operacionais ficam em `scripts/` e rodam diretamente com Python:

```bash
python scripts/test_ai_pause_flow.py
python scripts/test_chatwoot_handoff_flow.py
python scripts/test_guardrail_passthrough.py
python scripts/test_lid_greeting_stability.py
python scripts/test_supabase_agent.py
python scripts/test_tts_audio_reply.py
python scripts/test_urgency_guardrail.py
python scripts/test_webhook_message_safety.py
```

### Testes do disparador Biovita

```bash
node --test disparos_biovita/test/*.test.js
```

### Dashboard

```bash
cd whatsapp-agent-dashboard
pnpm check
pnpm build
```

### Validação manual mínima antes de produção

1. `GET /healthz` retorna `{"ok":true}`.
2. WAHA mostra a sessão conectada.
3. Mensagem de teste no WhatsApp chega no log do `agent`.
4. Agent responde no WhatsApp.
5. Conversa aparece no Chatwoot, quando sync estiver ligado.
6. Mensagem humana enviada pelo Chatwoot chega no WhatsApp.
7. Dashboard carrega tenants e conversas do Supabase.

## Troubleshooting

### Agent sobe, mas falha ao responder

Verifique:

- `OPENAI_API_KEY` está preenchida.
- `OPENAI_MODEL` é compatível com a versão instalada da SDK.
- Logs do container `agent`.
- Se a resposta é vazia, confira limites de tokens e instruções do perfil.

### WAHA recebe mensagem, mas agent não recebe webhook

Verifique:

- `WHATSAPP_HOOK_URL=http://agent:8000/webhook/waha` dentro do Docker.
- `WHATSAPP_HOOK_EVENTS=message`.
- Sessão WAHA conectada.
- Logs do `waha`.

### Agent tenta enviar, mas WAHA recusa

Verifique:

- `WAHA_BASE_URL=http://waha:3000`.
- `WAHA_API_KEY_PLAIN` igual à chave configurada no WAHA.
- `WAHA_SESSION` existe e está conectada.

### Chatwoot não entrega mensagem humana ao WhatsApp

Verifique:

- Webhook outbound do Chatwoot aponta para `http://agent:8000/webhook/chatwoot`.
- `chatwoot_worker` está na rede que alcança `agent`.
- `CHATWOOT_WEBHOOK_SECRET` bate com o segredo configurado no Chatwoot.
- Logs de `chatwoot_worker` e `agent`.

### Dashboard sem dados

Verifique:

- `SUPABASE_URL` e `SUPABASE_SERVICE_ROLE_KEY` no ambiente do dashboard.
- `DASHBOARD_DB_SCHEMA=agent`.
- Existem linhas em `agent.tenants`, `agent.conversations` e `agent.messages`.
- O tenant acessado existe pelo `slug`.

### IA parou de responder para um contato

Possíveis causas:

- Conversa está em handoff humano no Chatwoot.
- Pausa manual foi ativada por mensagem `fromMe` no WhatsApp.
- Estado de pausa está persistido no SQLite do agent.
- Conversa foi resolvida no Chatwoot, mas o webhook de retomada não chegou.

Primeiro confira logs do `agent` e o estado da conversa no Chatwoot.

### Mensagens duplicadas ou antigas

O agent possui deduplicação e descarte por idade de evento. Se houver perda ou atraso:

- Verifique `MAX_MESSAGE_AGE_SECONDS`.
- Confira fila/backlog nos logs do WAHA.
- Confira latência do LLM e integrações externas.

## Segurança

- Nunca commite `.env`, `.env.prod`, `.env.aws`, `env.aws` ou chaves reais.
- Use `env.aws.example` apenas como modelo.
- Proteja o dashboard WAHA em produção.
- Evite expor WAHA publicamente sem autenticação.
- Prefira URLs internas Docker para comunicação entre serviços.
- Faça backup dos volumes antes de atualizar imagem, branch ou compose.

## Referências internas

- `docs/setup.md`: setup local curto.
- `docs/infra.md`: visão de infraestrutura.
- `docs/production.md`: notas de produção legadas.
- `docs/chatwoot-integration-plan.md`: diagnóstico e decisões da integração Chatwoot.
- `disparos_biovita/README.md`: operação do disparador Biovita.

## Referências externas

- Chatwoot Docker deployment: https://developers.chatwoot.com/self-hosted/deployment/docker
