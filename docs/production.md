# Producao

## Requisitos
- Docker + Docker Compose.
- DNS apontando para o servidor (`N8N_HOST`, `AGENT_HOST`, `MCP_HOST`).
- Portas 80/443 abertas.

## 1) Criar `.env.prod`
Crie um arquivo `.env.prod` baseado em `.env.prod.example` e ajuste os valores.

## 2) Subir a stack
```bash
docker compose -f docker-compose.prod.yml --env-file .env.prod up -d --build
```

## 3) Validacao rapida
- n8n: `https://$N8N_HOST`
- agent: `https://$AGENT_HOST/healthz`
- mcp: `https://$MCP_HOST`

## 4) MCP na OpenAI Platform
- O MCP precisa estar publico com HTTPS.
- Use o endpoint do MCP (`https://$MCP_HOST`) como servidor MCP.
- Se expor publicamente, coloque autenticacao no proxy (ex.: BasicAuth, Cloudflare Access).

## 5) WAHA em producao
- Por padrao, o WAHA nao fica publico neste compose.
- Para acessar o dashboard, use um port-forward temporario ou adicione uma rota autenticada no proxy.

## Portainer
- Crie uma Stack usando `docker-compose.prod.yml`.
- Informe as variaveis do `.env.prod` na UI do Portainer.
