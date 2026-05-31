-- ============================================================
-- OPCIONAL — otimização de leitura para o dashboard.
--
-- O dashboard funciona SEM rodar este script. Em produção, com
-- volume maior de mensagens, estes índices aceleram as consultas
-- por período (overview / timeseries) e reduzem a carga no banco.
--
-- É SQL puro: NÃO altera o código nem o comportamento do agente.
--
-- Rode no SQL Editor do Supabase. Os índices são criados sem
-- CONCURRENTLY porque o SQL Editor executa tudo dentro de uma
-- transação (CONCURRENTLY não é permitido nesse caso). Como as
-- tabelas ainda têm poucas linhas, o CREATE INDEX normal trava a
-- escrita por apenas alguns milissegundos — inofensivo.
--
-- Se um dia as tabelas ficarem grandes e movimentadas, recrie os
-- índices com CONCURRENTLY via psql (fora de transação) para não
-- bloquear os INSERT do agente.
-- ============================================================

create index if not exists messages_tenant_created_idx
  on agent.messages (tenant_id, created_at);

create index if not exists conversations_tenant_lastmsg_idx
  on agent.conversations (tenant_id, last_message_at desc);
