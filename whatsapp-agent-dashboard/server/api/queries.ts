// ============================================================
// Acesso ao banco do agente (schema `agent`) — somente leitura.
//
// O dashboard funciona sem nenhuma alteração no banco. Para ganho
// de performance em produção, recomenda-se UM índice (opcional):
//   create index if not exists messages_tenant_created_idx
//     on agent.messages (tenant_id, created_at);
// É um objeto SQL puro, não toca no código do agente.
// ============================================================

import { db } from "../supabase";

const ROW_LIMIT = 50_000;

export interface TenantRow {
  id: string;
  slug: string;
  label: string;
}

export interface MsgRow {
  conversation_id: string;
  contact_id: string | null;
  role: string;
  created_at: string;
  latency_ms: number | null;
}

export interface ConvRow {
  id: string;
  status: string;
  started_at: string;
  last_message_at: string | null;
  contacts: {
    display_name: string | null;
    full_name: string | null;
    phone: string | null;
  } | null;
  messages: { count: number }[];
}

export interface ConvMessageRow {
  id: string | number;
  role: string;
  content: string;
  message_type: string | null;
  created_at: string;
}

/** Lista os tenants (clientes). Cacheada na camada de cima. */
export async function listTenants(): Promise<TenantRow[]> {
  const { data, error } = await db().from("tenants").select("*").order("slug");
  if (error) throw new Error(`tenants: ${error.message}`);
  return (data ?? []).map((r: Record<string, unknown>) => ({
    id: String(r.id),
    slug: String(r.slug),
    label: String(r.label ?? r.name ?? r.display_name ?? r.slug),
  }));
}

/** Mensagens de um tenant numa janela [since, until). */
export async function fetchMessages(
  tenantId: string,
  sinceISO: string,
  untilISO: string,
): Promise<MsgRow[]> {
  const { data, error } = await db()
    .from("messages")
    .select("conversation_id,contact_id,role,created_at,latency_ms")
    .eq("tenant_id", tenantId)
    .gte("created_at", sinceISO)
    .lt("created_at", untilISO)
    .order("created_at", { ascending: true })
    .range(0, ROW_LIMIT);
  if (error) throw new Error(`messages: ${error.message}`);
  return (data ?? []) as MsgRow[];
}

/** Conversas recentes de um tenant, com dados do contato e contagem de msgs. */
export async function recentConversations(
  tenantId: string,
  limit: number,
): Promise<ConvRow[]> {
  const { data, error } = await db()
    .from("conversations")
    .select(
      "id,status,started_at,last_message_at," +
        "contacts(display_name,full_name,phone),messages(count)",
    )
    .eq("tenant_id", tenantId)
    .order("last_message_at", { ascending: false, nullsFirst: false })
    .limit(limit);
  if (error) throw new Error(`conversations: ${error.message}`);
  return (data ?? []) as unknown as ConvRow[];
}

/** Mensagens de uma conversa específica, em ordem cronológica. */
export async function conversationMessages(
  conversationId: string,
): Promise<ConvMessageRow[]> {
  const { data, error } = await db()
    .from("messages")
    .select("id,role,content,message_type,created_at")
    .eq("conversation_id", conversationId)
    .order("created_at", { ascending: true })
    .range(0, 2000);
  if (error) throw new Error(`messages: ${error.message}`);
  return (data ?? []) as ConvMessageRow[];
}
