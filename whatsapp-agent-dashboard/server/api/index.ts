// ============================================================
// API de leitura do dashboard.
//
// `apiMiddleware` é um middleware connect-compatível: serve tanto
// para o Express (produção, server/index.ts) quanto para o dev
// server do Vite (vite.config.ts). Toda resposta é JSON.
//
// Endpoints (todos GET):
//   /api/tenants
//   /api/overview?tenant=<slug>&range=<dias>
//   /api/timeseries?tenant=<slug>&range=<dias>
//   /api/conversations?tenant=<slug>&limit=<n>
//   /api/messages?conversationId=<uuid>
// ============================================================

import { cached, CACHE_TTL_MS } from "../cache";
import type {
  ConversationsDTO,
  KpiDTO,
  MessagesDTO,
  OverviewDTO,
  TenantsDTO,
  TimeseriesDTO,
} from "../../shared/api-types";
import {
  conversationMessages,
  fetchMessages,
  listTenants,
  recentConversations,
  type MsgRow,
  type TenantRow,
} from "./queries";
import {
  isMockTenantSlug,
  MOCK_TENANT,
  mockConversations,
  mockMessages,
  mockOverview,
  mockTimeseries,
} from "./mock";

const DAY_MS = 86_400_000;
const BRT_OFFSET_MS = 3 * 3_600_000; // America/Sao_Paulo (UTC-3)
const TENANTS_TTL_MS = 5 * 60_000;

// ---------- helpers de tempo ----------

function clampRange(raw: string | null): number {
  const n = Math.round(Number(raw));
  if (!Number.isFinite(n) || n <= 0) return 30;
  return Math.min(Math.max(n, 1), 180);
}

function brtDayKey(epochMs: number): string {
  return new Date(epochMs - BRT_OFFSET_MS).toISOString().slice(0, 10);
}

function brtHour(iso: string): number {
  return new Date(Date.parse(iso) - BRT_OFFSET_MS).getUTCHours();
}

// ---------- tenants ----------

async function tenants(): Promise<TenantRow[]> {
  return cached("tenants", TENANTS_TTL_MS, listTenants);
}

async function requireTenant(query: URLSearchParams): Promise<TenantRow> {
  const slug = (query.get("tenant") ?? "").trim();
  if (!slug) throw new Error("parâmetro 'tenant' é obrigatório");
  if (isMockTenantSlug(slug)) {
    return {
      id: MOCK_TENANT.slug,
      slug: MOCK_TENANT.slug,
      label: MOCK_TENANT.label,
    };
  }
  const found = (await tenants()).find((t) => t.slug === slug);
  if (!found) throw new Error(`tenant não encontrado: ${slug}`);
  return found;
}

// ---------- agregações sobre mensagens ----------

interface Summary {
  messages: number;
  conversations: number;
  contacts: number;
  assistant: number;
  user: number;
  responseSec: number | null;
}

/** Tempo médio de resposta, em segundos, a partir do latency_ms
 *  que o agente já grava em cada mensagem do assistente. */
function avgResponseSec(rows: MsgRow[]): number | null {
  let total = 0;
  let n = 0;
  for (const r of rows) {
    if (r.role === "assistant" && r.latency_ms != null && r.latency_ms >= 0) {
      total += r.latency_ms;
      n++;
    }
  }
  return n > 0 ? total / n / 1000 : null;
}

function summarize(rows: MsgRow[]): Summary {
  const conversations = new Set<string>();
  const contacts = new Set<string>();
  let assistant = 0;
  let user = 0;
  for (const r of rows) {
    conversations.add(r.conversation_id);
    if (r.contact_id) contacts.add(r.contact_id);
    if (r.role === "assistant") assistant++;
    else if (r.role === "user") user++;
  }
  return {
    messages: rows.length,
    conversations: conversations.size,
    contacts: contacts.size,
    assistant,
    user,
    responseSec: avgResponseSec(rows),
  };
}

function kpi(
  label: string,
  description: string,
  format: KpiDTO["format"],
  current: number,
  previous: number,
): KpiDTO {
  let trend: KpiDTO["trend"] = "stable";
  if (current > previous) trend = "up";
  else if (current < previous) trend = "down";
  return {
    label,
    description,
    format,
    value: current,
    previousValue: previous,
    trend,
  };
}

const ratio = (a: number, b: number): number =>
  b > 0 ? Math.round((a / b) * 10) / 10 : 0;

// ---------- handlers ----------

async function rangeMessages(
  tenantId: string,
  range: number,
  now: number,
): Promise<{ current: MsgRow[]; previous: MsgRow[] }> {
  const since = now - range * DAY_MS;
  const prevSince = now - 2 * range * DAY_MS;
  const [current, previous] = await Promise.all([
    cached(`msg:${tenantId}:${range}:cur`, CACHE_TTL_MS, () =>
      fetchMessages(tenantId, new Date(since).toISOString(), new Date(now).toISOString()),
    ),
    cached(`msg:${tenantId}:${range}:prev`, CACHE_TTL_MS, () =>
      fetchMessages(
        tenantId,
        new Date(prevSince).toISOString(),
        new Date(since).toISOString(),
      ),
    ),
  ]);
  return { current, previous };
}

async function overview(query: URLSearchParams): Promise<OverviewDTO> {
  const tenant = await requireTenant(query);
  const range = clampRange(query.get("range"));
  if (isMockTenantSlug(tenant.slug)) return mockOverview(range);
  const now = Date.now();
  const { current, previous } = await rangeMessages(tenant.id, range, now);
  const c = summarize(current);
  const p = summarize(previous);

  const kpis: KpiDTO[] = [
    kpi("Total de Conversas", `Conversas com atividade nos últimos ${range} dias`, "number", c.conversations, p.conversations),
    kpi("Total de Mensagens", "Mensagens trocadas no período", "number", c.messages, p.messages),
    kpi("Pacientes Atendidos", "Contatos distintos atendidos no período", "number", c.contacts, p.contacts),
    kpi("Mensagens por Conversa", "Média de mensagens por conversa", "number", ratio(c.messages, c.conversations), ratio(p.messages, p.conversations)),
    kpi("Mensagens do Agente", "Respostas enviadas pelo agente no período", "number", c.assistant, p.assistant),
    kpi("Mensagens do Paciente", "Mensagens recebidas dos pacientes no período", "number", c.user, p.user),
  ];

  return {
    range,
    generatedAt: new Date(now).toISOString(),
    kpis,
    raw: {
      messages: c.messages,
      conversations: c.conversations,
      contacts: c.contacts,
      assistant: c.assistant,
      user: c.user,
      responseSec: c.responseSec,
    },
  };
}

async function timeseries(query: URLSearchParams): Promise<TimeseriesDTO> {
  const tenant = await requireTenant(query);
  const range = clampRange(query.get("range"));
  if (isMockTenantSlug(tenant.slug)) return mockTimeseries(range);
  const now = Date.now();
  const rows = await cached(`msg:${tenant.id}:${range}:cur`, CACHE_TTL_MS, () =>
    fetchMessages(
      tenant.id,
      new Date(now - range * DAY_MS).toISOString(),
      new Date(now).toISOString(),
    ),
  );

  const dayMap = new Map<string, { conv: Set<string>; messages: number }>();
  const hourCounts = new Array<number>(24).fill(0);
  for (const r of rows) {
    const key = brtDayKey(Date.parse(r.created_at));
    let bucket = dayMap.get(key);
    if (!bucket) {
      bucket = { conv: new Set(), messages: 0 };
      dayMap.set(key, bucket);
    }
    bucket.conv.add(r.conversation_id);
    bucket.messages++;
    hourCounts[brtHour(r.created_at)]++;
  }

  const daily = [];
  for (let i = range - 1; i >= 0; i--) {
    const key = brtDayKey(now - i * DAY_MS);
    const bucket = dayMap.get(key);
    daily.push({
      date: key,
      conversations: bucket ? bucket.conv.size : 0,
      messages: bucket ? bucket.messages : 0,
    });
  }

  const hourly = hourCounts.map((messages, h) => ({
    hour: `${String(h).padStart(2, "0")}:00`,
    messages,
  }));

  return { range, daily, hourly };
}

async function conversations(query: URLSearchParams): Promise<ConversationsDTO> {
  const tenant = await requireTenant(query);
  const limit = Math.min(Math.max(Number(query.get("limit")) || 80, 1), 200);
  if (isMockTenantSlug(tenant.slug)) return mockConversations(limit);
  const rows = await cached(`conv:${tenant.id}:${limit}`, CACHE_TTL_MS, () =>
    recentConversations(tenant.id, limit),
  );
  const items = rows.map((r) => ({
    id: r.id,
    patientName:
      r.contacts?.display_name ||
      r.contacts?.full_name ||
      r.contacts?.phone ||
      "Sem identificação",
    phone: r.contacts?.phone || "",
    status: (r.status === "closed" ? "concluida" : "em_andamento") as
      | "concluida"
      | "em_andamento",
    startedAt: r.started_at,
    lastMessageAt: r.last_message_at || r.started_at,
    messages: r.messages?.[0]?.count ?? 0,
  }));
  return { items, total: items.length };
}

async function messages(query: URLSearchParams): Promise<MessagesDTO> {
  const id = (query.get("conversationId") ?? "").trim();
  if (!id) throw new Error("parâmetro 'conversationId' é obrigatório");
  if (id.startsWith("mock-conv-")) return mockMessages(id);
  const rows = await cached(`msgs:${id}`, CACHE_TTL_MS, () =>
    conversationMessages(id),
  );
  return {
    items: rows.map((r) => ({
      id: String(r.id),
      role: r.role,
      content: r.content,
      createdAt: r.created_at,
      messageType: r.message_type,
    })),
  };
}

// ---------- dispatch ----------

interface DispatchResult {
  status: number;
  body: unknown;
}

async function dispatch(
  pathname: string,
  query: URLSearchParams,
): Promise<DispatchResult> {
  try {
    switch (pathname) {
      case "/api/tenants": {
        const rows = await tenants().catch(() => []);
        const items = [
          MOCK_TENANT,
          ...rows
            .filter((t) => !isMockTenantSlug(t.slug))
            .map((t) => ({
              slug: t.slug,
              label: t.label,
            })),
        ];
        return { status: 200, body: { items } satisfies TenantsDTO };
      }
      case "/api/overview":
        return { status: 200, body: await overview(query) };
      case "/api/timeseries":
        return { status: 200, body: await timeseries(query) };
      case "/api/conversations":
        return { status: 200, body: await conversations(query) };
      case "/api/messages":
        return { status: 200, body: await messages(query) };
      default:
        return { status: 404, body: { error: "rota não encontrada" } };
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { status: 500, body: { error: message } };
  }
}

/** Middleware connect-compatível (Express + Vite dev server). */
export function apiMiddleware(req: any, res: any, next: () => void): void {
  const url: string = req.url || "";
  if (!url.startsWith("/api/")) {
    next();
    return;
  }
  const parsed = new URL(url, "http://localhost");
  dispatch(parsed.pathname, parsed.searchParams)
    .then(({ status, body }) => {
      res.statusCode = status;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.setHeader("Cache-Control", "no-store");
      res.end(JSON.stringify(body));
    })
    .catch((err: unknown) => {
      res.statusCode = 500;
      res.setHeader("Content-Type", "application/json; charset=utf-8");
      res.end(
        JSON.stringify({
          error: err instanceof Error ? err.message : String(err),
        }),
      );
    });
}
