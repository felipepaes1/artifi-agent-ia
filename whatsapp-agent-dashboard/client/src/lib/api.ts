// ============================================================
// Cliente HTTP do dashboard — fala com a API de leitura (/api/*)
// servida pelo próprio servidor do dashboard (Express em produção,
// plugin do Vite em dev). Nunca acessa o Supabase direto.
// ============================================================

import type {
  ConversationsDTO,
  MessagesDTO,
  OverviewDTO,
  TenantsDTO,
  TimeseriesDTO,
} from "@shared/api-types";

async function get<T>(path: string): Promise<T> {
  let res: Response;
  try {
    res = await fetch(path);
  } catch {
    throw new Error("Falha de rede ao consultar a API.");
  }
  const body = (await res.json().catch(() => null)) as
    | (T & { error?: string })
    | null;
  if (!res.ok || !body) {
    throw new Error(body?.error || `Erro ${res.status} ao consultar ${path}`);
  }
  return body as T;
}

const q = (params: Record<string, string | number>): string =>
  Object.entries(params)
    .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`)
    .join("&");

export const api = {
  tenants: () => get<TenantsDTO>("/api/tenants"),

  overview: (tenant: string, range: number) =>
    get<OverviewDTO>(`/api/overview?${q({ tenant, range })}`),

  timeseries: (tenant: string, range: number) =>
    get<TimeseriesDTO>(`/api/timeseries?${q({ tenant, range })}`),

  conversations: (tenant: string, limit = 80) =>
    get<ConversationsDTO>(`/api/conversations?${q({ tenant, limit })}`),

  messages: (conversationId: string) =>
    get<MessagesDTO>(`/api/messages?${q({ conversationId })}`),
};
