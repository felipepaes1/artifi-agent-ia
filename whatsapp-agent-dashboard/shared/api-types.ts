// ============================================================
// DTOs compartilhados entre o servidor (server/api) e o cliente
// (client/src). Toda a comunicação do dashboard passa por aqui.
// ============================================================

export interface TenantDTO {
  slug: string;
  label: string;
}

export interface TenantsDTO {
  items: TenantDTO[];
}

/** Compatível estruturalmente com o KPICard. */
export interface KpiDTO {
  label: string;
  value: number;
  previousValue: number;
  format: "number" | "percent" | "time";
  trend: "up" | "down" | "stable";
  description: string;
}

/** Números crus do período atual — para a página de Desempenho. */
export interface OverviewRaw {
  messages: number;
  conversations: number;
  contacts: number;
  assistant: number;
  user: number;
  responseSec: number | null;
}

export interface OverviewDTO {
  range: number;
  generatedAt: string;
  kpis: KpiDTO[];
  raw: OverviewRaw;
}

export interface DailyPointDTO {
  date: string; // YYYY-MM-DD (horário de Brasília)
  conversations: number;
  messages: number;
}

export interface HourlyPointDTO {
  hour: string; // "HH:00"
  messages: number;
}

export interface TimeseriesDTO {
  range: number;
  daily: DailyPointDTO[];
  hourly: HourlyPointDTO[];
}

export interface ConversationDTO {
  id: string;
  patientName: string;
  phone: string;
  status: "em_andamento" | "concluida";
  startedAt: string;
  lastMessageAt: string;
  messages: number;
}

export interface ConversationsDTO {
  items: ConversationDTO[];
  total: number;
}

export interface MessageDTO {
  id: string;
  role: "user" | "assistant" | string;
  content: string;
  createdAt: string;
  messageType: string | null;
}

export interface MessagesDTO {
  items: MessageDTO[];
}

export interface ApiErrorDTO {
  error: string;
}
