import type {
  ConversationsDTO,
  DailyPointDTO,
  HourlyPointDTO,
  KpiDTO,
  MessagesDTO,
  OverviewDTO,
  TenantDTO,
  TimeseriesDTO,
} from "../../shared/api-types";
import { MOCK_TENANT_SLUG } from "../../shared/const";

const DAY_MS = 86_400_000;
const HOUR_MS = 3_600_000;
const MINUTE_MS = 60_000;
const BRT_OFFSET_MS = 3 * HOUR_MS;

export const MOCK_TENANT = {
  slug: MOCK_TENANT_SLUG,
  label: "Clínica + Saúde",
} satisfies TenantDTO;

export const isMockTenantSlug = (slug: string): boolean =>
  slug === MOCK_TENANT_SLUG;

interface Summary {
  messages: number;
  conversations: number;
  contacts: number;
  assistant: number;
  user: number;
  responseSec: number;
}

interface MockConversationTemplate {
  id: string;
  patientName: string;
  phone: string;
  status: "em_andamento" | "concluida";
  startedHoursAgo: number;
  lastMinutesAgo: number;
  messages: number;
  topic: string;
}

const conversationTemplates: MockConversationTemplate[] = [
  {
    id: "mock-conv-ana-paula",
    patientName: "Ana Paula Ribeiro",
    phone: "+55 11 98834-2740",
    status: "em_andamento",
    startedHoursAgo: 2.5,
    lastMinutesAgo: 9,
    messages: 18,
    topic: "confirmar retorno com a dermatologista",
  },
  {
    id: "mock-conv-marcos-lima",
    patientName: "Marcos Lima",
    phone: "+55 11 97642-1180",
    status: "concluida",
    startedHoursAgo: 5,
    lastMinutesAgo: 37,
    messages: 14,
    topic: "remarcar exame de sangue",
  },
  {
    id: "mock-conv-helena-costa",
    patientName: "Helena Costa",
    phone: "+55 11 99102-6032",
    status: "em_andamento",
    startedHoursAgo: 7,
    lastMinutesAgo: 54,
    messages: 22,
    topic: "tirar dúvidas sobre preparo para ultrassom",
  },
  {
    id: "mock-conv-roberto-nunes",
    patientName: "Roberto Nunes",
    phone: "+55 11 98215-4499",
    status: "concluida",
    startedHoursAgo: 10,
    lastMinutesAgo: 86,
    messages: 11,
    topic: "enviar endereço e estacionamento da unidade",
  },
  {
    id: "mock-conv-camila-moura",
    patientName: "Camila Moura",
    phone: "+55 11 97455-8031",
    status: "concluida",
    startedHoursAgo: 19,
    lastMinutesAgo: 210,
    messages: 16,
    topic: "agendar consulta de primeira avaliação",
  },
  {
    id: "mock-conv-lucas-andrade",
    patientName: "Lucas Andrade",
    phone: "+55 11 99341-7654",
    status: "em_andamento",
    startedHoursAgo: 23,
    lastMinutesAgo: 340,
    messages: 27,
    topic: "validar cobertura do convênio",
  },
  {
    id: "mock-conv-beatriz-santos",
    patientName: "Beatriz Santos",
    phone: "+55 11 98173-2205",
    status: "concluida",
    startedHoursAgo: 30,
    lastMinutesAgo: 480,
    messages: 9,
    topic: "confirmar horário da consulta pediátrica",
  },
  {
    id: "mock-conv-eduardo-pires",
    patientName: "Eduardo Pires",
    phone: "+55 11 97980-6637",
    status: "concluida",
    startedHoursAgo: 36,
    lastMinutesAgo: 620,
    messages: 20,
    topic: "solicitar segunda via de pedido médico",
  },
  {
    id: "mock-conv-sofia-almeida",
    patientName: "Sofia Almeida",
    phone: "+55 11 99622-4108",
    status: "em_andamento",
    startedHoursAgo: 44,
    lastMinutesAgo: 780,
    messages: 24,
    topic: "orientações antes de consulta cardiológica",
  },
  {
    id: "mock-conv-paulo-ferraz",
    patientName: "Paulo Ferraz",
    phone: "+55 11 98731-5542",
    status: "concluida",
    startedHoursAgo: 52,
    lastMinutesAgo: 930,
    messages: 13,
    topic: "confirmar pagamento e recibo",
  },
  {
    id: "mock-conv-julia-martins",
    patientName: "Julia Martins",
    phone: "+55 11 97041-3368",
    status: "concluida",
    startedHoursAgo: 60,
    lastMinutesAgo: 1120,
    messages: 17,
    topic: "reagendar retorno pós-exame",
  },
  {
    id: "mock-conv-ricardo-teixeira",
    patientName: "Ricardo Teixeira",
    phone: "+55 11 98994-0311",
    status: "em_andamento",
    startedHoursAgo: 70,
    lastMinutesAgo: 1310,
    messages: 21,
    topic: "orientação sobre documentos para check-up",
  },
];

function brtDayKey(epochMs: number): string {
  return new Date(epochMs - BRT_OFFSET_MS).toISOString().slice(0, 10);
}

function hashString(value: string): number {
  let hash = 0;
  for (let i = 0; i < value.length; i++) {
    hash = (hash * 31 + value.charCodeAt(i)) % 10_000;
  }
  return hash;
}

function dayPoint(
  date: string,
  index: number,
  range: number,
  scale = 1
): DailyPointDTO {
  const seed = hashString(date);
  const weekday = new Date(`${date}T12:00:00.000Z`).getUTCDay();
  const businessBoost = weekday === 0 ? -5 : weekday === 6 ? -2 : 4;
  const trend = range > 1 ? (index / (range - 1)) * 8 : 4;
  const wave = Math.round(4 * Math.sin((seed % 31) / 5));
  const baseConversations = 14 + businessBoost + (seed % 9) + wave + trend;
  const conversations = Math.max(5, Math.round(baseConversations * scale));
  const messagesPerConversation =
    8 + (seed % 5) + (weekday >= 1 && weekday <= 5 ? 2 : 0);
  const messages = Math.max(
    conversations,
    Math.round((conversations * messagesPerConversation + (seed % 17)) * scale)
  );

  return { date, conversations, messages };
}

function buildDaily(
  range: number,
  now: number,
  offsetDays = 0,
  scale = 1
): DailyPointDTO[] {
  const points: DailyPointDTO[] = [];
  for (let i = range - 1; i >= 0; i--) {
    const date = brtDayKey(now - (i + offsetDays) * DAY_MS);
    points.push(dayPoint(date, range - 1 - i, range, scale));
  }
  return points;
}

function summarizeDaily(points: DailyPointDTO[], responseSec: number): Summary {
  const messages = points.reduce((acc, p) => acc + p.messages, 0);
  const conversations = points.reduce((acc, p) => acc + p.conversations, 0);
  const assistant = Math.round(messages * 0.57);

  return {
    messages,
    conversations,
    contacts: Math.round(conversations * 0.72),
    assistant,
    user: messages - assistant,
    responseSec,
  };
}

function kpi(
  label: string,
  description: string,
  format: KpiDTO["format"],
  current: number,
  previous: number
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

function buildHourly(totalMessages: number): HourlyPointDTO[] {
  const weights = Array.from({ length: 24 }, (_, h) => {
    const morning = Math.exp(-((h - 9) ** 2) / 7);
    const afternoon = Math.exp(-((h - 14) ** 2) / 9);
    const evening = Math.exp(-((h - 18) ** 2) / 6);
    const base = h >= 7 && h <= 21 ? 0.2 : 0.04;
    return base + morning * 1.2 + afternoon + evening * 0.7;
  });
  const totalWeight = weights.reduce((acc, w) => acc + w, 0);

  return weights.map((weight, h) => ({
    hour: `${String(h).padStart(2, "0")}:00`,
    messages: Math.round((totalMessages * weight) / totalWeight),
  }));
}

export function mockOverview(range: number, now = Date.now()): OverviewDTO {
  const current = summarizeDaily(buildDaily(range, now), 74);
  const previous = summarizeDaily(buildDaily(range, now, range, 0.86), 96);

  const kpis: KpiDTO[] = [
    kpi(
      "Total de Conversas",
      `Conversas com atividade nos últimos ${range} dias`,
      "number",
      current.conversations,
      previous.conversations
    ),
    kpi(
      "Total de Mensagens",
      "Mensagens trocadas no período",
      "number",
      current.messages,
      previous.messages
    ),
    kpi(
      "Pacientes Atendidos",
      "Contatos distintos atendidos no período",
      "number",
      current.contacts,
      previous.contacts
    ),
    kpi(
      "Mensagens por Conversa",
      "Média de mensagens por conversa",
      "number",
      ratio(current.messages, current.conversations),
      ratio(previous.messages, previous.conversations)
    ),
    kpi(
      "Mensagens do Agente",
      "Respostas enviadas pelo agente no período",
      "number",
      current.assistant,
      previous.assistant
    ),
    kpi(
      "Mensagens do Paciente",
      "Mensagens recebidas dos pacientes no período",
      "number",
      current.user,
      previous.user
    ),
  ];

  return {
    range,
    generatedAt: new Date(now).toISOString(),
    kpis,
    raw: current,
  };
}

export function mockTimeseries(range: number, now = Date.now()): TimeseriesDTO {
  const daily = buildDaily(range, now);
  const totalMessages = daily.reduce((acc, p) => acc + p.messages, 0);

  return {
    range,
    daily,
    hourly: buildHourly(totalMessages),
  };
}

export function mockConversations(
  limit: number,
  now = Date.now()
): ConversationsDTO {
  const items = conversationTemplates
    .map(item => ({
      id: item.id,
      patientName: item.patientName,
      phone: item.phone,
      status: item.status,
      startedAt: new Date(now - item.startedHoursAgo * HOUR_MS).toISOString(),
      lastMessageAt: new Date(
        now - item.lastMinutesAgo * MINUTE_MS
      ).toISOString(),
      messages: item.messages,
    }))
    .sort((a, b) => Date.parse(b.lastMessageAt) - Date.parse(a.lastMessageAt))
    .slice(0, limit);

  return { items, total: items.length };
}

export function mockMessages(
  conversationId: string,
  now = Date.now()
): MessagesDTO {
  const conversation = conversationTemplates.find(c => c.id === conversationId);
  if (!conversation) return { items: [] };

  const start = now - conversation.startedHoursAgo * HOUR_MS;
  const messageTexts = [
    {
      role: "user",
      content: `Olá, preciso de ajuda para ${conversation.topic}.`,
    },
    {
      role: "assistant",
      content:
        "Olá! Posso ajudar por aqui. Vou confirmar alguns dados para localizar seu cadastro.",
    },
    {
      role: "user",
      content:
        "Claro. Meu nome completo e telefone são os mesmos deste WhatsApp.",
    },
    {
      role: "assistant",
      content:
        "Encontrei seu cadastro. Temos horários disponíveis hoje às 15h40 e amanhã às 09h20.",
    },
    {
      role: "user",
      content: "Prefiro amanhã às 09h20.",
    },
    {
      role: "assistant",
      content:
        "Perfeito. Deixei a solicitação registrada e enviei as orientações da clínica por aqui.",
    },
  ];

  return {
    items: messageTexts.map((message, index) => ({
      id: `${conversation.id}-msg-${index + 1}`,
      role: message.role,
      content: message.content,
      createdAt: new Date(start + index * 4 * MINUTE_MS).toISOString(),
      messageType: "text",
    })),
  };
}
