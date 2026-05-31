// ============================================================
// DESIGN: "Pulse" — Página de Desempenho do Agente
// Métricas reais derivadas das mensagens trocadas no período.
// ============================================================

import {
  MessageSquare,
  Bot,
  User,
  Clock,
  MessagesSquare,
  Users,
} from "lucide-react";
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  AreaChart,
  Area,
} from "recharts";
import AsyncState from "@/components/AsyncState";
import { useTenant } from "@/contexts/TenantContext";
import { useApi } from "@/hooks/useApi";
import { api } from "@/lib/api";

const CustomTooltip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="glass-card rounded-lg px-3 py-2 text-xs shadow-lg border border-border/50">
      <p className="text-muted-foreground mb-1">{label}</p>
      {payload.map((entry: any, i: number) => (
        <p key={i} style={{ color: entry.color }} className="font-medium">
          {entry.name}: {entry.value}
        </p>
      ))}
    </div>
  );
};

interface MetricCardProps {
  icon: React.ElementType;
  label: string;
  value: string;
  subtext: string;
  color: string;
  bgColor: string;
}

function MetricCard({
  icon: Icon,
  label,
  value,
  subtext,
  color,
  bgColor,
}: MetricCardProps) {
  return (
    <div className="glass-card rounded-lg p-4">
      <div className="flex items-center gap-3">
        <div className={`p-2.5 rounded-lg ${bgColor}`}>
          <Icon size={18} className={color} />
        </div>
        <div className="flex-1">
          <p className="text-xs text-muted-foreground">{label}</p>
          <p className="font-display text-2xl font-bold text-foreground leading-tight">
            {value}
          </p>
          <p className="text-[11px] text-muted-foreground/60">{subtext}</p>
        </div>
      </div>
    </div>
  );
}

const num = (n: number) => n.toLocaleString("pt-BR");

export default function Performance() {
  const { slug, range } = useTenant();

  const overview = useApi(() => api.overview(slug, range), [slug, range], !!slug);
  const series = useApi(() => api.timeseries(slug, range), [slug, range], !!slug);

  const raw = overview.data?.raw;
  const msgsPerConv =
    raw && raw.conversations > 0
      ? (raw.messages / raw.conversations).toFixed(1)
      : "0";
  const responseMin =
    raw && raw.responseSec != null
      ? `${(raw.responseSec / 60).toFixed(1)}min`
      : "—";
  const totalRoleMsgs = raw ? raw.assistant + raw.user : 0;
  const agentPct = totalRoleMsgs > 0 ? (raw!.assistant / totalRoleMsgs) * 100 : 0;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="font-display text-2xl font-bold text-foreground">
          Desempenho do Agente
        </h1>
        <p className="text-sm text-muted-foreground">
          Análise de volume e tempo de resposta — últimos {range} dias
        </p>
      </div>

      {/* Metric cards */}
      <AsyncState loading={overview.loading} error={overview.error}>
        {raw && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <MetricCard
              icon={MessageSquare}
              label="Total de Mensagens"
              value={num(raw.messages)}
              subtext={`~${msgsPerConv} msgs/conversa`}
              color="text-primary"
              bgColor="bg-primary/10"
            />
            <MetricCard
              icon={MessagesSquare}
              label="Conversas"
              value={num(raw.conversations)}
              subtext="Conversas com atividade no período"
              color="text-cyan-400"
              bgColor="bg-cyan-500/10"
            />
            <MetricCard
              icon={Users}
              label="Pacientes Atendidos"
              value={num(raw.contacts)}
              subtext="Contatos distintos no período"
              color="text-emerald-400"
              bgColor="bg-emerald-500/10"
            />
            <MetricCard
              icon={Bot}
              label="Mensagens do Agente"
              value={num(raw.assistant)}
              subtext="Respostas enviadas pelo agente"
              color="text-violet-400"
              bgColor="bg-violet-500/10"
            />
            <MetricCard
              icon={User}
              label="Mensagens do Paciente"
              value={num(raw.user)}
              subtext="Mensagens recebidas dos pacientes"
              color="text-amber-400"
              bgColor="bg-amber-500/10"
            />
            <MetricCard
              icon={Clock}
              label="Tempo Médio de Resposta"
              value={responseMin}
              subtext="Tempo do agente responder (aprox.)"
              color="text-rose-400"
              bgColor="bg-rose-500/10"
            />
          </div>
        )}
      </AsyncState>

      {/* Agente vs Paciente */}
      <AsyncState loading={overview.loading} error={overview.error}>
        {raw && (
          <div className="glass-card rounded-lg p-5">
            <h3 className="font-display font-semibold text-foreground mb-1">
              Equilíbrio da Conversa
            </h3>
            <p className="text-xs text-muted-foreground mb-4">
              Proporção de mensagens entre agente e paciente
            </p>
            <div className="h-3 rounded-full bg-secondary overflow-hidden flex">
              <div
                className="h-full bg-[oklch(0.72_0.19_300)]"
                style={{ width: `${agentPct}%` }}
              />
              <div
                className="h-full bg-[oklch(0.82_0.16_85)]"
                style={{ width: `${100 - agentPct}%` }}
              />
            </div>
            <div className="flex items-center justify-between mt-2 text-xs">
              <span className="flex items-center gap-1.5 text-muted-foreground">
                <span className="w-2 h-2 rounded-full bg-[oklch(0.72_0.19_300)]" />
                Agente — {num(raw.assistant)} ({agentPct.toFixed(0)}%)
              </span>
              <span className="flex items-center gap-1.5 text-muted-foreground">
                Paciente — {num(raw.user)} ({(100 - agentPct).toFixed(0)}%)
                <span className="w-2 h-2 rounded-full bg-[oklch(0.82_0.16_85)]" />
              </span>
            </div>
          </div>
        )}
      </AsyncState>

      {/* Daily volume */}
      <div className="glass-card rounded-lg p-5">
        <h3 className="font-display font-semibold text-foreground mb-1">
          Volume de Mensagens
        </h3>
        <p className="text-xs text-muted-foreground mb-4">
          Mensagens trocadas por dia nos últimos {range} dias
        </p>
        <AsyncState loading={series.loading} error={series.error}>
          <ResponsiveContainer width="100%" height={240}>
            <AreaChart data={series.data?.daily ?? []}>
              <defs>
                <linearGradient id="gradPerf" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="oklch(0.82 0.15 195)" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="oklch(0.82 0.15 195)" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis
                dataKey="date"
                tickFormatter={(v) => {
                  const d = new Date(`${v}T00:00:00`);
                  return `${d.getDate()}/${d.getMonth() + 1}`;
                }}
                tick={{ fill: "oklch(0.65 0.02 250)", fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: "oklch(0.65 0.02 250)", fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                width={35}
              />
              <RechartsTooltip content={<CustomTooltip />} />
              <Area
                type="monotone"
                dataKey="messages"
                name="Mensagens"
                stroke="oklch(0.82 0.15 195)"
                fill="url(#gradPerf)"
                strokeWidth={2}
              />
            </AreaChart>
          </ResponsiveContainer>
        </AsyncState>
      </div>
    </div>
  );
}
