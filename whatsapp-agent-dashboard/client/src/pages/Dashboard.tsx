// ============================================================
// DESIGN: "Pulse" — Visão Geral do Dashboard
// Dados reais do banco do agente (schema agent.*), por cliente.
// ============================================================

import {
  MessageSquare,
  MessagesSquare,
  Users,
  Hash,
  Bot,
  User,
  ArrowRight,
} from "lucide-react";
import { Link } from "wouter";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
} from "recharts";
import KPICard from "@/components/KPICard";
import StatusBadge from "@/components/StatusBadge";
import AsyncState from "@/components/AsyncState";
import { useTenant } from "@/contexts/TenantContext";
import { useApi } from "@/hooks/useApi";
import { api } from "@/lib/api";

const HERO_URL =
  "https://d2xsxph8kpxj0f.cloudfront.net/310519663114833595/XKGboP74ak8U9ZQCLu7t87/hero-dashboard-hT8yUCeigQspCZfFgMbm2Q.webp";

const kpiIcons = [MessageSquare, MessagesSquare, Users, Hash, Bot, User];
const kpiGlows = [
  "glow-cyan",
  "glow-cyan",
  "glow-success",
  "glow-amber",
  "glow-amber",
  "glow-success",
];

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

function formatDateTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export default function Dashboard() {
  const { slug, range, tenants } = useTenant();
  const tenantLabel = tenants.find((t) => t.slug === slug)?.label ?? "";

  const overview = useApi(
    () => api.overview(slug, range),
    [slug, range],
    !!slug,
  );
  const series = useApi(
    () => api.timeseries(slug, range),
    [slug, range],
    !!slug,
  );
  const recent = useApi(
    () => api.conversations(slug, 8),
    [slug],
    !!slug,
  );

  return (
    <div className="space-y-6">
      {/* Hero banner */}
      <div
        className="relative rounded-xl overflow-hidden h-28 sm:h-36 flex items-end"
        style={{
          backgroundImage: `url(${HERO_URL})`,
          backgroundSize: "cover",
          backgroundPosition: "center",
        }}
      >
        <div className="absolute inset-0 bg-gradient-to-t from-background via-background/60 to-transparent" />
        <div className="relative px-4 sm:px-6 pb-3 sm:pb-4">
          <h1 className="font-display text-xl sm:text-2xl font-bold text-foreground">
            Visão Geral{tenantLabel ? ` — ${tenantLabel}` : ""}
          </h1>
          <p className="text-sm text-muted-foreground">
            Métricas de atendimento — últimos {range} dias
          </p>
        </div>
      </div>

      {/* KPI Grid */}
      <AsyncState loading={overview.loading} error={overview.error}>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 lg:gap-4">
          {overview.data?.kpis.map((kpi, i) => (
            <KPICard
              key={kpi.label}
              data={kpi}
              icon={kpiIcons[i % kpiIcons.length]}
              glowClass={kpiGlows[i % kpiGlows.length]}
              delay={i * 80}
            />
          ))}
        </div>
      </AsyncState>

      {/* Area chart - Conversas e Mensagens */}
      <div className="glass-card rounded-lg p-5">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="font-display font-semibold text-foreground">
              Conversas e Mensagens
            </h3>
            <p className="text-xs text-muted-foreground">
              Volume diário — últimos {range} dias
            </p>
          </div>
          <div className="flex items-center gap-4 text-xs">
            <span className="flex items-center gap-1.5">
              <span className="w-2.5 h-2.5 rounded-full bg-[oklch(0.82_0.15_195)]" />
              Conversas
            </span>
            <span className="flex items-center gap-1.5">
              <span className="w-2.5 h-2.5 rounded-full bg-[oklch(0.72_0.17_155)]" />
              Mensagens
            </span>
          </div>
        </div>
        <AsyncState loading={series.loading} error={series.error}>
          <ResponsiveContainer width="100%" height={260}>
            <AreaChart data={series.data?.daily ?? []}>
              <defs>
                <linearGradient id="gradCyan" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="oklch(0.82 0.15 195)" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="oklch(0.82 0.15 195)" stopOpacity={0} />
                </linearGradient>
                <linearGradient id="gradGreen" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="oklch(0.72 0.17 155)" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="oklch(0.72 0.17 155)" stopOpacity={0} />
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
              <Area type="monotone" dataKey="messages" name="Mensagens" stroke="oklch(0.72 0.17 155)" fill="url(#gradGreen)" strokeWidth={2} />
              <Area type="monotone" dataKey="conversations" name="Conversas" stroke="oklch(0.82 0.15 195)" fill="url(#gradCyan)" strokeWidth={2} />
            </AreaChart>
          </ResponsiveContainer>
        </AsyncState>
      </div>

      {/* Hourly + Recent conversations */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <div className="glass-card rounded-lg p-5">
          <h3 className="font-display font-semibold text-foreground mb-1">
            Distribuição por Horário
          </h3>
          <p className="text-xs text-muted-foreground mb-4">
            Mensagens ao longo do dia (horário de Brasília)
          </p>
          <AsyncState loading={series.loading} error={series.error}>
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={series.data?.hourly ?? []}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                <XAxis
                  dataKey="hour"
                  interval={2}
                  tick={{ fill: "oklch(0.65 0.02 250)", fontSize: 10 }}
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
                <Bar dataKey="messages" name="Mensagens" fill="oklch(0.82 0.15 195)" radius={[3, 3, 0, 0]} maxBarSize={20} />
              </BarChart>
            </ResponsiveContainer>
          </AsyncState>
        </div>

        <div className="lg:col-span-2 glass-card rounded-lg p-5">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="font-display font-semibold text-foreground">
                Conversas Recentes
              </h3>
              <p className="text-xs text-muted-foreground">
                Últimas interações do agente
              </p>
            </div>
            <Link href="/conversas">
              <span className="text-xs text-primary hover:text-primary/80 flex items-center gap-1 transition-colors">
                Ver todas <ArrowRight size={12} />
              </span>
            </Link>
          </div>
          <AsyncState
            loading={recent.loading}
            error={recent.error}
            empty={(recent.data?.items.length ?? 0) === 0}
            emptyLabel="Nenhuma conversa registrada ainda."
          >
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border/50">
                    <th className="text-left py-2 px-2 text-muted-foreground font-medium">Paciente</th>
                    <th className="text-left py-2 px-2 text-muted-foreground font-medium">Status</th>
                    <th className="text-left py-2 px-2 text-muted-foreground font-medium hidden sm:table-cell">Última mensagem</th>
                    <th className="text-right py-2 px-2 text-muted-foreground font-medium">Msgs</th>
                  </tr>
                </thead>
                <tbody>
                  {recent.data?.items.slice(0, 6).map((conv) => (
                    <tr
                      key={conv.id}
                      className="border-b border-border/30 hover:bg-secondary/30 transition-colors"
                    >
                      <td className="py-2.5 px-2">
                        <p className="font-medium text-foreground">{conv.patientName}</p>
                        <p className="text-muted-foreground/70 text-[10px]">{conv.phone}</p>
                      </td>
                      <td className="py-2.5 px-2">
                        <StatusBadge status={conv.status} />
                      </td>
                      <td className="py-2.5 px-2 text-muted-foreground hidden sm:table-cell">
                        {formatDateTime(conv.lastMessageAt)}
                      </td>
                      <td className="py-2.5 px-2 text-right font-display text-muted-foreground">
                        {conv.messages}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </AsyncState>
        </div>
      </div>
    </div>
  );
}
