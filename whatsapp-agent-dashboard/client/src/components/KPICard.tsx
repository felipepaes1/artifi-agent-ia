// ============================================================
// DESIGN: "Pulse" — KPI Card com glow e animação
// Glass card com números grandes estilo painel de monitoramento
// ============================================================

import { useEffect, useState, useRef } from "react";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import type { KpiDTO } from "@shared/api-types";

function useCountUp(end: number, duration: number = 1200) {
  const [value, setValue] = useState(0);
  const startTime = useRef<number | null>(null);
  const rafId = useRef<number>(0);

  useEffect(() => {
    startTime.current = null;
    const animate = (timestamp: number) => {
      if (!startTime.current) startTime.current = timestamp;
      const progress = Math.min((timestamp - startTime.current) / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setValue(end * eased);
      if (progress < 1) {
        rafId.current = requestAnimationFrame(animate);
      }
    };
    rafId.current = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(rafId.current);
  }, [end, duration]);

  return value;
}

function formatValue(value: number, format: KpiDTO["format"]): string {
  switch (format) {
    case "percent":
      return `${value.toFixed(1)}%`;
    case "time":
      return `${value.toFixed(1)}min`;
    default:
      return value >= 10
        ? Math.round(value).toLocaleString("pt-BR")
        : value.toFixed(1);
  }
}

function getChangePercent(current: number, previous: number): number {
  if (previous === 0) return 0;
  return ((current - previous) / previous) * 100;
}

interface KPICardProps {
  data: KpiDTO;
  icon: React.ElementType;
  glowClass?: string;
  delay?: number;
}

export default function KPICard({
  data,
  icon: Icon,
  glowClass = "glow-cyan",
  delay = 0,
}: KPICardProps) {
  const animatedValue = useCountUp(data.value, 1200);
  const change = getChangePercent(data.value, data.previousValue);
  const isPositive =
    data.label === "Tempo Médio de Resposta"
      ? data.trend === "down"
      : data.trend === "up";

  return (
    <div
      className={`glass-card rounded-lg p-5 ${glowClass} animate-fade-in-up`}
      style={{ animationDelay: `${delay}ms` }}
    >
      <div className="flex items-start justify-between mb-4">
        <div className="p-2 rounded-md bg-primary/10">
          <Icon size={18} className="text-primary" />
        </div>
        <div
          className={`flex items-center gap-1 text-xs font-medium px-2 py-1 rounded-full ${
            isPositive
              ? "bg-emerald-500/10 text-emerald-400"
              : data.trend === "stable"
                ? "bg-muted text-muted-foreground"
                : "bg-rose-500/10 text-rose-400"
          }`}
        >
          {data.trend === "up" ? (
            <TrendingUp size={12} />
          ) : data.trend === "down" ? (
            <TrendingDown size={12} />
          ) : (
            <Minus size={12} />
          )}
          <span>{Math.abs(change).toFixed(1)}%</span>
        </div>
      </div>

      <p className="font-display text-3xl font-bold text-foreground tracking-tight leading-none mb-1">
        {formatValue(animatedValue, data.format)}
      </p>
      <p className="text-sm text-muted-foreground">{data.label}</p>
      <p className="text-[11px] text-muted-foreground/60 mt-1">
        {data.description}
      </p>
    </div>
  );
}
