// ============================================================
// DESIGN: "Pulse" — Status badges com glow sutil
// ============================================================

interface StatusBadgeProps {
  status: string;
  size?: "sm" | "md";
}

const statusConfig: Record<
  string,
  { label: string; bgClass: string; textClass: string; dotClass: string }
> = {
  concluida: {
    label: "Concluída",
    bgClass: "bg-emerald-500/10",
    textClass: "text-emerald-400",
    dotClass: "bg-emerald-400",
  },
  em_andamento: {
    label: "Em Andamento",
    bgClass: "bg-cyan-500/10",
    textClass: "text-cyan-400",
    dotClass: "bg-cyan-400",
  },
  abandonada: {
    label: "Abandonada",
    bgClass: "bg-amber-500/10",
    textClass: "text-amber-400",
    dotClass: "bg-amber-400",
  },
  transferida: {
    label: "Transferida",
    bgClass: "bg-rose-500/10",
    textClass: "text-rose-400",
    dotClass: "bg-rose-400",
  },
  confirmado: {
    label: "Confirmado",
    bgClass: "bg-emerald-500/10",
    textClass: "text-emerald-400",
    dotClass: "bg-emerald-400",
  },
  pendente: {
    label: "Pendente",
    bgClass: "bg-amber-500/10",
    textClass: "text-amber-400",
    dotClass: "bg-amber-400",
  },
  cancelado: {
    label: "Cancelado",
    bgClass: "bg-rose-500/10",
    textClass: "text-rose-400",
    dotClass: "bg-rose-400",
  },
  realizado: {
    label: "Realizado",
    bgClass: "bg-cyan-500/10",
    textClass: "text-cyan-400",
    dotClass: "bg-cyan-400",
  },
  remarcado: {
    label: "Remarcado",
    bgClass: "bg-violet-500/10",
    textClass: "text-violet-400",
    dotClass: "bg-violet-400",
  },
};

export default function StatusBadge({ status, size = "sm" }: StatusBadgeProps) {
  const config = statusConfig[status] || {
    label: status,
    bgClass: "bg-muted",
    textClass: "text-muted-foreground",
    dotClass: "bg-muted-foreground",
  };

  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full font-medium ${config.bgClass} ${config.textClass} ${
        size === "sm" ? "px-2 py-0.5 text-[11px]" : "px-2.5 py-1 text-xs"
      }`}
    >
      <span
        className={`w-1.5 h-1.5 rounded-full ${config.dotClass} ${
          status === "em_andamento" ? "animate-pulse-dot" : ""
        }`}
      />
      {config.label}
    </span>
  );
}
