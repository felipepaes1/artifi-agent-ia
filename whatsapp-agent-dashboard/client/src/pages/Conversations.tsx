// ============================================================
// DESIGN: "Pulse" — Página de Conversas
// Lista real de conversas do cliente selecionado.
// ============================================================

import { useMemo, useState } from "react";
import { MessageSquare, Search, CheckCircle2, Loader2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import StatusBadge from "@/components/StatusBadge";
import AsyncState from "@/components/AsyncState";
import { useTenant } from "@/contexts/TenantContext";
import { useApi } from "@/hooks/useApi";
import { api } from "@/lib/api";

function formatDateTime(iso: string): string {
  const d = new Date(iso);
  return `${d.toLocaleDateString("pt-BR")} ${d.toLocaleTimeString("pt-BR", {
    hour: "2-digit",
    minute: "2-digit",
  })}`;
}

export default function Conversations() {
  const { slug } = useTenant();
  const [searchTerm, setSearchTerm] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");

  const { data, loading, error } = useApi(
    () => api.conversations(slug, 200),
    [slug],
    !!slug,
  );

  const items = data?.items ?? [];

  const filtered = useMemo(() => {
    return items.filter((conv) => {
      const matchSearch =
        !searchTerm ||
        conv.patientName.toLowerCase().includes(searchTerm.toLowerCase()) ||
        conv.phone.includes(searchTerm);
      const matchStatus =
        statusFilter === "all" || conv.status === statusFilter;
      return matchSearch && matchStatus;
    });
  }, [items, searchTerm, statusFilter]);

  const stats = useMemo(
    () => ({
      total: items.length,
      andamento: items.filter((c) => c.status === "em_andamento").length,
      concluidas: items.filter((c) => c.status === "concluida").length,
      mensagens: items.reduce((acc, c) => acc + c.messages, 0),
    }),
    [items],
  );

  return (
    <div className="space-y-6">
      <div>
        <h1 className="font-display text-2xl font-bold text-foreground">
          Conversas
        </h1>
        <p className="text-sm text-muted-foreground">
          Histórico de interações do agente WhatsApp
        </p>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {[
          { label: "Total", value: stats.total, icon: MessageSquare, color: "text-primary" },
          { label: "Em Andamento", value: stats.andamento, icon: Loader2, color: "text-cyan-400" },
          { label: "Concluídas", value: stats.concluidas, icon: CheckCircle2, color: "text-emerald-400" },
          { label: "Mensagens", value: stats.mensagens, icon: MessageSquare, color: "text-amber-400" },
        ].map((stat) => (
          <div key={stat.label} className="glass-card rounded-lg p-4">
            <div className="flex items-center gap-2 mb-2">
              <stat.icon size={14} className={stat.color} />
              <span className="text-xs text-muted-foreground">{stat.label}</span>
            </div>
            <p className="font-display text-xl font-bold text-foreground">
              {stat.value.toLocaleString("pt-BR")}
            </p>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="flex flex-col sm:flex-row gap-3">
        <div className="relative flex-1">
          <Search
            size={14}
            className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground"
          />
          <Input
            placeholder="Buscar por paciente ou telefone..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="pl-9 h-9 bg-secondary/50 border-border/50 text-sm"
          />
        </div>
        <Select value={statusFilter} onValueChange={setStatusFilter}>
          <SelectTrigger className="w-full sm:w-44 h-9 bg-secondary/50 border-border/50 text-sm">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Todos os Status</SelectItem>
            <SelectItem value="em_andamento">Em Andamento</SelectItem>
            <SelectItem value="concluida">Concluída</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {/* Table */}
      <div className="glass-card rounded-lg overflow-hidden">
        <AsyncState
          loading={loading}
          error={error}
          empty={items.length === 0}
          emptyLabel="Nenhuma conversa registrada para este cliente."
        >
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border/50 bg-secondary/30">
                  <th className="text-left py-3 px-4 text-muted-foreground font-medium text-xs">Paciente</th>
                  <th className="text-left py-3 px-4 text-muted-foreground font-medium text-xs">Status</th>
                  <th className="text-left py-3 px-4 text-muted-foreground font-medium text-xs">Início</th>
                  <th className="text-left py-3 px-4 text-muted-foreground font-medium text-xs">Última mensagem</th>
                  <th className="text-right py-3 px-4 text-muted-foreground font-medium text-xs">Msgs</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((conv, i) => (
                  <tr
                    key={conv.id}
                    className="border-b border-border/20 hover:bg-secondary/20 transition-colors animate-fade-in-up"
                    style={{ animationDelay: `${Math.min(i, 20) * 30}ms` }}
                  >
                    <td className="py-3 px-4">
                      <p className="font-medium text-foreground text-sm">{conv.patientName}</p>
                      <p className="text-muted-foreground/70 text-xs">{conv.phone}</p>
                    </td>
                    <td className="py-3 px-4">
                      <StatusBadge status={conv.status} />
                    </td>
                    <td className="py-3 px-4 text-muted-foreground text-xs">
                      {formatDateTime(conv.startedAt)}
                    </td>
                    <td className="py-3 px-4 text-muted-foreground text-xs">
                      {formatDateTime(conv.lastMessageAt)}
                    </td>
                    <td className="py-3 px-4 text-right font-display text-sm text-muted-foreground">
                      {conv.messages}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {filtered.length === 0 && (
            <div className="py-12 text-center text-muted-foreground text-sm">
              Nenhuma conversa encontrada com os filtros selecionados.
            </div>
          )}
        </AsyncState>
      </div>
    </div>
  );
}
