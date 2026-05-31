// ============================================================
// DESIGN: "Pulse" — Página de Agendamentos
// A tabela `agent.appointments` existe no banco, mas o agente
// ainda não grava agendamentos (Fase D — tools de escrita).
// Mostramos um estado informativo em vez de dados fictícios.
// ============================================================

import { CalendarClock } from "lucide-react";

export default function Appointments() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="font-display text-2xl font-bold text-foreground">
          Agendamentos
        </h1>
        <p className="text-sm text-muted-foreground">
          Gestão de agendamentos realizados pelo agente WhatsApp
        </p>
      </div>

      <div className="glass-card rounded-lg p-10 flex flex-col items-center justify-center text-center gap-3">
        <div className="p-3 rounded-xl bg-primary/10">
          <CalendarClock size={28} className="text-primary" />
        </div>
        <h3 className="font-display font-semibold text-foreground">
          Sem dados de agendamento ainda
        </h3>
        <p className="text-sm text-muted-foreground max-w-md">
          A tabela de agendamentos já existe no banco, mas o agente ainda não
          registra agendamentos automaticamente. Quando a escrita de
          agendamentos for habilitada no agente (Fase D), esta página passará a
          ser preenchida sozinha — sem nenhuma mudança no dashboard.
        </p>
      </div>
    </div>
  );
}
