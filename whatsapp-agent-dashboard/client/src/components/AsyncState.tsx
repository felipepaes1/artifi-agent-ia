// ============================================================
// Estados de carregamento / erro / vazio reaproveitados pelas
// páginas que consomem a API.
// ============================================================

import { AlertCircle, Inbox, Loader2 } from "lucide-react";
import type { ReactNode } from "react";

interface AsyncStateProps {
  loading: boolean;
  error: string | null;
  empty?: boolean;
  emptyLabel?: string;
  children: ReactNode;
}

export default function AsyncState({
  loading,
  error,
  empty = false,
  emptyLabel = "Sem dados para o período selecionado.",
  children,
}: AsyncStateProps) {
  if (loading) {
    return (
      <div className="flex items-center justify-center gap-2 py-16 text-muted-foreground text-sm">
        <Loader2 size={16} className="animate-spin" />
        Carregando dados…
      </div>
    );
  }
  if (error) {
    return (
      <div className="flex flex-col items-center justify-center gap-2 py-16 text-rose-400 text-sm text-center px-4">
        <AlertCircle size={20} />
        <span>{error}</span>
      </div>
    );
  }
  if (empty) {
    return (
      <div className="flex flex-col items-center justify-center gap-2 py-16 text-muted-foreground text-sm">
        <Inbox size={20} />
        <span>{emptyLabel}</span>
      </div>
    );
  }
  return <>{children}</>;
}
