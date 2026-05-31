// ============================================================
// Estado global do cliente selecionado (tenant) e da janela de
// tempo (range em dias). Sem login: o seletor é livre. As escolhas
// ficam no localStorage.
// ============================================================

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import type { TenantDTO } from "@shared/api-types";
import { api } from "@/lib/api";

export const RANGE_OPTIONS = [7, 30, 90] as const;

interface TenantContextValue {
  tenants: TenantDTO[];
  slug: string;
  setSlug: (slug: string) => void;
  range: number;
  setRange: (range: number) => void;
  loading: boolean;
  error: string | null;
}

const TenantContext = createContext<TenantContextValue | null>(null);

const SLUG_KEY = "dashboard.tenant";
const RANGE_KEY = "dashboard.range";

export function TenantProvider({ children }: { children: ReactNode }) {
  const [tenants, setTenants] = useState<TenantDTO[]>([]);
  const [slug, setSlugState] = useState<string>(
    () => localStorage.getItem(SLUG_KEY) ?? "",
  );
  const [range, setRangeState] = useState<number>(() => {
    const stored = Number(localStorage.getItem(RANGE_KEY));
    return RANGE_OPTIONS.includes(stored as (typeof RANGE_OPTIONS)[number])
      ? stored
      : 30;
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    api
      .tenants()
      .then((res) => {
        if (!alive) return;
        setTenants(res.items);
        setSlugState((current) =>
          current && res.items.some((t) => t.slug === current)
            ? current
            : (res.items[0]?.slug ?? ""),
        );
        setError(res.items.length ? null : "Nenhum tenant encontrado no banco.");
      })
      .catch((err: unknown) => {
        if (alive) setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, []);

  const setSlug = (next: string) => {
    setSlugState(next);
    localStorage.setItem(SLUG_KEY, next);
  };
  const setRange = (next: number) => {
    setRangeState(next);
    localStorage.setItem(RANGE_KEY, String(next));
  };

  const value = useMemo<TenantContextValue>(
    () => ({ tenants, slug, setSlug, range, setRange, loading, error }),
    [tenants, slug, range, loading, error],
  );

  return (
    <TenantContext.Provider value={value}>{children}</TenantContext.Provider>
  );
}

export function useTenant(): TenantContextValue {
  const ctx = useContext(TenantContext);
  if (!ctx) throw new Error("useTenant precisa estar dentro de <TenantProvider>");
  return ctx;
}
