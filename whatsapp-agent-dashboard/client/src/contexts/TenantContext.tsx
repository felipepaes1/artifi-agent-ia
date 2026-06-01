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
import { MOCK_TENANT_SLUG } from "@shared/const";
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
const MOCK_DEFAULT_KEY = "dashboard.mockDefault.v1";

export function TenantProvider({ children }: { children: ReactNode }) {
  const [tenants, setTenants] = useState<TenantDTO[]>([]);
  const [slug, setSlugState] = useState<string>(() =>
    localStorage.getItem(MOCK_DEFAULT_KEY) === "1"
      ? (localStorage.getItem(SLUG_KEY) ?? MOCK_TENANT_SLUG)
      : MOCK_TENANT_SLUG
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
        setSlugState((current) => {
          const shouldUseMockDefault =
            localStorage.getItem(MOCK_DEFAULT_KEY) !== "1";
          const fallback =
            res.items.find((t) => t.slug === MOCK_TENANT_SLUG)?.slug ??
            res.items[0]?.slug ??
            "";
          const next =
            shouldUseMockDefault && fallback === MOCK_TENANT_SLUG
              ? fallback
              : current && res.items.some((t) => t.slug === current)
                ? current
                : fallback;
          if (next) localStorage.setItem(SLUG_KEY, next);
          localStorage.setItem(MOCK_DEFAULT_KEY, "1");
          return next;
        });
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
