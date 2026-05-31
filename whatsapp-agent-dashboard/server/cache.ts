// ============================================================
// Cache em memória com TTL.
// Protege o banco do agente: leituras repetidas do dashboard
// são servidas da memória por ~60s em vez de bater no Postgres.
// ============================================================

import { CACHE_TTL_MS } from "./env";

interface Entry {
  value: unknown;
  expires: number;
}

const store = new Map<string, Entry>();

export async function cached<T>(
  key: string,
  ttlMs: number,
  producer: () => Promise<T>,
): Promise<T> {
  const now = Date.now();
  const hit = store.get(key);
  if (hit && hit.expires > now) {
    return hit.value as T;
  }
  const value = await producer();
  store.set(key, { value, expires: now + ttlMs });
  return value;
}

export { CACHE_TTL_MS };
