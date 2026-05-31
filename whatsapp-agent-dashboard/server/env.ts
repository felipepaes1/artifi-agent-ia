// ============================================================
// Configuração de ambiente do servidor do dashboard.
// As chaves do Supabase ficam SÓ aqui (backend) — nunca no bundle
// do navegador. O .env é lido do diretório de execução (cwd),
// que é a raiz do dashboard tanto em `vite` (dev) quanto em
// `node dist/index.js` (produção).
// ============================================================

import dotenv from "dotenv";

dotenv.config();

export const SUPABASE_URL = (process.env.SUPABASE_URL ?? "").trim();

export const SUPABASE_SERVICE_ROLE_KEY = (
  process.env.SUPABASE_SERVICE_ROLE_KEY ??
  process.env.SUPABASE_KEY ??
  ""
).trim();

export const DB_SCHEMA = (process.env.DASHBOARD_DB_SCHEMA ?? "agent").trim();

export const CACHE_TTL_MS = Number(process.env.DASHBOARD_CACHE_TTL_MS ?? 60_000);

/** Lança erro claro se faltar configuração. Chamado de forma preguiçosa,
 *  na primeira query — assim o servidor sobe mesmo sem .env. */
export function assertSupabaseConfig(): void {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error(
      "Supabase não configurado. Defina SUPABASE_URL e " +
        "SUPABASE_SERVICE_ROLE_KEY em whatsapp-agent-dashboard/.env " +
        "(use .env.example como base).",
    );
  }
}
