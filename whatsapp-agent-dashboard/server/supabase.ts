// ============================================================
// Cliente Supabase (service-role) — exclusivo do servidor.
// Aponta para o schema `agent`, o mesmo que o agente escreve.
// Só faz SELECT: o dashboard nunca grava no banco.
// ============================================================

import { createClient, type SupabaseClient } from "@supabase/supabase-js";

import {
  DB_SCHEMA,
  SUPABASE_SERVICE_ROLE_KEY,
  SUPABASE_URL,
  assertSupabaseConfig,
} from "./env";

let client: SupabaseClient | null = null;

export function db(): SupabaseClient {
  if (!client) {
    assertSupabaseConfig();
    client = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
      db: { schema: DB_SCHEMA },
    }) as unknown as SupabaseClient;
  }
  return client;
}
