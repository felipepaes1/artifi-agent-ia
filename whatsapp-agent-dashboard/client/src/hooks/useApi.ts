// ============================================================
// Hook genérico de data-fetching. Sem libs extras: useEffect +
// estado. Refaz a busca quando as dependências mudam (tenant,
// range, etc.) e ignora respostas obsoletas.
// ============================================================

import { useEffect, useState } from "react";

export interface AsyncResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
}

export function useApi<T>(
  fetcher: () => Promise<T>,
  deps: ReadonlyArray<unknown>,
  enabled = true,
): AsyncResult<T> {
  const [state, setState] = useState<AsyncResult<T>>({
    data: null,
    loading: enabled,
    error: null,
  });

  useEffect(() => {
    if (!enabled) {
      setState({ data: null, loading: false, error: null });
      return;
    }
    let alive = true;
    setState((s) => ({ data: s.data, loading: true, error: null }));
    fetcher()
      .then((data) => {
        if (alive) setState({ data, loading: false, error: null });
      })
      .catch((err: unknown) => {
        if (alive) {
          setState({
            data: null,
            loading: false,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, ...deps]);

  return state;
}
