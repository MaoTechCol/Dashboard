import { useCallback, useEffect, useRef, useState } from "react";

import { apiJson, DASHBOARD_REFRESH_MS } from "../api";
import type { DashboardSnapshot } from "../types";

function nextAlignedRefreshDelay(intervalMs: number) {
  const safeInterval = Math.max(intervalMs, 60_000);
  const now = Date.now();
  const remainder = now % safeInterval;
  const delay = remainder === 0 ? safeInterval : safeInterval - remainder;
  return Math.max(delay, 1_000);
}

export function useDashboardStream(companySlug: string | null) {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const inflightRef = useRef<Promise<void> | null>(null);
  const companyRef = useRef<string | null>(companySlug);

  useEffect(() => {
    companyRef.current = companySlug;
    if (!companySlug) {
      setSnapshot(null);
      setLoading(false);
      setError(null);
    }
  }, [companySlug]);

  const loadSnapshot = useCallback(async (force = false) => {
    const nextCompany = companyRef.current;
    if (!nextCompany) return;
    if (inflightRef.current) {
      await inflightRef.current;
      if (!force) {
        return;
      }
    }

    const request = (async () => {
      const params = new URLSearchParams({ company: nextCompany });
      if (force) {
        params.set("_ts", String(Date.now()));
      }
      const payload = await apiJson<DashboardSnapshot>(`/dashboard?${params.toString()}`);
      setSnapshot(payload);
      setLoading(false);
      setError(null);
    })();

    inflightRef.current = request.finally(() => {
      inflightRef.current = null;
    });

    return inflightRef.current;
  }, []);

  useEffect(() => {
    if (!companySlug) return;
    let closed = false;
    let refreshTimer: number | null = null;

    const runLoad = async (showSpinner: boolean, force = false) => {
      if (showSpinner) {
        setLoading(true);
      }
      try {
        await loadSnapshot(force);
      } catch (nextError) {
        if (!closed) {
          setError(nextError instanceof Error ? nextError.message : "No se pudo cargar el dashboard");
          setLoading(false);
        }
      }
    };

    const scheduleNextRefresh = () => {
      if (closed) {
        return;
      }
      const delay = nextAlignedRefreshDelay(DASHBOARD_REFRESH_MS);
      refreshTimer = window.setTimeout(async () => {
        await runLoad(false, true);
        scheduleNextRefresh();
      }, delay);
    };

    void runLoad(true, true);
    scheduleNextRefresh();

    return () => {
      closed = true;
      if (refreshTimer !== null) {
        window.clearTimeout(refreshTimer);
      }
    };
  }, [companySlug, loadSnapshot]);

  return {
    snapshot,
    loading,
    error,
    refresh: useCallback(async () => {
      if (!companyRef.current) return;
      setLoading(true);
      await loadSnapshot(true).catch((nextError) => {
        setError(nextError instanceof Error ? nextError.message : "No se pudo recargar el dashboard");
        setLoading(false);
      });
    }, [loadSnapshot]),
  };
}
