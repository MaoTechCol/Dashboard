import { useEffect, useRef, useState } from "react";

import { apiJson, FEED_REFRESH_MS } from "../api";
import type { FeedPollPayload } from "../types";

export function useFeedStatus(companySlug: string | null, knownCycleAt: string | null) {
  const [payload, setPayload] = useState<FeedPollPayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const knownCycleRef = useRef<string | null>(knownCycleAt);

  useEffect(() => {
    knownCycleRef.current = knownCycleAt;
  }, [knownCycleAt]);

  useEffect(() => {
    if (!companySlug) {
      setPayload(null);
      setError(null);
      return;
    }
    let closed = false;

    const load = async () => {
      try {
        const params = new URLSearchParams({ company: companySlug });
        if (knownCycleRef.current) {
          params.set("known_cycle_at", knownCycleRef.current);
        }
        const nextPayload = await apiJson<FeedPollPayload>(`/feed?${params.toString()}`);
        if (!closed) {
          setPayload(nextPayload);
          setError(null);
        }
      } catch (nextError) {
        if (!closed) {
          setError(nextError instanceof Error ? nextError.message : "No se pudo cargar el estado del feed");
        }
      }
    };

    void load();
    const timer = window.setInterval(() => {
      void load();
    }, FEED_REFRESH_MS);

    return () => {
      closed = true;
      window.clearInterval(timer);
    };
  }, [companySlug]);

  return { payload, error };
}
