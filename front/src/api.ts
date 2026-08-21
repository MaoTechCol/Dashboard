const rawBase = import.meta.env.VITE_API_BASE_URL?.trim() || "/api";

export const API_BASE = rawBase.replace(/\/$/, "");
export const DASHBOARD_REFRESH_MS = Number(import.meta.env.VITE_DASHBOARD_REFRESH_MS?.trim() || "900000");
export const FEED_REFRESH_MS = Number(import.meta.env.VITE_FEED_REFRESH_MS?.trim() || "60000");
export const API_REQUEST_TIMEOUT_MS = Number(import.meta.env.VITE_API_REQUEST_TIMEOUT_MS?.trim() || "20000");

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export function buildApiUrl(path: string): string {
  if (/^https?:\/\//.test(path)) {
    return path;
  }
  const normalizedPath = path.replace(/^\//, "");
  if (/^https?:\/\//.test(API_BASE)) {
    return new URL(normalizedPath, `${API_BASE}/`).toString();
  }
  if (typeof window !== "undefined") {
    return new URL(normalizedPath, new URL(`${API_BASE.replace(/^\./, "").replace(/\/?$/, "/")}`, window.location.origin)).toString();
  }
  throw new Error("Relative VITE_API_BASE_URL requires a browser environment");
}

type ApiRequestInit = RequestInit & { timeoutMs?: number };

const pendingJsonGets = new Map<string, Promise<unknown>>();

export async function apiFetch(path: string, init: ApiRequestInit = {}) {
  const { timeoutMs = API_REQUEST_TIMEOUT_MS, signal: callerSignal, ...requestInit } = init;
  const controller = new AbortController();
  const abortFromCaller = () => controller.abort(callerSignal?.reason);

  if (callerSignal?.aborted) {
    abortFromCaller();
  } else {
    callerSignal?.addEventListener("abort", abortFromCaller, { once: true });
  }

  const timeout = window.setTimeout(() => controller.abort("api-timeout"), timeoutMs);
  try {
    return await fetch(buildApiUrl(path), {
      cache: "no-store",
      credentials: "include",
      ...requestInit,
      signal: controller.signal,
    });
  } catch (error) {
    if (controller.signal.aborted && !callerSignal?.aborted) {
      throw new ApiError("El servicio no respondio a tiempo. Intenta nuevamente.", 0);
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
    callerSignal?.removeEventListener("abort", abortFromCaller);
  }
}

export async function apiJson<T>(path: string, init: ApiRequestInit = {}) {
  const method = String(init.method || "GET").toUpperCase();
  const canShareRequest = method === "GET" && !init.signal && init.body == null;
  const key = canShareRequest ? `${method}:${buildApiUrl(path)}` : "";
  const existing = key ? pendingJsonGets.get(key) : undefined;
  if (existing) return existing as Promise<T>;

  const request = (async () => {
    const response = await apiFetch(path, init);
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new ApiError(payload.detail || `Request failed (${response.status})`, response.status);
    }
    return (await response.json()) as T;
  })();
  if (key) pendingJsonGets.set(key, request);
  try {
    return await request;
  } finally {
    if (key && pendingJsonGets.get(key) === request) pendingJsonGets.delete(key);
  }
}

export async function waitForBackgroundJob<T extends { status: string; last_error?: string | null }>(
  jobId: string,
  options: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<T | null> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  const intervalMs = options.intervalMs ?? 1_000;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await apiJson<T>(`/jobs/${encodeURIComponent(jobId)}`, { timeoutMs: Math.min(10_000, timeoutMs) });
    if (job.status === "succeeded") return job;
    if (job.status === "failed") {
      throw new ApiError(job.last_error || "El trabajo en segundo plano fallo.", 500);
    }
    await new Promise<void>((resolve) => window.setTimeout(resolve, intervalMs));
  }
  return null;
}
