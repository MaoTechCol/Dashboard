const rawBase = import.meta.env.VITE_API_BASE_URL?.trim() || "/api";

export const API_BASE = rawBase.replace(/\/$/, "");
export const DASHBOARD_REFRESH_MS = Number(import.meta.env.VITE_DASHBOARD_REFRESH_MS?.trim() || "900000");
export const FEED_REFRESH_MS = Number(import.meta.env.VITE_FEED_REFRESH_MS?.trim() || "60000");

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

export async function apiFetch(path: string, init: RequestInit = {}) {
  const response = await fetch(buildApiUrl(path), {
    cache: "no-store",
    credentials: "include",
    ...init,
  });
  return response;
}

export async function apiJson<T>(path: string, init: RequestInit = {}) {
  const response = await apiFetch(path, init);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new ApiError(payload.detail || `Request failed (${response.status})`, response.status);
  }
  return (await response.json()) as T;
}
