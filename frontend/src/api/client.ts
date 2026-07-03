import type { ApiEnvelope } from "../types";

const configuredApiBaseUrl = (import.meta.env.VITE_API_BASE_URL || "").trim();

export const API_BASE_URL = configuredApiBaseUrl.replace(/\/$/, "");
export const API_BASE_LABEL = API_BASE_URL || "same-origin /api proxy";

export class ApiError extends Error {
  code?: string;
  requestId?: string;
  status: number;

  constructor(message: string, status: number, code?: string, requestId?: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.requestId = requestId;
  }
}

type QueryValue = string | number | boolean | null | undefined;
type HeaderValue = string | undefined;

function buildUrl(path: string, params?: Record<string, QueryValue>): string {
  const baseUrl =
    API_BASE_URL ||
    (typeof window !== "undefined" ? window.location.origin : "http://localhost:5004");
  const url = new URL(path, baseUrl);
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value === null || value === undefined || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url.toString();
}

function readErrorMessage(data: Partial<ApiEnvelope>, fallback: string): string {
  return data.error_detail?.message || data.error || fallback;
}

function createRequestId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `req_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

async function parseJson<T>(response: Response): Promise<T> {
  const text = await response.text();
  if (!text) {
    return {} as T;
  }
  return JSON.parse(text) as T;
}

export async function apiGet<T extends ApiEnvelope>(
  path: string,
  params?: Record<string, QueryValue>,
  extraHeaders?: Record<string, HeaderValue>,
): Promise<T> {
  const response = await fetch(buildUrl(path, params), {
    method: "GET",
    credentials: "include",
    headers: {
      Accept: "application/json",
      "X-Request-Id": createRequestId(),
      ...(extraHeaders || {}),
    },
  });
  const data = await parseJson<T>(response);
  if (!response.ok || data.success === false) {
    throw new ApiError(
      readErrorMessage(data, response.statusText),
      response.status,
      data.error_code || data.error_detail?.code,
      data.request_id || data.error_detail?.request_id,
    );
  }
  return data;
}

export async function apiPost<T extends ApiEnvelope>(
  path: string,
  payload: Record<string, unknown>,
): Promise<T> {
  const response = await fetch(buildUrl(path), {
    method: "POST",
    credentials: "include",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Request-Id": createRequestId(),
    },
    body: JSON.stringify(payload),
  });
  const data = await parseJson<T>(response);
  if (!response.ok || data.success === false) {
    throw new ApiError(
      readErrorMessage(data, response.statusText),
      response.status,
      data.error_code || data.error_detail?.code,
      data.request_id || data.error_detail?.request_id,
    );
  }
  return data;
}
