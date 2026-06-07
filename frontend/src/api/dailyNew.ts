import { apiGet } from "./client";
import type { DailyNewResponse } from "../types";

export function getDailyNew(limit = 10): Promise<DailyNewResponse> {
  return apiGet<DailyNewResponse>("/api/scholar/daily_new", { limit });
}
