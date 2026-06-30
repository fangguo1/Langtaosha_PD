import { apiGet } from "./client";
import type { SearchMode, SearchResponse } from "../types";

export interface SearchParams {
  query: string;
  mode: SearchMode;
  topK: number;
  offset?: number;
  sourceList?: string;
  correctionDecision?: "accept" | "reject";
}

export function searchPapers(params: SearchParams): Promise<SearchResponse> {
  return apiGet<SearchResponse>(
    "/api/scholar/search",
    {
      query: params.query,
      mode: params.mode,
      top_k: params.topK,
      offset: params.offset ?? 0,
      source_list: params.sourceList,
      correction_decision: params.correctionDecision,
    },
    {
      "X-Langtaosha-Client-Surface": "search_page",
    },
  );
}
