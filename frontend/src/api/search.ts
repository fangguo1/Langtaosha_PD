import { apiGet } from "./client";
import type { SearchMode, SearchResponse } from "../types";

export interface SearchParams {
  query: string;
  mode: SearchMode;
  topK: number;
  sourceList?: string;
}

export function searchPapers(params: SearchParams): Promise<SearchResponse> {
  return apiGet<SearchResponse>("/api/scholar/search", {
    query: params.query,
    mode: params.mode,
    top_k: params.topK,
    source_list: params.sourceList,
  });
}
