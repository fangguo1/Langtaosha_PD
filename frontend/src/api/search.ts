import { apiGet } from "./client";
import type { SearchMode, SearchResponse } from "../types";

export interface SearchParams {
  query: string;
  mode: SearchMode;
  limit: number;
  offset?: number;
  sourceList?: string;
}

export function searchPapers(params: SearchParams): Promise<SearchResponse> {
  return apiGet<SearchResponse>(
    "/api/scholar/search",
    {
      query: params.query,
      mode: params.mode,
      limit: params.limit,
      offset: params.offset ?? 0,
      source_list: params.sourceList,
    },
    {
      "X-Langtaosha-Client-Surface": "search_page",
    },
  );
}
