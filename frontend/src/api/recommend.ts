import { apiGet } from "./client";
import type { RecommendResponse } from "../types";

export function getRecommendations(params: {
  workId?: string;
  paperId?: number;
  topK?: number;
  sourceList?: string;
}): Promise<RecommendResponse> {
  return apiGet<RecommendResponse>("/api/recommend", {
    work_id: params.workId,
    paper_id: params.paperId,
    top_k: params.topK || 5,
    source_list: params.sourceList,
  });
}
