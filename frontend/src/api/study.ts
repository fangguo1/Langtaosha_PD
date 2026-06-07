import { apiGet, apiPost } from "./client";
import type { SearchMode, StudySearchResponse, ApiEnvelope } from "../types";

export function studySearch(params: {
  query: string;
  participantId: string;
  studySessionId?: string;
  mode: SearchMode;
  topK: number;
  sourceList?: string;
}): Promise<StudySearchResponse> {
  return apiGet<StudySearchResponse>("/api/study/search", {
    query: params.query,
    participant_id: params.participantId,
    study_session_id: params.studySessionId,
    mode: params.mode,
    top_k: params.topK,
    source_list: params.sourceList,
  });
}

export function submitStudyFeedback(payload: Record<string, unknown>): Promise<ApiEnvelope> {
  return apiPost<ApiEnvelope>("/api/study/feedback", payload);
}
