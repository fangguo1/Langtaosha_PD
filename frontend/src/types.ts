export type SearchMode = "smart" | "vector";

export interface ApiErrorDetail {
  code?: string;
  message?: string;
  request_id?: string;
}

export interface ApiEnvelope {
  success: boolean;
  request_id?: string;
  error?: string;
  error_code?: string;
  error_detail?: ApiErrorDetail;
}

export interface RetrievalReason {
  key: string;
  label: string;
  score?: number;
  matched_concepts?: string[];
}

export interface SearchResult {
  work_id?: string;
  paper_id?: number;
  source_name?: string;
  similarity?: number;
  similarity_score?: number;
  title?: string;
  abstract?: string;
  authors?: string;
  doi?: string;
  online_date?: string;
  source?: string;
  source_key?: string;
  link?: string;
  retrieval_reasons?: RetrievalReason[];
  retrieval_reason_tags?: string[];
  highlight?: Record<string, unknown>;
}

export interface SearchResultGroup {
  0: string;
  1: SearchResult[];
}

export interface QueryUnderstanding {
  intent?: string;
  route?: string;
  corrected_query?: string | null;
  matched_author?: string | null;
  suggested_author?: string | null;
  confidence?: number;
  reason?: string;
  [key: string]: unknown;
}

export interface SearchNoticeAction {
  label: string;
  mode: SearchMode;
  query: string;
  correction_decision?: "accept" | "reject";
}

export interface SearchNotice {
  type?: string;
  message?: string;
  action?: SearchNoticeAction | null;
  actions?: SearchNoticeAction[];
  fallback_mode?: SearchMode;
  fallback_query?: string;
  action_label?: string;
  [key: string]: unknown;
}

export interface SearchMeta {
  count?: number;
  elapsed_ms?: number;
  request_id?: string;
}

export interface SearchResponse extends ApiEnvelope {
  query: {
    input: string;
    executed?: string | null;
    mode: SearchMode;
    intent?: string;
    route?: string;
    corrected_query?: string | null;
    matched_author?: string | null;
    suggested_author?: string | null;
    correction_status?: string;
  };
  search_query?: string | null;
  search_mode: SearchMode;
  query_understanding?: QueryUnderstanding;
  result_policy?: Record<string, unknown>;
  notice?: SearchNotice | null;
  meta?: SearchMeta;
  count: number;
  results: Array<SearchResult | SearchResultGroup>;
}

export interface DailyNewPaper {
  paper_id?: number;
  work_id?: string;
  title?: string;
  authors?: string;
  online_at?: string;
  online_date?: string;
  source?: string;
  source_key?: string;
  link?: string;
}

export interface DailyNewResponse extends ApiEnvelope {
  count: number;
  results: DailyNewPaper[];
}

export interface RecommendResponse extends ApiEnvelope {
  mode: string;
  seed?: SearchResult;
  query_terms?: Record<string, unknown>;
  count: number;
  results: SearchResult[];
}

export interface StudySearchResponse extends SearchResponse {
  study?: {
    study_session_id: string;
    participant_id: string;
    query_index: number;
    search_event_id: number;
    result_snapshot_count: number;
  };
}
