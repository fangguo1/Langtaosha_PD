-- Request-level logging summary table for /api/scholar/search.

CREATE TABLE IF NOT EXISTS frontend_search_request_logs (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_id TEXT NOT NULL UNIQUE,
    client_surface TEXT NOT NULL DEFAULT 'unknown',
    query_input TEXT NOT NULL,
    query_executed TEXT,
    search_mode TEXT NOT NULL,
    query_intent TEXT,
    query_route TEXT,
    corrected_query TEXT,
    matched_author TEXT,
    suggested_author TEXT,
    notice_type TEXT,
    result_count INTEGER NOT NULL DEFAULT 0,
    limit_count INTEGER,
    offset_count INTEGER,
    has_more BOOLEAN,
    elapsed_ms INTEGER,
    status TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

COMMENT ON TABLE frontend_search_request_logs IS '正式前端 /api/scholar/search 请求级日志摘要表。';

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_created_at
    ON frontend_search_request_logs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_surface_created
    ON frontend_search_request_logs (client_surface, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_route_created
    ON frontend_search_request_logs (query_route, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_status_created
    ON frontend_search_request_logs (status, created_at DESC);
