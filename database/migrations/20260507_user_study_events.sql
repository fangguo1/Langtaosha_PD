-- Study Mode event log for first-round free-exploration user study.

CREATE TABLE IF NOT EXISTS user_study_events (
    id BIGSERIAL PRIMARY KEY,

    event_type VARCHAR(32) NOT NULL,
    study_session_id VARCHAR(80) NOT NULL,
    participant_id VARCHAR(120),

    query_index INTEGER,
    search_event_id BIGINT,

    query TEXT,
    search_mode VARCHAR(32),
    search_query TEXT,
    query_understanding_route VARCHAR(64),
    result_count INTEGER,

    result_rank INTEGER,
    work_id VARCHAR(200),
    paper_id INTEGER,
    title TEXT,
    source VARCHAR(64),
    year VARCHAR(16),
    similarity_score DOUBLE PRECISION,

    feedback VARCHAR(32),
    reason_text TEXT,

    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE user_study_events IS '用户研究事件日志：统一记录 Study Mode 搜索事件和结果级反馈事件。';

CREATE INDEX IF NOT EXISTS idx_user_study_events_session
    ON user_study_events(study_session_id, created_at);

CREATE INDEX IF NOT EXISTS idx_user_study_events_participant
    ON user_study_events(participant_id, created_at);

CREATE INDEX IF NOT EXISTS idx_user_study_events_type
    ON user_study_events(event_type, created_at);

CREATE INDEX IF NOT EXISTS idx_user_study_events_search_event
    ON user_study_events(search_event_id);
