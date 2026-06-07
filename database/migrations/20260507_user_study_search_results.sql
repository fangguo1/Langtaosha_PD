-- Study Mode search-result snapshots.
--
-- user_study_events remains append-only. This table records the full ranked
-- result list returned by each Study Mode search so unclicked results are also
-- available for later analysis.

CREATE TABLE IF NOT EXISTS user_study_search_results (
    id BIGSERIAL PRIMARY KEY,

    search_event_id BIGINT NOT NULL
        REFERENCES user_study_events(id) ON DELETE CASCADE,
    study_session_id VARCHAR(80) NOT NULL,
    participant_id VARCHAR(120),
    query_index INTEGER,

    result_rank INTEGER NOT NULL,
    work_id VARCHAR(200),
    paper_id INTEGER,
    title TEXT,
    source VARCHAR(64),
    source_key VARCHAR(64),
    year VARCHAR(16),
    online_date VARCHAR(32),
    similarity_score DOUBLE PRECISION,

    payload JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (search_event_id, result_rank)
);

COMMENT ON TABLE user_study_search_results IS '用户研究搜索结果快照：记录每次 Study Mode 搜索返回给用户的 rank 级结果列表。';

CREATE INDEX IF NOT EXISTS idx_user_study_search_results_search_event
    ON user_study_search_results(search_event_id, result_rank);

CREATE INDEX IF NOT EXISTS idx_user_study_search_results_session
    ON user_study_search_results(study_session_id, query_index, result_rank);

CREATE INDEX IF NOT EXISTS idx_user_study_search_results_work_id
    ON user_study_search_results(work_id);

CREATE INDEX IF NOT EXISTS idx_user_study_search_results_paper_id
    ON user_study_search_results(paper_id);
