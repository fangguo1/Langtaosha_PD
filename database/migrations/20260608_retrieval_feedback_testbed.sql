-- Retrieval feedback testbed schema.
--
-- Historical Study Mode labels may originate from the mimic environment,
-- while the canonical testbed and evaluation runs live in the use
-- environment. The schema therefore preserves origin identity and config
-- fingerprints alongside resolved use-environment judgments.

CREATE TABLE IF NOT EXISTS retrieval_testbed_queries (
    query_id BIGSERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    normalized_query TEXT NOT NULL,
    query_type VARCHAR(32) NOT NULL DEFAULT 'topic',
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    exclusion_reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (normalized_query, query_type)
);

COMMENT ON TABLE retrieval_testbed_queries IS 'Canonical retrieval testbed queries for topic/semantic evaluation.';

CREATE INDEX IF NOT EXISTS idx_retrieval_testbed_queries_status
    ON retrieval_testbed_queries(status, query_type);

CREATE TABLE IF NOT EXISTS retrieval_testbed_judgments (
    query_id BIGINT NOT NULL REFERENCES retrieval_testbed_queries(query_id) ON DELETE CASCADE,
    work_id VARCHAR(200) NOT NULL,
    relevance SMALLINT NOT NULL CHECK (relevance IN (0, 1)),
    judgment_source VARCHAR(32) NOT NULL DEFAULT 'user_feedback',
    source_event_id BIGINT REFERENCES user_study_events(id),
    source_search_event_id BIGINT REFERENCES user_study_events(id),
    annotator_id VARCHAR(120),
    origin_rank INTEGER,
    origin_search_mode VARCHAR(32),
    origin_search_query TEXT,
    origin_environment VARCHAR(64) NOT NULL,
    origin_work_id VARCHAR(200),
    identity_match_type VARCHAR(32) NOT NULL,
    identity_match_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (query_id, work_id)
);

COMMENT ON TABLE retrieval_testbed_judgments IS 'Current resolved judgments keyed by use-environment work_id.';

CREATE INDEX IF NOT EXISTS idx_retrieval_testbed_judgments_query_relevance
    ON retrieval_testbed_judgments(query_id, relevance);

CREATE INDEX IF NOT EXISTS idx_retrieval_testbed_judgments_origin
    ON retrieval_testbed_judgments(origin_environment, origin_work_id);

CREATE TABLE IF NOT EXISTS retrieval_testbed_versions (
    testbed_version_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(160) NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL DEFAULT 'draft',
    selection_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    query_count INTEGER NOT NULL DEFAULT 0,
    judgment_count INTEGER NOT NULL DEFAULT 0,
    positive_count INTEGER NOT NULL DEFAULT 0,
    negative_count INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    testbed_config_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    frozen_at TIMESTAMPTZ
);

COMMENT ON TABLE retrieval_testbed_versions IS 'Frozen retrieval feedback testbed versions.';

CREATE INDEX IF NOT EXISTS idx_retrieval_testbed_versions_status
    ON retrieval_testbed_versions(status, created_at);

CREATE TABLE IF NOT EXISTS retrieval_testbed_version_items (
    testbed_version_id BIGINT NOT NULL REFERENCES retrieval_testbed_versions(testbed_version_id) ON DELETE CASCADE,
    query_id BIGINT NOT NULL REFERENCES retrieval_testbed_queries(query_id) ON DELETE CASCADE,
    work_id VARCHAR(200) NOT NULL,
    relevance SMALLINT NOT NULL CHECK (relevance IN (0, 1)),
    PRIMARY KEY (testbed_version_id, query_id, work_id)
);

COMMENT ON TABLE retrieval_testbed_version_items IS 'Frozen query-document judgments for one testbed version.';

CREATE INDEX IF NOT EXISTS idx_retrieval_testbed_version_items_query
    ON retrieval_testbed_version_items(testbed_version_id, query_id, relevance);

CREATE TABLE IF NOT EXISTS retrieval_evaluation_runs (
    run_id BIGSERIAL PRIMARY KEY,
    testbed_version_id BIGINT NOT NULL REFERENCES retrieval_testbed_versions(testbed_version_id) ON DELETE RESTRICT,
    strategy_name VARCHAR(64) NOT NULL,
    strategy_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    config_path TEXT,
    evaluation_config_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
    corpus_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    index_version JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_top_k INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'running',
    aggregate_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_summary TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

COMMENT ON TABLE retrieval_evaluation_runs IS 'Evaluation run metadata for one retrieval strategy against one frozen testbed version.';

CREATE INDEX IF NOT EXISTS idx_retrieval_evaluation_runs_version
    ON retrieval_evaluation_runs(testbed_version_id, strategy_name, started_at);

CREATE TABLE IF NOT EXISTS retrieval_evaluation_results (
    run_id BIGINT NOT NULL REFERENCES retrieval_evaluation_runs(run_id) ON DELETE CASCADE,
    query_id BIGINT NOT NULL REFERENCES retrieval_testbed_queries(query_id) ON DELETE CASCADE,
    work_id VARCHAR(200) NOT NULL,
    rank INTEGER NOT NULL,
    score DOUBLE PRECISION,
    is_judged BOOLEAN NOT NULL DEFAULT FALSE,
    relevance SMALLINT,
    retrieval_debug JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (run_id, query_id, rank)
);

COMMENT ON TABLE retrieval_evaluation_results IS 'Ranked retrieval outputs for one run and query.';

CREATE INDEX IF NOT EXISTS idx_retrieval_evaluation_results_work
    ON retrieval_evaluation_results(run_id, query_id, work_id);

CREATE TABLE IF NOT EXISTS retrieval_evaluation_query_metrics (
    run_id BIGINT NOT NULL REFERENCES retrieval_evaluation_runs(run_id) ON DELETE CASCADE,
    query_id BIGINT NOT NULL REFERENCES retrieval_testbed_queries(query_id) ON DELETE CASCADE,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_summary TEXT,
    PRIMARY KEY (run_id, query_id)
);

COMMENT ON TABLE retrieval_evaluation_query_metrics IS 'Per-query metric payloads and failures for one evaluation run.';

CREATE OR REPLACE FUNCTION update_retrieval_feedback_testbed_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_retrieval_testbed_queries_updated_at ON retrieval_testbed_queries;
CREATE TRIGGER trigger_update_retrieval_testbed_queries_updated_at
    BEFORE UPDATE ON retrieval_testbed_queries
    FOR EACH ROW
    EXECUTE FUNCTION update_retrieval_feedback_testbed_updated_at();

DROP TRIGGER IF EXISTS trigger_update_retrieval_testbed_judgments_updated_at ON retrieval_testbed_judgments;
CREATE TRIGGER trigger_update_retrieval_testbed_judgments_updated_at
    BEFORE UPDATE ON retrieval_testbed_judgments
    FOR EACH ROW
    EXECUTE FUNCTION update_retrieval_feedback_testbed_updated_at();
