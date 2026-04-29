-- paper_keywords trigram recall support
-- Date: 2026-04-27
--
-- Goal:
--   Enable pg_trgm-backed fuzzy candidate recall for query correction while
--   keeping the existing rapidfuzz-based final ranking in application code.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_paper_keywords_lower_keyword_trgm
ON paper_keywords
USING gin (lower(keyword) gin_trgm_ops);

COMMIT;
