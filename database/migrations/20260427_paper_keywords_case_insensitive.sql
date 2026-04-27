-- paper_keywords case-insensitive keyword identity migration
-- Date: 2026-04-27
--
-- Goal:
--   Treat keyword text as case-insensitive at ingestion time within the same
--   paper_id + keyword_type + source. This prevents duplicates such as
--   CRISPR/crispr while preserving the display casing of the retained row.

BEGIN;

WITH ranked AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY paper_id, lower(keyword_type), lower(keyword), source
            ORDER BY created_at ASC NULLS LAST, weight DESC NULLS LAST, keyword ASC
        ) AS rn,
        MAX(COALESCE(weight, 1.0)) OVER (
            PARTITION BY paper_id, lower(keyword_type), lower(keyword), source
        ) AS max_weight
    FROM paper_keywords
),
keepers AS (
    UPDATE paper_keywords pk
    SET weight = ranked.max_weight
    FROM ranked
    WHERE pk.ctid = ranked.ctid
      AND ranked.rn = 1
    RETURNING pk.ctid
)
DELETE FROM paper_keywords pk
USING ranked
WHERE pk.ctid = ranked.ctid
  AND ranked.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pk_keyword_case_insensitive_unique
ON paper_keywords(paper_id, lower(keyword_type), lower(keyword), source);

CREATE INDEX IF NOT EXISTS idx_pk_lower_keyword
ON paper_keywords(lower(keyword));

COMMIT;
