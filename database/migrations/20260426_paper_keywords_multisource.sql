-- paper_keywords multi-source primary key migration
-- Date: 2026-04-26
--
-- Goal:
--   Allow the same paper_id + keyword_type + keyword to coexist under
--   multiple sources, e.g. biorxiv/category and GLM-4.5-air-generated.

BEGIN;

UPDATE paper_keywords
SET source = 'paper_metadata'
WHERE source IS NULL OR btrim(source) = '';

ALTER TABLE paper_keywords
    ALTER COLUMN source SET DEFAULT 'paper_metadata';

ALTER TABLE paper_keywords
    ALTER COLUMN source SET NOT NULL;

ALTER TABLE paper_keywords
    DROP CONSTRAINT IF EXISTS paper_keywords_pkey;

ALTER TABLE paper_keywords
    ADD CONSTRAINT paper_keywords_pkey
    PRIMARY KEY (paper_id, keyword_type, keyword, source);

CREATE INDEX IF NOT EXISTS idx_pk_source ON paper_keywords(source);
CREATE INDEX IF NOT EXISTS idx_pk_source_keyword ON paper_keywords(source, keyword);

COMMIT;
