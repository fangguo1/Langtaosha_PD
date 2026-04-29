-- Migration: 添加 version 字段和 partial unique indexes
-- 说明：支持同 source 版本比较与覆盖策略
-- 创建时间: 2026-04-21

-- 添加 version 字段到 paper_sources 表
ALTER TABLE paper_sources ADD COLUMN IF NOT EXISTS version VARCHAR(50);

-- 添加注释
COMMENT ON COLUMN paper_sources.version IS '版本号：用于同 source 的版本比较与覆盖策略。使用 packaging.version 进行语义化版本比较。';

-- 创建 partial unique indexes（同 source 下的唯一性约束）

-- 同 source 下的唯一性约束（DOI）
CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_source_doi
    ON paper_sources(source_name, doi)
    WHERE doi IS NOT NULL;

COMMENT ON INDEX idx_ps_source_doi IS '同 source 下的 DOI 唯一性约束，防止同 source 重复写入相同 DOI 的记录';

-- 同 source 下的唯一性约束（arXiv ID）
CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_source_arxiv
    ON paper_sources(source_name, arxiv_id)
    WHERE arxiv_id IS NOT NULL;

COMMENT ON INDEX idx_ps_source_arxiv IS '同 source 下的 arXiv ID 唯一性约束，防止同 source 重复写入相同 arXiv ID 的记录';

-- 同 source 下的唯一性约束（PubMed ID）
CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_source_pubmed
    ON paper_sources(source_name, pubmed_id)
    WHERE pubmed_id IS NOT NULL;

COMMENT ON INDEX idx_ps_source_pubmed IS '同 source 下的 PubMed ID 唯一性约束，防止同 source 重复写入相同 PubMed ID 的记录';

-- 验证迁移结果
DO $$
BEGIN
    RAISE NOTICE '✅ Migration completed successfully!';
    RAISE NOTICE 'Added fields:';
    RAISE NOTICE '  - paper_sources.version (VARCHAR(50))';
    RAISE NOTICE 'Added indexes:';
    RAISE NOTICE '  - idx_ps_source_doi (partial unique index on source_name, doi)';
    RAISE NOTICE '  - idx_ps_source_arxiv (partial unique index on source_name, arxiv_id)';
    RAISE NOTICE '  - idx_ps_source_pubmed (partial unique index on source_name, pubmed_id)';
END $$;
