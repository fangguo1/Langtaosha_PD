-- 数据库迁移脚本：添加论文分类字段和关键词表
-- 执行时间: 2025
-- 说明: 根据新的需求添加论文分类字段和结构化关键词表

-- ============================================================================
-- 1. 添加新字段到papers表
-- ============================================================================
ALTER TABLE papers ADD COLUMN IF NOT EXISTS paper_type VARCHAR(100);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS primary_field VARCHAR(100);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS target_application_domain VARCHAR(200);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS is_llm_era BOOLEAN DEFAULT FALSE;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS short_reasoning VARCHAR(200);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_papers_paper_type ON papers(paper_type);
CREATE INDEX IF NOT EXISTS idx_papers_primary_field ON papers(primary_field);
CREATE INDEX IF NOT EXISTS idx_papers_target_application_domain ON papers(target_application_domain);
CREATE INDEX IF NOT EXISTS idx_papers_is_llm_era ON papers(is_llm_era);

-- ============================================================================
-- 2. 创建paper_keywords表
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_keywords (
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    keyword_type VARCHAR(50) NOT NULL,  -- task, method, contribution, dataset, metric, library, domain, concept, model
    keyword VARCHAR(200) NOT NULL,
    weight REAL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),  -- 权重/置信度 (0.0-1.0)
    source VARCHAR(50),  -- manual, ai_extract, keyword_match, llm_tag, paper_metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (paper_id, keyword_type, keyword)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pk_paper_id ON paper_keywords(paper_id);
CREATE INDEX IF NOT EXISTS idx_pk_keyword_type ON paper_keywords(keyword_type);
CREATE INDEX IF NOT EXISTS idx_pk_keyword ON paper_keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_pk_paper_id_keyword_type ON paper_keywords(paper_id, keyword_type);

-- ============================================================================
-- 完成提示
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✅ 数据库迁移完成！';
    RAISE NOTICE '已添加的字段到papers表：';
    RAISE NOTICE '  - papers.paper_type (VARCHAR(100))';
    RAISE NOTICE '  - papers.primary_field (VARCHAR(100))';
    RAISE NOTICE '  - papers.target_application_domain (VARCHAR(200))';
    RAISE NOTICE '  - papers.is_llm_era (BOOLEAN, DEFAULT FALSE)';
    RAISE NOTICE '  - papers.short_reasoning (VARCHAR(200))';
    RAISE NOTICE '已创建的新表：';
    RAISE NOTICE '  - paper_keywords (论文关键词表)';
    RAISE NOTICE '    * 支持多种关键词类型：task, method, contribution, dataset, metric, library, domain, concept, model';
    RAISE NOTICE '    * 支持关键词权重和来源追踪';
END $$;

