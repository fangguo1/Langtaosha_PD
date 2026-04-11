-- 数据库迁移脚本：添加work_id和相关字段
-- 执行时间: 2025
-- 说明: 根据新的JSON格式调整数据库表结构

-- ============================================================================
-- 1. 添加work_id字段到papers表
-- ============================================================================
ALTER TABLE papers ADD COLUMN IF NOT EXISTS work_id VARCHAR(200);
CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_work_id ON papers(work_id) WHERE work_id IS NOT NULL;

-- ============================================================================
-- 2. 添加semantic_scholar_id和pubmed_id字段
-- ============================================================================
ALTER TABLE papers ADD COLUMN IF NOT EXISTS semantic_scholar_id VARCHAR(100);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS pubmed_id VARCHAR(100);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_papers_semantic_scholar_id ON papers(semantic_scholar_id) WHERE semantic_scholar_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_papers_pubmed_id ON papers(pubmed_id) WHERE pubmed_id IS NOT NULL;

-- ============================================================================
-- 3. 添加contribution_types字段（JSONB数组）
-- ============================================================================
ALTER TABLE papers ADD COLUMN IF NOT EXISTS contribution_types JSONB;

-- 创建GIN索引支持JSONB查询
CREATE INDEX IF NOT EXISTS idx_papers_contribution_types ON papers USING GIN(contribution_types) WHERE contribution_types IS NOT NULL;

-- ============================================================================
-- 4. 更新keywords字段类型（如果需要支持JSONB）
-- ============================================================================
-- 注意：如果keywords需要支持JSONB查询，可以执行以下语句
-- ALTER TABLE papers ALTER COLUMN keywords TYPE JSONB USING keywords::jsonb;
-- 但考虑到兼容性，暂时保持TEXT类型，可以在应用层处理

-- ============================================================================
-- 完成提示
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✅ 数据库迁移完成！';
    RAISE NOTICE '已添加的字段：';
    RAISE NOTICE '  - papers.work_id (VARCHAR(200), UNIQUE)';
    RAISE NOTICE '  - papers.semantic_scholar_id (VARCHAR(100))';
    RAISE NOTICE '  - papers.pubmed_id (VARCHAR(100))';
    RAISE NOTICE '  - papers.contribution_types (JSONB)';
END $$;

