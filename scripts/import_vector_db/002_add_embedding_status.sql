-- 数据库迁移脚本：添加 embedding_status 字段
-- 执行时间: 2025
-- 说明: 为大规模文献向量化系统添加状态跟踪字段

-- ============================================================================
-- 添加 embedding_status 字段到 papers 表
-- ============================================================================
ALTER TABLE papers ADD COLUMN IF NOT EXISTS embedding_status SMALLINT DEFAULT 0;

-- 添加注释说明状态含义
COMMENT ON COLUMN papers.embedding_status IS '向量化状态: 0=raw(未处理), 1=exported(已导出为Arrow batch), 2=ready(已写入FAISS索引)';

-- 创建索引以优化查询性能
CREATE INDEX IF NOT EXISTS idx_papers_embedding_status ON papers(embedding_status);

-- 为已存在的记录设置默认值（虽然 DEFAULT 0 已经处理，但显式设置更安全）
UPDATE papers SET embedding_status = 0 WHERE embedding_status IS NULL;

