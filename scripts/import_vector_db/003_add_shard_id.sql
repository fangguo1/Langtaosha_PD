-- 数据库迁移脚本：添加 shard_id 字段
-- 执行时间: 2025
-- 说明: 为大规模文献向量化系统添加 shard_id 字段，用于记录文献所属的 shard

-- ============================================================================
-- 添加 shard_id 字段到 papers 表
-- ============================================================================
ALTER TABLE papers ADD COLUMN IF NOT EXISTS shard_id INTEGER;

-- 添加注释说明字段含义
COMMENT ON COLUMN papers.shard_id IS '文献所属的 shard ID，仅在 embedding_status=2 时赋值';

-- 创建单列索引以优化按 shard_id 查询的性能
CREATE INDEX IF NOT EXISTS idx_papers_shard_id ON papers(shard_id);

-- 创建复合索引以优化 embedding_status 和 shard_id 的联合查询
CREATE INDEX IF NOT EXISTS idx_papers_embedding_status_shard_id ON papers(embedding_status, shard_id);

-- 创建复合索引以优化 embedding_status 和 paper_id 的联合查询
-- 注意：CONCURRENTLY 不能在事务中执行，且不支持 IF NOT EXISTS
-- 如果索引已存在，此命令会报错，需要先检查或删除旧索引
-- 如需在事务中执行，请使用：CREATE INDEX IF NOT EXISTS idx_papers_embedding_status_paper_id ON papers(embedding_status, paper_id);
CREATE INDEX CONCURRENTLY idx_papers_embedding_status_paper_id
ON papers(embedding_status, paper_id);

