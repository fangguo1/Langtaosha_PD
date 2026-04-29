-- ============================================================================
-- 迁移脚本：为 papers 表添加 work_id 字段
-- ============================================================================
-- 目的：为 papers 表添加全局唯一的 work_id 字段，支持：
--   1. Vector DB 集成（需要全局唯一标识符）
--   2. 跨系统数据迁移（自增 ID 在多数据库环境下会冲突）
--   3. API 安全性（避免暴露内部数据量）
--   4. 分布式系统支持（UUID 天然适合分布式环境）
--
-- 执行时间：约 1-5 秒（取决于数据量）
-- 回滚：支持（见脚本末尾）
-- ============================================================================

-- ============================================================================
-- Step 1: 添加 work_id 字段
-- ============================================================================
-- 说明：使用 VARCHAR(200) 以支持 UUID v7 格式（前缀 W + 36 字符 UUID）
-- 允许 NULL 以保证向后兼容（现有记录先为 NULL，后续批量生成）
ALTER TABLE papers ADD COLUMN IF NOT EXISTS work_id VARCHAR(200);

COMMENT ON COLUMN papers.work_id IS '全局唯一标识符（UUID v7 格式，前缀 W），用于：
1. Vector DB 向量与元数据的关联
2. 跨系统数据交换和迁移
3. API 对外接口（避免暴露自增 ID）
4. 分布式系统中的唯一识别

示例：W019b73d6-1634-77d3-9574-b6014f85b118';

-- ============================================================================
-- Step 2: 创建唯一索引
-- ============================================================================
-- 说明：
-- - 使用部分索引（WHERE work_id IS NOT NULL）提高性能
-- - 允许多个 NULL 值存在（PostgreSQL 特性）
-- - 唯一索引确保 work_id 全局唯一
CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_work_id
ON papers(work_id)
WHERE work_id IS NOT NULL;

COMMENT ON INDEX idx_papers_work_id IS 'work_id 唯一索引，确保全局唯一性';

-- ============================================================================
-- Step 3: 为现有记录生成 work_id（可选，推荐执行）
-- ============================================================================
-- 说明：
-- - 使用 PostgreSQL 内置的 gen_random_uuid() 生成 UUID
-- - 添加前缀 'W' 与旧格式保持一致
-- - 分批更新以避免长事务锁表
-- ============================================================================
-- 方法 1：直接更新（适用于小数据量，< 10万条）
-- ============================================================================
UPDATE papers
SET work_id = 'W' || gen_random_uuid()::text
WHERE work_id IS NULL;

-- ============================================================================
-- 方法 2：分批更新（适用于大数据量，> 10万条）
-- ============================================================================
-- 取消注释以下代码以使用分批更新：

-- DO $$
-- DECLARE
--     batch_size INT := 1000;
--     updated_count INT := 1;
--     total_updated INT := 0;
-- BEGIN
--     WHILE updated_count > 0 LOOP
--         UPDATE papers
--         SET work_id = 'W' || gen_random_uuid()::text
--         WHERE paper_id IN (
--             SELECT paper_id FROM papers
--             WHERE work_id IS NULL
--             LIMIT batch_size
--         );
--
--         GET DIAGNOSTICS updated_count = ROW_COUNT;
--         total_updated := total_updated + updated_count;
--
--         RAISE NOTICE '已更新 % 条记录，总计 % 条', updated_count, total_updated;
--
--         -- 避免锁表，每次更新后短暂暂停
--         PERFORM pg_sleep(0.1);
--     END LOOP;
--
--     RAISE NOTICE '✅ work_id 生成完成！总计更新 % 条记录', total_updated;
-- END $$;

-- ============================================================================
-- Step 4: 设置 NOT NULL 约束（可选，仅在确认所有记录都有 work_id 后执行）
-- ============================================================================
-- 说明：如果执行了 Step 3，可以安全地设置 NOT NULL 约束
-- 取消注释以下代码：

-- ALTER TABLE papers ALTER COLUMN work_id SET NOT NULL;

-- ============================================================================
-- Step 5: 创建辅助索引（可选，用于优化查询性能）
-- ============================================================================
-- 说明：如果经常通过 work_id 查询，可以创建 B-tree 索引
-- 注意：由于已经有唯一索引，此步骤可选（唯一索引本身也是 B-tree 索引）

-- CREATE INDEX IF NOT EXISTS idx_papers_work_id_lookup
-- ON papers(work_id) WHERE work_id IS NOT NULL;

-- ============================================================================
-- Step 6: 添加触发器（可选，自动生成 work_id）
-- ============================================================================
-- 说明：如果希望新插入的记录自动生成 work_id，可以创建触发器
-- 注意：应用层代码也会生成 work_id，触发器作为保底方案

-- CREATE OR REPLACE FUNCTION generate_work_id_if_null()
-- RETURNS TRIGGER AS $$
-- BEGIN
--     IF NEW.work_id IS NULL THEN
--         NEW.work_id := 'W' || gen_random_uuid()::text;
--     END IF;
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;

-- CREATE TRIGGER trigger_generate_work_id
--     BEFORE INSERT ON papers
--     FOR EACH ROW
--     EXECUTE FUNCTION generate_work_id_if_null();

-- ============================================================================
-- 验证迁移结果
-- ============================================================================
-- 查询 work_id 统计信息
SELECT
    COUNT(*) as total_papers,
    COUNT(work_id) as papers_with_work_id,
    COUNT(*) - COUNT(work_id) as papers_without_work_id,
    COUNT(DISTINCT work_id) as unique_work_ids
FROM papers;

-- 检查是否有重复的 work_id（应该返回 0）
SELECT work_id, COUNT(*) as count
FROM papers
WHERE work_id IS NOT NULL
GROUP BY work_id
HAVING COUNT(*) > 1;

-- 查看示例 work_id
SELECT paper_id, work_id, canonical_title
FROM papers
WHERE work_id IS NOT NULL
LIMIT 5;

-- ============================================================================
-- 回滚脚本（仅在需要回滚时执行）
-- ============================================================================
-- 警告：回滚将删除所有 work_id 数据！

-- DROP INDEX IF EXISTS idx_papers_work_id;
-- ALTER TABLE papers DROP COLUMN IF EXISTS work_id;
-- DROP TRIGGER IF EXISTS trigger_generate_work_id ON papers;
-- DROP FUNCTION IF EXISTS generate_work_id_if_null;

-- ============================================================================
-- 完成提示
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE '✅ work_id 字段迁移完成！';
    RAISE NOTICE '';
    RAISE NOTICE '后续步骤：';
    RAISE NOTICE '1. 验证迁移结果（执行 Step 7 中的查询）';
    RAISE NOTICE '2. 修改应用层代码（参考 docs/migrations/add_work_id_code_changes.md）';
    RAISE NOTICE '3. 更新 API 文档（使用 work_id 代替 paper_id 对外暴露）';
    RAISE NOTICE '4. 集成 Vector DB（使用 work_id 关联元数据和向量）';
    RAISE NOTICE '';
    RAISE NOTICE '迁移信息：';
    RAISE NOTICE '  - 迁移脚本: docs/migrations/add_work_id_migration.sql';
    RAISE NOTICE '  - 代码修改: docs/migrations/add_work_id_code_changes.md';
    RAISE NOTICE '  - 执行时间: %秒', EXTRACT(EPOCH FROM NOW());
    RAISE NOTICE '============================================================================';
END $$;
