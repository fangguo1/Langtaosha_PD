# work_id 字段迁移文档

**版本**: v1.0
**日期**: 2026-04-15
**状态**: 待执行

---

## 📋 概述

本次迁移为 `papers` 表添加 `work_id` 字段，用于：

1. ✅ **Vector DB 集成** - 全局唯一标识符关联元数据和向量
2. ✅ **跨系统数据迁移** - 自增 ID 在多数据库环境下会冲突
3. ✅ **API 安全性** - 避免暴露内部数据量（不使用自增 ID 对外）
4. ✅ **分布式系统支持** - UUID 天然适合分布式环境

---

## 📁 文档清单

| 文件 | 说明 | 用途 |
|------|------|------|
| [`add_work_id_migration.sql`](./add_work_id_migration.sql) | 数据库迁移脚本 | 执行 SQL 添加 work_id 字段 |
| [`add_work_id_code_changes.md`](./add_work_id_code_changes.md) | 代码修改指南 | 详细的 Python 代码修改说明 |
| [`README.md`](./README.md) | 本文档 | 迁移概述和快速开始 |

---

## 🚀 快速开始

### 1. 数据库迁移（5分钟）

```bash
# 执行迁移脚本
psql -U your_user -d your_database -f docs/migrations/add_work_id_migration.sql

# 验证迁移结果
psql -U your_user -d your_database -c "
SELECT
    COUNT(*) as total_papers,
    COUNT(work_id) as papers_with_work_id
FROM papers;
"
```

**预期输出**:
```
 total_papers | papers_with_work_id
--------------+---------------------
        10000 |                10000
```

### 2. 代码修改（2-3小时）

```bash
# 查看详细的代码修改指南
cat docs/migrations/add_work_id_code_changes.md
```

**需要修改的文件**:
1. `src/docset_hub/metadata/extractor.py` - UUID 生成函数（可能已存在）
2. `src/docset_hub/metadata/db_mapper.py` - 在 payload 中生成 work_id
3. `src/docset_hub/storage/metadata_db.py` - 支持 work_id 查询
4. `src/docset_hub/metadata/transformer.py` - 返回 work_id
5. `tests/metadata/test_*.py` - 更新测试用例

### 3. 验证测试（30分钟）

```bash
# 运行单元测试
pytest tests/metadata/test_db_mapper.py -v
pytest tests/db/test_metadata_db.py -v

# 运行集成测试
pytest tests/metadata/test_integration.py -v
```

---

## 📊 work_id vs paper_id 对比

| 特性 | paper_id (自增) | work_id (UUID v7) |
|------|----------------|-------------------|
| **唯一性范围** | 单数据库 | 全局（跨数据库、跨系统） |
| **格式** | 整数 (1, 2, 3, ...) | W{UUID} (W019b73d6-...) |
| **长度** | 4-8 bytes | 37 bytes |
| **索引性能** | ⭐⭐⭐ 最优 | ⭐⭐ 良好（UUID v7 有序） |
| **分布式支持** | ❌ 需要协调 | ✅ 天然支持 |
| **API 安全性** | ❌ 暴露数据量 | ✅ 不暴露 |
| **Vector DB 集成** | ⚠️ 需要映射 | ✅ 直接关联 |
| **数据迁移** | ❌ ID 会冲突 | ✅ 依然唯一 |

---

## 🎯 使用场景

### 场景 1: Vector DB 集成

```python
# 存储向量
vector_db.insert({
    "id": str(uuid.uuid4()),
    "work_id": "W019b73d6-1634-77d3-9574-b6014f85b118",  # ← 使用 work_id
    "vector": embedding,
    "metadata": {"title": "论文标题"}
})

# 查询向量
results = vector_db.search(query_vector, top_k=10)
work_ids = [r["work_id"] for r in results]

# 通过 work_id 批量获取元数据
papers = metadata_db.get_papers_by_work_ids(work_ids)
```

### 场景 2: API 对外接口

```python
# ❌ 旧方式（暴露自增 ID）
GET /api/papers/1
GET /api/papers/2
# 问题：攻击者可以推断总数据量

# ✅ 新方式（使用 UUID）
GET /api/papers/W019b73d6-1634-77d3-9574-b6014f85b118
# 优势：无法推断系统状态
```

### 场景 3: 跨系统数据迁移

```python
# 导出数据
papers = metadata_db.search_by_condition(limit=10000)
export_data = [
    {
        "work_id": p["work_id"],  # ← 全局唯一
        "title": p["canonical_title"],
        "abstract": p["canonical_abstract"]
    }
    for p in papers
]

# 导入到另一个数据库（work_id 依然唯一）
for paper_data in export_data:
    new_db.insert(paper_data)  # work_id 不会冲突
```

---

## ⚠️ 注意事项

### 1. 索引性能

UUID v7 是**时间有序**的，索引性能接近自增 ID：

```python
# UUID v7 格式
W019b73d6-1634-77d3-9574-b6014f85b118
 ↑----↑
 时间戳  (前48位是毫秒级时间戳)
```

**优势**:
- 新生成的 UUID 有序插入
- 索引页分裂少
- 查询性能接近自增 ID

### 2. 存储开销

每条记录增加约 **20-30 bytes**：

```sql
-- VARCHAR(200) 实际存储空间
-- 'W' + 36 字符 UUID = 37 bytes
-- PostgreSQL 内部压缩后约 20-30 bytes
```

**影响评估**:
- 10 万条记录：约 +2-3 MB
- 100 万条记录：约 +20-30 MB
- **影响可忽略**

### 3. 向后兼容

✅ **完全兼容现有代码**:

```python
# 现有查询方式（继续支持）
paper = metadata_db.get_paper_info_by_paper_id(123)

# 新增查询方式（推荐）
paper = metadata_db.get_paper_info_by_work_id("W019b73d6-...")
```

---

## 📅 实施时间表

| 阶段 | 任务 | 预计时间 | 负责人 |
|------|------|---------|--------|
| **阶段 1** | 数据库迁移 | 1天 | DBA |
| **阶段 2** | 代码修改 | 2-3天 | 开发 |
| **阶段 3** | 测试验证 | 1-2天 | 测试 |
| **阶段 4** | API 和 Vector DB 集成 | 1-2天 | 开发 |
| **总计** | - | **5-8天** | - |

---

## ✅ 验证清单

### 数据库验证

- [ ] papers 表有 work_id 字段
- [ ] idx_papers_work_id 唯一索引存在
- [ ] 所有现有记录都有 work_id
- [ ] 无重复的 work_id

### 代码验证

- [ ] extractor.py 有 generate_work_id() 函数
- [ ] db_mapper.py 生成的 payload 包含 work_id
- [ ] metadata_db.py 支持通过 work_id 查询
- [ ] transformer.py 返回结果包含 work_id

### 功能验证

- [ ] 插入新记录时自动生成 work_id
- [ ] 可通过 work_id 查询论文
- [ ] 可通过 work_id 删除论文
- [ ] work_id 全局唯一（无重复）

---

## 🔄 回滚方案

如果迁移出现问题，可以回滚：

```sql
-- 回滚数据库修改
DROP INDEX IF EXISTS idx_papers_work_id;
ALTER TABLE papers DROP COLUMN IF EXISTS work_id;

-- 恢复代码版本
git checkout HEAD -- src/docset_hub/metadata/extractor.py
git checkout HEAD -- src/docset_hub/metadata/db_mapper.py
git checkout HEAD -- src/docset_hub/storage/metadata_db.py
git checkout HEAD -- src/docset_hub/metadata/transformer.py
```

---

## 📞 支持

如有问题，请参考：
1. **数据库迁移**: `docs/migrations/add_work_id_migration.sql`
2. **代码修改**: `docs/migrations/add_work_id_code_changes.md`
3. **测试用例**: `tests/metadata/test_db_mapper.py`
4. **数据库 Schema**: `database/schema.sql`

---

## 📚 相关文档

- [Metadata Pipeline 文档](../src/docset_hub/metadata/README.md)
- [MetadataDB 文档](../src/docset_hub/storage/METADATA_DB_README.md)
- [数据库 Schema](../database/schema.md)

---

**最后更新**: 2026-04-15
**维护者**: Claude Code
**状态**: 待审核
