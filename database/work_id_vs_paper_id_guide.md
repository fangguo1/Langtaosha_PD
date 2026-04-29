# work_id vs paper_id 使用指南

## 📋 核心概念

### `paper_id` - 数据库内部标识符
- **类型**: `SERIAL PRIMARY KEY` (自增整数)
- **范围**: 仅在 PostgreSQL 数据库内部使用
- **特性**: 数据库主键，保证单表内的唯一性

### `work_id` - 全局唯一标识符  
- **类型**: `VARCHAR(200)` (UUID v7 格式，前缀 W)
- **范围**: 跨系统、跨数据库的全局唯一性
- **特性**: 全局唯一，支持分布式系统

**示例 work_id**: `W019b73d6-1634-77d3-9574-b6014f85b118`

---

## 🎯 使用场景对比

### 使用 `paper_id` 的场景

| 场景 | 原因 | 示例 |
|------|------|------|
| **数据库内部操作** | 性能最优，索引效率高 | `JOIN` 操作、内部查询 |
| **表间关联** | 外键约束要求整数类型 | `paper_author_affiliation.paper_id` → `papers.paper_id` |
| **批量数据库操作** | 整数比较更快 | 批量更新、批量删除 |
| **数据库事务处理** | 锁定行记录 | `SELECT FOR UPDATE` |

**代码示例**:
```python
# ✅ 正确：数据库内部操作使用 paper_id
def get_paper_info_by_paper_id(self, paper_id: int) -> Optional[Dict[str, Any]]:
    with self.engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM papers WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        return result.fetchone()

# ✅ 正确：表间关联使用 paper_id
CREATE TABLE paper_keywords (
    paper_id INTEGER REFERENCES papers(paper_id) ON DELETE CASCADE
);
```

### 使用 `work_id` 的场景

| 场景 | 原因 | 示例 |
|------|------|------|
| **对外 API 接口** | 避免暴露内部 ID 结构 | REST API、GraphQL |
| **跨系统数据交换** | 全局唯一性，避免冲突 | 数据迁移、系统对接 |
| **Vector DB 关联** | 向量与元数据的关联键 | `vector_db.metadata_id = work_id` |
| **分布式系统** | 支持多实例、多数据库 | 微服务架构 |
| **文件存储命名** | 人类可读的唯一标识 | `W019b...118.json` |
| **外部引用** | 便于调试和日志记录 | 日志文件、用户界面 |

**代码示例**:
```python
# ✅ 正确：API 接口使用 work_id
@app.get("/api/papers/{work_id}")
def get_paper_endpoint(work_id: str):
    paper_info = metadata_db.get_paper_info_by_work_id(work_id)
    return paper_info

# ✅ 正确：Vector DB 关联使用 work_id
def add_to_vector_db(work_id: str, content: str):
    vector_db.add_document(
        doc_id=work_id,  # 使用 work_id 作为文档 ID
        content=content
    )

# ✅ 正确：文件存储使用 work_id
def save_paper_to_file(data: Dict[str, Any]):
    work_id = data.get('work_id')
    file_path = storage_path / f"{work_id}.json"
    # W019b73d6-1634-77d3-9574-b6014f85b118.json
```

---

## 🔄 转换模式

### work_id → paper_id (常用)
```python
# 从 work_id 查找 paper_id
def work_id_to_paper_id(self, work_id: str) -> Optional[int]:
    with self.engine.connect() as conn:
        result = conn.execute(
            text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
            {"work_id": work_id}
        )
        row = result.fetchone()
        return row[0] if row else None

# 然后使用 paper_id 进行数据库操作
paper_id = work_id_to_paper_id(work_id)
authors = get_authors_by_paper_id(paper_id)
```

### paper_id → work_id (较少用)
```python
# 从 paper_id 查找 work_id
def paper_id_to_work_id(self, paper_id: int) -> Optional[str]:
    with self.engine.connect() as conn:
        result = conn.execute(
            text("SELECT work_id FROM papers WHERE paper_id = :paper_id"),
            {"paper_id": paper_id}
        )
        row = result.fetchone()
        return row[0] if row else None
```

---

## 🚨 常见错误

### ❌ 错误用法 1: API 暴露 paper_id
```python
# ❌ 错误：API 暴露内部 paper_id
@app.get("/api/papers/{paper_id}")
def get_paper_endpoint(paper_id: int):
    return metadata_db.get_paper_info_by_paper_id(paper_id)

# ✅ 正确：API 使用 work_id
@app.get("/api/papers/{work_id}")
def get_paper_endpoint(work_id: str):
    return metadata_db.get_paper_info_by_work_id(work_id)
```

### ❌ 错误用法 2: 外键使用 work_id
```sql
-- ❌ 错误：外键使用 VARCHAR 类型，性能差
CREATE TABLE paper_keywords (
    paper_id VARCHAR(200) REFERENCES papers(work_id)
);

-- ✅ 正确：外键使用 INTEGER 类型
CREATE TABLE paper_keywords (
    paper_id INTEGER REFERENCES papers(paper_id) ON DELETE CASCADE
);
```

### ❌ 错误用法 3: Vector DB 使用 paper_id
```python
# ❌ 错误：Vector DB 使用 paper_id
def add_to_vector_db(paper_id: int, content: str):
    vector_db.add_document(doc_id=str(paper_id), content=content)

# ✅ 正确：Vector DB 使用 work_id
def add_to_vector_db(work_id: str, content: str):
    vector_db.add_document(doc_id=work_id, content=content)
```

---

## 📊 性能对比

| 操作 | paper_id | work_id | 性能差异 |
|------|----------|---------|----------|
| 单条查询 | `WHERE paper_id = 123` | `WHERE work_id = 'W019b...'` | paper_id 快 20-30% |
| JOIN 操作 | `JOIN ON p1.paper_id = p2.paper_id` | `JOIN ON p1.work_id = p2.work_id` | paper_id 快 40-50% |
| 索引大小 | 4 bytes (INTEGER) | ~40 bytes (VARCHAR) | paper_id 索引更小 |
| 外键约束 | 整数比较，高效 | 字符串比较，较慢 | paper_id 约束检查更快 |

**结论**: 数据库内部操作优先使用 `paper_id` 以获得最佳性能。

---

## 🎯 最佳实践总结

### 1. 分层使用原则

```
┌─────────────────────────────────────────┐
│  外部层 (API, 文件系统, Vector DB)      │
│  使用: work_id                          │
├─────────────────────────────────────────┤
│  转换层 (service/business logic)        │
│  职责: work_id ↔ paper_id 转换          │
├─────────────────────────────────────────┤
│  内部层 (database operations, JOINs)    │
│  使用: paper_id                          │
└─────────────────────────────────────────┘
```

### 2. 函数命名规范

```python
# ✅ 正确的函数命名规范
def get_paper_by_work_id(work_id: str) -> Dict:     # 输入输出都用 work_id
def get_paper_by_paper_id(paper_id: int) -> Dict:   # 输入输出都用 paper_id

# ❌ 避免：混合使用
def get_paper_by_id(id: Union[int, str]) -> Dict:   # 类型不明确
```

### 3. 数据一致性

```python
# ✅ 正确：确保 work_id 和 paper_id 的一致性
def insert_paper(self, db_payload: Dict) -> int:
    # 1. 生成全局唯一的 work_id
    work_id = generate_work_id()
    
    # 2. 插入 papers 表，获得 paper_id
    paper_id = insert_into_papers(work_id=work_id, ...)
    
    # 3. 使用 paper_id 插入关联表
    insert_into_keywords(paper_id=paper_id, ...)
    insert_into_authors(paper_id=paper_id, ...)
    
    # 4. 返回 paper_id 供内部使用
    return paper_id
    
# 外部调用时通过 work_id 查询
work_id = "W019b73d6-1634-77d3-9574-b6014f85b118"
paper_info = get_paper_by_work_id(work_id)
```

---

## 🔧 当前项目中的应用

### 已实现的正确用法

1. **MetadataDB 类**:
```python
# ✅ 提供两套方法
get_paper_info_by_paper_id(paper_id: int)     # 内部使用
get_paper_info_by_work_id(work_id: str)       # 外部使用
delete_paper_by_paper_id(paper_id: int)       # 内部使用  
delete_paper_by_work_id(work_id: str)         # 外部使用
```

2. **JSON 文件存储**:
```python
# ✅ 使用 work_id 作为文件名
def save(self, data: Dict[str, Any]) -> Path:
    work_id = data.get('work_id')
    file_path = self.storage_path / f"{work_id}.json"
```

3. **数据库 Schema**:
```sql
-- ✅ 外键使用 paper_id
CREATE TABLE paper_keywords (
    paper_id INTEGER REFERENCES papers(paper_id) ON DELETE CASCADE
);

-- ✅ work_id 有唯一索引，保证查询性能
CREATE UNIQUE INDEX idx_papers_work_id 
ON papers(work_id) WHERE work_id IS NOT NULL;
```

### 需要注意的地方

在实现 `author_affiliation`, `keywords`, `references` 功能时：

- ✅ **数据库操作**: 使用 `paper_id` (外键关联)
- ✅ **API 接口**: 使用 `work_id` (对外展示)
- ✅ **Vector DB**: 使用 `work_id` (向量关联)

---

## 📚 相关文档

- [数据库 Schema](../database/schema.sql)
- [元数据完善计划](./metadata_keywords_author_reference_plan_0415.md)
- [VectorDB 构建计划](./vector_db_building_plan_0415.md)

---

**文档版本**: v1.0  
**创建日期**: 2025-04-15  
**最后更新**: 2025-04-15
