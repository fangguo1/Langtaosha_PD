# work_id 字段代码修改指南

**版本**: v1.0
**日期**: 2026-04-15
**相关迁移脚本**: [add_work_id_migration.sql](./add_work_id_migration.sql)

---

## 📋 概述

本文档详细说明为支持 `work_id` 字段需要修改的所有 Python 文件及其具体修改内容。

### 修改文件列表

| 文件路径 | 修改类型 | 优先级 | 影响范围 |
|---------|---------|--------|---------|
| `src/docset_hub/storage/metadata_db.py` | 核心修改 | ⭐⭐⭐ | 所有数据库操作 |
| `src/docset_hub/metadata/db_mapper.py` | 映射修改 | ⭐⭐⭐ | 数据库 payload 生成 |
| `src/docset_hub/metadata/extractor.py` | 工具函数 | ⭐⭐ | UUID 生成（可能已存在） |
| `src/docset_hub/metadata/transformer.py` | 转发修改 | ⭐⭐ | 一键式转换接口 |
| `tests/metadata/test_*.py` | 测试更新 | ⭐ | 单元测试 |
| `src/docset_hub/api/*.py` | API 修改 | ⭐ | REST API 端点（如果存在） |

---

## 🔧 详细修改说明

### 1. `src/docset_hub/metadata/extractor.py`

**修改目的**: 添加/确保 UUID v7 生成函数存在

**修改位置**: 文件顶部（已存在，需确认）

**修改内容**:

```python
# 文件: src/docset_hub/metadata/extractor.py

import time
import uuid
from typing import Dict, Any

# ============================================================================
# 新增/确认：work_id 生成函数
# ============================================================================

def generate_work_id() -> str:
    """生成新的 work_id（UUID v7 格式）

    UUID v7 优势：
    - 时间有序（适合索引和排序）
    - 全局唯一（分布式安全）
    - 包含时间戳（便于调试和追溯）

    Returns:
        str: UUID v7 格式的 work_id，前缀 W
        示例: W019b73d6-1634-77d3-9574-b6014f85b118
    """
    def uuid_v7():
        """生成 UUID v7 格式的字符串"""
        ts_ms = int(time.time() * 1000)
        rand_a = uuid.uuid4().int & ((1 << 12) - 1)
        rand_b = uuid.uuid4().int & ((1 << 62) - 1)
        uuid_int = (ts_ms & ((1 << 48) - 1)) << 80
        uuid_int |= 0x7 << 76
        uuid_int |= rand_a << 64
        uuid_int |= 0x2 << 62
        uuid_int |= rand_b
        return str(uuid.UUID(int=uuid_int))

    return f"W{uuid_v7()}"

# ============================================================================
# 修改：extract_work_id 方法（如果存在）
# ============================================================================

@staticmethod
def extract_work_id(data: Dict[str, Any]) -> str:
    """提取 work_id

    Args:
        data: DocSet格式的数据字典

    Returns:
        str: work_id，如果不存在则返回空字符串

    Note:
        如果数据中没有 work_id，调用方应使用 generate_work_id() 生成
    """
    return data.get('work_id', '')
```

**测试**:

```python
# 测试 UUID v7 生成
work_id = generate_work_id()
assert work_id.startswith('W')
assert len(work_id) == 37  # W + 36 字符 UUID
print(f"Generated work_id: {work_id}")
```

---

### 2. `src/docset_hub/metadata/db_mapper.py`

**修改目的**: 在生成数据库 payload 时自动生成 work_id

**修改位置**: `_map_papers_table()` 方法中

**修改内容**:

```python
# 文件: src/docset_hub/metadata/db_mapper.py

from .extractor import generate_work_id

class MetadataDBMapper:
    # ... 其他代码 ...

    def _map_papers_table(self, record: NormalizedRecord) -> Dict[str, Any]:
        """映射归一化记录到 papers 表

        Args:
            record: 归一化记录

        Returns:
            Dict: papers 表数据字典
        """
        # ... 现有的映射逻辑 ...

        # =====================================================================
        # 新增：生成 work_id
        # =====================================================================
        # 说明：
        # - work_id 在这里生成，确保每条新记录都有全局唯一标识符
        # - 格式：W{uuid_v7}，例如 W019b73d6-1634-77d3-9574-b6014f85b118
        # - 用于：Vector DB 关联、API 对外接口、跨系统数据交换
        work_id = generate_work_id()

        papers_data = {
            # =================================================================
            # 新增字段
            # =================================================================
            'work_id': work_id,  # ← 新增

            # =================================================================
            # 现有字段（保持不变）
            # =================================================================
            'canonical_title': record.core.get('title'),
            'canonical_abstract': record.core.get('abstract'),
            'canonical_language': record.core.get('language'),
            'canonical_publisher': record.core.get('publisher'),
            'submitted_at': self._parse_timestamp(record.core.get('submitted_at')),
            'online_at': self._parse_timestamp(record.core.get('online_at')),
            'published_at': self._parse_timestamp(record.core.get('published_at')),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }

        return papers_data
```

**修改后的 DB Payload 结构**:

```python
{
    "papers": {
        "work_id": "W019b73d6-1634-77d3-9574-b6014f85b118",  # ← 新增
        "canonical_title": "...",
        "canonical_abstract": "...",
        # ... 其他字段 ...
    },
    "paper_sources": {...},
    "paper_source_metadata": {...},
    # ... 其他表 ...
}
```

---

### 3. `src/docset_hub/storage/metadata_db.py`

**修改目的**:
1. 在插入 papers 时包含 work_id
2. 添加通过 work_id 查询的方法
3. 支持通过 work_id 更新和删除

#### 3.1 修改 `_get_or_create_paper_from_payload()` 方法

**修改位置**: 文件中 `_get_or_create_paper_from_payload()` 方法

**修改内容**:

```python
# 文件: src/docset_hub/storage/metadata_db.py

def _get_or_create_paper_from_payload(
    self,
    conn: Connection,
    db_payload: Dict[str, Any]
) -> int:
    """创建新的 papers 记录

    Args:
        conn: SQLAlchemy 连接对象
        db_payload: 数据库 payload

    Returns:
        int: paper_id
    """
    papers_data = db_payload.get('papers', {})

    # =====================================================================
    # 修改：添加 work_id 字段
    # =====================================================================
    result = conn.execute(
        text("""
            INSERT INTO papers (
                work_id,  -- ← 新增
                canonical_title, canonical_abstract, canonical_language,
                canonical_publisher, submitted_at, online_at, published_at,
                created_at, updated_at
            ) VALUES (
                :work_id,  -- ← 新增
                :canonical_title, :canonical_abstract, :canonical_language,
                :canonical_publisher, :submitted_at, :online_at, :published_at,
                :created_at, :updated_at
            )
            RETURNING paper_id
        """),
        {
            # =================================================================
            # 新增参数
            # =================================================================
            "work_id": papers_data.get('work_id'),  # ← 新增

            # =================================================================
            # 现有参数（保持不变）
            # =================================================================
            "canonical_title": papers_data.get('canonical_title'),
            "canonical_abstract": papers_data.get('canonical_abstract'),
            "canonical_language": papers_data.get('canonical_language'),
            "canonical_publisher": papers_data.get('canonical_publisher'),
            "submitted_at": papers_data.get('submitted_at'),
            "online_at": papers_data.get('online_at'),
            "published_at": papers_data.get('published_at'),
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
    )

    return result.scalar()
```

#### 3.2 添加通过 work_id 查询的方法

**修改位置**: 文件末尾（在其他查询方法附近）

**修改内容**:

```python
# 文件: src/docset_hub/storage/metadata_db.py

# ============================================================================
# 新增方法：通过 work_id 查询
# ============================================================================

def get_paper_info_by_work_id(self, work_id: str) -> Optional[Dict[str, Any]]:
    """根据 work_id 获取论文完整信息

    Args:
        work_id: 论文的全局唯一标识符（UUID v7 格式）

    Returns:
        Optional[Dict]: 论文完整信息，包含所有 source 记录
        如果不存在则返回 None

    Example:
        >>> paper_info = metadata_db.get_paper_info_by_work_id(
        ...     "W019b73d6-1634-77d3-9574-b6014f85b118"
        ... )
        >>> print(paper_info['canonical_title'])
    """
    with self.engine.connect() as conn:
        # 通过 work_id 查询 paper_id
        result = conn.execute(
            text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
            {"work_id": work_id}
        )
        row = result.fetchone()

        if not row:
            return None

        paper_id = row[0]

        # 使用现有的 get_paper_info_by_paper_id 方法
        return self.get_paper_info_by_paper_id(paper_id)


def read_paper_by_work_id(self, work_id: str) -> Optional[Dict[str, Any]]:
    """读取完整论文数据（通过 work_id）

    这是 get_paper_info_by_work_id 的简化别名方法

    Args:
        work_id: 论文的 work_id

    Returns:
        Optional[Dict]: 完整论文数据
    """
    return self.get_paper_info_by_work_id(work_id)


def delete_paper_by_work_id(self, work_id: str) -> bool:
    """删除论文（通过 work_id）

    注意：由于外键约束设置了 ON DELETE CASCADE，
    删除 papers 表的记录会自动级联删除所有关联数据。

    Args:
        work_id: 论文的 work_id

    Returns:
        bool: 成功返回 True，不存在返回 False

    Example:
        >>> success = metadata_db.delete_paper_by_work_id(
        ...     "W019b73d6-1634-77d3-9574-b6014f85b118"
        ... )
    """
    with self.engine.connect() as conn:
        try:
            # 先检查论文是否存在
            result = conn.execute(
                text("SELECT paper_id FROM papers WHERE work_id = :work_id"),
                {"work_id": work_id}
            )
            row = result.fetchone()

            if not row:
                logging.warning(f"论文不存在: work_id={work_id}")
                return False

            # 删除论文（级联删除关联数据）
            result = conn.execute(
                text("DELETE FROM papers WHERE work_id = :work_id"),
                {"work_id": work_id}
            )

            deleted_count = result.rowcount
            conn.commit()

            if deleted_count > 0:
                logging.info(f"成功删除论文: work_id={work_id}")
                return True
            else:
                logging.warning(f"删除论文失败: work_id={work_id}")
                return False

        except Exception as e:
            conn.rollback()
            logging.error(f"删除论文时发生错误: work_id={work_id}, error={str(e)}")
            raise e


def get_papers_by_work_ids(
    self,
    work_ids: List[str],
    include_sources: bool = True
) -> List[Dict[str, Any]]:
    """批量获取论文信息（通过 work_id 列表）

    Args:
        work_ids: work_id 列表
        include_sources: 是否包含 source 记录（默认 True）

    Returns:
        List[Dict]: 论文列表

    Example:
        >>> work_ids = ["Wxxx", "Wyyy", "Wzzz"]
        >>> papers = metadata_db.get_papers_by_work_ids(work_ids)
    """
    if not work_ids:
        return []

    with self.engine.connect() as conn:
        # 查询所有匹配的 paper_id
        result = conn.execute(
            text("""
                SELECT paper_id
                FROM papers
                WHERE work_id = ANY(:work_ids)
            """),
            {"work_ids": work_ids}
        )
        paper_ids = [row[0] for row in result.fetchall()]

        # 批量获取完整信息
        papers = []
        for paper_id in paper_ids:
            if include_sources:
                paper_info = self.get_paper_info_by_paper_id(paper_id)
            else:
                # 只返回 papers 表数据
                result = conn.execute(
                    text("""
                        SELECT paper_id, work_id, canonical_title,
                               canonical_abstract, canonical_source_id
                        FROM papers
                        WHERE paper_id = :paper_id
                    """),
                    {"paper_id": paper_id}
                )
                row = result.fetchone()
                if row:
                    paper_info = {
                        'paper_id': row[0],
                        'work_id': row[1],
                        'canonical_title': row[2],
                        'canonical_abstract': row[3],
                        'canonical_source_id': row[4]
                    }
                else:
                    paper_info = None

            if paper_info:
                papers.append(paper_info)

        return papers
```

#### 3.3 修改 `search_by_condition()` 方法（可选）

**修改位置**: `search_by_condition()` 方法的返回结果中

**修改内容**:

```python
# 在 search_by_condition 方法的查询中添加 work_id
result = conn.execute(
    text("""
        SELECT
            paper_id,
            work_id,  -- ← 新增
            canonical_title,
            canonical_abstract,
            -- ... 其他字段 ...
        FROM papers
        WHERE {where_clause}
        ORDER BY online_at DESC
        LIMIT :limit
    """)
)
```

---

### 4. `src/docset_hub/metadata/transformer.py`

**修改目的**: 确保转换结果中包含 work_id

**修改位置**: `transform_file()` 和 `transform_dict()` 方法的返回结果中

**修改内容**:

```python
# 文件: src/docset_hub/metadata/transformer.py

class MetadataTransformer:
    # ... 其他代码 ...

    def transform_file(
        self,
        input_path: str,
        source_name: str
    ) -> TransformResult:
        """从文件转换

        Args:
            input_path: 输入文件路径
            source_name: 来源名称（langtaosha, biorxiv）

        Returns:
            TransformResult: 转换结果
        """
        start_time = time.time()

        try:
            # ... 现有的转换逻辑 ...

            # db_mapper._map_papers_table() 会自动生成 work_id
            db_payload = self.db_mapper.map_to_db_payload(normalized_record)

            # =================================================================
            # 新增：提取 work_id 到结果中
            # =================================================================
            work_id = db_payload['papers'].get('work_id')

            return TransformResult(
                success=True,
                db_payload=db_payload,
                upsert_key=upsert_key,
                execution_time=time.time() - start_time,
                work_id=work_id  # ← 新增到返回结果
            )

        except Exception as e:
            return TransformResult(
                success=False,
                error=str(e),
                execution_time=time.time() - start_time
            )

    def transform_dict(
        self,
        raw_payload: Dict[str, Any],
        source_name: str
    ) -> TransformResult:
        """从字典转换

        Args:
            raw_payload: 原始数据字典
            source_name: 来源名称

        Returns:
            TransformResult: 转换结果
        """
        # 类似修改，添加 work_id 到返回结果
        # ... 代码逻辑同上 ...
```

**修改 TransformResult 数据类**:

```python
# 文件: src/docset_hub/metadata/transformer.py

@dataclass
class TransformResult:
    """转换结果"""
    success: bool
    db_payload: Optional[Dict[str, Any]] = None
    upsert_key: Optional[Dict[str, Any]] = None
    execution_time: float = 0.0
    error: Optional[str] = None
    work_id: Optional[str] = None  # ← 新增字段
```

---

### 5. 测试文件更新

**文件**: `tests/metadata/test_db_mapper.py`

**新增测试**:

```python
# 文件: tests/metadata/test_db_mapper.py

def test_work_id_generation(db_mapper):
    """测试 work_id 生成"""
    # 准备测试数据
    normalized_record = create_test_normalized_record()

    # 映射到数据库 payload
    db_payload = db_mapper.map_to_db_payload(normalized_record)

    # 验证 work_id 存在且格式正确
    assert 'work_id' in db_payload['papers']
    work_id = db_payload['papers']['work_id']

    # 验证格式：W + 36字符 UUID
    assert work_id.startswith('W')
    assert len(work_id) == 37

    # 验证唯一性（多次生成应不同）
    db_payload_2 = db_mapper.map_to_db_payload(normalized_record)
    work_id_2 = db_payload_2['papers']['work_id']
    assert work_id != work_id_2


def test_work_id_uniqueness(metadata_db, sample_data):
    """测试 work_id 唯一性约束"""
    # 插入两条记录
    result_1 = metadata_db.insert_paper(sample_data[0]['db_payload'], sample_data[0]['upsert_key'])
    result_2 = metadata_db.insert_paper(sample_data[1]['db_payload'], sample_data[1]['upsert_key'])

    # 验证 work_id 不同
    paper_1 = metadata_db.get_paper_info_by_paper_id(result_1)
    paper_2 = metadata_db.get_paper_info_by_paper_id(result_2)

    assert paper_1['work_id'] != paper_2['work_id']
```

**文件**: `tests/db/test_metadata_db.py`

**新增测试**:

```python
# 文件: tests/db/test_metadata_db.py

def test_get_paper_by_work_id(metadata_db, sample_paper):
    """测试通过 work_id 查询"""
    # 插入测试数据
    paper_id = metadata_db.insert_paper(
        sample_paper['db_payload'],
        sample_paper['upsert_key']
    )

    # 获取 work_id
    paper = metadata_db.get_paper_info_by_paper_id(paper_id)
    work_id = paper['work_id']

    # 通过 work_id 查询
    paper_by_work_id = metadata_db.get_paper_info_by_work_id(work_id)

    # 验证结果
    assert paper_by_work_id is not None
    assert paper_by_work_id['paper_id'] == paper_id
    assert paper_by_work_id['work_id'] == work_id


def test_delete_paper_by_work_id(metadata_db, sample_paper):
    """测试通过 work_id 删除"""
    # 插入测试数据
    paper_id = metadata_db.insert_paper(
        sample_paper['db_payload'],
        sample_paper['upsert_key']
    )

    # 获取 work_id
    paper = metadata_db.get_paper_info_by_paper_id(paper_id)
    work_id = paper['work_id']

    # 删除
    success = metadata_db.delete_paper_by_work_id(work_id)
    assert success is True

    # 验证已删除
    deleted_paper = metadata_db.get_paper_info_by_paper_id(paper_id)
    assert deleted_paper is None
```

---

## 📊 修改影响范围

### 数据库层面

| 操作 | 影响 | 风险 |
|------|------|------|
| **INSERT** | 需要包含 work_id 字段 | 低（有默认值或触发器保底） |
| **SELECT** | 可通过 work_id 查询 | 无 |
| **UPDATE** | work_id 不可修改（唯一约束） | 低（应用层控制） |
| **DELETE** | 可通过 work_id 删除 | 无 |

### 应用层面

| 模块 | 影响 | 修改工作量 |
|------|------|-----------|
| **Metadata Pipeline** | 需生成 work_id | 中（3 个文件） |
| **Database Layer** | 需支持 work_id 查询 | 中（1 个文件） |
| **API Layer** | 建议使用 work_id 对外暴露 | 低（可选） |
| **Vector DB 集成** | 使用 work_id 关联 | 低（新功能） |

### 兼容性

| 场景 | 兼容性 | 说明 |
|------|--------|------|
| **现有代码** | ✅ 完全兼容 | work_id 允许 NULL，不影响现有逻辑 |
| **现有数据** | ✅ 兼容 | 迁移脚本为现有记录生成 work_id |
| **API 调用** | ✅ 兼容 | 可逐步迁移到 work_id |
| **Vector DB** | ✅ 原生支持 | work_id 专为 Vector DB 设计 |

---

## 🚀 实施步骤

### 阶段 1：数据库迁移（1天）

1. ✅ 执行迁移脚本 `add_work_id_migration.sql`
2. ✅ 验证迁移结果（所有记录都有 work_id）
3. ✅ 备份数据库

### 阶段 2：代码修改（2-3天）

1. ✅ 修改 `extractor.py`（确认 UUID 生成函数）
2. ✅ 修改 `db_mapper.py`（在 payload 中生成 work_id）
3. ✅ 修改 `metadata_db.py`（支持 work_id 查询）
4. ✅ 修改 `transformer.py`（返回 work_id）
5. ✅ 更新测试文件

### 阶段 3：测试验证（1-2天）

1. ✅ 单元测试（test_db_mapper, test_metadata_db）
2. ✅ 集成测试（完整流程测试）
3. ✅ 性能测试（查询性能对比）

### 阶段 4：API 和 Vector DB 集成（1-2天）

1. ✅ 更新 API 端点（使用 work_id 对外暴露）
2. ✅ 集成 Vector DB（使用 work_id 关联）
3. ✅ 更新文档

---

## 🔍 验证检查清单

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

## 📞 支持

如有问题，请参考：
1. 迁移脚本: `docs/migrations/add_work_id_migration.sql`
2. 测试用例: `tests/metadata/test_db_mapper.py`
3. 数据库 Schema: `database/schema.sql`

---

**最后更新**: 2026-04-15
**维护者**: Claude Code
