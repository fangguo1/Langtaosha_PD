# MetadataDB - 元数据库操作类

**位置**: `src/docset_hub/storage/metadata_db.py`  
**版本**: v2.0 (新架构 - 多源支持)  
**更新日期**: 2026-04-21

---

## 📋 概述

`MetadataDB` 是一个用于将 DocSet 格式的论文元数据存储到 PostgreSQL 数据库的操作类。新架构支持**多源论文管理**，即同一篇论文可以从多个来源（如 bioRxiv、LangTaoSha）获取。

### 核心特性

- ✅ **多源支持**: 一篇论文可对应多个来源记录
- ✅ **统一查重分流**: 先判定 `same_source / cross_source / no_match` 再执行写入
- ✅ **同 source 覆盖策略**: 基于 `version` + `online_at` 决定覆盖或跳过
- ✅ **Canonical 选择**: 自动或手动选择"最佳版本"
- ✅ **灵活的 Upsert**: 插入、更新、插入或更新三种操作模式

---

## 🎯 核心方法

### 1. insert_paper - 幂等性插入

```python
def insert_paper(
    db_payload: Dict[str, Any],
    upsert_key: Dict[str, Any]
) -> int:
    """确保论文存在（如果已存在则按同 source 策略覆盖或跳过）
    
    语义:
        - same_source 命中：比较 version/online_at，决定覆盖或跳过
        - cross_source/no_match：插入新的 source 记录（必要时新建 paper）
    
    返回: paper_id (现有或新建的)
    """
```

**使用场景**:
- 批量导入论文（可重复执行）
- 确保论文存在但不覆盖已有数据

**示例**:
```python
transformer = MetadataTransformer()
metadata_db = MetadataDB(config_path)

result = transformer.transform_file(file_path, "biorxiv")
paper_id = metadata_db.insert_paper(
    db_payload=result.db_payload,
    upsert_key=result.upsert_key
)
# 第一次执行：插入新记录
# 第二次执行：同 source 命中，依据 version/online_at 策略覆盖或跳过
```

---

### 2. update_paper - 强制更新

```python
def update_paper(
    db_payload: Dict[str, Any],
    upsert_key: Dict[str, Any],
    canonical_source_id: Optional[int] = None,
    auto_select_canonical: bool = True
) -> Optional[int]:
    """更新已存在的论文
    
    语义:
        - 仅 same_source 命中时更新
        - 非 same_source（cross_source/no_match）返回 None
    
    返回: 更新的 paper_id，如果不存在则返回 None
    """
```

**使用场景**:
- 更新已存在论文的数据
- 手动指定 canonical source

**示例**:
```python
result = transformer.transform_file(updated_file, "biorxiv")

paper_id = metadata_db.update_paper(
    db_payload=result.db_payload,
    upsert_key=result.upsert_key,
    auto_select_canonical=True  # 自动选择 canonical
)
```

---

### 3. upsert_paper - 插入或更新

```python
def upsert_paper(
    db_payload: Dict[str, Any],
    upsert_key: Dict[str, Any],
    canonical_source_id: Optional[int] = None,
    auto_select_canonical: bool = True
) -> int:
    """插入或更新论文
    
    语义:
        - same_source 命中：强制更新
        - cross_source/no_match：插入（必要时新建 paper）
    
    返回: 插入或更新的 paper_id
    """
```

**使用场景**:
- API 端点操作
- 不确定论文是否存在的场景

**示例**:
```python
@app.post("/papers")
def create_or_update_paper(file: UploadFile):
    result = transformer.transform_file(file, "biorxiv")
    paper_id = metadata_db.upsert_paper(
        db_payload=result.db_payload,
        upsert_key=result.upsert_key
    )
    return {"paper_id": paper_id}
```

---

## 🔍 查询方法

### get_paper_info_by_paper_id

```python
def get_paper_info_by_paper_id(paper_id: int) -> Optional[Dict[str, Any]]:
    """根据 paper_id 获取论文完整信息（包含所有 source 记录）"""
```

**返回结构**:
```python
{
    'paper_id': 1,
    'canonical_title': '论文标题',
    'canonical_abstract': '摘要',
    'canonical_source_id': 101,  # 主来源 ID
    'sources': [  # 所有来源记录
        {
            'paper_source_id': 101,
            'source_name': 'biorxiv',
            'online_at': '2026-04-01',
            ...
        },
        {
            'paper_source_id': 102,
            'source_name': 'langtaosha',
            'online_at': '2026-04-05',
            ...
        }
    ]
}
```

---

### read_paper

```python
def read_paper(paper_id: int) -> Optional[Dict[str, Any]]:
    """读取完整论文数据（包含所有关联信息）"""
```

**别名方法**: `get_paper_info_by_paper_id()` 的简化版本

---

### search_by_condition

```python
def search_by_condition(
    title: Optional[str] = None,
    author: Optional[str] = None,
    category: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """基于条件搜索论文"""
```

**示例**:
```python
# 搜索标题包含 "machine learning" 的论文
results = metadata_db.search_by_condition(
    title="machine learning",
    year=2026,
    limit=20
)
```

---

### delete_paper_by_paper_id

```python
def delete_paper_by_paper_id(paper_id: int) -> bool:
    """删除论文（级联删除所有关联数据）"""
```

**注意**: 由于外键约束设置了 `ON DELETE CASCADE`，删除论文会自动删除所有关联数据。

---

### 🔑 work_id 相关方法（新增）

**说明**: `work_id` 是全局唯一标识符（UUID v7 格式），用于：
- Vector DB 向量与元数据的关联
- 跨系统数据交换和迁移
- API 对外接口（避免暴露内部数据量）

#### get_paper_info_by_work_id

```python
def get_paper_info_by_work_id(work_id: str) -> Optional[Dict[str, Any]]:
    """根据 work_id 获取论文完整信息（包含所有 source 记录）

    Args:
        work_id: 论文的全局唯一标识符（UUID v7 格式）
            例如：W019b73d6-1634-77d3-9574-b6014f85b118

    Returns:
        Optional[Dict]: 论文完整信息，包含所有 source 记录
            如果不存在则返回 None

    Example:
        >>> paper_info = metadata_db.get_paper_info_by_work_id(
        ...     "W019b73d6-1634-77d3-9574-b6014f85b118"
        ... )
        >>> print(paper_info['canonical_title'])
    """
```

**使用场景**:
- Vector DB 检索后获取完整元数据
- 跨系统数据查询
- API 对外接口（使用 work_id 而非 paper_id）

#### read_paper_by_work_id

```python
def read_paper_by_work_id(work_id: str) -> Optional[Dict[str, Any]]:
    """读取完整论文数据（通过 work_id）

    这是 get_paper_info_by_work_id 的简化别名方法

    Args:
        work_id: 论文的 work_id

    Returns:
        Optional[Dict]: 完整论文数据
    """
```

#### delete_paper_by_work_id

```python
def delete_paper_by_work_id(work_id: str) -> bool:
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
```

#### get_papers_by_work_ids

```python
def get_papers_by_work_ids(
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
```

**使用场景**:
- Vector DB 批量检索后批量获取元数据
- 跨系统数据批量查询
- 性能优化（一次查询多条记录）

---

---

## 🛠️ 辅助方法（内部使用）

### 统一查重与分流（重点）

```python
def _resolve_match_by_identity(conn, upsert_key) -> Dict[str, Any]:
    """基于 identity bundle 判定命中类型"""
```

**判定顺序**：
1. `same_source`: 仅在当前 `source_name` 下匹配
2. `cross_source`: 在其他 source 下用通用标识符匹配
3. `no_match`: 未命中

**参与匹配的标识符**：
- 同 source：`source_record_id`、`doi`、`arxiv_id`、`pubmed_id`、`semantic_scholar_id`
- 跨 source：`doi`、`arxiv_id`、`pubmed_id`、`semantic_scholar_id`

---

### 统一执行主流程（重点）

```python
def _resolve_and_apply(
    db_payload, upsert_key, mode,
    canonical_source_id, auto_select_canonical
) -> Optional[int]:
    """统一写入主流程"""
```

**固定流程**：
1. 校验 source 一致性
2. 统一判定命中类型（`same_source/cross_source/no_match`）
3. 按 `mode` 执行写入策略（insert/update/upsert）
4. 执行 canonical 处理（自动或手动）

### Canonical Source 选择

```python
def _set_canonical_source_by_online_at(conn, paper_id: int) -> None:
    """根据 online_at 时间法则设置 canonical_source
    
    法则: 选择 online_at 最晚的 source 作为 canonical
    """

def _set_canonical_source_by_user(conn, paper_id: int, canonical_source_id: int) -> None:
    """根据用户指定设置 canonical_source"""
```

**重算时机**：
- 同 source 覆盖后
- 跨 source 追加后
- `update/upsert` 走自动 canonical 时

**默认规则**：选择 `online_at` 最晚的 source 作为 canonical

### 数据库操作

```python
def _get_or_create_paper_from_payload(conn, db_payload) -> int:
    """创建新的 papers 记录"""

def _insert_source_record_from_payload(conn, paper_id, db_payload) -> int:
    """插入新的 paper_sources 记录"""

def _update_source_record_from_payload(conn, paper_source_id, db_payload) -> None:
    """更新 paper_sources 记录"""

def _upsert_source_metadata_from_payload(conn, paper_source_id, db_payload) -> None:
    """插入或更新 paper_source_metadata 记录"""
```

## 📊 数据模型

### 新架构表结构

```
papers (统一视图)
├── paper_id (主键，内部使用)
├── work_id (全局唯一标识符，UUID v7) ← 新增！
├── canonical_title
├── canonical_abstract
├── canonical_source_id (外键 → paper_sources)
└── ...

paper_sources (多来源记录)
├── paper_source_id (主键)
├── paper_id (外键 → papers)
├── source_name (来源名称: biorxiv, langtaosha)
├── source_record_id (来源 ID)
├── version (来源版本号，用于同 source 覆盖判定)
├── online_at (上线时间，用于 canonical 选择)
└── ...

paper_source_metadata (原始元数据)
├── paper_source_id (外键 → paper_sources)
├── raw_metadata_json (原始 JSON)
├── normalized_json (规范化 JSON)
└── ...
```

### 🆔 ID 关系总览

```
┌─────────────────────────────────────────────────────────────┐
│                        论文实体                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ papers 表                                              │  │
│  │                                                        │  │
│  │  paper_id (主键)    work_id (全局唯一)                │  │
│  │  ┌─────────┐       ┌──────────────────┐              │  │
│  │  │   1     │ ┄┄┄┄▶ │ W019b7...-b118   │              │  │
│  │  └─────────┘       └──────────────────┘              │  │
│  │       │                                         ▲     │  │
│  │       │                                         │     │  │
│  │       ▼                                         │     │  │
│  │  canonical_source_id (当前主来源指针)           │     │  │
│  │  ┌─────────┐                                    │     │  │
│  │  │   101   │ ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄│     │  │
│  │  └─────────┘                            同一   │     │  │
│  │       │                                  论文   │     │  │
│  │       ▼                                         │     │  │
│  │  ┌──────────────────────────────────────────┐ │     │  │
│  │  │ paper_sources 表 (来源记录)              │ │     │  │
│  │  │                                           │ │     │  │
│  │  │  paper_source_id  source_name  online_at  │ │     │  │
│  │  │  ┌───────────┐     ┌─────┐    ┌─────┐   │ │     │  │
│  │  │  │   101     │     │biorxiv│   │2026-│   │ │     │  │
│  │  │  └───────────┘     └─────┘    │04-01│   │ │     │  │
│  │  │                                 └─────┘   │ │     │  │
│  │  │  ┌───────────┐     ┌─────┐    ┌─────┐   │ │     │  │
│  │  │  │   102     │     │langta │   │2026-│   │ │     │  │
│  │  │  └───────────┘     └─────┘    │04-05│   │ │     │  │
│  │  │                                 └─────┘   │ │     │  │
│  │  └──────────────────────────────────────────┘ │     │  │
│  └──────────────────────────────────────────────┘     │  │
└─────────────────────────────────────────────────────────┘
```

**核心关系**：
- **1 篇论文** → **1 个 paper_id** → **1 个 work_id** → **N 个 paper_source_id**
- **canonical_source_id** 指向 N 个来源中的"当前主来源"（默认为 `online_at` 最晚）

### 多 `paper_source` 的写入语义

- 同 source 命中：
  - `insert_paper`: 按 `version -> online_at` 决定覆盖或跳过
  - `update_paper`: 强制更新（仅命中时）
  - `upsert_paper`: 强制更新
- 跨 source 命中：
  - 追加一条新的 `paper_sources` 记录到已有 `paper_id`
  - 重算 `canonical_source_id`
- 未命中：
  - 新建 `papers` + 插入首条 `paper_sources`

---

### 🎯 各 ID 的职责边界

| ID | 职责 | 使用场景 | 稳定性 | 示例 |
|----|------|----------|--------|------|
| **paper_id** | 论文主实体的内部标识 | 数据库关联、内部查询 | ⚠️ 迁移时可能变化 | `1, 2, 3` |
| **work_id** | 对外稳定标识符 | API 接口、Vector DB、跨系统集成 | ✅ 永久不变 | `W019b73d6-1634-77d3-9574-b6014f85b118` |
| **paper_source_id** | 单个来源记录的标识 | 来源级别操作、元数据查询 | ⚠️ 重建来源时变化 | `101, 102` |
| **canonical_source_id** | "当前主来源"的指针 | 获取最新/最权威版本、默认显示 | 🔄 可动态调整 | `101` 或 `102` |

---

#### 职责边界详解

**🔹 paper_id - 论文主实体**
- **定位**：内部数据库主键
- **用途**：
  - 数据库表间关联（外键）
  - 内部查询和操作
- **限制**：
  - ⚠️ **不用于 API 对外接口**（暴露系统规模）
  - ⚠️ **不用于跨系统**（可能冲突）
  - ⚠️ **迁移时可能变化**（不是稳定标识）

---

**🔹 work_id - 对外稳定标识符**
- **定位**：全局唯一、永久的论文标识
- **用途**：
  - ✅ **API 对外接口**（避免暴露 paper_id）
  - ✅ **Vector DB 关联**（向量与元数据绑定）
  - ✅ **跨系统数据交换**（分布式友好）
  - ✅ **数据迁移/合并**（不会冲突）
- **特性**：
  - UUID v7 格式（时间有序 + 全局唯一）
  - 一旦生成永久不变
  - 可直接对外暴露

---

**🔹 paper_source_id - 来源记录标识**
- **定位**：单个来源记录的主键
- **用途**：
  - 标识特定来源的论文记录（如 biorxiv 版本、langtaosha 版本）
  - 来源级别的操作（更新、删除某来源）
  - 元数据查询（paper_source_metadata）
- **限制**：
  - ⚠️ **不表示论文唯一性**（同一篇论文可有多个 source_id）
  - ⚠️ **重建来源时变化**（如重新抓取）

---

**🔹 canonical_source_id - 主来源指针**
- **定位**：指向"当前主来源"的指针
- **用途**：
  - 获取论文的"最佳版本"（通常是最新的）
  - API 默认显示的来源
  - 统一视图（canonical_title、canonical_abstract 来源）
- **选择规则**：
  - 默认：选择 `online_at` 最晚的 source
  - 手动：用户可指定特定的 source_id
- **特性**：
  - 🔄 **可动态调整**（新来源上线时自动切换）
  - 一篇论文同时只有 1 个 canonical_source

---

### work_id 的设计优势

| 场景 | 说明 | 优势 |
|------|------|------|
| **Vector DB 集成** | 向量与元数据关联 | 全局唯一，跨系统可用 |
| **跨系统数据交换** | 数据迁移、合并 | ID 不会冲突 |
| **API 对外接口** | 避免暴露内部数据量 | 不暴露系统规模 |
| **分布式系统** | 多数据中心部署 | 天然支持分布式 |

---

## 🎬 使用示例

### 示例 1: 批量导入论文

```python
from src.docset_hub.metadata.transformer import MetadataTransformer
from src.docset_hub.storage.metadata_db import MetadataDB

transformer = MetadataTransformer()
metadata_db = MetadataDB(config_path="config.yaml")

# 批量导入（幂等性，可重复执行）
for file_path in file_list:
    result = transformer.transform_file(file_path, source_name="biorxiv")
    paper_id = metadata_db.insert_paper(
        db_payload=result.db_payload,
        upsert_key=result.upsert_key
    )
    print(f"论文 ID: {paper_id}")
```

---

### 示例 2: 多源论文管理

```python
# 同一篇论文从 biorxiv 插入
result = transformer.transform_file(biorxiv_file, "biorxiv")
paper_id = metadata_db.insert_paper(result.db_payload, result.upsert_key)

# 同一篇论文从 langtaosha 插入（会识别为同一篇论文）
result = transformer.transform_file(langtaosha_file, "langtaosha")
paper_id_2 = metadata_db.insert_paper(result.db_payload, result.upsert_key)

# 两个返回相同的 paper_id，但有两个不同的 paper_source_id
assert paper_id == paper_id_2  # 同一篇论文
```

---

### 示例 3: 查询论文信息

```python
# 根据 paper_id 获取完整信息
paper_info = metadata_db.get_paper_info_by_paper_id(paper_id)

print(f"标题: {paper_info['canonical_title']}")
print(f"Canonical Source ID: {paper_info['canonical_source_id']}")

# 遍历所有来源
for source in paper_info['sources']:
    print(f"- {source['source_name']}: {source['online_at']}")
```

---

### 示例 4: 搜索论文

```python
# 搜索 2026 年的机器学习相关论文
results = metadata_db.search_by_condition(
    title="machine learning",
    year=2026,
    limit=10
)

for paper in results:
    print(f"{paper['canonical_title']} ({paper['online_at']})")
```

---

### 示例 5: 使用 work_id（新增）

```python
from docset_hub.metadata.transformer import MetadataTransformer
from docset_hub.storage.metadata_db import MetadataDB

transformer = MetadataTransformer()
metadata_db = MetadataDB(config_path="config.yaml")

# 转换并插入论文（自动生成 work_id）
result = transformer.transform_file("paper.json", "biorxiv")
paper_id = metadata_db.insert_paper(
    db_payload=result.db_payload,
    upsert_key=result.upsert_key
)

print(f"✅ 论文插入成功！")
print(f"   paper_id: {paper_id}")
print(f"   work_id: {result.work_id}")  # 全局唯一标识符

# 通过 work_id 查询论文
paper_info = metadata_db.get_paper_info_by_work_id(result.work_id)
print(f"标题: {paper_info['canonical_title']}")

# Vector DB 集成示例
vector_db.insert({
    "id": str(uuid.uuid4()),
    "work_id": result.work_id,  # ← 使用 work_id 关联
    "vector": embedding,
    "metadata": {"title": paper_info['canonical_title']}
})

# 批量查询示例
work_ids = [result.work_id, ...]
papers = metadata_db.get_papers_by_work_ids(work_ids)
```

---

## ⚠️ 重要注意事项

### 1. 必须使用 MetadataTransformer

新架构要求所有数据必须先通过 `MetadataTransformer` 转换：

```python
# ✅ 正确
transformer = MetadataTransformer()
result = transformer.transform_file(file_path, "biorxiv")
paper_id = metadata_db.insert_paper(result.db_payload, result.upsert_key)

# ❌ 错误（旧架构方式，已弃用）
paper_id = metadata_db.insert_paper(raw_data)
```

---

### 2. 操作语义区别

| 操作 | 存在时的行为 | 不存在时的行为 | 幂等性 |
|------|-------------|---------------|--------|
| `insert_paper` | 同 source 命中后按 version/online_at 覆盖或跳过 | 插入新记录（或跨 source 追加） | ✅ 是（面向导入） |
| `update_paper` | 仅 same_source 命中时强制更新 | 返回 None | ⚠️ 条件幂等 |
| `upsert_paper` | same_source 命中时强制更新 | 插入新记录（或跨 source 追加） | ⚠️ 条件幂等 |

---

### 2.1 操作状态表

> 建议在 `resolve + apply` 后输出状态码（而不只返回 `paper_id`），用于明确实际执行结果。

| mode | 状态码 (`status_code`) | resolve 命中类型 | apply 动作 | work_id 行为（当前实现） | 说明 |
|------|------------------------|------------------|-----------|--------------------------|------|
| `insert_paper` | `INSERT_NEW_PAPER` | `no_match` | `insert` | 新生成并写入 `papers.work_id` | 新建 `paper` 并插入 `source` |
| `insert_paper` | `INSERT_APPEND_SOURCE` | `cross_source` | `insert` | 保持不变（复用已有 `paper`） | 命中已有 `paper`，追加新 `source` |
| `insert_paper` | `INSERT_UPDATE_SAME_SOURCE` | `same_source` | `update` | 保持不变 | 同 `source` 命中且满足覆盖条件，执行更新 |
| `insert_paper` | `INSERT_SKIP_SAME_SOURCE` | `same_source` | `skip` | 保持不变 | 同 `source` 命中但不满足覆盖条件，跳过更新 |
| `update_paper` | `UPDATE_SAME_SOURCE` | `same_source` | `update` | 保持不变 | 仅在同 `source` 命中时更新 |
| `update_paper` | `UPDATE_NOT_ALLOWED_NON_SAME_SOURCE` | `cross_source/no_match` | `reject` | 保持不变（无写入） | 非同 `source` 命中，不执行更新 |
| `upsert_paper` | `UPSERT_NEW_PAPER` | `no_match` | `insert` | 新生成并写入 `papers.work_id` | 新建 `paper` 并插入 `source` |
| `upsert_paper` | `UPSERT_APPEND_SOURCE` | `cross_source` | `insert` | 保持不变（复用已有 `paper`） | 命中已有 `paper`，追加新 `source` |
| `upsert_paper` | `UPSERT_UPDATE_SAME_SOURCE` | `same_source` | `update` | 保持不变 | 同 `source` 命中时执行更新 |

> 备注：`work_id` 在新建 `paper` 时写入一次，后续更新与跨 source 追加都不再修改；数据库层另有唯一索引约束（全局唯一）。

---

### 2.2 操作返回值（结构化）

`insert_paper` / `update_paper` / `upsert_paper` 建议统一返回结构化结果（而非仅 `paper_id`）：

```python
{
    "ok": True,
    "mode": "insert|update|upsert",
    "status_code": "INSERT_SKIP_SAME_SOURCE",
    "paper_id": 123,
    "paper_source_id": 456,
    "resolve": {
        "match_type": "same_source|cross_source|no_match",
        "matched_paper_id": 123,
        "matched_paper_source_id": 456
    },
    "apply": {
        "action": "insert|update|skip|reject",
        "reason": "same_source_older_or_equal_version"
    },
    "canonical": {
        "strategy": "auto_online_at|manual|None",
        "before_canonical_source_id": 455,
        "canonical_source_id": 456,
        "changed": True
    }
}
```

字段说明：

| 字段 | 含义 |
|------|------|
| `ok` | 是否成功执行到业务返回（异常会直接抛出） |
| `mode` | 调用模式：`insert / update / upsert` |
| `status_code` | 操作状态码（见 2.1 操作状态表） |
| `paper_id` | 目标论文 ID；`update` 非同源拒绝时可能为 `None` |
| `paper_source_id` | 命中的同源 source 或新插入 source 的 ID |
| `resolve.match_type` | 命中类型：`same_source / cross_source / no_match` |
| `resolve.matched_paper_id` | resolve 阶段命中的 paper_id（若有） |
| `resolve.matched_paper_source_id` | resolve 阶段命中的同源 source_id（若有） |
| `apply.action` | 实际动作：`insert / update / skip / reject` |
| `apply.reason` | 动作原因（版本比较、命中类型等） |
| `canonical.strategy` | canonical 策略：自动/手动/未执行 |
| `canonical.before_canonical_source_id` | canonical 处理前 source_id |
| `canonical.canonical_source_id` | canonical 处理后 source_id |
| `canonical.changed` | canonical source 是否发生变化 |

---

### 3. Canonical Source 选择规则

**自动选择（基于 online_at）**:
- 选择 `online_at` 时间最晚的 source 作为 canonical
- 适用于大多数场景

**手动指定**:
```python
metadata_db.update_paper(
    db_payload, upsert_key,
    canonical_source_id=12345,  # 用户指定
    auto_select_canonical=False
)
```

---

## 🔄 旧架构兼容

### 保留的方法（向后兼容）

以下方法仍可使用，但不推荐：

```python
# 旧方法（仍可用）
get_or_create_category(conn, domain, subdomain) -> int
get_or_create_venue(conn, venue_name, venue_type) -> int
get_or_create_field(conn, field_name, field_name_en) -> int

# 新方法（推荐）
# 使用 MetadataTransformer 和新架构方法
```

---

## 📚 相关文档

- **去重重构计划**: [METADATA_DEDUP_REFACTOR_IMPLEMENTATION_PLAN.md](../METADATA_DEDUP_REFACTOR_IMPLEMENTATION_PLAN.md)
- **迁移计划**: [metadata_db_migration_plan_0414.md](../../docs/metadata_db_migration_plan_0414.md)
- **迁移报告**: [metadata_db_migration_completion_report_0415.md](../../docs/metadata_db_migration_completion_report_0415.md)
- **Transformer 文档**: [transformer.py](../metadata/transformer.py)
- **数据库 Schema**: [database_schema.md](../../docs/database_schema.md)

---

## 📞 支持

如有问题，请参考：
1. 测试用例: `tests/db/test_metadata_db.py`
2. 迁移文档: `docs/metadata_db_migration_plan_0414.md`
3. Issues: [GitHub Issues](https://github.com/your-repo/issues)

---

**最后更新**: 2026-04-21
