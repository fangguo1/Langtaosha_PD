# VectorDB 模块使用指南

本模块提供基于腾讯云 VectorDB 的语义检索能力，使用服务端自动 embedding 模式。
**位置**: `src/docset_hub/storage/vector_db.py`  
---

## 目录

- [快速开始](#快速开始)
- [架构设计](#架构设计)
- [配置说明](#配置说明)
- [API 参考](#api-参考)
- [使用示例](#使用示例)
- [最佳实践](#最佳实践)
- [常见问题](#常见问题)

---

## 快速开始

### 安装依赖

```bash
pip install requests pyyaml
```

### 基本使用

```python
from pathlib import Path
from src.docset_hub.storage.vector_db import VectorDB

# 1. 使用配置文件初始化
config_path = Path("src/config/config_tecent_backend_server_test.yaml")
vector_db = VectorDB(config_path=config_path)

# 2. 添加文档
vector_db.add_document(
    source_name="biorxiv_history",
    work_id="work_123",
    text="论文标题和摘要",
    text_type="abstract"
)

# 3. 搜索文档（方法1：使用统一入口）
results = vector_db.search(
    query="机器学习算法",
    source_list=["biorxiv_history"],
    top_k=10,
    search_type="dense"
)

# 或者（方法2：直接调用 dense_search）
# results = vector_db.dense_search(
#     query="机器学习算法",
#     source_list=["biorxiv_history"],
#     top_k=10
# )

for result in results:
    print(f"work_id: {result.work_id}, score: {result.score}")
```

---

## 架构设计

### 两层架构

```
┌─────────────────────────────────────────┐
│           VectorDB (业务层)              │
│  - Source 管理                           │
│  - Collection 映射                       │
│  - 业务逻辑封装                          │
└──────────────┬──────────────────────────┘
               │
               │ 调用
               │
┌──────────────▼──────────────────────────┐
│      VectorDBClient (HTTP 适配层)        │
│  - 腾讯云 API 封装                       │
│  - 认证和错误处理                        │
│  - 请求/响应解析                         │
└──────────────┬──────────────────────────┘
               │
               │ HTTP
               │
┌──────────────▼──────────────────────────┐
│     腾讯云 VectorDB 服务                 │
│  - 服务端 Embedding                      │
│  - 向量索引和检索                        │
└─────────────────────────────────────────┘
```

### 设计原则

1. **配置驱动**: 所有配置来自 YAML 文件，不直接使用环境变量
2. **Source 隔离**: 一个 source 对应一个 collection
3. **最小必要字段**: 只保存检索需要的字段
4. **统一入口**: 使用 `init_config()` 和 `get_vector_db_config()`

---

## 配置说明

### 配置文件位置

```yaml
# src/config/config_tecent_backend_server_test.yaml

vector_db:
  # 基础连接信息
  url: "http://172.21.0.3:80"
  account: root
  api_key: your_api_key_here

  # Embedding 配置
  embedding_source: tecent_made  # 当前仅支持 tecent_made
  embedding_model: BAAI/bge-m3

  # 数据库和 Collection 配置
  database: langtaosha_test
  collection_prefix: "lt_"

  # 允许的 Source 列表
  allowed_sources:
    - biorxiv_history
    - biorxiv_daily
    - langtaosha
```

### 配置项说明

| 配置项 | 必需 | 说明 |
|-------|------|------|
| `url` | ✅ | VectorDB 服务 URL |
| `account` | ✅ | 账户名 |
| `api_key` | ✅ | API 密钥 |
| `embedding_source` | ✅ | Embedding 来源 (仅支持 `tecent_made`) |
| `embedding_model` | ✅ | Embedding 模型名称 |
| `database` | ✅ | 数据库名称 |
| `collection_prefix` | ❌ | Collection 名称前缀 (默认 `lt_`) |
| `allowed_sources` | ✅ | 允许的 source 列表 |

---

## API 参考

### VectorDB

业务层，管理 source 与 collection 映射。

#### 初始化

```python
def __init__(self, config_path: Optional[Path] = None)
```

**参数**:
- `config_path`: 配置文件路径（必需）

**异常**:
- `ValueError`: 配置文件未指定或配置不完整
- `NotImplementedError`: 使用了不支持的 `embedding_source`

#### ensure_database

```python
def ensure_database(self) -> bool
```

确保数据库存在，不存在则创建。

#### ensure_collection

```python
def ensure_collection(self, source_name: str) -> bool
```

确保 source 对应的 collection 存在。

**参数**:
- `source_name`: 来源名称（必须在 `allowed_sources` 中）

#### add_document

```python
def add_document(
    self,
    source_name: str,
    work_id: str,
    text: str,
    text_type: str = "abstract",
    paper_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> bool
```

添加文档到向量数据库。

**参数**:
- `source_name`: 来源名称
- `work_id`: 作品 ID（全局唯一标识符）
- `text`: 待索引的文本内容（通常是 title + abstract）
- `text_type`: 文本类型（默认 "abstract"）
- `paper_id`: 论文 ID（可选）
- `metadata`: 额外元数据（可选，暂未使用）

**返回**: `bool` - 成功返回 True

#### delete_document

```python
def delete_document(
    self,
    source_name: str,
    work_id: str,
    text_type: str = "abstract"
) -> bool
```

删除文档。

**参数**:
- `source_name`: 来源名称
- `work_id`: 作品 ID
- `text_type`: 文本类型（默认 "abstract"）

**返回**: `bool` - 成功返回 True

#### dense_search

```python
def dense_search(
    self,
    query: str,
    source_list: Optional[List[str]] = None,
    top_k: int = 10
) -> List[SearchResult]
```

稠密向量搜索（基于 embedding 的语义搜索）。

**参数**:
- `query`: 查询文本
- `source_list`: 来源列表（None 表示搜索所有允许的 source）
- `top_k`: 返回结果数量

**返回**: `List[SearchResult]` - 搜索结果列表

#### sparse_search

```python
def sparse_search(
    self,
    query: str,
    source_list: Optional[List[str]] = None,
    top_k: int = 10
) -> List[SearchResult]
```

稀疏向量搜索（基于 BM25 的关键词搜索）。

**注意**: 当前版本暂未实现，计划在后续版本支持。

**参数**:
- `query`: 查询文本
- `source_list`: 来源列表（None 表示搜索所有允许的 source）
- `top_k`: 返回结果数量

**返回**: `List[SearchResult]` - 搜索结果列表

**异常**: `NotImplementedError` - 当前版本暂不支持

#### search

```python
def search(
    self,
    query: str,
    source_list: Optional[List[str]] = None,
    top_k: int = 10,
    search_type: str = "dense"
) -> List[SearchResult]
```

统一搜索入口，支持多种搜索模式。

**参数**:
- `query`: 查询文本
- `source_list`: 来源列表（None 表示搜索所有允许的 source）
- `top_k`: 返回结果数量
- `search_type`: 搜索类型
    - `"dense"`: 稠密向量搜索（默认，基于 embedding）
    - `"sparse"`: 稀疏向量搜索（BM25，暂未实现）
    - `"hybrid"`: 混合搜索（暂未实现）

**返回**: `List[SearchResult]` - 搜索结果列表

### SearchResult

搜索结果数据类。

```python
@dataclass
class SearchResult:
    source_name: str      # 来源名称
    work_id: str          # 作品 ID
    score: float          # 相似度分数（0-1）
    text_type: str        # 文本类型
    paper_id: Optional[str] = None  # 论文 ID
```

---

## 使用示例

### 示例 1: 添加单篇文档

```python
from pathlib import Path
from src.docset_hub.storage.vector_db import VectorDB

# 初始化
vector_db = VectorDB(config_path=Path("src/config/config_tecent_backend_server_test.yaml"))

# 准备文本（使用 title + abstract）
title = "Deep Learning for Bioinformatics"
abstract = "This paper presents a novel approach..."
text = f"{title} {abstract}".strip()

# 添加文档
vector_db.add_document(
    source_name="biorxiv_history",
    work_id="W019b73d6-1634-77d3-9574-b6014f85b118",
    text=text,
    text_type="abstract",
    paper_id="10.1101/2024.01.01.123456"
)
```

### 示例 2: 批量添加文档

```python
from src.docset_hub.storage.vector_db import VectorDB

vector_db = VectorDB(config_path=Path("src/config/config_tecent_backend_server_test.yaml"))

# 假设有一批论文数据
papers = [
    {"work_id": "work_1", "title": "...", "abstract": "...", "paper_id": "..."},
    {"work_id": "work_2", "title": "...", "abstract": "...", "paper_id": "..."},
]

# 批量添加
for paper in papers:
    text = f"{paper['title']} {paper['abstract']}".strip()

    vector_db.add_document(
        source_name="biorxiv_history",
        work_id=paper['work_id'],
        text=text,
        text_type="abstract",
        paper_id=paper['paper_id']
    )
```

### 示例 3: 搜索文档

```python
from src.docset_hub.storage.vector_db import VectorDB

vector_db = VectorDB(config_path=Path("src/config/config_tecent_backend_server_test.yaml"))

# 方法1：使用统一搜索入口（推荐）
results = vector_db.search(
    query="machine learning algorithms for genomics",
    source_list=["biorxiv_history"],
    top_k=5,
    search_type="dense"
)

# 方法2：直接调用 dense_search
# results = vector_db.dense_search(
#     query="machine learning algorithms for genomics",
#     source_list=["biorxiv_history"],
#     top_k=5
# )

# 遍历结果
for result in results:
    print(f"Score: {result.score:.4f}")
    print(f"Work ID: {result.work_id}")
    print(f"Source: {result.source_name}")
    print(f"Text Type: {result.text_type}")
    print("---")
```

### 示例 4: 多 Source 搜索

```python
from src.docset_hub.storage.vector_db import VectorDB

vector_db = VectorDB(config_path=Path("src/config/config_tecent_backend_server_test.yaml"))

# 从多个 source 搜索
results = vector_db.search(
    query="CRISPR gene editing",
    source_list=["biorxiv_history", "biorxiv_daily", "langtaosha"],
    top_k=10,
    search_type="dense"
)

# 按来源分组
from collections import defaultdict
by_source = defaultdict(list)

for result in results:
    by_source[result.source_name].append(result)

# 输出每个 source 的结果
for source, items in by_source.items():
    print(f"\n{source}: {len(items)} results")
    for item in items[:3]:  # 每个 source 显示前 3 个
        print(f"  - {item.work_id}: {item.score:.4f}")
```

### 示例 5: 删除文档

```python
from src.docset_hub.storage.vector_db import VectorDB

vector_db = VectorDB(config_path=Path("src/config/config_tecent_backend_server_test.yaml"))

# 删除单个文档
vector_db.delete_document(
    source_name="biorxiv_history",
    work_id="work_123",
    text_type="abstract"
)
```

---

## 最佳实践

### 1. 文本构造

推荐使用 `title + abstract` 作为索引文本：

```python
# ✅ 推荐
text = f"{paper['title']} {paper['abstract']}".strip()

# ❌ 不推荐：只用 abstract
text = paper['abstract']
```

### 2. work_id 使用

使用全局唯一的 `work_id`（UUID v7 格式）：

```python
# ✅ 推荐：使用 UUID v7
work_id = "W019b73d6-1634-77d3-9574-b6014f85b118"

# ❌ 不推荐：使用简单自增 ID
work_id = "123"
```

### 3. Source 命名

使用清晰、具有描述性的 source 名称：

```python
# ✅ 推荐
source_name = "biorxiv_history"
source_name = "langtaosha"

# ❌ 不推荐：过于简单或模糊
source_name = "bio"
source_name = "source1"
```

### 4. 搜索优化

根据需要调整 `top_k` 参数：

```python
# 精确搜索：只需要最相关的几个结果
results = vector_db.search(query, top_k=5)

# 广泛搜索：需要更多候选
results = vector_db.search(query, top_k=50)
```

### 5. 错误处理

始终处理可能的异常：

```python
from src.docset_hub.storage.vector_db import VectorDB, VectorDBError

try:
    vector_db = VectorDB(config_path=config_path)
    vector_db.add_document(...)
except ValueError as e:
    print(f"配置错误: {e}")
except VectorDBError as e:
    print(f"向量库错误: {e}")
```

### 6. 搜索类型选择

理解不同搜索类型的适用场景：

```python
# 稠密向量搜索（当前支持）
# 适用场景：
# - 语义相似度搜索
# - 同义词、概念相关查询
# - 跨语言搜索
# - 需要理解查询意图的场景
results = vector_db.search(query, search_type="dense")

# 稀疏向量搜索（暂未实现）
# 计划适用场景：
# - 关键词精确匹配
# - 专业术语搜索
# - 特定实体名称查找
# - 需要精确词汇匹配的场景
# results = vector_db.search(query, search_type="sparse")

# 混合搜索（暂未实现）
# 计划适用场景：
# - 结合语义和关键词优势
# - 提高召回率和准确率
# - 复杂查询场景
# results = vector_db.search(query, search_type="hybrid")
```

---

## 常见问题

### Q1: 如何选择 embedding_model？

当前使用的模型是 `BAAI/bge-m3`，这是一个支持多语言的高性能 embedding 模型。除非有特殊需求，建议使用此模型。

### Q2: Collection 名称是如何生成的？

Collection 名称格式为：`{collection_prefix}{source_name}`

例如：
- `source_name = "biorxiv_history"` → Collection 名称 = `"lt_biorxiv_history"`
- `source_name = "langtaosha"` → Collection 名称 = `"lt_langtaosha"`

### Q3: 文档 ID 是如何生成的？

文档 ID 格式为：`{source_name}:{work_id}:{text_type}`

例如：
- `"biorxiv_history:work_123:abstract"`
- `"langtaosha:work_456:abstract"`

这种设计可以：
- 避免不同 source 之间的 ID 冲突
- 支持同一文档的多个文本类型（如 abstract, full_text）
- 便于删除和排错

### Q4: 如何处理没有 title 或 abstract 的文档？

当前实现要求文档必须有 title 和 abstract：

```python
if not title or not abstract:
    raise ValueError(f"数据缺少 title 或 abstract")
```

如果某些数据可能缺失，建议：

```python
# 方案 1: 跳过
if not title or not abstract:
    logging.warning(f"跳过数据: 缺少 title 或 abstract")
    continue

# 方案 2: 使用默认值
title = data.get('title', 'Untitled')
abstract = data.get('abstract', '')
text = f"{title} {abstract}".strip()
```

### Q5: 如何实现分页？

当前 API 不直接支持分页，但可以通过多次搜索实现：

```python
# 第一次搜索
results = vector_db.search(query, top_k=20)

# 假设页面大小为 10
page_size = 10
page_1 = results[:page_size]
page_2 = results[page_size:page_size*2]
```

### Q6: 是否支持自定义 filter？

当前版本不支持复杂的 filter DSL，仅支持按 source 分离。如果需要更复杂的过滤，建议：

1. 使用多个 collection（按不同维度）
2. 在搜索后对结果进行二次过滤

### Q7: 如何调试和查看日志？

VectorDB 使用 Python 标准的 `logging` 模块：

```python
import logging

# 启用详细日志
logging.basicConfig(level=logging.INFO)

# 或只查看 vector_db 的日志
logging.getLogger('src.docset_hub.storage.vector_db').setLevel(logging.DEBUG)
```

### Q8: embedding_source=local_made 何时支持？

当前版本仅支持 `tecent_made`（腾讯云服务端 embedding）。`local_made` 模式（本地生成向量）计划在后续版本实现。

### Q9: dense_search 和 sparse_search 有什么区别？

**稠密向量搜索 (Dense Search)**:
- 使用 embedding 模型将文本转换为高维向量
- 基于语义相似度进行检索
- 支持同义词、概念相关查询
- 适合理解查询意图的场景
- **当前版本已支持**

**稀疏向量搜索 (Sparse Search)**:
- 使用 BM25 算法基于词频统计
- 基于关键词精确匹配
- 适合专业术语、实体名称搜索
- **计划在后续版本实现**

**推荐使用方式**:
```python
# 当前使用稠密向量搜索
results = vector_db.search(query, search_type="dense")
# 或直接调用
results = vector_db.dense_search(query)

# 未来可以使用稀疏向量搜索
# results = vector_db.search(query, search_type="sparse")
# 或直接调用
# results = vector_db.sparse_search(query)
```

### Q10: 如何选择搜索类型？

根据查询场景选择：

| 场景 | 推荐搜索类型 | 示例 |
|------|------------|------|
| 语义相似度查询 | `dense` | "机器学习在生物学中的应用" |
| 同义词、概念查询 | `dense` | "基因编辑" → 找到 CRISPR 相关论文 |
| 关键词精确匹配 | `sparse` (暂未实现) | "CRISPR-Cas9" 精确匹配 |
| 专业术语搜索 | `sparse` (暂未实现) | "p53 gene mutation" |
| 综合查询 | `hybrid` (暂未实现) | 结合语义和关键词 |

---

## 相关文档

- [VectorDB 构建计划](../../docs/vector_db_building_plan_0415.md)
- [腾讯云 VectorDB 手册](../../docs/tencent_vectordb_embedding_manual.md)
- [实现完成报告](../../docs/vector_db_implementation_report_0415.md)

---

## 更新日志

### v1.1 (2026-04-20)

- ✅ 重构搜索接口
  - 将 `search` 重命名为 `dense_search`，明确语义为稠密向量搜索
  - 新增 `sparse_search` 接口（暂未实现，为 BM25 预留）
  - 新增统一 `search` 入口，支持 `search_type` 参数
- ✅ 完善文档，添加搜索类型说明和选择指南

### v1.0 (2026-04-15)

- ✅ 初始版本
- ✅ 支持 `tecent_made` embedding 模式
- ✅ 实现基本的 CRUD 操作
- ✅ 支持多 source 搜索
- ✅ 完整的单元测试

---

**维护者**: Langtaosha 开发团队
**最后更新**: 2026-04-15
