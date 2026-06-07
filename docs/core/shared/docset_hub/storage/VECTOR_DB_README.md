# VectorDB 模块使用指南

**位置**: `src/docset_hub/storage/vector_db.py`
**配套 HTTP Client**: `src/docset_hub/storage/vector_db_client.py`
**更新日期**: 2026-06-06

---

## 概述

本模块提供基于腾讯云 VectorDB 的检索能力，当前包含三条检索路径：

- dense search：服务端 embedding 语义检索
- sparse search：本地 BM25 编码后写入 sparse collection 的关键词检索
- hybrid search：Langtaosha 侧对 dense + sparse 结果做 RRF 融合

整体是两层架构：

- `VectorDB`：业务层，负责 source 管理、collection 映射、写入编排和检索路由
- `VectorDBClient`：HTTP 适配层，负责腾讯云 API 调用、认证、错误处理和响应解析

---

## 阅读顺序

本 README 按当前源码顺序和职责组织：

1. 核心数据结构与初始化
2. 配置项与运行约束
3. source / collection / doc_id 规则
4. 数据库与 collection 生命周期管理
5. 观测与管理接口
6. dense 文档写入
7. sparse 文档写入
8. 删除接口
9. dense / sparse / hybrid 检索
10. `VectorDBClient` HTTP 分层
11. 使用示例与注意事项

---

## 1. 核心数据结构与初始化

### `SearchResult`

```python
@dataclass
class SearchResult:
    source_name: str
    work_id: str
    score: float
    text_type: str
    paper_id: Optional[str] = None
    retrieval_debug: Optional[Dict[str, Any]] = None
```

字段含义：

- `source_name`: 命中文档所属 source
- `work_id`: 文档的稳定业务标识
- `score`: 检索分数
- `text_type`: 文本类型
- `paper_id`: 可选的论文 ID
- `retrieval_debug`: hybrid merge 等调试信息

### `VectorDB.__init__(config_path)`

初始化时会：

- 加载 `vector_db` 配置
- 创建 `VectorDBClient`
- 读取 dense / sparse / hybrid 运行参数
- 初始化 ensured collection 缓存
- 延迟初始化 BM25 sparse encoder

异常：

- `ValueError`: 缺少配置或配置不完整
- `NotImplementedError`: 使用了当前不支持的 `embedding_source`

---

## 2. 配置项与运行约束

配置来自 YAML：

```yaml
vector_db:
  url: "http://172.21.0.3:80"
  account: root
  api_key: your_api_key_here

  embedding_source: tecent_made
  embedding_model: BAAI/bge-m3

  database: langtaosha_test
  collection_prefix: "lt_"
  sparse_collection_prefix: "lt_bm25_"

  allowed_sources:
    - biorxiv_history
    - biorxiv_daily
    - langtaosha

  sparse:
    bm25_language: en
    max_non_zero: 1024
    terminate_after: 4000
    cutoff_frequency: 0.1

  hybrid:
    candidate_multiplier: 5
    min_candidate_k: 50
    rrf_k: 60
    dense_weight: 1.0
    sparse_weight: 1.0
```

关键约束：

- `embedding_source` 当前仅支持 `tecent_made`
- `local_made` 已被显式拒绝
- `allowed_sources` 必须非空
- `embedding_model` 必须配置

核心配置项：

| 配置项 | 必需 | 说明 |
|---|---|---|
| `url` | 是 | VectorDB 服务地址 |
| `account` | 是 | 账户名 |
| `api_key` | 是 | API 密钥 |
| `embedding_source` | 是 | 当前仅支持 `tecent_made` |
| `embedding_model` | 是 | dense embedding 模型 |
| `database` | 是 | 目标数据库名 |
| `collection_prefix` | 否 | dense collection 前缀，默认 `lt_` |
| `sparse_collection_prefix` | 否 | sparse collection 前缀，默认 `collection_prefix + "bm25_"` |
| `allowed_sources` | 是 | 允许写入和检索的 source 列表 |
| `sparse.*` | 否 | BM25 编码和检索参数 |
| `hybrid.*` | 否 | dense+sparse 融合参数 |

---

## 3. source / collection / doc_id 规则

### source 校验

```python
_validate_source(source_name)
```

所有 public API 基本都要求 `source_name` 必须属于 `allowed_sources`。

### collection 命名

```python
_get_collection_name(source_name)
_get_sparse_collection_name(source_name)
```

dense collection：

```text
{collection_prefix}{source_name}
```

sparse collection：

```text
{sparse_collection_prefix}{source_name}
```

例如：

- dense: `lt_biorxiv_history`
- sparse: `lt_bm25_biorxiv_history`

### doc_id 规则

```python
_generate_doc_id(source_name, work_id, text_type)
```

当前实现的真实语义是：

- `doc_id` 直接等于 `work_id`

也就是说，虽然函数签名保留了 `source_name` 和 `text_type`，但当前业务约束是：

- 一个向量文档与一个 `work_id` 一对一
- `doc_id` 不再拼接 `source_name:text_type`

这一点和旧文档认知不同，当前应以源码为准。

### sparse encoder

```python
_get_sparse_encoder()
```

BM25 encoder 采用 lazy-load：

- dense-only 路径不会强依赖 sparse 编码组件
- 首次 sparse 写入或 sparse 搜索时才初始化

---

## 4. 数据库与 collection 生命周期管理

### `ensure_database()`

确保数据库存在：

- 已存在则直接返回
- 不存在则创建

### `ensure_collection(source_name)`

确保 dense collection 存在：

- 先校验 source
- 先查 collection 是否存在
- 不存在则调用 `VectorDBClient.create_collection()`
- 内部用 `_ensured_collections` 做轻量缓存

### `ensure_sparse_collection(source_name)`

确保 sparse collection 存在：

- 与 dense collection 类似
- 实际调用 `VectorDBClient.create_sparse_collection()`

### `_document_exists(collection_name, doc_id, retries=3, retry_delay=0.5)`

内部 helper，用于：

- dense 写入前判断文档是 insert 还是 update
- 删除前判断文档是否存在

实现特点：

- 通过 `query_documents()` 查询
- 使用 `strongConsistency`
- 查询异常时会重试
- 最终失败时保守地返回“不存在”

---

## 5. 观测与管理接口

### `get_collection_info(source_name=None, collection_name=None)`

获取单个 collection 详细信息。

约束：

- `source_name` 和 `collection_name` 必须二选一

### `get_collection_list(with_info=False, source_list=None)`

列出 collection：

- 可仅返回名称
- 也可返回详细信息
- 可按 `source_list` 做过滤

### `get_vector_db_info()`

返回当前运行状态摘要：

- URL
- database
- database 是否存在
- dense/sparse collection 前缀
- allowed sources
- embedding source / model
- 当前 collection 列表

这组方法适合健康检查、调试和运维观察。

---

## 6. dense 文档写入

### `add_document(...) -> Dict[str, Any]`

```python
def add_document(
    source_name: str,
    work_id: str,
    text: str,
    text_type: str = "abstract",
    paper_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    skip_ensure_collection: bool = False,
) -> Dict[str, Any]:
```

职责：

- 可选自动确保 dense collection 存在
- 生成 `doc_id`
- 先判断文档是否已存在
- 调用 `upsert_documents()` 写入原始文本
- 由腾讯云服务端 embedding 建索引

当前写入的最小字段：

```python
{
    "id": doc_id,
    "text": text,
    "work_id": work_id,
    "source_name": source_name,
    "text_type": text_type,
    "paper_id": paper_id,  # 可选
}
```

返回结构：

```python
{
    "success": True,
    "action": "inserted|updated",
    "doc_id": "W...",
    "affected_count": 1,
}
```

说明：

- `metadata` 参数当前未实际写入
- `action` 来自写入前的 existence check

---

## 7. sparse 文档写入

### `add_sparse_document(...)`

单篇 sparse 文档写入接口，本质是对 `add_sparse_documents()` 的单条封装。

### `add_sparse_documents(source_name, documents, skip_ensure_collection=False)`

批量写入 BM25 sparse 文档。

每条 document 至少需要：

- `work_id`
- `text` 或预先计算好的 `sparse_vector`

流程：

1. 校验 source
2. 可选自动确保 sparse collection 存在
3. 对未提供 `sparse_vector` 的文档做 BM25 编码
4. 组装 sparse 文档
5. 调用 `upsert_documents()` 写入 sparse collection

写入到 sparse collection 的字段：

```python
{
    "id": work_id,
    "sparse_vector": [...],
    "work_id": work_id,
    "source_name": source_name,
    "text_type": "abstract",
    "paper_id": "...",  # 可选
}
```

注意：

- sparse collection 不保存完整 index text
- 它只保存检索所需字段和 `sparse_vector`

返回结构：

```python
{
    "success": True,
    "action": "upserted",
    "affected_count": 10,
    "document_count": 10,
}
```

---

## 8. 删除接口

### `delete_document(source_name, work_id, text_type='abstract')`

职责：

- 校验 source
- 基于 `work_id` 生成 `doc_id`
- 删除前检查文档是否存在
- 若不存在，则返回成功但 `deleted=False`
- 若存在，则调用 `delete_documents()`

返回结构：

```python
{
    "success": True,
    "deleted": True|False,
    "doc_id": "W...",
    "delete_count": 1,
}
```

当前删除的是 dense collection 中的文档；如果你同时维护 sparse collection，需要单独处理 sparse 文档生命周期。

---

## 9. dense / sparse / hybrid 检索

### `dense_search(query, source_list=None, top_k=10)`

语义搜索入口。

流程：

1. 确定 source 范围
2. 逐个 source 检查 dense collection 是否存在
3. 调用 `VectorDBClient.search_documents()`
4. 把返回结果映射成 `SearchResult`
5. 全局按 score 排序并截断 `top_k`

特点：

- 使用腾讯云服务端 embedding
- 适合语义相似、概念相关、同义表达检索

### `sparse_search(query, source_list=None, top_k=10)`

BM25 sparse 检索入口。

流程：

1. 对 query 做 BM25 sparse 编码
2. 逐个 source 检查 sparse collection 是否存在
3. 调用 `VectorDBClient.fulltext_search_documents()`
4. 把返回结果映射成 `SearchResult`
5. 全局按 score 排序并截断 `top_k`

特点：

- 适合关键词、术语、实体名精确匹配
- 依赖 sparse collection 预先完成 backfill / upsert

### `hybrid_search(query, source_list=None, top_k=10)`

混合检索入口。

流程：

1. 用更大的 `candidate_k` 分别跑 dense 和 sparse
2. 调用 `_rrf_merge_results()` 做 RRF 融合
3. 返回 top-k 融合结果

融合参数来自 `hybrid` 配置：

- `candidate_multiplier`
- `min_candidate_k`
- `rrf_k`
- `dense_weight`
- `sparse_weight`

### `_rrf_merge_results(...)`

内部 RRF 融合逻辑：

- 以 `work_id` 为 merge key
- 汇总 dense/sparse rank 与原始 score
- 计算融合分数
- 在 `retrieval_debug` 中保留调试信息

### `search(query, source_list=None, top_k=10, search_type='dense')`

统一搜索入口，负责把请求路由到：

- `dense`
- `sparse`
- `hybrid`

不支持的 `search_type` 会直接抛 `ValueError`。

### 搜索类型如何选择

| 场景 | 推荐 | 说明 |
|---|---|---|
| 语义相似查询 | `dense` | 适合理解概念与意图 |
| 专业术语精确匹配 | `sparse` | 适合词汇和实体名命中 |
| 综合召回 | `hybrid` | 同时利用 dense 和 sparse |

---

## 10. `VectorDBClient` HTTP 分层

`vector_db_client.py` 的职责更底层，建议这样理解。

### 错误模型

```python
VectorDBError
VectorDBClientError
VectorDBServerError
```

语义：

- `VectorDBClientError`: HTTP 请求、JSON 解析、调用方式错误
- `VectorDBServerError`: 腾讯云 API 返回了业务错误码

### 统一请求入口

```python
VectorDBClient.__init__(url, account, api_key)
VectorDBClient._request(method, endpoint, data=None)
```

负责：

- 认证头设置
- GET/POST/DELETE 分发
- HTTP 状态检查
- 业务 `code != 0` 检查
- 错误细节提取

### Database APIs

- `create_database`
- `drop_database`
- `list_databases`

### Collection APIs

- `create_collection`
- `create_sparse_collection`
- `drop_collection`
- `list_collections`
- `list_collections_with_info`
- `describe_collection`

其中：

- `create_collection()` 创建 dense collection，带 embedding 和 HNSW vector index
- `create_sparse_collection()` 创建 sparse collection，带 `sparse_vector` inverted index

### Document APIs

- `upsert_documents`
- `delete_documents`
- `search_documents`
- `fulltext_search_documents`
- `query_documents`

对应关系：

- dense search -> `search_documents()`
- sparse search -> `fulltext_search_documents()`
- existence check / query -> `query_documents()`

---

## 11. 使用示例

### 初始化

```python
from pathlib import Path
from src.docset_hub.storage.vector_db import VectorDB

vector_db = VectorDB(
    config_path=Path("src/config/config_tecent_backend_server_test.yaml")
)
```

### dense 文档写入

```python
title = "Deep Learning for Bioinformatics"
abstract = "This paper presents a novel approach..."
text = f"{title} {abstract}".strip()

result = vector_db.add_document(
    source_name="biorxiv_history",
    work_id="W019b73d6-1634-77d3-9574-b6014f85b118",
    text=text,
    text_type="abstract",
    paper_id="12345",
)
```

### sparse 文档写入

```python
result = vector_db.add_sparse_document(
    source_name="biorxiv_history",
    work_id="W019b73d6-1634-77d3-9574-b6014f85b118",
    text="Deep Learning for Bioinformatics This paper presents a novel approach...",
    text_type="abstract",
    paper_id="12345",
)
```

### dense 搜索

```python
results = vector_db.search(
    query="machine learning algorithms for genomics",
    source_list=["biorxiv_history"],
    top_k=5,
    search_type="dense",
)
```

### hybrid 搜索

```python
results = vector_db.search(
    query="CRISPR gene editing",
    source_list=["biorxiv_history", "biorxiv_daily", "langtaosha"],
    top_k=10,
    search_type="hybrid",
)
```

### 删除文档

```python
result = vector_db.delete_document(
    source_name="biorxiv_history",
    work_id="W019b73d6-1634-77d3-9574-b6014f85b118",
    text_type="abstract",
)
```

---

## 12. 使用约束与注意事项

### 文本构造建议

推荐使用：

```python
text = f"{title} {abstract}".strip()
```

相比只用 abstract，`title + abstract` 更稳定。

### `work_id` 是向量文档主标识

当前实现里：

- `doc_id == work_id`
- dense / sparse merge 也以 `work_id` 为键

因此 `work_id` 应保持稳定且全局唯一，推荐使用 UUID v7 风格 ID。

### source 必须可控

不要传任意字符串作为 `source_name`，所有 source 都应在配置中的 `allowed_sources` 里。

### sparse 使用前提

使用 `sparse_search()` 前，需要满足：

- 已创建 sparse collection
- 已写入 sparse 文档或完成 sparse backfill

### 当前不支持的路径

- `embedding_source=local_made`
- 更复杂的 filter DSL
- 自动联动删除 sparse collection 文档

### 错误处理建议

```python
from src.docset_hub.storage.vector_db import VectorDB, VectorDBError

try:
    vector_db = VectorDB(config_path=config_path)
    results = vector_db.search("CRISPR", search_type="hybrid")
except ValueError as e:
    print(f"配置或参数错误: {e}")
except VectorDBError as e:
    print(f"VectorDB 调用失败: {e}")
```

---

## 相关文档

- `src/docset_hub/storage/vector_db.py`
- `src/docset_hub/storage/vector_db_client.py`
- `src/docset_hub/storage/sparse_encoder.py`
- `docs/vector_db_building_plan_0415.md`
- `docs/tencent_vectordb_embedding_manual.md`
- `docs/vector_db_implementation_report_0415.md`

---

## 变更说明

本次 README 重构重点是：

- 按 `vector_db.py` / `vector_db_client.py` 当前职责与源码顺序重排
- 明确 dense / sparse / hybrid 三条路径的边界
- 补上 `VectorDBClient` 的分层说明
- 修正文档中已经过时的 `doc_id` 描述，和当前源码保持一致
