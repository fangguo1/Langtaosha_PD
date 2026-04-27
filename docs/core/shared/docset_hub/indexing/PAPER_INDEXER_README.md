# PaperIndexer - 论文索引编排器

**位置**: `src/docset_hub/indexing/paper_indexer.py`
**版本**: v2.2 (Phrase-aware Query Understanding / Keyword Enrichment 已接入)
**更新日期**: 2026-04-27

---

## 概述

`PaperIndexer` 是 DocSet Hub 的统一索引入口，负责把论文原始数据从输入、转换、写入 metadata、向量化到检索串联起来。它本身不直接实现字段映射、数据库写入或向量检索，而是编排以下组件：

- `MetadataTransformer`: 将不同 source 的原始论文数据转换为统一 DB payload
- `MetadataDB`: 持久化论文 metadata，并处理多 source、去重、canonical source 选择
- `VectorDB`: 写入和搜索语义向量
- `KeywordEnrichmentService`: 使用本地 scispaCy 双模型抽取结构化关键词，并写入 `paper_keywords`
- `QueryUnderstandingService`: 对搜索 query 做规范化、作者识别和关键词纠错，并为 `smart_search()` 选择检索路由

适合上层 API、批处理脚本和集成测试使用，避免调用方手动拼接 transformer、metadata db 和 vector db。

### 核心特性

- 支持 `default_sources` 配置体系下的多 source 索引
- 支持字典和文件两种输入方式
- 统一返回 metadata 写入结果与向量化结果
- 支持按 source 过滤的语义搜索
- 支持 `smart_search()`：作者姓名走 metadata 作者检索，普通主题走向量检索
- 支持高置信度拼写纠错：基于 `paper_keywords` 生成候选词后再搜索
- 支持 phrase-aware correction：一句 query 内多个短语可分别纠错，并返回 `corrections`
- 支持可选关键词扩充：本地 scispaCy 生成细粒度 keyword，按模型来源分开写入
- 支持按 `work_id` 或 `paper_id` 读取完整论文信息
- 支持按 `work_id` 删除 metadata 与向量文档
- 当前索引入口为 **insert-only**，非 `insert` 模式会被降级为 `insert`

---

## 快速开始

### 初始化

```python
from pathlib import Path

from docset_hub.indexing import PaperIndexer

config_path = Path("src/config/config_tecent_backend_server_test.yaml")

indexer = PaperIndexer(
    config_path=config_path,
    enable_vectorization=True,
    enable_keyword_enrichment=True,
)
```

`config_path` 必须存在。初始化时会读取配置中的 `default_sources`，并创建 `MetadataTransformer`、`MetadataDB`、`QueryUnderstandingService`，以及可选的 `VectorDB` 和 `KeywordEnrichmentService`。

如果只需要写入 metadata，不希望连接或写入向量库、也不希望加载本地 scispaCy 模型：

```python
indexer = PaperIndexer(
    config_path=config_path,
    enable_vectorization=False,
    enable_keyword_enrichment=False,
)
```

### 索引字典

```python
result = indexer.index_dict(
    raw_payload={
        "title": "Deep Learning for Bioinformatics",
        "abstract": "This paper presents a novel approach...",
    },
    source_name="langtaosha",
    mode="insert",
)

if result["success"]:
    print(result["work_id"], result["paper_id"])
else:
    print(result["error"])
```

### 索引文件

```python
result = indexer.index_file(
    input_path="/path/to/paper.json",
    source_name="biorxiv_daily",
    mode="insert",
)
```

当 `enable_keyword_enrichment=True` 时，`index_dict()` / `index_file()` 会在 metadata 写入后尝试执行关键词扩充。关键词扩充失败不会改变索引主结果的 `success=True`，调用方应单独检查 `result["keyword_enrichment"]`。

### 搜索论文

```python
results = indexer.search(
    query="machine learning algorithms for genomics",
    source_list=["biorxiv_history", "biorxiv_daily"],
    top_k=10,
    hydrate=True,
)

for item in results:
    print(item["similarity"], item["work_id"], item["source_name"])
```

### 智能搜索

```python
result = indexer.smart_search(
    query="Alce Zhang",
    source_list=["biorxiv_daily"],
    top_k=10,
    hydrate=True,
)

print(result["search_query"])
print(result["query_understanding"]["route"])
for item in result["results"]:
    print(item["canonical_title"])
```

`smart_search()` 会先调用 Query Understanding：

- 作者姓名高置信度命中：路由到 `MetadataDB.search_by_author(...)`
- 普通主题 query：路由到原有 `search(...)`
- 英文主题拼写错误且纠错置信度足够高：使用 `corrected_query` 调用向量搜索
- 空 query：返回 `success=False`、`route="none"` 和空结果

### 读取论文

```python
paper = indexer.read(work_id="W019b73d6-1634-77d3-9574-b6014f85b118")

# 或者
paper = indexer.read(paper_id=123)
```

### 删除论文

```python
result = indexer.delete(
    work_id="W019b73d6-1634-77d3-9574-b6014f85b118",
    source_name="langtaosha",
)

print(result["metadata_deleted"], result["vector_deleted"])
```

---

## 架构设计

### 调用链路

```text
raw dict / file
    -> PaperIndexer
    -> MetadataTransformer
    -> MetadataDB.insert_paper
    -> KeywordEnrichmentService.extract_keywords
    -> MetadataDB.upsert_generated_keywords
    -> PaperIndexer._build_index_text
    -> VectorDB.add_document
    -> embedding status update
```

搜索链路：

```text
PaperIndexer.smart_search(query)
    -> QueryUnderstandingService.analyze(query)
        -> QueryNormalizer
        -> AuthorMatcher -> MetadataDB.suggest_author_names
        -> QueryCorrector -> MetadataDB.suggest_query_terms
    -> route:
        metadata_author -> MetadataDB.search_by_author
        vector          -> PaperIndexer.search
```

### 组件职责

| 组件 | 职责 |
|------|------|
| `PaperIndexer` | 编排转换、写库、向量化、搜索、读取、删除 |
| `MetadataTransformer` | 解析 source 数据并生成 `db_payload` / `upsert_key` |
| `MetadataDB` | 写入 metadata、分配 `work_id`、维护 canonical source、embedding 状态、作者检索和 query term 候选 |
| `VectorDB` | 按 source collection 写入和搜索向量文档 |
| `KeywordEnrichmentService` | 本地运行 `en_core_sci_lg` 与 `en_ner_bionlp13cg_md`，输出结构化关键词 |
| `QueryUnderstandingService` | 规范化 query、识别作者意图、执行主题 query 纠错并选择 route |

`PaperIndexer` 的定位是业务入口层。字段清洗、去重策略、数据库 schema 和向量库 HTTP 细节应分别放在对应组件内维护。

---

## 配置说明

### default_sources

`PaperIndexer` 使用配置文件中的 `default_sources` 作为 source 白名单：

```yaml
default_sources:
  - langtaosha
  - biorxiv_history
  - biorxiv_daily
```

规则：

- 显式传入 `source_name` 时，必须存在于 `default_sources`
- 未传入 `source_name` 且只有一个默认 source 时，自动使用该 source
- 未传入 `source_name` 且有多个默认 source 时，抛出错误，要求调用方显式指定
- 搜索时未传入 `source_list`，默认搜索全部 `default_sources`
- 搜索时传入 `source_list`，其中每个 source 都必须存在于 `default_sources`

### 依赖配置

索引流程依赖 metadata 数据库配置：

```yaml
metadata_db:
  host: example-metadata-db
  port: 5432
  user: example_db_user
  password: example_db_password
  name: langtaosha_test
```

启用向量化或搜索时，还依赖向量数据库配置：

```yaml
vector_db:
  url: http://example-vector-db:80
  account: example_account
  api_key: example_api_key
  embedding_source: tecent_made
  embedding_model: BAAI/bge-m3
  database: langtaosha_test
  collection_prefix: "lt_"
```

### keyword_enrichment

关键词扩充默认使用本地 scispaCy 模型，不调用远程 LLM。可通过配置覆盖模型、来源名和单模型关键词上限：

```yaml
keyword_enrichment:
  models:
    - en_core_sci_lg
    - en_ner_bionlp13cg_md
  sources:
    en_core_sci_lg: scispacy-en_core_sci_lg-generated
    en_ner_bionlp13cg_md: scispacy-en_ner_bionlp13cg_md-generated
  max_keywords: 12
  timeout: 60
```

默认来源：

- `scispacy-en_core_sci_lg-generated`
- `scispacy-en_ner_bionlp13cg_md-generated`

测试写入使用对应的 `*-test` source，避免污染长期候选词库。

---

## API 参考

### 初始化

```python
def __init__(
    self,
    config_path: Path,
    enable_vectorization: bool = True,
    enable_keyword_enrichment: bool = True,
)
```

**参数**:

- `config_path`: YAML 配置文件路径
- `enable_vectorization`: 是否启用向量化与向量搜索
- `enable_keyword_enrichment`: 是否启用本地 scispaCy 关键词扩充。默认启用；如环境缺少模型或只做 metadata 写入，建议显式关闭

**异常**:

- `ValueError`: 配置文件不存在

### index_dict

```python
def index_dict(
    self,
    raw_payload: Dict[str, Any],
    source_name: Optional[str] = None,
    mode: str = "insert",
) -> Dict[str, Any]
```

索引内存中的论文原始字典。

**参数**:

- `raw_payload`: 原始论文 metadata 字典
- `source_name`: 来源名称
- `mode`: 索引模式。当前仅支持 `insert`，其他值会被记录 warning 并降级为 `insert`

**返回结构**:

```python
{
    "success": True,
    "source_name": "langtaosha",
    "work_id": "W019b73d6-1634-77d3-9574-b6014f85b118",
    "paper_id": 123,
    "mode": "insert",
    "metadata": {
        "success": True,
        "paper_id": 123,
        "work_id": "W019b73d6-1634-77d3-9574-b6014f85b118",
        "action": "insert",
        "status_code": "INSERT_NEW_PAPER",
        "canonical_changed": True,
        "canonical_source_id": 456,
        "canonical_source_name": "langtaosha",
        "write_result": {},
    },
    "vectorization": {
        "success": True,
        "enabled": True,
        "action": "inserted",
        "message": "向量化成功: inserted",
    },
    "keyword_enrichment": {
        "enabled": True,
        "success": True,
        "source": "scispacy-en_core_sci_lg-generated+scispacy-en_ner_bionlp13cg_md-generated",
        "sources": [
            "scispacy-en_core_sci_lg-generated",
            "scispacy-en_ner_bionlp13cg_md-generated",
        ],
        "model_name": "en_core_sci_lg+en_ner_bionlp13cg_md",
        "inserted": 16,
        "updated": 0,
        "skipped": 0,
        "keyword_count": 16,
        "model_results": [],
        "write_results": {},
    },
}
```

失败时返回：

```python
{
    "success": False,
    "source_name": "langtaosha",
    "error": "错误信息",
    "mode": "insert",
}
```

### index_file

```python
def index_file(
    self,
    input_path: Union[str, Path],
    source_name: Optional[str] = None,
    mode: str = "insert",
) -> Dict[str, Any]
```

索引本地论文文件。返回结构与 `index_dict` 一致。

### search

```python
def search(
    self,
    query: str,
    source_list: Optional[List[str]] = None,
    top_k: int = 10,
    hydrate: bool = True,
) -> List[Dict[str, Any]]
```

执行语义搜索。内部调用 `VectorDB.search(..., search_type="dense")`。

**参数**:

- `query`: 查询文本
- `source_list`: 搜索的 source 列表，不传则使用全部 `default_sources`
- `top_k`: 返回数量
- `hydrate`: 是否根据 `work_id` 补全完整 metadata

`hydrate=True` 返回：

```python
[
    {
        "work_id": "W019b73d6-1634-77d3-9574-b6014f85b118",
        "paper_id": 123,
        "source_name": "biorxiv_daily",
        "similarity": 0.87,
        "text_type": "abstract",
        "metadata": {},
    }
]
```

`hydrate=False` 返回轻量结果：

```python
[
    {
        "work_id": "W019b73d6-1634-77d3-9574-b6014f85b118",
        "paper_id": "123",
        "source_name": "biorxiv_daily",
        "similarity": 0.87,
        "text_type": "abstract",
    }
]
```

### smart_search

```python
def smart_search(
    self,
    query: str,
    source_list: Optional[List[str]] = None,
    top_k: int = 10,
    hydrate: bool = True,
) -> Dict[str, Any]
```

在原有语义搜索前增加 Query Understanding 层。`smart_search()` 不改变 `search()` 的语义，适合作为用户搜索框的入口。

主题 query 纠错示例：

```python
{
    "success": True,
    "query": "machien learing",
    "search_query": "machine learning",
    "query_understanding": {
        "original_query": "machien learing",
        "normalized_query": "machien learing",
        "intent": "semantic_search",
        "route": "vector",
        "corrected_query": "machine learning",
        "matched_author": None,
        "confidence": 0.95,
        "candidates": [],
        "reason": "query_term_high_confidence",
    },
    "results": [],
}
```

句子级多短语纠错示例：

```python
{
    "success": True,
    "query": "machine learing for RNA structre",
    "search_query": "machine learning for RNA structure",
    "query_understanding": {
        "original_query": "machine learing for RNA structre",
        "normalized_query": "machine learing for RNA structre",
        "intent": "semantic_search",
        "route": "vector",
        "corrected_query": "machine learning for RNA structure",
        "matched_author": None,
        "confidence": 0.96,
        "reason": "phrase_query_terms_high_confidence",
        "corrections": [
            {
                "original": "machine learing",
                "corrected": "machine learning",
                "start": 0,
                "end": 15,
                "confidence": 0.96,
                "source": "rule_split",
                "candidate_source": "scispacy-en_core_sci_lg-generated",
            },
            {
                "original": "RNA structre",
                "corrected": "RNA structure",
                "start": 20,
                "end": 32,
                "confidence": 0.96,
                "source": "rule_split",
                "candidate_source": "scispacy-en_core_sci_lg-generated",
            },
        ],
    },
    "results": [],
}
```

作者检索示例：

```python
{
    "success": True,
    "query": "Alice Zhang",
    "search_query": "Alice Zhang",
    "query_understanding": {
        "intent": "author_name",
        "route": "metadata_author",
        "matched_author": "Alice Zhang",
        "confidence": 1.0,
    },
    "results": [],
}
```

### read

```python
def read(
    self,
    work_id: Optional[str] = None,
    paper_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]
```

读取完整论文信息。必须提供 `work_id` 或 `paper_id` 之一。

### delete

```python
def delete(
    self,
    work_id: str,
    source_name: Optional[str] = None,
    text_type: str = "abstract",
) -> Dict[str, Any]
```

按 `work_id` 删除论文 metadata，并在启用向量化时删除对应向量文档。

**返回结构**:

```python
{
    "success": True,
    "source_name": "langtaosha",
    "work_id": "W019b73d6-1634-77d3-9574-b6014f85b118",
    "metadata_deleted": True,
    "vector_deleted": True,
}
```

---

## 向量化触发规则

`PaperIndexer` 当前只通过 `MetadataDB.insert_paper(...)` 写入 metadata。metadata 写入完成后，会根据写入状态和 canonical source 变化决定是否向量化。

| `status_code` | 向量化规则 |
|---------------|------------|
| `INSERT_NEW_PAPER` | 新论文，执行向量化 |
| `INSERT_APPEND_SOURCE` | 仅当 append 后 canonical source 发生变化时执行 |
| `INSERT_UPDATE_SAME_SOURCE` | 仅当当前 source 是 canonical source 时执行 |
| `INSERT_SKIP_SAME_SOURCE` | 跳过向量化 |

向量化文本构造规则：

1. 同时存在 title 和 abstract：索引 `title + "\n" + abstract`，`text_type="abstract"`
2. 只有 title：索引 title，`text_type="title"`
3. title 和 abstract 都为空：跳过向量化

执行向量化前会写入 pending 状态：

```python
metadata_db.upsert_embedding_status_pending(...)
```

向量化成功后标记：

```python
metadata_db.mark_embedding_succeeded(paper_id)
```

向量化失败后标记：

```python
metadata_db.mark_embedding_failed(paper_id, error_message=...)
```

---

## 关键词扩充触发规则

`PaperIndexer` 在 metadata 写入成功后独立执行关键词扩充。该流程与向量化互不阻塞：向量化失败不影响关键词扩充，关键词扩充失败也不影响 `index_dict()` / `index_file()` 的主成功状态。

| `status_code` | 关键词扩充规则 |
|---------------|----------------|
| `INSERT_NEW_PAPER` | 新论文，执行关键词扩充 |
| `INSERT_APPEND_SOURCE` | 仅当 append 后 canonical source 发生变化时执行 |
| `INSERT_UPDATE_SAME_SOURCE` | 执行关键词扩充 |
| `INSERT_SKIP_SAME_SOURCE` | 若缺少任一 configured generated source，则补生成；否则跳过 |

写库路径：

```text
KeywordEnrichmentService.extract_keywords(...)
    -> 按 keyword["source"] 分组
    -> MetadataDB.upsert_generated_keywords(...)
    -> paper_keywords(paper_id, keyword_type, keyword, source)
```

关键词类型白名单：

```text
domain, concept, method, task, disease, gene, protein,
model, dataset, metric, organism, chemical
```

`paper_keywords` 的主键包含 `source`，因此同一 paper 的同一个 keyword 可以同时保留 metadata 原始来源和两个 scispaCy generated 来源。

同一 `paper_id + keyword_type + source` 内，keyword 入库按大小写不敏感处理，例如 `Machine Learning` 和 `machine learning` 只保留一条。纠错结果会按用户输入短语的大小写风格输出：全小写输入返回小写纠错，包含 `RNA` 这类大写缩写时保留候选中的缩写大小写。

---

## Query Understanding

当前 MVP 使用确定性规则，不引入远程模型：

- `QueryNormalizer`: trim、压缩空格、去除边缘无意义标点，保留原始 query
- `AuthorMatcher`: 调用 `MetadataDB.suggest_author_names()`，只有高置信度作者候选才路由到作者检索
- `QueryCorrector`: 调用 `MetadataDB.suggest_query_terms()`，从 generated keywords 中取候选词做英文 query 纠错
- `PhraseSegmenter`: 对句子 query 生成可纠错短语，优先规则切分并保留 n-gram fallback
- `PhraseAwareQueryCorrector`: 对每个 phrase 复用 `MetadataDB.suggest_query_terms()`，选择不重叠高置信度 correction 后重建 `corrected_query`
- `QueryUnderstandingService`: 固定执行 `normalize -> author match -> phrase-aware correction -> route`

路由优先级：

1. 作者 exact / 高置信度 fuzzy match 优先，走 `metadata_author`
2. 作者不确定时只返回 candidates，不强行路由作者检索
3. 普通主题 query 默认走 `vector`
4. 高置信度英文纠错会设置 `corrected_query`，由 `smart_search()` 作为实际搜索词
5. 句子级多短语纠错会在 `query_understanding.corrections` 中返回每个局部替换，便于前端展示

---

## 使用示例

### 示例 1: 批量索引多个 source

```python
from pathlib import Path

from docset_hub.indexing import PaperIndexer

indexer = PaperIndexer(
    config_path=Path("src/config/config_tecent_backend_server_test.yaml"),
    enable_vectorization=True,
    enable_keyword_enrichment=False,
)

index_plan = [
    ("langtaosha", langtaosha_papers),
    ("biorxiv_history", biorxiv_history_papers),
    ("biorxiv_daily", biorxiv_daily_papers),
]

indexed = []

for source_name, papers in index_plan:
    for paper in papers:
        result = indexer.index_dict(
            raw_payload=paper,
            source_name=source_name,
            mode="insert",
        )
        if result["success"]:
            indexed.append((result["work_id"], source_name))
        else:
            print(f"{source_name} failed: {result['error']}")
```

### 示例 2: 先索引再搜索

```python
indexer.index_file(
    input_path="/path/to/biorxiv_daily.json",
    source_name="biorxiv_daily",
)

results = indexer.search(
    query="CRISPR gene editing",
    source_list=["biorxiv_daily"],
    top_k=5,
    hydrate=True,
)
```

### 示例 3: 轻量搜索结果

```python
results = indexer.search(
    query="virus genomics",
    top_k=20,
    hydrate=False,
)

for item in results:
    print(item["work_id"], item["similarity"])
```

### 示例 4: 智能搜索

```python
result = indexer.smart_search(
    query="machien learing",
    source_list=["biorxiv_daily"],
    top_k=5,
)

print(result["search_query"])
print(result["query_understanding"]["reason"])
```

### 示例 5: 安全清理测试数据

```python
for work_id, source_name in indexed:
    result = indexer.delete(
        work_id=work_id,
        source_name=source_name,
    )
    if not result["success"]:
        print(result.get("error"))
```

---

## 最佳实践

### 1. 总是显式传入 source_name

当配置里有多个 `default_sources` 时，不传 `source_name` 会报错。批处理、API 和测试中都建议显式传入。

```python
indexer.index_dict(raw_payload=paper, source_name="biorxiv_daily")
```

### 2. 将 PaperIndexer 作为上层统一入口

如果调用方需要完整的“转换 + 写库 + 向量化”链路，优先使用 `PaperIndexer`，不要在业务层重复拼接 `MetadataTransformer`、`MetadataDB` 和 `VectorDB`。

### 3. 只需要 metadata 时关闭向量化

单元测试、离线清洗或只验证 DB payload 的场景，可以关闭向量化，减少外部服务依赖。

```python
indexer = PaperIndexer(
    config_path=config_path,
    enable_vectorization=False,
    enable_keyword_enrichment=False,
)
```

### 4. 搜索前确认启用了向量化

`search(...)` 依赖 `VectorDB`。如果初始化时 `enable_vectorization=False`，调用搜索会抛出 `ValueError`。

### 5. 关注 vectorization.skipped

索引成功不代表一定写入了向量库。调用方如需确认语义检索可用，应检查：

```python
result["vectorization"].get("success")
result["vectorization"].get("skipped")
result["vectorization"].get("message")
```

### 6. 关注 keyword_enrichment 子结果

索引成功不代表关键词扩充一定成功。启用 scispaCy 关键词扩充时，调用方如需确认 query correction 候选词已补齐，应检查：

```python
result["keyword_enrichment"].get("success")
result["keyword_enrichment"].get("sources")
result["keyword_enrichment"].get("error")
```

本地缺少 scispaCy 模型时，`keyword_enrichment.success=False`，但主索引结果仍可成功。

### 7. 使用 smart_search 承接用户搜索框

`search()` 保持纯向量搜索语义，适合内部显式语义检索；`smart_search()` 适合用户输入不稳定的场景，包括作者姓名、拼写错误和普通主题 query。

### 8. 使用 work_id 作为跨库关联主键

`work_id` 是 metadata 与 vector document 的关联键。删除、读取和检索回填都依赖它，调用方应保存索引结果中的 `work_id`。

---

## 常见问题

### 为什么传入 mode="upsert" 仍然返回 mode="insert"？

当前 `index_dict` 和 `index_file` 是 insert-only 入口。任何非 `insert` 的 `mode` 都会被降级为 `insert`，并写入 warning 日志。

### 为什么 index 成功但没有向量化？

常见原因：

- 初始化时 `enable_vectorization=False`
- metadata 写入状态是 `INSERT_SKIP_SAME_SOURCE`
- append source 后 canonical source 没有切换
- 同 source 更新时当前 source 不是 canonical source
- title 和 abstract 都为空
- `paper_id` 或 `work_id` 缺失

### 为什么 search 返回结果少于 top_k？

可能原因：

- 向量库本身返回候选不足
- `source_list` 过滤后结果较少
- `hydrate=True` 时，找不到对应 metadata 的搜索结果会被跳过

### 为什么 smart_search 没有纠正拼写？

常见原因：

- query 不是英文或长度太短
- `paper_keywords` 中 generated source 候选不足
- 最佳候选分数低于自动纠错阈值
- 当前 query 已经是候选词库中的 exact term

### 为什么候选里显示 Machine Learning，但 corrected_query 是 machine learning？

候选词保留数据库中首次入库的展示大小写；真正应用到 `corrected_query` 时，会按用户输入短语的大小写风格调整。比如用户输入 `machine learing`，即使命中候选 `Machine Learning`，最终也会返回 `machine learning`；输入 `RNA structre` 时会保留 `RNA structure`。

### 为什么 smart_search 没有按作者检索？

只有作者库高置信度命中时才会走 `metadata_author`。单 token、多候选接近、包含明显主题词或候选分数不足时，会默认回到 `vector` 路由。

### 为什么 keyword_enrichment 失败但 index 仍然成功？

关键词扩充是搜索体验增强能力，不是 metadata 写入的必要条件。`PaperIndexer` 会把失败记录在 `result["keyword_enrichment"]` 中，但不回滚已经成功的 metadata 或 vectorization。

### delete 会删除所有 source 吗？

`PaperIndexer.delete(...)` 按 `work_id` 调用 `MetadataDB.delete_paper_by_work_id(...)`，metadata 删除语义由 `MetadataDB` 负责；启用向量化时，同时按传入 `source_name + work_id + text_type` 删除向量文档。

---

## 相关文档

- `docs/core/shared/docset_hub/metadata/TRANSFORMER_README.md`
- `docs/core/shared/docset_hub/storage/METADATA_DB_README.md`
- `docs/core/shared/docset_hub/storage/VECTOR_DB_README.md`
- `database/migrations/20260426_paper_keywords_multisource.sql`
- `database/migrations/20260427_paper_keywords_case_insensitive.sql`
- `scripts/backfill_generated_keywords.py`
