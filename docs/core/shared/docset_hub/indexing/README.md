# Indexing 模块架构

`src/docset_hub/indexing` 是 Docset Hub 的**应用编排层**。它连接 metadata 与 storage，承载论文索引、关键词处理、query understanding 和多路检索融合。

按功能梳理的 `PaperIndexer` 调用关系见 [PAPER_INDEXER_FUNCTION_MAP.md](PAPER_INDEXER_FUNCTION_MAP.md)；API 与示例见 [PAPER_INDEXER_README.md](PAPER_INDEXER_README.md)。

---

## 1. 核心职责

Indexing 模块负责：

- 编排单条和文件级论文入库；
- 根据 MetadataDB 写入结果决定是否刷新检索资产；
- 构造统一的 Dense 与 Sparse 索引文本；
- 执行关键词抽取、概念扩充、筛选和写回；
- 分析用户 query 并形成检索计划；
- 并行执行多路检索（Dense、Sparse、Keyword Lookup、**Expanded Sparse**）；
- 对候选结果过滤、融合、去重和 hydrate；
- 可选地对结果附加 **semantic coverage** 注解。

它**不负责**：

- 来源原始数据抓取；
- PostgreSQL 或 VectorDB 的底层协议；
- 每日任务生命周期；
- HTTP 请求校验和 API 响应结构。

---

## 2. 核心组件

| 组件 | 源码 | 说明 |
| --- | --- | --- |
| `PaperIndexer` | `paper_indexer.py` | 跨 metadata 与 storage 的统一编排入口 |
| `QueryUnderstandingService` | `query_understanding.py` | query normalization、作者匹配、纠错和路由 |
| `QueryPhraseAnalyzer` | `query_phrase_analyzer.py` | 短语归一化、候选提取、词典匹配 |
| **Span Matcher 族** | `span_matcher.py`, `span_matcher_pipeline.py` | query span → 概念证据 → 非重叠概念；见 [SPAN_MATCHER_README.md](SPAN_MATCHER_README.md) |
| **Query Semantic Plan** | `query_semantic_plan.py` | matcher 输出 → 结构化语义 plan；见 [QUERY_SEMANTIC_PLAN_README.md](QUERY_SEMANTIC_PLAN_README.md) |
| **Expanded Sparse Retrieval** | `expanded_sparse_retrieval.py` | 第四种检索分支；见 [EXPANDED_SPARSE_RETRIEVAL_README.md](EXPANDED_SPARSE_RETRIEVAL_README.md) |
| **Coverage Engine** | `coverage_engine.py` | 多概念覆盖评分；见 [COVERAGE_ENGINE_README.md](COVERAGE_ENGINE_README.md) |
| `PaperKeywordLookup` | `paper_keyword_lookup.py` | 关键词检索计划与候选查询 |
| `KeywordEnrichmentService` | `keyword_enrichment.py` | 从论文文本抽取和扩充关键词 |
| `dense_result_filter` | `dense_result_filter.py` | 融合前过滤低质量 Dense 候选 |
| `search_highlighting` | `search_highlighting.py` | 检索结果高亮 |
| `entity_filter_policy` | `entity_filter_policy.py` | ontology evidence 过滤（UMLS/MeSH） |

---

## 3. Query Understanding → 检索 总览

```text
用户 Query
  │
  ├─ smart_search / scholar search
  │     └─ QueryUnderstandingService（路由：作者 / 语义 / 向量）
  │
  └─ expanded_sparse / coverage annotate
        └─ SpanMatcherPipeline
              └─ build_query_semantic_plan()  →  QuerySemanticPlan
                        ├─ expanded_sparse_retrieval  →  论文召回（coverage 作 score）
                        └─ coverage_engine            →  单篇文档覆盖分析
```

---

## 4. `PaperIndexer` 的两类编排

### 4.1 写入编排

```text
index_dict()
  -> MetadataTransformer.transform_dict()
  -> MetadataDB.insert_paper()
  -> Dense vectorization
  -> Sparse vectorization
  -> Keyword enrichment
  -> unified result
```

### 4.2 检索编排

**正式 hybrid（生产主路径）**：

```text
hybrid_retrieval_search()
  -> Dense branch
  -> Sparse branch
  -> Keyword Lookup branch
  -> weighted RRF
  -> hydrate by work_id
```

**Expanded Sparse（独立分支，尚未并入 RRF）**：

```text
expanded_sparse_search()
  -> build_query_semantic_plan()
  -> match_papers_by_expanded_sparse_plan()
  -> hydrate（coverage_ratio 作为 similarity）
```

**Coverage 注解（dev / compare）**：

```text
search(..., include_coverage=True)
  -> dense 或 sparse 召回
  -> analyze_document_coverage()  per result
```

---

## 5. Public 入口语义

| 入口 | 用途 | 注意事项 |
| --- | --- | --- |
| `index_dict()` / `index_file()` | 写入 | 当前将非 `insert` mode 降级为 `insert` |
| `search()` | 统一检索 | 支持 `dense` / `sparse` / `hybrid` / `hybrid_retrieval` / **`expanded_sparse`**；可选 **`include_coverage`** |
| `hybrid_retrieval_search()` | 三路 RRF | 正式 scholar search 主召回路径 |
| `expanded_sparse_search()` | 第四路检索 | 不依赖 vector_db |
| `build_query_semantic_plan()` | 构建 semantic plan | 公开 Domain API；供 dev `/api/semantic-plan` |
| `smart_search()` | QU + route + search | 与正式 API 编排路径不完全一致 |
| `read()` / `delete()` | 读删论文 | 通过 storage 层 |

---

## 6. 检索策略

Hybrid 默认权重：

```text
dense: 0.4
sparse: 0.4
keyword_lookup: 0.2
```

Expanded sparse **独立排序**：`coverage_ratio`（见 [COVERAGE_ENGINE_README.md](COVERAGE_ENGINE_README.md)）。

主要规则：

- hybrid 三分支并行，单分支失败可降级；
- Expanded sparse 与 hybrid **分开验证**，尚未融合；
- 融合结果保留 `retrieval_debug`；
- 使用 `work_id` 去重。

---

## 7. 开发者文档索引

| 文档 | 读者目标 |
| --- | --- |
| [SPAN_MATCHER_README.md](SPAN_MATCHER_README.md) | 理解 query span → 概念证据 |
| [QUERY_SEMANTIC_PLAN_README.md](QUERY_SEMANTIC_PLAN_README.md) | 理解 semantic plan 契约与 tier 规则 |
| [EXPANDED_SPARSE_RETRIEVAL_README.md](EXPANDED_SPARSE_RETRIEVAL_README.md) | 理解第四检索分支 |
| [COVERAGE_ENGINE_README.md](COVERAGE_ENGINE_README.md) | 理解 coverage 评分语义 |
| [PAPER_INDEXER_README.md](PAPER_INDEXER_README.md) | PaperIndexer API 与示例 |
| [PAPER_INDEXER_FUNCTION_MAP.md](PAPER_INDEXER_FUNCTION_MAP.md) | 函数级调用地图 |

---

## 8. Review 重点

1. `PaperIndexer` 是否仍承担过多 HTTP 无关职责（应向 `app/routes` 收敛）。
2. `smart_search()` 与 `/api/scholar/search` 是否应收束到同一编排。
3. Expanded sparse 何时进入 hybrid RRF，权重如何定。
4. Coverage Python 路径与 MetadataDB SQL 是否保持公式一致。
5. Semantic plan 重复构建的性能（compare 页一次 query 多次 plan）。

---

## 9. 相关文档

- [Docset Hub 总览](../README.md)
- [核心契约](../CORE_CONTRACTS.md)
- [MetadataDB](../storage/METADATA_DB_README.md)
- [数据写入与索引流](../flows/DATA_INGESTION_FLOW.md)
- [查询与检索流](../flows/SEARCH_RETRIEVAL_FLOW.md)
- 实现记录：`docs/implementation_log/20260610/`、`docs/implementation_log/20260612/`
