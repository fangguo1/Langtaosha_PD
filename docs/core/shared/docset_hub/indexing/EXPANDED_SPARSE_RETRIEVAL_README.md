# Expanded Sparse Retrieval README

**源码**: `src/docset_hub/indexing/expanded_sparse_retrieval.py`  
**存储层**: `src/docset_hub/storage/metadata_db.py` → `lookup_papers_by_expanded_sparse_groups()`  
**上游输入**: [Query Semantic Plan](QUERY_SEMANTIC_PLAN_README.md)  
**编排入口**: `PaperIndexer.expanded_sparse_search()`、`PaperIndexer.search(search_type="expanded_sparse")`

本文档面向开发者，说明 **Expanded Sparse Retrieval** 要解决什么问题、在检索体系中的位置，以及 Python 层与 SQL 层的分工。

---

## 1. 设计理念

### 1.1 要解决的问题

生物医学 query 常见两类召回缺口，Dense/BM25/Keyword Lookup 难以覆盖：

| 问题 | 示例 |
|------|------|
| **Alias Missing** | 用户搜 `kidney`，论文写 `renal tissue` |
| **Multi-Concept Dominance** | query 含 `adhesion protein` + `kidney`，但高分结果只覆盖其中一个概念 |

Expanded Sparse 的思路：**不依赖向量相似度或 BM25 词面**，而是用 Semantic Plan 展开的 **ontology 别名 + 子短语**，在 `title / abstract / paper_keywords` 上做分组 term 匹配，并以 **multi-concept coverage** 作为排序依据。

### 1.2 在检索体系中的位置

```text
Dense          → 语义泛化
Sparse/BM25    → 词面匹配
Keyword Lookup → concept-level 关键词表
Expanded Sparse → 语义 plan 展开别名 + 多概念覆盖（第四种分支）
```

**明确边界**：

- 是**新的独立检索分支**，不是 `keyword_lookup` 的增强；
- **尚未**并入 hybrid RRF 融合（当前独立验证阶段）；
- 召回与 coverage 打分在 **MetadataDB SQL** 中完成；本模块负责 plan → payload 转换与结果 shaping。

### 1.3 设计原则

| 原则 | 含义 |
|------|------|
| **Plan-driven** | 只消费 `QuerySemanticPlan`，不重新做 query understanding |
| **Grouped matching** | 每个 top-level span 是一组；组内 tier1/tier2 + child terms 共同决定该 span 是否命中 |
| **Coverage as score** | `coverage_ratio` 作为该分支的 similarity/score，而非 BM25 或向量分 |
| **可解释** | 返回 `matched_spans` 明细，便于 compare 页与 debug |

---

## 2. 核心数据结构

### 2.1 `ExpandedSparseGroup`

Semantic Plan 中一个 top-level span 的检索视图：

```text
ExpandedSparseGroup
├── group_id: int          # 1-based，对应 plan span 顺序
├── span_id: str
├── canonical_text: str
├── own_tier1_terms: [{text, match_mode}, ...]
├── own_tier2_terms: [{text, match_mode}, ...]
└── children: [{span_id, own_tier1_terms, own_tier2_terms, ...}, ...]
```

### 2.2 `ExpandedSparseCandidate`

单篇论文的召回结果：

| 字段 | 含义 |
|------|------|
| `paper_id` / `work_id` | 论文标识 |
| `matched_span_count` | 有命中的 top-level span 数（span_score > 0） |
| `total_span_count` | plan 中 top-level span 总数 |
| `coverage_ratio` | 排序主分数（见 Coverage README 公式） |
| `matched_spans` | 每个 span 的命中 term、scope、span_score 等 |
| `retrieval_debug` | `retriever: expanded_sparse` 等 |

---

## 3. 核心 API

### 3.1 `build_expanded_sparse_groups(plan)`

`QuerySemanticPlan` → `List[ExpandedSparseGroup]`。  
纯转换，不访问数据库。

### 3.2 `build_expanded_sparse_query_rows(plan)`

将 plan 中所有 parent/child 的 tier1/tier2 term **展开为 flat rows**，供 SQL 与调试页展示：

```text
每 row: group_id, span_id, span_scope(parent|child), child_span_id,
        term_tier(tier1|tier2), term, match_mode
```

去重键：`(group_id, span_scope, child_span_id, term_tier, term, match_mode)`。

### 3.3 `match_papers_by_expanded_sparse_plan(...)`

```python
match_papers_by_expanded_sparse_plan(
    *,
    metadata_db: MetadataDB,
    plan: QuerySemanticPlan,
    source_list: Optional[Sequence[str]] = None,
    keyword_sources: Optional[Sequence[str]] = None,
    top_k: int = 50,
) -> List[ExpandedSparseCandidate]
```

流程：

```text
plan
  -> build_expanded_sparse_query_rows()
  -> metadata_db.lookup_papers_by_expanded_sparse_groups(...)
  -> List[ExpandedSparseCandidate]
```

### 3.4 `PaperIndexer` 封装

- `PaperIndexer.expanded_sparse_search(query, ...)` — 内部 build plan → match → hydrate metadata；
- `PaperIndexer.search(..., search_type="expanded_sparse")` — 统一检索入口 dispatch；
- HTTP：`GET /api/search?search_type=expanded_sparse`（见 `app/routes/paper.py`）。

---

## 4. 执行流程

```text
Query
  -> SpanMatcherPipeline / build_query_semantic_plan()
  -> QuerySemanticPlan
  -> build_expanded_sparse_query_rows()
  -> MetadataDB.lookup_papers_by_expanded_sparse_groups()   # PostgreSQL FTS + coverage SQL
  -> ExpandedSparseCandidate[] (coverage_ratio 已在 SQL 中计算)
  -> PaperIndexer 补全 metadata（可选 hydrate）
  -> API / compare 页展示
```

**Python vs SQL 分工**：

| 层 | 职责 |
|----|------|
| `expanded_sparse_retrieval.py` | plan 序列化、row 展开、candidate dataclass |
| `metadata_db.py` | 全文匹配、coverage 聚合、top_k 排序 |
| `coverage_engine.py` | 单文档 Python 参考实现（见 Coverage README）；expanded sparse **主路径用 SQL** |

---

## 5. 与 Keyword Lookup 的区别

| | Keyword Lookup | Expanded Sparse |
|--|----------------|-----------------|
| 输入 | 关键词/概念表 | Semantic Plan 展开词项 |
| 别名来源 | 预建 keyword 索引 | ontology tier2 + subphrase child |
| 多概念 | 弱 | 强（coverage_ratio 驱动） |
| 分数 | keyword 相关分 | coverage_ratio |

---

## 6. 测试

| 文件 | 覆盖点 |
|------|--------|
| `tests/indexing/test_expanded_sparse_retrieval.py` | group/row 构建、PaperIndexer dispatch、fake MetadataDB |
| `tests/integration/test_expanded_sparse_retrieval_real_services.py` | 真实 PostgreSQL（gated） |

---

## 7. 调试 checklist

1. `build_expanded_sparse_query_rows` 是否包含预期 tier2 别名；
2. SQL 返回的 `coverage_ratio` 是否与 plan span 数一致；
3. `matched_spans` 中 `matched_scopes` 是否区分 parent/child；
4. `source_list` / `keyword_sources` 过滤是否生效；
5. plan 为 `None` 或空 spans → 应返回空列表，不抛 SQL 错误。

---

## 8. 相关文档

- [Query Semantic Plan README](QUERY_SEMANTIC_PLAN_README.md)
- [Coverage Engine README](COVERAGE_ENGINE_README.md)
- [MetadataDB README](../storage/METADATA_DB_README.md) — SQL 路径（待补 expanded sparse 专节）
- [PaperIndexer README](PAPER_INDEXER_README.md)
- 设计：`docs/implementation_log/20260610/Expanded Sparse Retrieval & Coverage Engine Design.md`（主仓库）
- 实现：`docs/implementation_log/20260610/EXPANDED_SPARSE_RETRIEVAL_AND_COVERAGE_PLAN_20260610.md`（Task 2）
