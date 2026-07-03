# Coverage Engine README

**源码**: `src/docset_hub/indexing/coverage_engine.py`  
**上游输入**: [Query Semantic Plan](QUERY_SEMANTIC_PLAN_README.md)  
**编排入口**: `PaperIndexer.search(..., include_coverage=True)`、`PaperIndexer.expanded_sparse_search()`（via `summarize_expanded_sparse_matches`）

本文档面向开发者，说明 **Coverage Engine** 的设计目标、评分语义，以及 Python 实现与 MetadataDB SQL 两条路径的关系。

---

## 1. 设计理念

### 1.1 要解决的问题

多概念 query 下，传统检索分数（向量相似度、BM25）往往**无法表达「这篇论文覆盖了多少 query 意图」**：

- 一篇只讨论 `kidney` 的论文可能对 `adhesion protein in kidney` 仍有较高 BM25 分；
- 用户需要的是：**每个 semantic span 是否在文档中被充分表达**。

Coverage Engine 回答：**给定一篇文档的全文字段，query 的 semantic plan 覆盖了多少？**

### 1.2 两种使用场景

| 场景 | 入口 | 实现路径 |
|------|------|----------|
| **单文档分析** | dense/sparse 结果 annotate | `analyze_document_coverage()` — Python 逐篇计算 |
| **批量召回排序** | expanded sparse 分支 | MetadataDB SQL 内嵌 coverage；Python 侧用 `summarize_expanded_sparse_matches()` 归一化 SQL 输出 |

设计意图：**同一套 span_score / coverage_ratio 公式**，Python 为参考实现与 dev 注解，SQL 为 expanded sparse 生产路径。

### 1.3 设计原则

| 原则 | 含义 |
|------|------|
| **Span-average，非 binary** | 不是「全中/全不中」，而是每个 span 有 0~1 的 partial score |
| **共享 term 规则** | 与 Expanded Sparse 使用相同 tier1/tier2 + exact/prefix 语义 |
| **字段固定 v1** | 仅 `title + abstract + paper_keywords` |
| **可解释输出** | `matched_spans` / `missing_spans` 分开返回 |

---

## 2. 覆盖字段

v1 仅支持三类 document field（传入 `document_fields` dict）：

| 键 | 来源（hydrate 后典型路径） |
|----|---------------------------|
| `title` | `canonical_title` / `title` |
| `abstract` | `canonical_abstract` / `abstract` |
| `paper_keywords` | `paper_keywords` / `keywords`（list 或 str） |

匹配前统一：**小写 + 空白折叠**，对 keyword list 拼接为单一字符串。

---

## 3. 评分模型

### 3.1 Term 匹配规则

与 Semantic Plan 的 `match_mode` 一致：

| match_mode | 行为 |
|------------|------|
| `exact` | 词边界/短语边界匹配（非任意子串） |
| `prefix` | 词干前缀，如 `immun-` 匹配 `immunotherapy` |

parent 与 child 的 tier1/tier2 term 均参与匹配；命中 scope 记录在 `matched_scopes`（`parent` / `child`）。

### 3.2 Span score

对每个 **top-level span**：

```text
if parent own_term 任一命中:
    span_score = 1.0
elif span 有 children:
    span_score = matched_children / total_children
else:
    span_score = 0.0
```

- `matched_children`：至少有一个 child term 命中的 child span 数量；
- parent 自身命中即满分，**不要求** child 也命中。

### 3.3 Coverage ratio

```text
coverage_ratio = sum(span_score for each top-level span) / total_span_count
matched_span_count = count(span where span_score > 0)   # 兼容/debug 计数
```

**不是** binary「命中 span 数 / 总 span 数」；partial child coverage 会贡献 fractional score。

---

## 4. 核心 API

### 4.1 `analyze_document_coverage(plan, document_fields) -> CoverageReport`

**单文档** Python 路径：遍历 plan 每个 span，在 normalized fields 上匹配 term，计算 span_score，汇总为 `CoverageReport`。

典型调用：`PaperIndexer._annotate_results_with_coverage()` — 对 dense/sparse hydrate 结果逐条 annotate。

返回 `CoverageReport`：

```text
CoverageReport
├── matched_span_count
├── total_span_count
├── coverage_ratio
├── matched_spans: [...]   # 含 span_score, matched_terms, matched_fields, matched_scopes
└── missing_spans: [...]   # span_score == 0 的 span
```

### 4.2 `analyze_document_coverage_loose(plan, document_fields) -> CoverageReport`

**单文档 loose 路径**：与 strict 相同的 span 循环与 `CoverageReport` 形状，额外支持 **Guard-1 suffix** 匹配。

| | strict | loose |
|--|--------|-------|
| exact | ✅ | ✅ |
| prefix | ✅ | ✅ |
| suffix（Guard 1） | ❌ | ✅ |
| 生产默认 | `include_coverage` | **不默认接入** |

**Suffix 规则（Guard 1 only）**：

1. `term.match_mode == "exact"`（prefix 词项不走 suffix）
2. normalized term 为单词（无空格），或来自 span surface 拆词（len≥8 的 token）
3. `len(term) >= 8`
4. field 拆词后存在 `word`：`word != term`、`word.endswith(term)`、`len(term)/len(word) >= 0.5`

**评分**：

- exact / prefix 命中 parent：`span_score = 1.0`（与 strict 一致）
- **仅** suffix 命中 parent：`span_score = 0.5`（`LOOSE_SUFFIX_PARENT_SPAN_SCORE`）
- 同一 span 多种 parent 命中取 max（exact 覆盖 suffix）
- child partial 与 strict 相同

loose 的 `matched_spans[]` 额外含 `match_kinds: ["exact"|"prefix"|"suffix", ...]`（debug 用）；strict payload **不变**。

典型 case：`developmental` 在 `neurodevelopmental disorders` 中 → loose ratio=0.5；`renal` 在 `adrenal` 中 → loose ratio=0（len=5 < 8）。

Dev compare 页（`/expanded-compare`）对 dense / sparse / expanded sparse 三列并发请求 `include_loose_coverage=1`，展示每条结果的 `loose_coverage_ratio`，列头展示 `timings_ms.search`（检索耗时）与 `timings_ms.loose_coverage`（loose 计算耗时）。

### 4.3 `summarize_expanded_sparse_matches(plan, matched_spans) -> CoverageReport`

**归一化 SQL 输出**：MetadataDB 已返回 per-paper `matched_spans` 片段时，补齐 `span_score`、聚合 `coverage_ratio`，与 Python 路径对齐。

Expanded sparse 主流程：`match_papers_by_expanded_sparse_plan` → SQL 带 coverage → 本函数或直接消费 SQL 的 ratio。

---

## 5. 在 PaperIndexer 中的用法

```text
# 路径 A：dense/sparse 结果附加 coverage（dev compare）
search(..., search_type="dense", include_coverage=True, hydrate=True)
  -> _annotate_results_with_coverage()
  -> analyze_document_coverage()  per result

# 路径 B：expanded sparse 分支
expanded_sparse_search(...)
  -> match_papers_by_expanded_sparse_plan()
  -> summarize_expanded_sparse_matches()  # 或直接用 SQL coverage_ratio 作为 similarity
```

HTTP：`GET /api/search?search_type=dense&include_coverage=1`

---

## 6. Python 路径 vs SQL 路径

```text
                    QuerySemanticPlan
                           │
           ┌───────────────┴───────────────┐
           ▼                               ▼
 analyze_document_coverage()     MetadataDB expanded sparse SQL
 (单篇 document_fields)           (批量 papers + 内嵌 coverage)
           │                               │
           └───────────────┬───────────────┘
                           ▼
                  同一 coverage_ratio 公式
                  同一 span_score 语义
```

修改评分规则时，**必须同时**更新 Python 与 SQL（见 MetadataDB README）。

---

## 7. 测试

| 文件 | 覆盖点 |
|------|--------|
| `tests/indexing/test_coverage_engine.py` | exact/prefix、parent/child partial、ratio 公式、missing spans、loose suffix Guard 1 |
| `tests/indexing/test_expanded_sparse_retrieval.py` | PaperIndexer include_coverage annotate |

---

## 8. 调试 checklist

1. `total_span_count` 是否等于 `len(plan.spans)`；
2. 仅 child 命中时 span_score 是否为 `matched_children/total_children`；
3. parent 命中时是否直接 `1.0`；
4. prefix term（如 `renal-`）是否在 abstract 中正确匹配；
5. dense annotate 与 expanded sparse 对同一 paper 的 ratio 是否量级一致（允许 SQL/Python 实现细节差异，不应方向相反）。

---

## 9. 相关文档

- [Query Semantic Plan README](QUERY_SEMANTIC_PLAN_README.md)
- [Expanded Sparse Retrieval README](EXPANDED_SPARSE_RETRIEVAL_README.md)
- [MetadataDB README](../storage/METADATA_DB_README.md)
- [PaperIndexer README](PAPER_INDEXER_README.md)
- 设计：`docs/implementation_log/20260610/Expanded Sparse Retrieval & Coverage Engine Design.md`（主仓库）
- 实现：`docs/implementation_log/20260610/EXPANDED_SPARSE_RETRIEVAL_AND_COVERAGE_PLAN_20260610.md`（Task 3）
