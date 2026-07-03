# Query Semantic Plan README

**源码**: `src/docset_hub/indexing/query_semantic_plan.py`  
**上游**: [Span Matcher](SPAN_MATCHER_README.md) → `SelectedConcept[]` + `SpanMatchResult[]`  
**下游**: [Expanded Sparse Retrieval](EXPANDED_SPARSE_RETRIEVAL_README.md)、[Coverage Engine](COVERAGE_ENGINE_README.md)

本文档面向开发者，说明 **Query Semantic Plan** 的设计目标、数据契约与构建规则。它是 Span Matcher 与检索分支之间的**稳定中间表示**。

---

## 1. 设计理念

### 1.1 为什么需要 Semantic Plan

Span Matcher 的输出是「概念证据集合」——适合解释与筛选，但不适合直接驱动检索：

- `SelectedConcept[]` 是**平面列表**，丢失了 parent/child 结构与 term 匹配语义；
- Expanded Sparse 与 Coverage 需要同一套 **span 分组 + tier1/tier2 词项 + exact/prefix** 规则；
- API 与调试页需要**可序列化、可 diff** 的结构化契约。

Semantic Plan 的定位：**把 query understanding 从「选了哪些概念」升级为「每个检索意图有哪些可匹配词项」**，但不承担召回或打分。

### 1.2 设计原则

| 原则 | 含义 |
|------|------|
| **单一契约** | Expanded Sparse 与 Coverage 共用同一 `QuerySemanticPlan`，避免两套语义漂移 |
| **一层子树** | 每个 top-level span 最多一层 child，不产生 grandchild |
| **tier 分离** | tier1 = 表面/主 canonical；tier2 = 仅来自 ontology（UMLS/MeSH）别名 |
| **显式 match_mode** | 词项不是 plain string，而是 `{text, match_mode}`，`exact` / `prefix` 分开处理 |
| **证据可追溯** | span 上保留 `ConceptMatchEvidence[]`，便于 trace 与调试 |

### 1.3 模块边界

**负责**：

- 定义 `QuerySemanticPlan` 及 span/term 数据结构；
- 从 matcher 输出构建 plan（`build_query_semantic_plan`）；
- JSON 序列化（`serialize_semantic_plan`），供 API 与页面共用。

**不负责**：

- 候选提取、ontology 匹配、概念选择（→ Span Matcher）；
- 论文召回、SQL、coverage 打分（→ Expanded Sparse / Coverage / MetadataDB）。

---

## 2. 核心数据结构

### 2.1 `QuerySemanticPlan`

```text
QuerySemanticPlan
├── original_query: str
├── normalized_query: str
└── spans: List[SemanticSpanGroup]
```

一个 query 对应一棵**语义 span 森林**（多个 top-level span，彼此表示不同检索意图）。

示例：`adhesion protein in kidney` → `S1=adhesion protein`，`S2=kidney`（不是拆成三个单词）。

### 2.2 `SemanticSpanGroup`（top-level span）

表示**一个独立检索意图**，来自 `MaximalConceptSelector` 选中的 `SelectedConcept`：

| 字段 | 含义 |
|------|------|
| `span_id` | 稳定 ID，如 `s1`, `s2` |
| `surface_text` / `normalized_text` | query 中的表面形式 |
| `start` / `end` | 在 normalized query 中的偏移 |
| `canonical_text` | primary evidence 的 canonical |
| `own_terms` | `SemanticTermBucket`（tier1 + tier2） |
| `children` | 一层 `SemanticChildSpan[]` |
| `evidence` | 完整证据链（trace 用） |

### 2.3 `SemanticChildSpan`

子 span **只**来自 `subphrase_ngram` 候选，且必须**完全包含**在 parent 区间内：

- 不允许与 parent 同起止；
- 不允许 grandchild；
- ontology alias **不会**生成新 child span。

### 2.4 `SemanticTerm` 与 tier 规则

```python
SemanticTerm(text: str, match_mode: str = "exact")  # match_mode: "exact" | "prefix"
```

| Tier | 来源 | 规则 |
|------|------|------|
| **tier1** | candidate text、normalized text、primary canonical | 全部 `exact` |
| **tier2** | ontology evidence（`umls` / `mesh`）的 canonical 与 aliases | 非 primary 的 canonical + aliases；alias 以 `-` 结尾 → `prefix` |

**禁止**混入：keyword-surface 别名、DB 候选词、子串扩展词。

---

## 3. 核心 API

### 3.1 `build_query_semantic_plan(...)`

```python
build_query_semantic_plan(
    *,
    original_query: str,
    normalized_query: str,
    selected_concepts: Sequence[SelectedConcept],
    span_results: Sequence[SpanMatchResult] | None = None,
) -> QuerySemanticPlan
```

**输入**：

- `selected_concepts`：selector 最终概念（必需）；
- `span_results`：全部 span 匹配结果（用于挂 child，可选但推荐）。

**输出**：按 selector 顺序编号的 `s1`, `s2`, …；无 evidence 的 concept 跳过。

`PaperIndexer.build_query_semantic_plan()` 是对 Span Matcher Pipeline + 本函数的封装，是对外推荐入口。

### 3.2 `serialize_semantic_plan(plan)`

Domain 层**唯一**序列化出口，供 `/api/semantic-plan`、span matcher 调试页、expanded compare 共用。  
序列化 payload **不含** evidence 细节（API 体积与稳定性考虑）。

---

## 4. 构建流程

```text
SelectedConcept[]          SpanMatchResult[]（含 subphrase_ngram）
        │                            │
        └──────────┬─────────────────┘
                   ▼
        build_query_semantic_plan()
                   │
                   ▼
        对每个 SelectedConcept:
          1. tier1 ← surface + primary canonical
          2. tier2 ← ontology aliases（UMLS/MeSH only）
          3. children ← 落在 parent 区间内的 subphrase_ngram
                   │
                   ▼
             QuerySemanticPlan
```

---

## 5. 与下游的契约

| 消费者 | 使用的 plan 内容 |
|--------|------------------|
| Expanded Sparse | 全部 span 的 tier1/tier2 + child terms → 展开为 query rows |
| Coverage Engine | 同样 term 集合 + match_mode → 对 document fields 做匹配 |
| MetadataDB SQL | 接收 expanded sparse 序列化后的 groups/rows（见 storage README） |

**关键约束**：下游不得自行从 `SelectedConcept[]` 重建 plan，应始终调用 `build_query_semantic_plan()` 或消费 `PaperIndexer` 返回的 plan。

---

## 6. 测试

| 文件 | 覆盖点 |
|------|--------|
| `tests/indexing/test_query_semantic_plan.py` | tier 来源、child 挂载、prefix 规则、serialize |
| `tests/indexing/test_span_matcher.py` | matcher → plan builder 兼容性 |

---

## 7. 调试 checklist

1. top-level span 数量是否与预期概念数一致；
2. tier2 是否**只**含 ontology 词项；
3. child 是否只来自 `subphrase_ngram` 且仅一层；
4. `renal-` 类 alias 是否变为 `{text: "renal", match_mode: "prefix"}`；
5. 空 `selected_concepts` → 空 plan（下游应短路返回）。

---

## 8. 相关文档

- [Span Matcher README](SPAN_MATCHER_README.md) — 上游证据与选择
- [Expanded Sparse Retrieval README](EXPANDED_SPARSE_RETRIEVAL_README.md) — 下游召回
- [Coverage Engine README](COVERAGE_ENGINE_README.md) — 下游覆盖分析
- [Indexing 模块架构](README.md)
- 设计：`docs/implementation_log/20260610/Span Matcher Modification Design_20260610.md`
- 实现：`docs/implementation_log/20260610/EXPANDED_SPARSE_RETRIEVAL_AND_COVERAGE_PLAN_20260610.md`（Task 1）
