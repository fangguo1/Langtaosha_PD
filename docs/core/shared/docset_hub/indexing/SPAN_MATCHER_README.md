# Span Matcher README

**位置**: `src/docset_hub/indexing/span_matcher.py`  
**相关编排**: `src/docset_hub/indexing/span_matcher_pipeline.py`  
**下游消费者**: `src/docset_hub/indexing/query_semantic_plan.py`、`src/docset_hub/indexing/expanded_sparse_retrieval.py`、`app/span_matcher_page.py`、`app/expanded_compare_page.py`、`PaperIndexer`

本文档描述当前 `span_matcher` 的真实职责、输出契约和调用链。它不是一个单独的检索器，也不负责结果融合；它负责把 query span 转成可解释的概念证据，并为后续的 semantic plan 与检索分支提供结构化输入。

## 1. 这个模块解决什么问题

`span_matcher` 负责把 query 中的短语候选映射为概念证据，回答三个问题：

1. 哪些 span 候选值得保留；
2. 每个 span 候选有哪些 ontology / keyword 证据；
3. 哪些候选可以进入最终的非重叠概念集合。

当前实现已经从“单点 matcher”演进为一组配套组件：

- `QueryPhraseAnalyzer` 负责 query 归一化与候选提取；
- `SpanMatcherExecutor` 负责候选扩展、匹配与结果对齐；
- `CompositeSpanMatcher` 负责把 ontology 与 keyword 匹配合并；
- `MaximalConceptSelector` 负责选择最终非重叠概念；
- `SpanMatcherPipeline` 负责把上述步骤编排成统一的运行结果；
- `build_query_semantic_plan()` 把 `SelectedConcept[] + SpanMatchResult[]` 转成面向下游检索的 semantic plan。

## 2. 核心数据结构

### 2.1 `ConceptMatchEvidence`

`ConceptMatchEvidence` 表示一次概念级命中，包含：

- `candidate_text`
- `normalized_text`
- `start` / `end`
- `candidate_kind`
- `source`
- `canonical`
- `concept_id`
- `confidence`
- `match_type`
- `aliases`
- `semantic_types`
- `payload`

它是 matcher 层最重要的可解释输出。下游如果要做 trace、semantic plan、expanded sparse query row 生成，都会从这里取证据。

### 2.2 `SpanMatchResult`

`SpanMatchResult` 是“一个候选 span 的全部证据”：

- `candidate`
- `evidence`

常见使用方式：

- `primary_evidence` 作为首选概念；
- `is_matched` 判断是否产生有效证据；
- `to_dict()` 用于 trace 或 API 返回。

### 2.3 `SelectedConcept`

`SelectedConcept` 是经过 `MaximalConceptSelector` 后保留下来的非重叠概念。它仍然保留原始候选与全部证据，方便下游做 semantic plan 和检索计划。

## 3. 主要组件

### 3.1 `KeywordSurfaceSpanMatcher`

这个 matcher 继续使用本地 phrase lexicon 做关键词表面匹配。

特点：

- 依赖 `PhraseLexiconMatcher`；
- 结果会被包装成 `ConceptMatchEvidence(source="keyword")`；
- `match_type` 会被映射成更细的内部类型，例如：
  - `keyword_exact`
  - `keyword_alias`
  - `keyword_normalized`
  - `keyword_surface`

它是 DB 本地词典路径，适合不依赖远程 ontology linker 的场景。

### 3.2 `RemoteOntologySpanMatcher`

这个 matcher 通过长驻 ontology linker 服务获取 UMLS / MeSH 证据。

特点：

- 通过 `POST /v1/link` 调用远程服务；
- 支持 `sources`、`top_k`、`threshold`、`timeout`；
- 统一把远程返回结果适配成 `ConceptMatchEvidence`；
- 会调用 `filter_ontology_evidence_items()` 做本地过滤。

它的职责是“请求和适配”，不是“决定 ontology 过滤策略”。过滤策略在 `entity_filter_policy.py`。

### 3.3 `CompositeSpanMatcher`

这个 matcher 把多个子 matcher 的证据合并并排序。

排序优先级默认是：

```text
umls > mesh > keyword
```

排序目标不是“语义最强”，而是“可解释且稳定”。

### 3.4 `SubphraseCandidateGenerator`

它从可信父 span 中生成一层子短语候选，供 `SpanMatcherExecutor` 和 semantic plan 使用。

关键规则：

- 只对特定 parent kinds 扩展；
- 只生成最多一层 subphrase；
- 过滤 stopwords 和低价值单 token；
- 不生成完全等于父 span 的候选。

这一步是当前 span matcher 能够支持“父 span + 子 span”结构的基础。

### 3.5 `SpanMatcherExecutor`

`SpanMatcherExecutor` 负责：

```text
extract candidates
  -> expand subphrases
  -> match_many()
  -> SpanMatchResult[]
```

它不做概念选择，不做 semantic plan，不做 retrieval。它只保证候选与证据一一对齐。

### 3.6 `MaximalConceptSelector`

`MaximalConceptSelector` 基于证据与候选类型打分，选择最终的非重叠概念。

当前它的职责是：

- 对 matched spans 排序；
- 按 offset 区间去重；
- 保留更优先的 span；
- 返回 `SelectedConcept[]`。

它不负责扩展 retrieval，也不负责 semantic plan 结构化。

## 4. 统一 Pipeline

### 4.1 `SpanMatcherProfile`

`SpanMatcherProfile` 是纯配置对象，描述运行行为，而不持有重对象。

当前常用 profile：

- `ontology_plus_keyword`
- `keyword_only`
- `ontology_only`

典型配置项包括：

- `enable_scispacy`
- `scispacy_model`
- `enable_ontology`
- `ontology_base_url`
- `ontology_sources`
- `ontology_top_k`
- `ontology_threshold`
- `enable_keyword`
- `paper_sources`
- `keyword_sources`
- `include_subphrases`
- `build_semantic_plan`

### 4.2 `SpanMatcherPipeline`

`SpanMatcherPipeline` 是当前对外最推荐的使用方式。它把 query 处理流程固定成一个稳定 contract：

```text
query
  -> normalize
  -> optional scispaCy
  -> extract candidates
  -> expand candidates
  -> match evidence
  -> filter / sort
  -> select non-overlapping concepts
  -> optionally build semantic plan
```

输出是 `SpanMatcherRunResult`，包含：

- `query`
- `normalized_query`
- `extractor_candidates`
- `expanded_candidates`
- `span_results`
- `selected_concepts`
- `semantic_plan`
- `timings_ms`
- `trace`

### 4.3 `SpanMatcherTrace`

trace 用于调试和可视化，不是正式检索契约。

它保存：

- `raw_ontology_items`
- `filtered_ontology_evidence`
- `keyword_evidence`

如果要排查“为什么某个 span 没有被选中”或“为什么某个 ontology 证据被过滤”，先看 trace。

## 5. 典型调用链

### 5.1 基础 matcher 链

```text
QueryPhraseAnalyzer
  -> SpanMatcherExecutor
  -> CompositeSpanMatcher
     -> RemoteOntologySpanMatcher
     -> KeywordSurfaceSpanMatcher
  -> MaximalConceptSelector
```

### 5.2 Pipeline 链

```text
SpanMatcherPipeline.from_profile(...)
  -> analyzer
  -> executor
  -> selector
  -> run(query)
```

### 5.3 下游 semantic plan 链

```text
SpanMatcherPipeline.run()
  -> selected_concepts
  -> span_results
  -> build_query_semantic_plan()
```

这个 semantic plan 目前是 expanded sparse retrieval、coverage 以及 trace 展示的共同输入。

## 6. 当前 semantic plan 约定

当前实现已经不再只输出平面 `SelectedConcept[]`。它会进一步构建 `QuerySemanticPlan`，其核心约定是：

- 顶层 semantic span 来自 `SelectedConcept[]`；
- 子 span 只来自 `subphrase_ngram` 候选；
- child 只允许一层，不产生 grandchild；
- term 以 `{text, match_mode}` 表示；
- `match_mode` 当前支持：
  - `exact`
  - `prefix`

其中，结尾带 `-` 的 alias 会被视为 prefix term，例如：

- `renal-` -> `{"text": "renal", "match_mode": "prefix"}`

这使 downstream 能区分“精确词项”和“词干前缀词项”。

### 6.1 下游流程浓缩图

```text
query
  -> SpanMatcherPipeline
  -> selected_concepts + span_results
  -> build_query_semantic_plan()
  -> QuerySemanticPlan
     -> coverage_engine.analyze_document_coverage()         # 单篇文档覆盖分析
     -> expanded_sparse_retrieval.match_papers_by_expanded_sparse_plan()
        -> MetadataDB.lookup_papers_by_expanded_sparse_groups()
        -> coverage_ratio / matched_spans
        -> PaperIndexer 作为 expanded_sparse branch score 参与融合
```

要点：

- `coverage_engine` 是语义覆盖的 Python 参考实现和报告归一层；
- 实际 expanded sparse 召回时，coverage 由 `MetadataDB` 的 SQL 直接计算；
- `PaperIndexer` 不重新算 coverage，只消费 `coverage_ratio` 和 `matched_spans`；
- 这三层共用同一个 `QuerySemanticPlan` 语义契约。

## 7. 在项目里的实际入口

### 7.1 Web 调试页

- `app/span_matcher_page.py`

它通过 `SpanMatcherPipeline` 提供页面级调试能力，返回 selected concepts、semantic plan 和 timings。

### 7.2 对比页

- `app/expanded_compare_page.py`

它会复用同一套 span matcher 语义理解结果，保证页面对比与检索逻辑一致。

### 7.3 `PaperIndexer`

- `src/docset_hub/indexing/paper_indexer.py`

`PaperIndexer` 里的 query understanding / keyword lookup / expanded sparse 相关路径，会依赖 span matcher 的统一输出。

### 7.4 Expanded sparse retrieval

- `src/docset_hub/indexing/expanded_sparse_retrieval.py`

它把 semantic plan 转成 expanded sparse query rows，再交给 `MetadataDB` 做论文召回。

## 8. 调试建议

如果要排查 span matcher 问题，建议按这个顺序看：

1. `QueryPhraseAnalyzer` 是否提取到了正确的候选；
2. `SpanMatcherExecutor.expand_candidates()` 是否生成了预期的 subphrase；
3. `RemoteOntologySpanMatcher` 返回的原始 evidence 是否被 `filter_ontology_evidence_items()` 过滤掉；
4. `CompositeSpanMatcher` 排序后 primary evidence 是否符合预期；
5. `MaximalConceptSelector` 是否因为重叠区间丢掉了某个 span；
6. `build_query_semantic_plan()` 是否把 selected concepts 和 child spans 组织正确。

常见问题：

- 远程 ontology linker 不可用时，ontology 路径会失败并抛出 `OntologyLinkerServiceUnavailable`；
- `requests` 不可用时，`RemoteOntologySpanMatcher` 无法初始化；
- 空 query 会直接报错；
- 如果 profile 关闭了 keyword matcher，但 metadata_db 没传，会在构建 pipeline 时失败。

## 9. 相关文档

- [Indexing 模块架构](README.md)
- [PaperIndexer 功能地图](PAPER_INDEXER_FUNCTION_MAP.md)
- [PaperIndexer API 与示例](PAPER_INDEXER_README.md)
- [Query Semantic Plan 树结构实现计划](../../../../implementation_log/20260610/SPAN_MATCHER_TREE_PREFIX_IMPLEMENTATION_PLAN_20260610.md)
- [Span Matcher Pipeline 实现计划](../../../../implementation_log/20260610/SPAN_MATCHER_PIPELINE_PROFILE_IMPLEMENTATION_PLAN_20260610.md)
- [Expanded Sparse Retrieval 设计](../../../../implementation_log/20260610/EXPANDED_SPARSE_RETRIEVAL_AND_COVERAGE_PLAN_20260610.md)
