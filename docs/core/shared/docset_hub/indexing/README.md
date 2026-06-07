# Indexing 模块架构

`src/docset_hub/indexing` 是 Docset Hub 的应用编排层。它连接 metadata 与 storage，并承载论文索引、关键词处理、query understanding 和多路检索融合。

按功能梳理的 `PaperIndexer` 调用关系、决策与边界见
[PAPER_INDEXER_FUNCTION_MAP.md](PAPER_INDEXER_FUNCTION_MAP.md)；详细 API 与示例见
[PAPER_INDEXER_README.md](PAPER_INDEXER_README.md)。

## 1. 核心职责

Indexing 模块负责：

- 编排单条和文件级论文入库；
- 根据 MetadataDB 写入结果决定是否刷新检索资产；
- 构造统一的 Dense 与 Sparse 索引文本；
- 执行关键词抽取、概念扩充、筛选和写回；
- 分析用户 query 并形成检索计划；
- 并行执行 Dense、Sparse 和 Keyword Lookup；
- 对候选结果过滤、融合、去重和 hydrate。

它不负责：

- 来源原始数据抓取；
- PostgreSQL 或 VectorDB 的底层协议；
- 每日任务生命周期；
- HTTP 请求校验和 API 响应结构。

## 2. 核心组件

| 组件 | 核心功能 |
| --- | --- |
| `PaperIndexer` | 跨 metadata 与 storage 的统一编排入口 |
| `QueryUnderstandingService` | query normalization、作者匹配、纠错和路由 |
| `QueryPhraseAnalyzer` | 短语归一化、候选提取、词典匹配和 query type 判断 |
| `SpanMatcher` family | 将 query span 与关键词、ontology 概念匹配 |
| `PaperKeywordLookup` | 构造关键词检索计划并查询候选论文 |
| `KeywordEnrichmentService` | 从论文文本抽取和扩充关键词 |
| `KeywordCandidateSelector` | 对关键词候选聚合、排序与选择 |
| `dense_result_filter` | 在融合前过滤低质量 Dense 候选 |
| `search_highlighting` | 根据 query 与检索证据生成高亮信息 |

## 3. `PaperIndexer` 的两类编排

### 3.1 写入编排

```text
index_dict()
  -> MetadataTransformer.transform_dict()
  -> MetadataDB.insert_paper()
  -> Dense vectorization
  -> Sparse vectorization
  -> Keyword enrichment
  -> unified result
```

Metadata 写入失败会阻断后续阶段。Dense、Sparse 和关键词阶段分别报告结果，可能发生部分降级。

### 3.2 检索编排

```text
hybrid_retrieval_search()
  -> Dense branch
  -> Sparse branch
  -> Keyword Lookup branch
  -> weighted RRF
  -> hydrate by work_id
```

此路径是当前正式 API 的主要候选召回路径。

## 4. Public 入口语义

| 入口 | 用途 | 当前注意事项 |
| --- | --- | --- |
| `index_dict()` | 写入单条来源记录 | 当前将非 `insert` mode 降级为 `insert` |
| `index_file()` | 写入单个文件 | 仍委托相同写入编排 |
| `search()` | 统一检索入口 | 默认 `search_type="dense"` |
| `hybrid_retrieval_search()` | 三路检索与融合 | 当前正式 API 使用 |
| `smart_search()` | query understanding + route + search | 主题检索默认最终走 Dense，与正式 API 不完全一致 |
| `read()` | 读取论文 | 通过 storage 层读取 |
| `delete()` | 删除论文及相关资产 | 需要关注跨存储删除一致性 |

## 5. 写入决策

Indexing 不自行判断论文身份，而是依赖 MetadataDB 返回的写入状态和 canonical 信息：

```text
MetadataDB write result
  -> status_code
  -> canonical_changed
  -> canonical_source_name
  -> Dense/Sparse/Keyword trigger decision
```

这使写入语义集中在 MetadataDB，同时要求其返回结果稳定且可解释。

## 6. 检索策略

当前应用层混合检索默认权重：

```text
dense: 0.4
sparse: 0.4
keyword_lookup: 0.2
```

主要规则：

- 三个分支并行执行；
- Dense 候选先经过硬过滤；
- 单分支失败允许降级；
- 所有请求分支失败时整体失败；
- 使用 `work_id` 去重；
- 使用加权 RRF，不直接混合异构原始分数；
- 融合结果保留 `retrieval_debug`。

## 7. Review 重点

1. `PaperIndexer` 是否承担了过多不相干职责。
2. 写入子阶段的部分失败是否有统一状态语义。
3. `smart_search()` 与正式 API 是否应收束到同一个 Search Service。
4. `VectorDB.hybrid_search()` 与应用层三路混合是否需要统一命名或限制使用。
5. query understanding、phrase analyzer、span matcher 和 keyword lookup 的职责边界是否清晰。
6. 检索阈值、分支权重和过滤规则是否应版本化。

## 8. 相关文档

- [Docset Hub 总览](../README.md)
- [核心契约](../CORE_CONTRACTS.md)
- [PaperIndexer 功能地图](PAPER_INDEXER_FUNCTION_MAP.md)
- [PaperIndexer API 与示例](PAPER_INDEXER_README.md)
- [数据写入与索引流](../flows/DATA_INGESTION_FLOW.md)
- [查询与检索流](../flows/SEARCH_RETRIEVAL_FLOW.md)
