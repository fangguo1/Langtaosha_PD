# 搜索功能流交付地图

**日期**: 2026-06-05
**状态**: Delivery Review
**目标**: 定义并验证用户 query 从进入正式 API 到生成最终搜索结果的唯一正式执行链路。

## 1. 范围与交付结论

本文件覆盖：

```text
HTTP request
  -> query understanding
  -> retrieval
  -> fusion / filtering
  -> metadata hydrate
  -> API response
  -> trace / evaluation
```

数据抓取与入库链路见：

- `01_data_flow_delivery_map.md`
- `03_release_acceptance_checklist.md`

当前总体状态：`DEGRADED`

主要原因：

- 正式 API 已使用 `hybrid_retrieval`，支持 Dense、Sparse、Keyword Lookup 三路召回和单路失败降级。
- API 已生成或透传 `request_id`，但尚未形成贯穿 query understanding、各检索分支、过滤和最终结果的完整 Search Trace。
- `PaperIndexer.smart_search()` 与正式 API 中的智能搜索编排并非完全同一条代码路径，存在语义漂移风险。
- 用户反馈显示主要问题已从“无召回”转为“低相关结果展示过多”和“多概念约束不足”。

## 2. 唯一正式入口

### 2.1 当前正式 API

```http
GET /api/scholar/search
```

当前调用链：

```text
app/main.py::api_scholar_search
  -> run_scholar_search
  -> QueryUnderstandingService.analyze
  -> author route or _prioritized_vector_search
  -> PaperIndexer.search(search_type="hybrid_retrieval")
  -> PaperIndexer.hybrid_retrieval_search
  -> API result mapping
```

正式默认搜索类型：

```text
hybrid_retrieval
```

### 2.2 当前入口分叉风险

仓库同时存在：

- `app/main.py::run_scholar_search()`
- `PaperIndexer.smart_search()`
- `PaperIndexer.search()`
- 调试页面和回放脚本中的直接搜索调用

其中 `PaperIndexer.smart_search()` 的主题搜索默认调用 `PaperIndexer.search()`，而 `PaperIndexer.search()` 默认 `search_type="dense"`。正式 API 则显式使用 `hybrid_retrieval`。

交付要求：

- API、用户测试、回放脚本和管理面板必须验证同一条正式链路。
- 直接调用 `PaperIndexer.smart_search()` 时必须明确其是否属于正式入口。
- 非正式入口应标记为 internal/debug/legacy，避免被误用作验收依据。

建议后续收束为：

```text
SearchService.execute(SearchRequest) -> SearchResponse
```

## 3. 正式搜索功能流

```text
GET /api/scholar/search
  -> authentication / CORS / request_id
  -> request parameter validation
  -> query normalization
  -> query understanding
      -> author match
      -> author suggestion
      -> query correction
      -> semantic route
  -> retrieval policy
      -> metadata author search
      -> or per-source hybrid retrieval
  -> parallel retrieval
      -> Dense
      -> Sparse / BM25
      -> Keyword Lookup
  -> branch failure handling
  -> weighted RRF fusion
  -> result filtering
  -> metadata hydrate
  -> source-priority merge and dedupe
  -> result explanation / highlight
  -> API response
```

## 4. 阶段交付表

状态定义：

- `PASS`: 已满足当前交付要求。
- `DEGRADED`: 主流程可用，但存在已知降级或验证缺口。
- `FAIL`: 阻断正式搜索。
- `PENDING`: 尚未完成本轮验收。
- `NOT_IN_SCOPE`: 本次明确不交付。

| 阶段 | 核心输入 | 核心输出 | 当前状态 | 交付要求 |
| --- | --- | --- | --- | --- |
| API 鉴权与 CORS | HTTP request | 授权后的请求 | `PASS` | 公开与受保护 API 边界明确 |
| Request ID | request header | `request_id` | `PASS` | 响应头与 body 可定位请求 |
| 参数校验 | query、top_k、source_list、mode | 标准请求 | `PASS` | 非法请求返回稳定错误结构 |
| Query normalization | raw query | normalized query | `PASS` | 空白、标点和大小写行为稳定 |
| Intent / route | normalized query | route、confidence | `DEGRADED` | 作者与主题路由可回归 |
| Query correction | query、keyword candidates | corrected query | `DEGRADED` | 高置信纠错可解释，可回退 |
| Metadata author search | author query | 作者论文结果 | `PASS` | 高置信作者查询稳定 |
| Dense retrieval | semantic query | dense candidates | `PASS` | 可降级，低相关候选可过滤 |
| Sparse retrieval | lexical query | BM25 candidates | `PASS` | 仅正证据结果进入融合 |
| Keyword lookup | concept spans | keyword candidates | `PASS` | 无 concept 时空分支不算失败 |
| Branch degradation | branch exception | remaining branches | `PASS` | 单路失败不阻断搜索 |
| Weighted RRF | branch rankings | merged candidates | `PASS` | 不直接混合不可比分数 |
| Result filtering | merged candidates | retained results | `DEGRADED` | 多概念覆盖和低相关控制需继续验收 |
| Metadata hydrate | work_id/paper_id | complete metadata | `PASS` | 结果可展示，缺失可定位 |
| Source merge/dedupe | per-source results | final list | `DEGRADED` | 当前按 source 分组优先展示，需确认产品语义 |
| Explanation/highlight | retrieval debug | reasons、highlight | `DEGRADED` | 用户解释与内部 trace 需区分 |
| Search Trace | entire request | searchable trace | `PENDING` | 能按 request_id 还原完整链路 |

## 5. Query Understanding

### 5.1 当前主要路由

| Route | 行为 | 交付要求 |
| --- | --- | --- |
| `metadata_author` | 调用 MetadataDB 作者检索 | 高置信命中才自动路由 |
| `author_suggestion` | 返回作者建议，不返回论文 | 前端明确提示下一步 |
| `vector` / semantic route | 进入正式混合检索 | 必须使用冻结后的 retrieval policy |
| `none` | 空或无效 query | 返回参数错误或稳定空结果语义 |

### 5.2 Query Understanding Trace

每次请求至少记录：

```text
original_query
normalized_query
intent
route
confidence
matched_author
suggested_author
corrected_query
corrections
expansion status
reason
```

### 5.3 当前验收重点

- `xiao fan`、`fan xiao`、大小写和逗号变体。
- `machi learning` 等拼写纠错。
- `CAR-T` 等缩写和专业术语。
- 多概念 query 不应只命中其中一个泛化概念。

## 6. 三路召回与融合

### 6.1 分支职责

| 分支 | 主要职责 | 正向证据 | 失败后的行为 |
| --- | --- | --- | --- |
| Dense | 语义泛化召回 | similarity、dense keyword hard filter | 其他分支继续 |
| Sparse/BM25 | 精确词面召回 | BM25 raw score `> 0` | 其他分支继续 |
| Keyword Lookup | 高置信关键词/概念召回 | keyword lookup score `> 0` | 其他分支继续 |
| Metadata Author | 作者论文召回 | 作者 DB 匹配 | 不进入论文混合检索 |

### 6.2 并行与降级

当前三路检索通过线程池并行执行。

降级语义：

- 单路失败：记录失败，使用其他成功分支继续。
- 某一路无正证据：返回空列表，不算失败。
- 全部分支失败：正式搜索失败，不得伪装成正常空结果。

### 6.3 Weighted RRF

当前融合原则：

```text
final_score += branch_weight / (rrf_k + branch_rank)
```

默认权重：

```text
dense: 0.4
sparse: 0.4
keyword_lookup: 0.2
```

交付要求：

- API 中的 `similarity` 不应被解释为普通语义相似度。
- 每个结果应保留 matched retrievers 和各分支 rank/score，供内部 trace 使用。
- 分支权重和阈值需要版本化或进入配置快照。

## 7. 结果过滤、展示与解释

### 7.1 当前主要问题

现有用户反馈集中在：

- 多关键词 query 只命中部分概念。
- 固定短语被拆散后产生弱相关结果。
- 宽泛 query 返回过多低相关结果。
- 用户倾向于少看结果，也不希望展示明显不相关内容。

代表性失败 query：

- `enhancer-promoter interaction`
- `genome 3D structure regulation`
- `custom artificial intelligent architecture`
- `synapse`
- `developmental disorder`
- `obesity and macrophage`

### 7.2 冲刺阶段优化原则

优先级：

```text
减少错误展示
  > 强化多概念覆盖
  > 保留可解释召回证据
  > 增加更多召回分支
```

交付前需确认：

- 是否允许结果少于 `top_k`。
- 多概念 query 是否要求 required concept coverage。
- Dense hard filter 阈值是否适用于正式 API。
- Source 分组优先展示是否会扭曲全局排名。
- 低相关结果的隐藏阈值如何解释和回归。

## 8. API 契约

### 8.1 请求参数

| 参数 | 类型 | 当前语义 |
| --- | --- | --- |
| `query` | string | 必填 |
| `top_k` | integer | 当前按 source 拉取，最终结果可能超过该值 |
| `source_list` | CSV string | 限制检索 source |
| `mode` | `smart` / `vector` | `vector` 当前仍使用正式 hybrid retrieval，只跳过智能路由 |

### 8.2 成功响应核心字段

```json
{
  "success": true,
  "request_id": "...",
  "query": "...",
  "search_query": "...",
  "search_mode": "smart",
  "query_understanding": {},
  "result_policy": {},
  "notice": {},
  "count": 10,
  "results": []
}
```

### 8.3 需冻结的契约风险

| 风险 | 当前状态 | 交付建议 |
| --- | --- | --- |
| `top_k` 是 per-source，不是最终 limit | `DEGRADED` | 文档明确或调整语义 |
| hybrid `similarity` 不是语义相似度 | `DEGRADED` | 前端不展示为百分比 |
| 暂无分页 | `NOT_IN_SCOPE` | 明确第一版一次性结果列表 |
| 暂无 streaming | `NOT_IN_SCOPE` | 从交付承诺中移除 |
| 作者 API 尚未独立冻结 | `PENDING` | 明确是否属于本次交付 |

## 9. Search Trace

### 9.1 当前基础

- API 已生成或透传 `request_id`。
- 响应 header 和 body 均带 request id。
- 检索结果内部保留 `retrieval_debug`。
- Study Mode 可保存 query、route 和结果快照。

### 9.2 最小 Search Trace 契约

每次正式搜索至少记录：

```text
request_id
started_at
finished_at
latency_ms
query
normalized_query
route
corrected_query
source_list
retrieval_policy_version
branch_latency_ms
branch_candidate_count
branch_failure
merged_candidate_count
filtered_candidate_count
final_result_count
top_result_work_ids
error_code
```

### 9.3 Trace 验收

输入任意失败或慢请求的 `request_id`，负责人应能够回答：

1. Query 被识别为什么意图？
2. 实际使用了什么 query？
3. 哪些检索分支运行成功或失败？
4. 各分支召回多少候选、耗时多少？
5. 哪些结果被过滤，为什么？
6. 最终为什么返回这些论文？

当前状态：`PENDING`

## 10. 搜索质量回归集

### 10.1 分类

| 类型 | 示例 | 核心验收点 |
| --- | --- | --- |
| 作者名 | `xiao fan`、`fan xiao` | 作者路由与姓名变体 |
| 精确术语 | `CAR-T`、`human M-channel` | 精确词面与术语召回 |
| 拼写错误 | `machi learning` | 纠错可解释且不过度 |
| 固定短语 | `brain computer interface` | 短语完整性 |
| 多概念组合 | `obesity and macrophage` | required concept coverage |
| 关系型 query | `enhancer-promoter interaction` | 关系和短语不可丢失 |
| 宽泛主题 | `synapse` | 控制低相关结果 |
| 降级测试 | 人工使某分支失败 | 其他分支继续返回 |

### 10.2 单条 Query 验收模板

| 字段 | 内容 |
| --- | --- |
| Query |  |
| Query 类型 |  |
| 预期 route |  |
| 预期 correction |  |
| 必须出现的论文/概念 |  |
| 不应出现的典型结果 |  |
| Top 3 结果判断 |  |
| Top 10 低相关结果数 |  |
| 实际分支 |  |
| 总耗时 |  |
| 结论 | `PENDING` |

### 10.3 建议交付指标

阈值需要在当前回归集首次运行后由负责人确认：

| 指标 | 建议目标 | 状态 |
| --- | --- | --- |
| 核心 query 路由正确率 | `>= 95%` | `PENDING` |
| 作者名核心案例正确率 | `100%` | `PENDING` |
| Top 3 人工相关率 | `>= 80%` | `PENDING` |
| 多概念 query 部分命中错误数 | 明显低于当前基线 | `PENDING` |
| 单分支失败后的可用率 | `100%` | `PENDING` |
| 全部分支失败时正确报错 | `100%` | `PENDING` |
| API P95 latency | 负责人确认阈值 | `PENDING` |

## 11. 降级矩阵

| 故障 | 预期行为 | 用户可见结果 | 当前状态 |
| --- | --- | --- | --- |
| Dense 失败 | Sparse + Keyword Lookup 继续 | 可返回较精确结果 | `PASS` |
| Sparse 失败 | Dense + Keyword Lookup 继续 | 可返回语义与概念结果 | `PASS` |
| Keyword Lookup 失败 | Dense + Sparse 继续 | 可返回结果，解释能力下降 | `PASS` |
| MetadataDB 作者检索失败 | 作者 route 返回错误或明确降级 | 不应伪装为空作者结果 | `PENDING` |
| Metadata hydrate 失败 | 记录错误并避免返回不可展示结果 | 结果数可能下降 | `PENDING` |
| 全部检索分支失败 | 返回 `SEARCH_FAILED` | 明确错误与 request_id | `PASS` |
| Ontology linker 失败 | 正式搜索不应被阻断 | 概念增强能力下降 | `DEGRADED` |

## 12. 已知风险与非本次范围

### 已知风险

- 正式 API 与 `PaperIndexer.smart_search()` 存在编排语义分叉。
- Search Trace 尚未完整落库。
- `top_k` 和 source-priority display 可能导致前端误解全局排序。
- 当前主要质量风险是低相关结果展示，而不是完全无召回。

### 非本次范围

- 完整 MeSH/UMLS 在线本体推理。
- 复杂学习排序模型。
- 大规模 benchmark。
- Streaming search。
- 完整分页协议。

## 13. 最终签收

| 项目 | 内容 |
| --- | --- |
| 搜索功能流总体状态 | `DEGRADED` |
| 正式搜索入口 | `GET /api/scholar/search` |
| 默认检索策略 | `hybrid_retrieval` |
| 阻断项 | Search Trace、固定回归集与入口收束尚未完成 |
| 可接受降级 | 单个检索分支失败后继续服务 |
| 负责人 | `PENDING` |
| 验收日期 | `PENDING` |
