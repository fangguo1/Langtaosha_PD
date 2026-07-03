# Langtaosha Logging System 最小可执行方案

## 0. 目标

本方案目标不是一次性建设完整 observability 平台，而是在现有 Langtaosha 架构上补齐两个核心能力：

1. **数据录入流可判断**：一次 daily orchestrator 到底是否成功、哪里失败、失败是否影响主流程。
2. **检索流可复盘**：一次用户搜索为什么走了这个 route，三路召回各返回多少，融合后多少，hydrate 是否缺失。

当前阶段不引入复杂数据库 trace 表，不建设完整链路追踪系统。优先使用：

```text
本地 manifest + stdout/stderr + API structured log + request_id
```

---

## 1. 范围

### 1.1 本次纳入

- Orchestrator 本地 logging / manifest 规范化
- 搜索 API 的 request_id
- 搜索请求的 retrieval_debug JSON
- query understanding 摘要追踪
- dense / sparse / keyword_lookup 三路召回摘要追踪
- RRF fusion 摘要追踪
- Metadata hydration 摘要追踪
- 错误、降级、空结果的基本状态区分

### 1.2 本次不纳入

- PostgreSQL `pipeline_runs` / `pipeline_steps` 表
- 独立 ClickHouse / ELK / Loki 系统
- OpenTelemetry 全链路 trace
- 每个候选 paper 的完整日志落库
- 用户行为分析系统
- dashboard

---

## 2. 设计原则

### 2.1 数据录入流：manifest first

Orchestrator 已经保存：

```text
local_data/daily_orchestrator/<target_date>/manifest.json
local_data/daily_orchestrator/<target_date>/logs/
docs/daily_orchestrator_log/
```

因此当前不需要额外建数据库任务表。优先增强 manifest，使它能够回答：

```text
今天 pipeline 是否成功？
哪些步骤执行了？
哪些步骤失败了？
失败步骤是否 required？
是否存在可选步骤失败但整体可用的 degraded 状态？
每个步骤处理了多少记录？
失败后能否只重跑失败步骤？
```

### 2.2 检索流：request debug first

搜索请求不是后台 job，不应建模为 pipeline run。

每次搜索生成一个 `request_id`，在一次请求内记录一个 compact `retrieval_debug` JSON，用于 debug、回放和评估。

### 2.3 摘要优先，不记录完整大对象

日志里只记录：

```text
route
count
latency
status
score_type
missing count
top N sample
error summary
```

不默认记录完整候选列表、完整 abstract、完整用户上下文。

---

## 3. 数据录入流方案

## 3.1 当前问题

当前 Orchestrator 的状态语义偏粗：

```text
step status: ok / failed / skipped / dry_run
pipeline status: ok / partial_failure / failed
```

问题在于：

```text
allow_failure=True 的命令即使 returncode != 0，也可能被标记为 skipped；
整体 pipeline 可能把 skipped 当作成功。
```

这会造成：manifest 显示 ok，但实际上可选步骤异常，或者数据不完整。

---

## 3.2 修改目标

### 新增 step status

```text
ok
failed
skipped_expected
skipped_suspicious
dry_run
degraded
```

### 新增 pipeline status

```text
ok
partial_failure
failed
degraded
```

### 状态含义

| 状态 | 含义 |
|---|---|
| `ok` | 步骤成功完成 |
| `failed` | required 步骤失败，影响主流程 |
| `skipped_expected` | 符合预期的跳过，例如未开启 vector stage |
| `skipped_suspicious` | 不符合预期的跳过，例如输入缺失、returncode 非 0 但 allow_failure |
| `dry_run` | 只模拟执行 |
| `degraded` | 主流程可用，但可选步骤失败或数据不完整 |

---

## 3.3 Manifest 标准结构

建议每次运行输出：

```json
{
  "run_id": "daily_2026-06-07_153000",
  "target_date": "2026-06-07",
  "pipeline_status": "degraded",
  "started_at": "2026-06-07T15:30:00+08:00",
  "ended_at": "2026-06-07T15:42:00+08:00",
  "duration_seconds": 720,
  "config": {
    "dry_run": false,
    "run_vector_stage": false,
    "run_author_enrichment": true
  },
  "summary": {
    "steps_total": 8,
    "steps_ok": 6,
    "steps_failed": 0,
    "steps_degraded": 1,
    "steps_skipped_expected": 1,
    "steps_skipped_suspicious": 0,
    "records_raw": 120,
    "records_ingested": 118,
    "records_failed": 2
  },
  "steps": [
    {
      "step_name": "fetch_biorxiv",
      "required": true,
      "status": "ok",
      "command": "python scripts/bioarxiv/biorxiv_api.py ...",
      "returncode": 0,
      "started_at": "...",
      "ended_at": "...",
      "duration_seconds": 60,
      "stdout_path": "logs/fetch_biorxiv.stdout.log",
      "stderr_path": "logs/fetch_biorxiv.stderr.log",
      "inputs": [],
      "outputs": ["local_data/raw/biorxiv/2026-06-07.jsonl"],
      "metrics": {
        "records_out": 80
      },
      "reason": null,
      "error_summary": null
    }
  ]
}
```

---

## 3.4 Pipeline 状态判定规则

实现一个统一函数：

```python
def compute_pipeline_status(steps: list[dict]) -> str:
    if any(s["required"] and s["status"] == "failed" for s in steps):
        return "failed"

    if any(s["status"] in {"skipped_suspicious", "degraded"} for s in steps):
        return "degraded"

    if any(s["status"] == "failed" for s in steps):
        return "partial_failure"

    return "ok"
```

---

## 3.5 Step 状态判定规则

```python
def compute_step_status(returncode: int, required: bool, allow_failure: bool, skipped: bool, skip_reason: str | None) -> str:
    if skipped:
        if skip_reason in {"config_disabled", "no_data_expected"}:
            return "skipped_expected"
        return "skipped_suspicious"

    if returncode == 0:
        return "ok"

    if required:
        return "failed"

    if allow_failure:
        return "degraded"

    return "failed"
```

---

## 3.6 必改点 Checklist

### P0

- [x] 给每次 DailyPipeline 生成 `run_id`
- [x] manifest 顶层写入 `run_id`
- [x] 每个 step 写入 `required`
- [x] 子进程 step 写入 `returncode`
- [x] 跳过与子进程失败 step 写入 `reason`
- [ ] 拆分 `skipped` 为 `skipped_expected` / `skipped_suspicious`
- [x] 新增 `degraded`
- [x] pipeline status 不再把所有 skipped 都视为成功

### P1

- [x] 从子任务 summary JSON 提取 PG / Dense / Sparse 指标，并补充 fetch records_out
- [x] 在 docs log 中展示 degraded、required、metrics 与 substeps
- [x] 增加 `rerun_hint` 字段，提示失败子进程的重跑命令

### P2

- [ ] 增加“新增论文是否可检索”的验证步骤
- [ ] 增加 PostgreSQL / Dense / Sparse / Keyword 一致性检查

---

# 4. 检索流方案

## 4.1 当前正式检索链路

```text
GET /api/scholar/search
-> api_scholar_search
-> run_scholar_search
-> QueryUnderstandingService.analyze
-> author route or prioritized vector search
-> PaperIndexer.search(search_type="hybrid_retrieval")
-> PaperIndexer.hybrid_retrieval_search
-> result mapping
-> API response
```

检索流中最需要记录的是：

```text
query understanding 走了什么 route
三路召回各自是否成功
RRF 融合前后数量
hydrate 是否缺失 metadata
最终返回多少结果
总耗时是多少
```

---

## 4.2 不建表，只做 request-level debug

每次搜索请求生成：

```text
request_id = req_<uuid>
```

并在日志中输出一个完整 `retrieval_debug` JSON。

建议输出位置：

```text
服务 stdout structured log
本地 debug log 文件，可选
API response 中 debug=true 时返回，可选
```

---

## 4.3 retrieval_debug 标准结构

```json
{
  "request_id": "req_abc123",
  "endpoint": "GET /api/scholar/search",
  "raw_query": "cancer immunotherapy",
  "status": "success",
  "latency_ms": 420,
  "result_count": 19,

  "query_understanding": {
    "normalized_query": "cancer immunotherapy",
    "corrected_query": null,
    "route": "semantic_vector",
    "author_candidates_count": 0,
    "correction_applied": false,
    "latency_ms": 35
  },

  "retrieval": {
    "search_type": "hybrid_retrieval",
    "branches": {
      "dense": {
        "status": "ok",
        "candidate_count": 80,
        "latency_ms": 120,
        "error_summary": null
      },
      "sparse": {
        "status": "ok",
        "candidate_count": 60,
        "latency_ms": 90,
        "error_summary": null
      },
      "keyword_lookup": {
        "status": "empty",
        "candidate_count": 0,
        "latency_ms": 20,
        "error_summary": null
      }
    }
  },

  "fusion": {
    "method": "weighted_rrf",
    "score_type": "rrf_score",
    "weights": {
      "dense": 0.4,
      "sparse": 0.4,
      "keyword_lookup": 0.2
    },
    "before_dedupe_count": 140,
    "after_dedupe_count": 95,
    "top_k": 20
  },

  "hydration": {
    "input_count": 20,
    "hydrated_count": 19,
    "missing_metadata_count": 1,
    "missing_work_ids_sample": ["W123456"],
    "latency_ms": 60
  },

  "warnings": [
    "keyword_lookup_empty",
    "metadata_missing"
  ],
  "error_summary": null
}
```

---

## 4.4 Query Understanding 是否要追踪？

要，但只记录摘要。

原因：query understanding 会决定走作者检索、作者候选、主题检索或空结果。如果用户说“为什么这个 query 没搜到”，第一步必须知道 route 是否走错。

最低记录字段：

```json
{
  "normalized_query": "...",
  "corrected_query": "...",
  "route": "semantic_vector | metadata_author | author_suggestion | none",
  "author_candidates_count": 0,
  "correction_applied": false,
  "latency_ms": 35
}
```

不需要记录：

```text
完整 LLM prompt
完整模型输出
所有 author candidates 详情
```

---

## 4.5 Hydration 是否要追踪？

要，但只记录 count 和 sample。

原因：如果 VectorDB 召回了 work_id，但 MetadataDB hydrate 不出来，最终用户看不到这个结果。这属于跨存储一致性异常。

最低记录字段：

```json
{
  "input_count": 20,
  "hydrated_count": 19,
  "missing_metadata_count": 1,
  "missing_work_ids_sample": ["W123456"],
  "latency_ms": 60
}
```

不需要记录：

```text
完整 metadata
完整 paper abstract
所有 missing work_id
```

---

## 4.6 Branch 状态语义

每个 branch 使用统一状态：

```text
ok
empty
failed
skipped
```

| 状态 | 含义 |
|---|---|
| `ok` | 分支执行成功且返回候选 |
| `empty` | 分支执行成功但无候选 |
| `failed` | 分支执行报错 |
| `skipped` | 因 route 或策略未执行 |

整体检索状态：

```text
success
empty
degraded
failed
```

判定规则：

```python
def compute_search_status(branches, result_count):
    requested = [b for b in branches if b["status"] != "skipped"]

    if requested and all(b["status"] == "failed" for b in requested):
        return "failed"

    if result_count == 0:
        return "empty"

    if any(b["status"] == "failed" for b in requested):
        return "degraded"

    return "success"
```

---

## 4.7 Score 命名规范

不要把 RRF score 对外叫做 similarity。

建议统一：

```text
rrf_score
score_type = "rrf_score"
dense_similarity
sparse_score
keyword_score
```

API response 如果暂时不能改字段名，也至少在 debug 中明确：

```json
{
  "public_score_field": "similarity",
  "actual_score_type": "rrf_score"
}
```

---

## 4.8 与 `/api/scholar/search` 正式 API 对齐的 logging 方案

当前 `frontend_api_0608_xiongye.md` 是给前端开发者看的正式契约文档，因此不应在该文档中引入 logging 设计说明。

logging 方案应只放在 implementation log 中，并且尽量复用正式 API 已经定义好的 response 结构，降低联调和排障时的理解成本。

本阶段范围明确为：

```text
只覆盖 GET /api/scholar/search
不覆盖 /api/study/search
不改变正式 API 输入输出契约
```

---

## 4.9 两层记录策略

针对 `/api/scholar/search`，每次请求产出两层记录：

```text
1. 完整信息 JSON structured log
2. 数据库 summary 记录
```

分工如下：

```text
structured log：用于排障、复盘、联调
DB summary：用于统计、筛选、聚合分析
```

不再要求把 logging 信息暴露给前端 response，也不以 `debug=true` 作为本期核心目标。

---

## 4.10 完整信息 JSON 设计原则

完整信息 JSON 的主体直接按正式 API response 组织。

也就是说，日志中的核心 payload 应尽量复用：

```text
success
query
meta
notice
results
```

但为了满足排障需求，需要在 response 外再补一层观测字段：

```text
event_type
timestamp
request_id
client_surface
http.path
http.method
http.status_code
request_args
status
```

目标不是“复制一份前端文档”，而是：

```text
以 API response body 为核心
再补齐 response 外部不可见的调用上下文
```

---

## 4.11 完整信息 JSON 标准结构

建议结构：

```json
{
  "event_type": "frontend_scholar_search",
  "timestamp": "2026-06-08T15:04:05+08:00",
  "request_id": "frontend-search-001",
  "client_surface": "search_page",
  "http": {
    "path": "/api/scholar/search",
    "method": "GET",
    "status_code": 200
  },
  "request_args": {
    "query": "Nav1.7",
    "mode": "smart",
    "limit": 5,
    "offset": 0,
    "source_list": "langtaosha",
    "top_k": null
  },
  "response_body": {
    "success": true,
    "query": {
      "input": "Nav1.7",
      "executed": "Nav1.7",
      "mode": "smart",
      "intent": "semantic_search",
      "route": "vector",
      "corrected_query": null,
      "matched_author": null,
      "suggested_author": null
    },
    "meta": {
      "count": 2,
      "limit": 5,
      "offset": 0,
      "has_more": false,
      "elapsed_ms": 182,
      "request_id": "frontend-search-001"
    },
    "notice": null,
    "results": [
      {
        "work_id": "W_langtaosha_0001",
        "rank": 1,
        "title": "Paper title"
      }
    ]
  },
  "results_truncated": false,
  "results_logged_count": 2,
  "results_full_count": 2,
  "status": "ok"
}
```

---

## 4.12 `results` 截断策略

完整信息 JSON 中的 `results` 不保存全部结果，只保留前 10 条。

规则：

```text
最多记录前 10 条 response_body.results
完整返回数量仍以 response_body.meta.count 为准
```

新增辅助字段：

| 字段 | 含义 |
|---|---|
| `results_truncated` | 是否发生结果截断 |
| `results_logged_count` | 实际写入日志的 result 条数 |
| `results_full_count` | 本次 API 实际返回条数 |

判定规则：

```python
MAX_LOGGED_RESULTS = 10

results_truncated = len(results) > MAX_LOGGED_RESULTS
results_logged_count = min(len(results), MAX_LOGGED_RESULTS)
results_full_count = len(results)
```

这样既保留了“日志与 API response 同构”的优点，又避免 `limit=100` 时日志过大。

---

## 4.13 client_surface 记录方案

为了区分是谁在调用 `/api/scholar/search`，前端页面增加一个可选 header：

```text
X-Langtaosha-Client-Surface
```

建议值：

```text
/search -> search_page
/search-api-test -> search_api_test
未传 -> unknown
```

该 header：

```text
不属于正式业务参数
不影响现有 API 契约
不要求前端 response 展示
```

仅用于后端 logging 和后续统计分析。

---

## 4.14 数据库 summary 结构

数据库只保留一条 request-level 摘要，不保存完整 results。

建议字段：

```text
id
created_at
request_id
client_surface
query_input
query_executed
search_mode
query_intent
query_route
corrected_query
matched_author
suggested_author
notice_type
result_count
limit_count
offset_count
has_more
elapsed_ms
status
payload_json
```

其中：

```text
payload_json 只存 compact summary
不存完整 response_body.results
不存完整 request headers
```

summary 应从 structured log 或 API response body 中抽取，避免重新定义另一套语义。

---

## 4.15 `/api/scholar/search` 状态语义

本专项先使用以下 request 级状态：

```text
ok
empty
degraded
failed
```

含义：

| 状态 | 含义 |
|---|---|
| `ok` | 请求成功，且有结果 |
| `empty` | 请求成功，但结果为空 |
| `degraded` | 请求成功，但内部存在降级现象 |
| `failed` | 请求异常，返回错误 |

本期如果还没有完善 branch 级摘要，可先最小落地：

```text
ok
empty
failed
```

`degraded` 作为后续补充目标预留。

---

## 4.16 与正式 API 契约的关系

本方案默认不改变 `/api/scholar/search` 的正式输入输出定义。

明确约束：

```text
不新增必填 query 参数
不删除已有 response 字段
不重命名已有 response 字段
不把内部 logging/debug payload 暴露给普通前端调用方
```

唯一允许新增的是可选 header：

```text
X-Langtaosha-Client-Surface
```

它只影响日志，不影响业务逻辑。

---

# 5. 实施任务拆解

## Phase 1：Orchestrator manifest 修正

### 任务 1.1：增加 run_id

文件建议：

```text
src/docset_hub/orchestrator/daily_pipeline.py
```

实现：

```python
run_id = f"daily_{target_date}_{datetime.now().strftime('%H%M%S')}"
```

验收标准：

- [x] manifest 顶层包含 `run_id`
- [x] docs log 展示 `run_id`
- [ ] stdout / stderr 路径中可选包含 `run_id`

---

### 任务 1.2：统一 step status 判定

新增函数：

```text
compute_step_status(...)
compute_pipeline_status(...)
```

验收标准：

- [x] required step 非零 returncode -> `failed`
- [x] optional step 非零 returncode -> `degraded`
- [x] config 关闭步骤 -> `skipped_expected`
- [ ] 输入异常导致跳过 -> `skipped_suspicious`
- [x] pipeline 不再把 suspicious skipped 当作 ok

---

### 任务 1.3：manifest step 字段补齐

每个 step 至少包含：

```text
step_name
required
status
command
returncode
reason
stdout_path
stderr_path
inputs
outputs
metrics
error_summary
```

验收标准：

- [x] 任意一次运行后，manifest 可直接判断失败阶段
- [x] 不打开 stdout/stderr 也能看出是否 degraded

---

## Phase 2：`/api/scholar/search` request logging

### 任务 2.1：API 入口生成 request_id

位置建议：

```text
app/main.py::api_scholar_search
```

实现：

```python
request_id = uuid.uuid4().hex
```

验收标准：

- [ ] 每次搜索日志都包含 request_id
- [ ] API response header 返回 `X-Request-ID`
- [ ] 出错日志也包含 request_id

---

### 任务 2.2：记录 client_surface 与 request_args

在：

```text
templates/search.html
templates/search_api_test.html
app/main.py::api_scholar_search
```

实现：

```text
search 页面请求头带 X-Langtaosha-Client-Surface=search_page
search-api-test 页面请求头带 X-Langtaosha-Client-Surface=search_api_test
后端未收到时默认 unknown
```

验收标准：

- [ ] `/search` 请求日志中可见 `client_surface=search_page`
- [ ] `/search-api-test` 请求日志中可见 `client_surface=search_api_test`
- [ ] 未传 header 时仍可正常请求，并记为 `unknown`

---

### 任务 2.3：记录 query/notice/meta 摘要

直接复用正式 API response 中的字段：

```text
query
meta
notice
```

验收标准：

- [ ] structured log 中 `response_body.query` 与正式 response 一致
- [ ] structured log 中 `response_body.meta` 与正式 response 一致
- [ ] structured log 中 `response_body.notice` 与正式 response 一致

---

### 任务 2.4：完整信息 JSON 生成器

目标：

```text
以 API response body 为核心
补齐 event_type / timestamp / client_surface / http / request_args / status
```

验收标准：

- [ ] 一次 `/api/scholar/search` 生成一条 request-level JSON
- [ ] JSON 中包含 `request_args`
- [ ] JSON 中包含 `response_body`
- [ ] JSON 中包含 request 级 `status`

---

### 任务 2.5：results 截断

规则：

```text
structured log 中最多保留前 10 条 results
```

验收标准：

- [ ] `results <= 10` 时完整写入
- [ ] `results > 10` 时只保留前 10 条
- [ ] `results_truncated / results_logged_count / results_full_count` 正确

---

### 任务 2.6：数据库 summary 写入

新增 request-level summary 记录，字段与 4.14 保持一致。

验收标准：

- [ ] 每次 `/api/scholar/search` 成功请求写入一条 summary
- [ ] failed 请求可选择写入 failed summary 或至少输出 failed log
- [ ] summary 中不保存完整 `results` 列表

---

### 任务 2.7：补充 query_understanding 摘要

在：

```text
QueryUnderstandingService.analyze
```

返回或附带 debug summary：

```python
query_debug = {
    "normalized_query": normalized_query,
    "corrected_query": corrected_query,
    "route": route,
    "author_candidates_count": len(author_candidates or []),
    "correction_applied": corrected_query is not None,
    "latency_ms": latency_ms,
}
```

验收标准：

- [ ] 作者 query 能看出是否走 `metadata_author`
- [ ] 普通 topic query 能看出是否走 `semantic_vector`
- [ ] 空结果 query 能看出是否 route 为 `none` 或 retrieval empty

---

### 任务 2.8：记录三路召回摘要

在：

```text
PaperIndexer.hybrid_retrieval_search
```

记录：

```text
dense.status / count / latency / error_summary
sparse.status / count / latency / error_summary
keyword_lookup.status / count / latency / error_summary
```

验收标准：

- [ ] 单个分支失败时，整体状态可显示 degraded
- [ ] 三个分支都失败时，整体状态为 failed
- [ ] 分支 empty 和 failed 可区分

---

### 任务 2.9：记录 fusion 摘要

记录：

```text
method = weighted_rrf
weights
before_dedupe_count
after_dedupe_count
top_k
score_type = rrf_score
```

验收标准：

- [ ] debug 中能看出 RRF 权重
- [ ] debug 中能看出去重前后数量
- [ ] debug 中明确 score_type 不是 dense similarity

---

### 任务 2.10：记录 hydration 摘要

在 hydrate 阶段记录：

```text
input_count
hydrated_count
missing_metadata_count
missing_work_ids_sample
latency_ms
```

验收标准：

- [ ] MetadataDB 缺失时出现 warning
- [ ] debug 中能看出召回结果为什么少了
- [ ] missing sample 最多保留 10 个 work_id

---

## Phase 3：日志输出与 debug 开关

### 任务 3.1：structured log 输出

搜索完成后输出一条结构化日志。

推荐优先输出完整信息 JSON：

```python
logger.info("frontend_scholar_search %s", json.dumps(search_log_payload, ensure_ascii=False))
```

验收标准：

- [ ] 一次搜索只输出一条 request-level 完整日志
- [ ] 日志主体以正式 API response body 为核心
- [ ] 出错时输出 `search_failed`，包含 request_id 和 error_summary

---

### 任务 3.2：API debug 参数（降级为可选）

`debug=true` 不再是本期主目标。

如果后续需要支持：

```text
GET /api/scholar/search?q=xxx&debug=true
```

当 `debug=true` 时，response 中返回：

```json
{
  "results": [...],
  "debug": {...}
}
```

默认不返回 debug。

验收标准：

- [ ] 默认 response 不暴露 debug
- [ ] debug=true 时返回 compact retrieval_debug
- [ ] debug 中不包含敏感 token / DB URL / 完整用户隐私信息

---

# 6. 脱敏规则

日志不得记录：

```text
API key
数据库密码
完整 DB URL
用户 token
cookie
完整 request header
```

Query 记录策略：

```text
开发环境：允许记录 raw_query
生产环境：建议记录 raw_query + 长度限制，必要时 hash 或采样
```

建议实现：

```python
MAX_QUERY_LOG_LEN = 300
safe_query = raw_query[:MAX_QUERY_LOG_LEN]
```

---

# 7. 验收用例

## 7.1 Orchestrator 验收

### Case A：全部成功

预期：

```text
pipeline_status = ok
所有 required step = ok
```

### Case B：作者补全失败，但入库成功

预期：

```text
author_enrichment.status = degraded
pipeline_status = degraded
```

### Case C：raw file 缺失

预期：

```text
ensure_raw_file.status = failed
pipeline_status = failed
```

### Case D：vector stage 未开启

预期：

```text
vector_stage.status = skipped_expected
pipeline_status 不受影响
```

---

## 7.2 检索流验收

### Case A：普通 topic query

预期：

```text
route = semantic_vector
dense/sparse/keyword 至少一个 ok
fusion 有 before/after dedupe
hydration 有 input/hydrated count
```

### Case B：作者 query

预期：

```text
route = metadata_author
retrieval branches skipped
result_count > 0 或 empty
```

### Case C：keyword_lookup empty

预期：

```text
keyword_lookup.status = empty
search_status = success 或 empty，不是 failed
```

### Case D：dense failed, sparse ok

预期：

```text
dense.status = failed
sparse.status = ok
search_status = degraded
```

### Case E：hydrate 缺失

预期：

```text
missing_metadata_count > 0
warnings 包含 metadata_missing
最终 result_count < fusion top_k input_count
```

---

# 8. 最终交付物

## 8.1 代码交付

- [x] Orchestrator manifest 状态语义修正
- [x] run_id 生成与写入
- [ ] search request_id 生成与 header 返回
- [ ] `/api/scholar/search` structured JSON log 构建器
- [ ] `client_surface` 透传与记录
- [ ] `results` 截断逻辑
- [ ] request-level DB summary 写入
- [ ] query_understanding debug summary
- [ ] branch summary
- [ ] fusion summary
- [ ] hydration summary
- [ ] debug=true response 开关（可选）

## 8.2 文档交付

- [ ] `docs/logging_system_minimal.md`
- [ ] `docs/retrieval_debug_schema.md`
- [ ] `docs/orchestrator_manifest_schema.md`

## 8.3 测试交付

- [x] Orchestrator 状态判定单元测试
- [ ] Search debug schema 单元测试
- [ ] Dense failed / sparse ok 的降级测试
- [ ] Hydration missing metadata 测试

---

# 9. 推荐实现顺序

```text
Day 1:
- Orchestrator run_id
- step status 语义修正
- manifest 字段补齐

Day 2:
- Search request_id
- client_surface header
- structured JSON log
- DB summary

Day 3:
- query_understanding debug summary
- branch summary
- fusion summary
- hydration summary
- debug=true response（可选）
- 基础测试
```

---

# 10. 一句话版本

当前 Langtaosha 不需要完整复杂 logging 平台。

最小可执行版本是：

```text
Orchestrator：把本地 manifest 做准。
Search：对 /api/scholar/search 输出一条以正式 API response 为核心的完整 JSON log，再写一条 DB summary。
```

这样已经可以覆盖 80% 的上线前排障与评估需求。
