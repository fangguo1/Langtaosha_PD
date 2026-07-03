# `/api/scholar/search` Logging System 方案

## 0. 目标

本方案只讨论：

```text
GET /api/scholar/search
```

目标是让这条正式检索 API 同时具备两类能力：

1. **排障复盘**：一次请求为什么走这个 route，返回了哪些结果，是否发生截断、空结果或失败。
2. **基础统计**：哪些 query 在被搜，哪些 mode / route 更常见，接口耗时和结果量大致怎样。

本方案不覆盖：

```text
/api/study/search
Orchestrator manifest
完整 observability 平台
前端开发者文档说明
```

---

## 1. 文档放置位置

### 1.1 面向前端的正式 API 文档

继续保留在：

```text
docs/api/frontend_api_0608_xiongye.md
```

该文档职责是：

```text
定义 API 输入输出契约
说明字段语义
给前端联调用
```

因此：

```text
不在该文档中写 logging 设计
不在该文档中暴露内部实现细节
```

### 1.2 面向后端实现的 logging 文档

本次 logging system 的正式实现说明放在：

```text
docs/implementation_log/20260608/scholar_search_logging_system_20260608.md
```

该文档职责是：

```text
定义 structured log 格式
定义 DB summary 表结构
定义 request_id 规则
定义实施步骤和验收标准
```

### 1.3 与 20260607 总计划的关系

`20260607/langtaosha_logging_minimal_plan.md` 继续作为总方案。

本文件是它在检索侧的专项拆分，作用是：

```text
把 /api/scholar/search 的 logging 方案写实
避免 request_id / DB schema / 文档位置散落在多处
```

---

## 2. 总体设计

每次 `/api/scholar/search` 请求产出两层记录：

```text
1. 一条完整 structured JSON log
2. 一条数据库 summary 记录
```

分工：

```text
structured JSON log：保留完整请求上下文，用于排障
DB summary：保留结构化摘要，用于统计和筛选
```

设计原则：

```text
structured log 以正式 API response body 为核心
DB summary 从 structured log / response body 抽取摘要
不改变正式 API 输入输出契约
```

当前完整 structured JSON 同时落到：

```text
Python logger
local_data/search_api_logs/frontend_search_requests.jsonl
```

---

## 3. request_id 生成规则

## 3.1 目标

`request_id` 是这套 logging system 的主键。

它必须满足：

```text
每次请求唯一
由后端统一生成
可写入 response header
可写入 structured log
可写入 DB summary
```

## 3.2 生成规则

本项目不再把 `X-Request-Id` 或 `X-Correlation-Id` 作为正式规则。

原因：

```text
当前仓库里没有一份正式文档把这两个 header 定义为对外契约
虽然现状代码与个别前端 helper 会传 X-Request-Id，但它更像实现细节，不是明确约定
为了避免 request_id 来源不稳定，本方案要求 request_id 统一由后端生成
```

建议规则：

```python
def resolve_request_id() -> str:
    return uuid.uuid4().hex
```

说明：

1. 每次 API 请求进入后端时生成一个新的 `request_id`。
2. 不依赖客户端透传。
3. 不接受调用方指定 request_id 作为正式主键来源。

建议把 `app.main.assign_request_id()` 收紧为后端自生成版本，不再从 request headers 读取 request id。

## 3.3 写入位置

每次请求至少写入：

```text
Flask g.request_id
response header: X-Request-Id
response body.meta.request_id
structured JSON log.request_id
DB summary.request_id
```

## 3.4 不建议的做法

不要：

```text
同时存在 request_id / trace_id / search_id 三套主键
每一层自己重新生成不同 request_id
把前端透传 header 当作正式 request_id 来源
只写 header 不写 body
只写 body 不写 log
```

---

## 4. client_surface 规则

为了区分是谁在调用 `/api/scholar/search`，前端页面应增加一个可选 header：

```text
X-Langtaosha-Client-Surface
```

建议值：

```text
/search -> search_page
/search-api-test -> search_api_test
未传 -> unknown
```

注意：

```text
这不是业务必填参数
不影响 API 正常工作
不写入前端正式契约文档
```

它只进入：

```text
structured log
DB summary
```

---

## 5. Structured JSON Log 设计

## 5.1 设计原则

structured log 的主体直接按正式 API response 组织。

也就是说，它的核心字段尽量复用：

```text
success
query
meta
notice
results
```

但日志需要额外补足 response 外看不到的上下文：

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

## 5.2 标准结构

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

## 5.3 `results` 截断规则

完整日志不保存全部 `results`，只记录前 10 条。

规则：

```python
MAX_LOGGED_RESULTS = 10

logged_results = results[:MAX_LOGGED_RESULTS]
results_truncated = len(results) > MAX_LOGGED_RESULTS
results_logged_count = len(logged_results)
results_full_count = len(results)
```

这样保留两点：

```text
日志与正式 response 基本同构
日志体积不会因为 limit=100 迅速膨胀
```

## 5.4 request 级状态

本期先使用：

```text
ok
empty
failed
```

含义：

| 状态 | 含义 |
|---|---|
| `ok` | 请求成功且有结果 |
| `empty` | 请求成功但无结果 |
| `failed` | 请求异常 |

`degraded` 先预留，不作为本期必须落地状态。

---

## 6. 数据库 summary 表

## 6.1 目标

数据库 summary 用于：

```text
按 route / mode / surface 聚合
筛选慢请求
统计空结果比例
统计 notice / correction / author route 的出现情况
```

因此它应是一张：

```text
一请求一行
字段直接对应 API 语义
不存完整 results 列表
```

## 6.2 建议表名

建议新建：

```text
frontend_search_request_logs
```

对应 migration 文件：

```text
database/migrations/20260608_frontend_search_request_logs.sql
```

原因：

```text
它和 user_study_events 不是同一类数据
它是正式前端搜索 API 的 request summary
命名上需要看得出它是 request-level logging 表
```

## 6.3 建议字段

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
payload_json 只放 compact summary
不放完整 results
不放完整 headers
不放敏感信息
```

## 6.4 建议 DDL

```sql
CREATE TABLE IF NOT EXISTS frontend_search_request_logs (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    request_id TEXT NOT NULL UNIQUE,
    client_surface TEXT NOT NULL DEFAULT 'unknown',
    query_input TEXT NOT NULL,
    query_executed TEXT,
    search_mode TEXT NOT NULL,
    query_intent TEXT,
    query_route TEXT,
    corrected_query TEXT,
    matched_author TEXT,
    suggested_author TEXT,
    notice_type TEXT,
    result_count INTEGER NOT NULL DEFAULT 0,
    limit_count INTEGER,
    offset_count INTEGER,
    has_more BOOLEAN,
    elapsed_ms INTEGER,
    status TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_created_at
    ON frontend_search_request_logs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_surface_created
    ON frontend_search_request_logs (client_surface, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_route_created
    ON frontend_search_request_logs (query_route, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_frontend_search_request_logs_status_created
    ON frontend_search_request_logs (status, created_at DESC);
```

## 6.5 `payload_json` 建议内容

建议只放 compact summary，例如：

```json
{
  "notice": {
    "type": "query_correction"
  },
  "meta": {
    "request_id": "frontend-search-001"
  },
  "results_logged_count": 10,
  "results_full_count": 32
}
```

不要放：

```text
完整 response_body.results
完整 request headers
cookie
token
DB URL
```

---

## 7. 代码落点

## 7.1 API 入口

主入口：

```text
app/main.py::api_scholar_search
```

职责：

```text
收集 request_args
读取 client_surface
在成功和失败路径统一构建 request-level logging payload
写 structured log
写 DB summary
```

## 7.2 request_id

当前已有：

```text
app/main.py::assign_request_id
app/main.py::_request_id
app/main.py::attach_api_headers
```

应继续复用，不另起炉灶。

## 7.3 前端 header

前端落点：

```text
templates/search.html
templates/search_api_test.html
```

职责：

```text
发起 /api/scholar/search 请求时附带 X-Langtaosha-Client-Surface
```

---

## 8. 实施步骤

### Step 1：补齐 client_surface

- `/search` 调用 `/api/scholar/search` 时发送 `search_page`
- `/search-api-test` 调用 `/api/scholar/search` 时发送 `search_api_test`
- 后端缺省记 `unknown`

### Step 2：实现 structured JSON log builder

- 输入：request args、response body、http status、request_id、client_surface
- 输出：一条完整 request-level JSON
- `results` 只保留前 10 条

### Step 3：实现 DB summary builder

- 从 response body 和 logging payload 抽字段
- 构建 `frontend_search_request_logs` 的 insert payload

### Step 4：接入 `/api/scholar/search`

- 成功路径写 `ok/empty`
- 失败路径写 `failed`
- 所有路径都带 `request_id`

---

## 9. 验收标准

### Case A：普通搜索成功

预期：

```text
response header 带 X-Request-Id
response body.meta.request_id 存在
structured log 有一条完整记录
DB summary 有一行
status = ok
```

### Case B：空结果

预期：

```text
structured log.status = empty
DB summary.status = empty
result_count = 0
```

### Case C：请求失败

预期：

```text
日志中能看到 request_id
日志中能看到错误摘要
status = failed
DB summary 可选写入 failed，或至少有 failed structured log
```

### Case D：结果超过 10 条

预期：

```text
structured log 中只保留前 10 条 results
results_truncated = true
results_logged_count = 10
results_full_count = 实际返回条数
```

---

## 10. 一句话版本

`/api/scholar/search` 的 logging system 应单独写在 implementation log 中，而不是前端 API 文档中。

最小可执行版本是：

```text
沿用现有 request_id 机制
输出一条以正式 API response 为核心的 structured JSON log
落一张 request-level DB summary 表 frontend_search_request_logs
```
