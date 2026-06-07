# 前端 API 交接文档

**日期**: 2026-06-02
**状态**: v0.1
**适用范围**: 独立前端接入浪淘沙智能搜索后端 API。

## 1. 服务边界

后端服务只承载 JSON API。独立前端作为用户访问入口，默认通过同源 `/api` 调用后端代理，不依赖 Flask templates。

当前迁移期端口约定：

```text
frontend: http://localhost:5004
backend:  http://localhost:5173
```

前端默认调用：

```text
same-origin /api
```

开发期由 Vite 将 `/api` 代理到 `http://127.0.0.1:5173`。

## 2. 公开接口

| Method | Path | 用途 | 公开状态 |
| --- | --- | --- | --- |
| `GET` | `/api/health` | 服务存活检查 | 公开 |
| `GET` | `/api/ready` | 服务就绪检查 | 公开/部署探活 |
| `GET` | `/api/scholar/search` | 智能搜索主接口 | 公开 |
| `GET` | `/api/scholar/daily_new` | 每日新增论文 | 公开 |
| `GET` | `/api/recommend` | 相似论文推荐原型 | 灰度公开 |
| `GET` | `/api/study/search` | 用户测试搜索 | 内部/灰度 |
| `POST` | `/api/study/feedback` | 用户测试反馈 | 内部/灰度 |

## 3. 内部接口

| Method | Path | 说明 |
| --- | --- | --- |
| `GET` | `/api/span-matcher` | 研发调试接口，依赖 ontology linker，正式前端不要直接接入。 |
| `GET` | `/api/study/feedback-review-data` | 内部 review / 管理页面数据源，后续需要访问控制。 |

## 4. 通用约定

### 4.1 Request ID

后端会为每次 API 请求生成或透传 request id。

请求头可传：

```text
X-Request-Id: frontend-generated-id
```

响应头会返回：

```text
X-Request-Id: same-or-generated-id
```

错误响应里也会包含同一个 `request_id`。

### 4.2 CORS

如果前端绕过同源代理、直接请求后端 `5173`，后端通过环境变量配置允许的前端 origin：

```text
FRONTEND_ALLOWED_ORIGINS=http://localhost:5004,https://frontend.example.com
```

本地未配置时，默认允许：

```text
http://localhost:5004
http://127.0.0.1:5004
```

迁移期为了让 `5004` 暂时继续保持原 Flask 页面效果，Vite 会把以下路径代理到后端 `5173`：

```text
/
/search
/study
/future
/show_page
/span-matcher
/feedback-review
/static
```

`5173` 是 API 端。默认情况下，直接访问 `5173` 的非 `/api/*` 页面路径会返回 404；只有 `5004` 的 Vite proxy 带内部 legacy-page header 转发时，后端才临时放行这些旧页面。

### 4.3 错误结构

迁移期错误结构：

```json
{
  "success": false,
  "error": "query 不能为空",
  "error_code": "INVALID_REQUEST",
  "error_detail": {
    "code": "INVALID_REQUEST",
    "message": "query 不能为空",
    "request_id": "..."
  },
  "request_id": "..."
}
```

迁移期说明：

- 新前端应优先读取 `error_detail.message`。
- `error` 暂时保留为字符串，用于兼容现有 Flask templates 中的旧页面。
- `error_code` 用于前端分支处理和日志检索。

## 5. `GET /api/health`

用于服务存活检查，不访问数据库或 VectorDB。

示例：

```bash
curl "$API_BASE_URL/api/health"
```

成功响应：

```json
{
  "success": true,
  "status": "ok",
  "service": "langtaosha-api",
  "request_id": "..."
}
```

## 6. `GET /api/ready`

用于部署就绪检查。第一版检查 metadata DB 是否可连接。

示例：

```bash
curl "$API_BASE_URL/api/ready"
```

成功响应：

```json
{
  "success": true,
  "status": "ready",
  "checks": {
    "metadata_db": "ok"
  },
  "request_id": "..."
}
```

失败响应：

```json
{
  "success": false,
  "error": {
    "code": "READINESS_FAILED",
    "message": "metadata_db unavailable",
    "request_id": "..."
  },
  "checks": {
    "metadata_db": "failed"
  },
  "request_id": "..."
}
```

## 7. `GET /api/scholar/search`

核心智能搜索接口。

### 7.1 参数

| 参数 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `query` | string | 必填 | 用户搜索 query。 |
| `top_k` | integer | `100` | 后端归一化为 `1..100`。当前语义是每组 source 拉取 top_k 后合并，不是最终分页 limit。 |
| `source_list` | CSV string | 后端默认 source | 例如 `langtaosha,biorxiv_daily,biorxiv_history`。 |
| `mode` | string | `smart` | 只支持 `smart` 或 `vector`。 |

### 7.2 示例请求

```bash
curl "$API_BASE_URL/api/scholar/search?query=Nav1.7&mode=smart&top_k=10"
```

### 7.3 成功响应

```json
{
  "success": true,
  "query": "Nav1.7",
  "search_query": "Nav1.7",
  "search_mode": "smart",
  "query_understanding": {
    "intent": "semantic_search",
    "route": "vector"
  },
  "result_policy": {
    "langtaosha_top_k": 10,
    "biorxiv_top_k": 10,
    "dedupe_key": "work_id",
    "default_frontend_source": "langtaosha",
    "search_type": "hybrid_retrieval",
    "display": "show_langtaosha_first_then_biorxiv"
  },
  "notice": null,
  "count": 1,
  "results": [
    {
      "work_id": "W...",
      "paper_id": 1,
      "source_name": "langtaosha",
      "similarity": 0.0123,
      "similarity_score": 0.0123,
      "title": "Paper title",
      "abstract": "Paper abstract",
      "authors": "A, B",
      "doi": "10.xxxx/example",
      "online_date": "2026-04-13",
      "source": "Langtaosha",
      "source_key": "langtaosha",
      "link": "https://...",
      "retrieval_reasons": [],
      "retrieval_reason_tags": [],
      "highlight": {}
    }
  ],
  "request_id": "..."
}
```

### 7.4 前端展示注意事项

- `similarity` / `similarity_score` 在 hybrid retrieval 下可能是融合排序分，不等同于 dense semantic similarity。第一版前端建议隐藏，或展示为“排序分”而不是“相似度”。
- `top_k` 当前不是分页协议。第一版前端不做服务端分页。
- `work_id` 是对外主标识。前端跳转、推荐、反馈优先使用 `work_id`。

## 8. `GET /api/scholar/daily_new`

每日新增论文接口。

参数：

| 参数 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `limit` | integer | `10` | 后端限制为 `1..20`。 |

示例：

```bash
curl "$API_BASE_URL/api/scholar/daily_new?limit=10"
```

成功响应：

```json
{
  "success": true,
  "count": 1,
  "results": [
    {
      "paper_id": 1,
      "work_id": "W...",
      "title": "Paper title",
      "authors": "A, B",
      "online_at": "2026-04-13T00:00:00",
      "online_date": "2026-04-13",
      "source": "Langtaosha",
      "source_key": "langtaosha",
      "link": "https://..."
    }
  ],
  "request_id": "..."
}
```

## 9. `GET /api/recommend`

相似论文推荐原型接口。

参数：

| 参数 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `work_id` | string | 与 `paper_id` 二选一 | 推荐优先使用。 |
| `paper_id` | integer | 与 `work_id` 二选一 | 兼容内部数据库 ID。 |
| `top_k` | integer | `5` | 后端限制为 `1..20`。 |
| `source_list` | CSV string | 后端默认 source | 可选。 |

示例：

```bash
curl "$API_BASE_URL/api/recommend?work_id=W...&top_k=5"
```

## 10. Study API

Study API 用于用户测试和灰度反馈，不建议作为普通搜索前端的默认入口。

### 10.1 `GET /api/study/search`

在普通搜索参数基础上，额外需要：

| 参数 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `participant_id` | string | 必填 | 测试用户 ID。 |
| `study_session_id` | string | 自动生成或显式传入 | 跨域前端建议显式保存并传入。 |

成功响应会比 `/api/scholar/search` 多一个 `study` 字段：

```json
{
  "study": {
    "study_session_id": "s_...",
    "participant_id": "p01",
    "query_index": 1,
    "search_event_id": 123,
    "result_snapshot_count": 10
  }
}
```

### 10.2 `POST /api/study/feedback`

请求体：

```json
{
  "study_session_id": "s_...",
  "participant_id": "p01",
  "search_event_id": 123,
  "query_index": 1,
  "query": "Nav1.7",
  "search_mode": "smart",
  "search_query": "Nav1.7",
  "result_rank": 1,
  "work_id": "W...",
  "paper_id": 1,
  "title": "Paper title",
  "source": "Langtaosha",
  "year": "2026",
  "similarity_score": 0.0123,
  "feedback": "relevant",
  "reason_text": null
}
```

`feedback` 只支持：

```text
relevant
not_relevant
```

当 `feedback=not_relevant` 时，`reason_text` 必填。

成功响应：

```json
{
  "success": true,
  "event_id": 456,
  "request_id": "..."
}
```

## 11. 当前不支持项

- 独立作者详情 API：当前作者检索是 `/api/scholar/search` 的 `smart` route，不是 `/api/authors/...`。
- 服务端分页：当前只有 `top_k`。
- 流式返回：当前全部接口都是普通 JSON。
- 正式鉴权：当前计划中未完成，生产接入前需要补充。
- 完整管理面板 API：后续单独设计。
