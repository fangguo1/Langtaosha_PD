# 浪淘沙前端 Search API 交接文档

**日期**: 2026-06-12  
**接口状态**: Search API v1.1  
**本地联调地址**: `http://127.0.0.1:5173`

## 1. 接口

```http
GET /api/scholar/search
```

完整示例：

```http
GET http://127.0.0.1:5173/api/scholar/search?query=Nav1.7&mode=smart&top_k=5&source_list=langtaosha
```

## 2. 请求参数

| 参数 | 类型 | 必填 | 默认值 | 前端传值说明 |
| --- | --- | --- | --- | --- |
| `query` | string | 是 | 无 | 搜索框原始输入。提交前建议 `trim()`；空字符串不要提交。 |
| `mode` | string | 否 | `smart` | 可选值：`smart` / `vector`。常规搜索用 `smart`。 |
| `top_k` | integer | 否 | `100` | 返回条数上限。建议传 `1..100`。 |
| `source_list` | CSV string | 否 | 全部默认来源 | 例如 `langtaosha`、`biorxiv_daily`、`langtaosha,biorxiv_daily`。 |

前端不要传：

- `limit`
- `offset`

加载更多推荐方式：

```text
首次请求: top_k=20
加载更多: top_k=40
继续加载: top_k=60
```

前端可以用新返回的 `results[]` 替换当前列表，或按 `work_id` 合并。

## 3. 成功响应

### 3.1 顶层结构

```json
{
  "success": true,
  "query": {},
  "meta": {},
  "notice": null,
  "results": [],
  "request_id": "..."
}
```

| 字段 | 类型 | 前端用途 |
| --- | --- | --- |
| `success` | boolean | 是否成功。 |
| `query` | object | 本次搜索 query 信息，可用于搜索状态展示。 |
| `meta` | object | 返回数量、耗时、排查 ID。 |
| `notice` | object/null | 搜索提示条和可选动作按钮。 |
| `results` | array | 论文结果列表。 |
| `request_id` | string | 排查问题时提供给后端。 |

### 3.2 `query`

```json
{
  "input": "Nav1.7",
  "executed": "Nav1.7",
  "mode": "smart",
  "intent": "semantic_search",
  "route": "vector",
  "corrected_query": null,
  "matched_author": null,
  "suggested_author": null
}
```

| 字段 | 类型 | 前端用途 |
| --- | --- | --- |
| `input` | string | 用户输入。 |
| `executed` | string/null | 实际展示为“本次搜索词”时可使用。 |
| `mode` | string | 当前搜索模式。 |
| `intent` | string | 可用于调试或埋点。 |
| `route` | string | 可用于调试或埋点。 |
| `corrected_query` | string/null | 有值时可配合 `notice` 展示纠错提示。 |
| `matched_author` | string/null | 有值时可配合 `notice` 展示作者命中提示。 |
| `suggested_author` | string/null | 有值时可配合 `notice` 展示作者建议。 |

### 3.3 `meta`

```json
{
  "count": 10,
  "elapsed_ms": 182,
  "request_id": "frontend-search-001"
}
```

| 字段 | 类型 | 前端用途 |
| --- | --- | --- |
| `count` | integer | 本次返回条数，等于 `results.length`。 |
| `elapsed_ms` | integer | 请求耗时展示或埋点。 |
| `request_id` | string | 排查问题时提供给后端。 |

### 3.4 `notice`

无提示时：

```json
null
```

有提示时：

```json
{
  "type": "query_correction",
  "message": "已识别到可能的拼写错误，实际搜索 query 为: machine learning",
  "action": {
    "label": "使用原 query 检索",
    "mode": "vector",
    "query": "machi learningn"
  }
}
```

前端处理规则：

- `notice === null`：不展示提示条。
- `notice.message` 有值：展示提示条。
- `notice.action === null`：只展示提示，不展示按钮。
- `notice.action` 有值：展示一个按钮。
- 按钮文案使用 `notice.action.label`。
- 点击按钮后，用 `notice.action.query` 和 `notice.action.mode` 重新请求 `/api/scholar/search`。

已定义类型：

| `notice.type` | 按钮文案 | 点击按钮后传参 |
| --- | --- | --- |
| `author_name` | `改用向量检索` | `query=notice.action.query&mode=vector` |
| `author_suggestion` | `搜索作者 ...` | `query=notice.action.query&mode=smart` |
| `query_correction` | `使用原 query 检索` | `query=notice.action.query&mode=vector` |
| `vector` | 无按钮 | 不需要额外动作 |

### 3.5 `results[]`

```json
{
  "work_id": "W...",
  "rank": 1,
  "title": "Paper title",
  "abstract": "Paper abstract",
  "authors": "A, B",
  "source": "Langtaosha",
  "source_key": "langtaosha",
  "online_date": "2026-04-13",
  "link": "https://...",
  "doi": "10.xxxx/example",
  "ranking_score": 0.9124,
  "match_reasons": []
}
```

| 字段 | 类型 | 前端用途 |
| --- | --- | --- |
| `work_id` | string | 论文主标识。跳转、收藏、反馈优先使用。 |
| `rank` | integer | 展示排序，从 1 开始。 |
| `title` | string/null | 标题。 |
| `abstract` | string/null | 摘要。 |
| `authors` | string | 作者展示文本。 |
| `source` | string | 来源展示名，例如 `Langtaosha` / `Biorxiv`。 |
| `source_key` | string | 本地筛选用，例如 `langtaosha` / `biorxiv`。 |
| `online_date` | string/null | 日期，格式 `YYYY-MM-DD`。 |
| `link` | string/null | 外部跳转链接。 |
| `doi` | string/null | DOI。 |
| `ranking_score` | number/null | 排序分。建议用于调试，不建议展示为“相似度”。 |
| `match_reasons` | array | 命中原因标签。 |

`match_reasons[]`：

```json
{
  "key": "dense_recall",
  "label": "Dense recall",
  "score": 0.91
}
```

多来源展示建议：

- 接口返回一个统一的 `results[]`。
- 如果页面需要 Langtaosha / Biorxiv tab，用 `source_key` 在前端本地筛选。
- 不需要为了不同来源发多次请求。

## 4. 错误响应

### 4.1 空 query

HTTP status: `400`

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

### 4.2 非法 mode

HTTP status: `400`

```json
{
  "success": false,
  "error": "mode 只能是 smart 或 vector",
  "error_code": "INVALID_REQUEST",
  "error_detail": {
    "code": "INVALID_REQUEST",
    "message": "mode 只能是 smart 或 vector",
    "request_id": "..."
  },
  "request_id": "..."
}
```

### 4.3 前端错误处理建议

- `400`：展示用户可理解的错误文案，优先使用 `error`。
- `500`：展示通用错误，例如“搜索失败，请稍后重试”。
- 排查问题时记录 `request_id`。

## 5. 请求示例

### 5.1 普通搜索

```bash
curl -i \
  "http://127.0.0.1:5173/api/scholar/search?query=Nav1.7&mode=smart&top_k=5&source_list=langtaosha"
```

### 5.2 默认来源

```bash
curl -i \
  "http://127.0.0.1:5173/api/scholar/search?query=ion%20channel&mode=smart&top_k=20"
```

### 5.3 多来源

```bash
curl -i \
  "http://127.0.0.1:5173/api/scholar/search?query=single%20cell%20RNA-seq&mode=smart&top_k=20&source_list=langtaosha,biorxiv_daily"
```

### 5.4 强制向量检索

```bash
curl -i \
  "http://127.0.0.1:5173/api/scholar/search?query=machi%20learningn&mode=vector&top_k=20"
```

## 6. 前端接入 checklist

- 搜索框提交前做 `trim()`。
- 空 query 不发请求。
- 常规搜索传 `mode=smart`。
- 不传 `limit` / `offset`。
- 加载更多通过增大 `top_k` 重新请求。
- 提示条只依赖 `notice.message` 和 `notice.action`。
- 结果跳转、反馈、收藏优先使用 `work_id`。
- 多来源 tab 使用 `source_key` 本地筛选。
- 错误日志记录 `request_id`。
