# 浪淘沙前端 Search API 交接文档

**日期**: 2026-06-08  
**状态**: Search API v1

## 1. 请求参数

### 1.1 请求方式

```http
GET /api/scholar/search
```

### 1.2 参数说明

| 参数 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `query` | string | 是 | 无 | 用户原始搜索输入。前端直接传搜索框内容。 |
| `mode` | string | 否 | `smart` | 只支持 `smart` 或 `vector`。 |
| `limit` | integer | 否 | `100` | 本次希望返回多少条结果，后端归一化为 `1..100`。 |
| `offset` | integer | 否 | `0` | 从第几条结果开始返回。 |
| `source_list` | CSV string | 否 | 后端默认 source | 例如 `langtaosha` 或 `langtaosha,biorxiv_daily`。 |
| `top_k` | integer | 否 | 兼容参数 | 迁移期兼容保留；如果同时传 `limit` 和 `top_k`，以 `limit` 为准。 |

### 1.3 `query` 输入格式

`query` 是单个字符串字段，不需要前端做分词、纠错或作者识别。

允许的典型输入：

- `Nav1.7`
- `ion channel gating`
- `Nieng Yan`
- `machi learningn`
- `single cell RNA-seq`

后端行为：

- 先执行 `trim()`
- 空字符串返回 `400 INVALID_REQUEST`
- 再进入 query understanding / author routing / correction 流程

## 2. 成功响应结构

### 2.1 顶层结构

```json
{
  "success": true,
  "query": {},
  "meta": {},
  "notice": null,
  "results": []
}
```

### 2.2 `query` 字段

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

| 字段 | 含义 |
| --- | --- |
| `input` | 用户原始输入 |
| `executed` | 实际执行检索使用的 query |
| `mode` | 本次搜索模式 |
| `intent` | 后端判断的 query 意图 |
| `route` | 后端选择的搜索路由 |
| `corrected_query` | 如果发生纠错，这里是纠正后的 query |
| `matched_author` | 如果识别为作者名，这里是命中的作者 |
| `suggested_author` | 如果作者置信不足，这里是建议作者 |

### 2.3 `meta` 字段

```json
{
  "count": 10,
  "limit": 10,
  "offset": 0,
  "has_more": true,
  "elapsed_ms": 182,
  "request_id": "frontend-search-001"
}
```

| 字段 | 含义 |
| --- | --- |
| `count` | 本次实际返回条数 |
| `limit` | 本次生效的分页大小 |
| `offset` | 本次生效的分页起点 |
| `has_more` | 后面是否还有更多结果 |
| `elapsed_ms` | 请求耗时（毫秒） |
| `request_id` | 用于排查问题的请求 ID |

### 2.4 `notice` 字段

`notice` 用于告诉前端：本次搜索是否发生了纠错、作者识别、作者建议等额外语义事件，以及前端是否可以直接提供一个下一步动作按钮。

结构：

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

前端处理建议：

- 如果 `notice` 为空：不显示提示条
- 如果 `notice.message` 存在：显示提示条
- 如果 `notice.action` 非空：渲染一个按钮，点击后用其中的 `mode` 和 `query` 重新发起请求

前端执行规则：

- `notice.action.label` 只作为按钮文案展示，不建议前端用 `label` 做逻辑分支
- 前端应以 `notice.action.mode` 和 `notice.action.query` 作为重新发起搜索的真实参数
- 如果 `notice.action` 为 `null`，说明本次只有提示信息，没有后续动作按钮

当前已定义的典型场景：

| `notice.type` | `notice.action.label` | 前端执行方式 |
| --- | --- | --- |
| `author_name` | `改用向量检索` | 使用原始输入 `query`，并将 `mode` 改为 `vector`，重新请求 `/api/scholar/search` |
| `author_suggestion` | `搜索作者 {suggested_author}` | 使用 `suggested_author` 作为新 `query`，并保持 `mode=smart`，重新请求 `/api/scholar/search` |
| `query_correction` | `使用原 query 检索` | 放弃纠错后的 query，改用用户原始输入作为 `query`，并将 `mode` 改为 `vector`，重新请求 `/api/scholar/search` |
| `vector` | 无 | 只展示提示，不显示按钮 |

### 2.5 `results[]` 字段

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

| 字段 | 含义 |
| --- | --- |
| `work_id` | 对外主标识，前端跳转/反馈优先使用 |
| `rank` | 后端生成的排序位次 |
| `title` | 论文标题 |
| `abstract` | 摘要 |
| `authors` | 作者字符串 |
| `source` | 供展示的来源名称 |
| `source_key` | 程序侧来源标识 |
| `online_date` | 上线日期 |
| `link` | 跳转链接 |
| `doi` | DOI |
| `ranking_score` | 排序分，不建议直接展示成“语义相似度” |
| `match_reasons` | 可解释性标签 |

兼容字段说明：

- 当前响应里仍保留了 `search_query`、`search_mode`、`query_understanding`、`result_policy`、`similarity`、`similarity_score`、`retrieval_reasons` 等字段，用于兼容旧页面或内部链路。
- 新前端应优先以本文件中的正式字段为准。

## 3. Request / Response 样例

### 3.1 普通智能搜索

请求：

```http
GET /api/scholar/search?query=Nav1.7&mode=smart&limit=5&offset=0&source_list=langtaosha
```

curl 示例：

```bash
curl -i \
  "http://43.143.246.163:5173/api/scholar/search?query=Nav1.7&mode=smart&limit=5&offset=0&source_list=langtaosha"
```

响应：

```json
{
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
      "title": "Nav1.7 controls nociceptor excitability through state-dependent gating",
      "abstract": "This study investigates how Nav1.7 shapes neuronal excitability and pain signaling...",
      "authors": "A. Example, B. Example",
      "source": "Langtaosha",
      "source_key": "langtaosha",
      "online_date": "2026-04-13",
      "link": "https://example.org/paper/nav17-1",
      "doi": "10.1000/nav17.001",
      "ranking_score": 0.9124,
      "match_reasons": [
        {
          "key": "dense_recall",
          "label": "Dense recall",
          "score": 0.91
        }
      ]
    }
  ]
}
```

### 3.2 Query Correction 场景

请求：

```http
GET /api/scholar/search?query=machi%20learningn&mode=smart&limit=5&offset=0&source_list=langtaosha
```

curl 示例：

```bash
curl -i \
  "http://43.143.246.163:5173/api/scholar/search?query=machi%20learningn&mode=smart&limit=5&offset=0&source_list=langtaosha"
```

响应：

```json
{
  "success": true,
  "query": {
    "input": "machi learningn",
    "executed": "machine learning",
    "mode": "smart",
    "intent": "semantic_search",
    "route": "vector",
    "corrected_query": "machine learning",
    "matched_author": null,
    "suggested_author": null
  },
  "meta": {
    "count": 5,
    "limit": 5,
    "offset": 0,
    "has_more": true,
    "elapsed_ms": 205,
    "request_id": "frontend-search-002"
  },
  "notice": {
    "type": "query_correction",
    "message": "已识别到可能的拼写错误，实际搜索 query 为: machine learning",
    "action": {
      "label": "使用原 query 检索",
      "mode": "vector",
      "query": "machi learningn"
    }
  },
  "results": []
}
```

### 3.3 作者建议场景

请求：

```http
GET /api/scholar/search?query=niang%20yan&mode=smart&limit=5&offset=0&source_list=langtaosha
```

curl 示例：

```bash
curl -i \
  "http://43.143.246.163:5173/api/scholar/search?query=niang%20yan&mode=smart&limit=5&offset=0&source_list=langtaosha"
```

响应：

```json
{
  "success": true,
  "query": {
    "input": "niang yan",
    "executed": null,
    "mode": "smart",
    "intent": "author_name",
    "route": "author_suggestion",
    "corrected_query": null,
    "matched_author": null,
    "suggested_author": "Nieng Yan"
  },
  "meta": {
    "count": 0,
    "limit": 5,
    "offset": 0,
    "has_more": false,
    "elapsed_ms": 121,
    "request_id": "frontend-search-003"
  },
  "notice": {
    "type": "author_suggestion",
    "message": "未找到 \"niang yan\" 的高置信作者匹配，是否搜索作者 Nieng Yan？",
    "action": {
      "label": "搜索作者 Nieng Yan",
      "mode": "smart",
      "query": "Nieng Yan"
    }
  },
  "results": []
}
```

## 4. 错误响应

### 4.1 空 query

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
