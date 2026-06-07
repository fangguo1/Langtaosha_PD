# 浪淘沙前端测试 API 说明

**日期**: 2026-06-02
**状态**: v0.3
**用途**: 交给浪淘沙前端开发人员测试后端 API。
**测试范围**: `health`、`ready`、`search` 三个接口。

## 1. 服务地址

当前后端 API 端口为 `5173`。

服务器本机测试：

```text
http://127.0.0.1:5173
```

外部测试地址：

```text
http://43.143.246.163:5173
```

说明：

- `5173` 是 API 端，只直接测试 `/api/*`。
- 页面入口仍在 `5004`。
- 直接访问 `http://43.143.246.163:5173/search`、`/study`、`/future` 等页面路径，默认返回 `404`。

## 2. 推荐本地验证方式

优先使用下面这一行 curl 验证 search API：

```bash
curl -i "http://127.0.0.1:5173/api/scholar/search?query=machi%20learningn&mode=smart&top_k=5&source_list=langtaosha"
```

如果需要验证外部地址，将 host 换成服务器公网地址：

```bash
curl -i "http://43.143.246.163:5173/api/scholar/search?query=machi%20learningn&mode=smart&top_k=5&source_list=langtaosha"
```

## 3. 通用约定

本轮三个接口均为 `GET`。

建议请求头：

```text
Accept: application/json
X-Request-Id: <frontend-generated-request-id>
```

`X-Request-Id` 可选。如果前端不传，后端会自动生成。

成功响应都会包含：

```json
{
  "success": true,
  "request_id": "..."
}
```

错误响应都会包含：

```json
{
  "success": false,
  "error": "错误信息",
  "error_code": "ERROR_CODE",
  "error_detail": {
    "code": "ERROR_CODE",
    "message": "错误信息",
    "request_id": "..."
  },
  "request_id": "..."
}
```

前端建议：

- 使用 `error_detail.message` 作为展示文案。
- 使用 `error_code` 做分支处理和日志定位。
- 问题反馈时附带 `request_id`。

## 4. Health API

### 4.1 接口

```http
GET /api/health
```

本地测试：

```bash
curl -i "http://127.0.0.1:5173/api/health"
```

外部测试：

```bash
curl -i "http://43.143.246.163:5173/api/health"
```

### 4.2 用途

检查 API 服务进程是否存活。

该接口不检查数据库，也不检查 VectorDB。

### 4.3 成功响应示例

```json
{
  "success": true,
  "status": "ok",
  "service": "langtaosha-api",
  "request_id": "..."
}
```

## 5. Ready API

### 5.1 接口

```http
GET /api/ready
```

本地测试：

```bash
curl -i "http://127.0.0.1:5173/api/ready"
```

外部测试：

```bash
curl -i "http://43.143.246.163:5173/api/ready"
```

### 5.2 用途

检查 API 服务是否就绪。

当前版本检查：

- `metadata_db` 是否可连接。

暂不检查：

- Tencent VectorDB 检索是否可用。
- ontology linker 是否可用。

### 5.3 成功响应示例

```json
{
  "success": true,
  "status": "ready",
  "service": "langtaosha-api",
  "checks": {
    "metadata_db": "ok"
  },
  "request_id": "..."
}
```

### 5.4 失败响应示例

如果 metadata DB 不可连接，返回 HTTP `503`：

```json
{
  "success": false,
  "error": "metadata_db unavailable",
  "error_code": "READINESS_FAILED",
  "error_detail": {
    "code": "READINESS_FAILED",
    "message": "metadata_db unavailable",
    "request_id": "..."
  },
  "status": "not_ready",
  "service": "langtaosha-api",
  "checks": {
    "metadata_db": "failed"
  },
  "request_id": "..."
}
```

## 6. Search API

### 6.1 接口

```http
GET /api/scholar/search
```

本地测试：

```bash
curl -i "http://127.0.0.1:5173/api/scholar/search?query=machi%20learningn&mode=smart&top_k=5&source_list=langtaosha"
```

外部测试：

```bash
curl -i "http://43.143.246.163:5173/api/scholar/search?query=machi%20learningn&mode=smart&top_k=5&source_list=langtaosha"
```

### 6.2 参数

| 参数 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `query` | string | 是 | 无 | 用户搜索 query。 |
| `mode` | string | 否 | `smart` | 可选 `smart` 或 `vector`。 |
| `top_k` | integer | 否 | `100` | 后端会归一化为 `1..100`。当前是每组 source 拉取 top_k 后合并，不是分页 limit。 |
| `source_list` | CSV string | 否 | 后端默认 sources | 逗号分隔，例如 `langtaosha` 或 `langtaosha,biorxiv_daily,biorxiv_history`。 |

### 6.3 成功响应结构

```json
{
  "success": true,
  "query": "machi learningn",
  "search_query": "machine learning",
  "search_mode": "smart",
  "query_understanding": {
    "intent": "semantic_search",
    "route": "vector"
  },
  "result_policy": {
    "langtaosha_top_k": 5,
    "biorxiv_top_k": 5,
    "dedupe_key": "work_id",
    "default_frontend_source": "langtaosha",
    "search_type": "hybrid_retrieval",
    "display": "show_langtaosha_first_then_biorxiv"
  },
  "notice": {
    "type": "query_correction",
    "message": "已识别到可能的拼写错误，实际搜索 query 为: machine learning"
  },
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

### 6.4 前端展示注意事项

- `results[].work_id` 是前端推荐使用的论文主标识。
- `results[].paper_id` 是内部数据库 ID，可展示但不建议作为跨系统主键。
- `similarity` / `similarity_score` 在 hybrid retrieval 下可能是融合排序分，不等同于普通语义相似度。前端第一版建议隐藏，或标注为“排序分”。
- `count` 是本次返回数量，不代表全量命中数量。
- `notice` 不为空时，前端可以展示 query correction 或 fallback 提示。
- 当前没有分页协议。
- 当前没有 streaming。

### 6.5 常见错误

空 query：

```bash
curl -i "http://127.0.0.1:5173/api/scholar/search?query="
```

响应 HTTP `400`：

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

非法 mode：

```bash
curl -i "http://127.0.0.1:5173/api/scholar/search?query=Nav1.7&mode=bad"
```

响应 HTTP `400`：

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

## 7. 浏览器直连 CORS

如果前端开发人员直接从自己的本地前端页面请求：

```text
http://43.143.246.163:5173/api/scholar/search
```

后端启动时仍需要允许对应的浏览器 origin。测试期可以使用：

```bash
FRONTEND_ALLOWED_ORIGINS=*
```

正式环境不建议长期使用 `*`，应收紧为具体前端域名。

如果只是用 `curl`、Postman、Apifox 测试，通常不受浏览器 CORS 限制。

## 8. 后端侧开放 5173 的操作清单

给外部测试人员测试 `5173` 前，后端侧需要确认以下事项。

### 8.1 启动后端监听 5173

开发态启动：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD
conda activate langtaosha

FRONTEND_ALLOWED_ORIGINS=* \
PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_mimic.yaml \
python app/main.py
```

生产 / 准生产建议使用 WSGI：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD
conda activate langtaosha

FRONTEND_ALLOWED_ORIGINS=* \
PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_use.yaml \
gunicorn -w 2 -b 0.0.0.0:5173 app.main:app
```

### 8.2 确认本机可访问

在服务器本机运行：

```bash
curl -i "http://127.0.0.1:5173/api/health"
curl -i "http://127.0.0.1:5173/api/ready"
curl -i "http://127.0.0.1:5173/api/scholar/search?query=machi%20learningn&mode=smart&top_k=5&source_list=langtaosha"
```

### 8.3 确认进程监听公网地址

确认监听的是 `0.0.0.0:5173`，不是只监听 `127.0.0.1:5173`：

```bash
ss -lntp | grep 5173
```

预期类似：

```text
LISTEN 0 128 0.0.0.0:5173 ...
```

### 8.4 开放云服务器安全组 / 防火墙

需要在云服务器安全组或防火墙放行 TCP `5173`。

最低要求：

```text
TCP 5173 inbound
```

如果服务器上启用了系统防火墙，也需要放行：

```bash
sudo ufw allow 5173/tcp
```

### 8.5 外部机器验证

让测试人员在自己的机器上运行：

```bash
curl -i "http://43.143.246.163:5173/api/health"
curl -i "http://43.143.246.163:5173/api/ready"
curl -i "http://43.143.246.163:5173/api/scholar/search?query=machi%20learningn&mode=smart&top_k=5&source_list=langtaosha"
```

### 8.6 确认 5173 不是页面入口

外部测试人员可以验证：

```bash
curl -i "http://43.143.246.163:5173/search"
```

预期：

```text
HTTP/1.1 404 NOT FOUND
```

这说明 `5173` 只作为 API 端暴露，页面入口仍应走 `5004`。

## 9. 问题反馈时需要提供的信息

前端测试人员反馈问题时，请至少提供：

- 请求 URL。
- 请求参数。
- HTTP status code。
- 响应 JSON。
- 响应里的 `request_id`。
- 测试时间。
- 是否是浏览器请求，还是 curl / Postman / Apifox 请求。
