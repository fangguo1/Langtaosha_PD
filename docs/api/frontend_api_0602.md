# 浪淘沙前端测试 API 说明

**日期**: 2026-06-02
**状态**: v0.2
**用途**: 交给浪淘沙前端开发人员测试后端 API。
**测试范围**: `health`、`ready`、`search` 三个接口。

## 1. 服务地址

当前后端 API 端口为 `5173`。

服务器本机测试：

```text
http://127.0.0.1:5173
```

外部测试人员访问：

```text
http://43.143.246.163:5173
```

说明：

- `5173` 是 API 端，只应直接访问 `/api/*`。
- 直接访问 `http://43.143.246.163:5173/search`、`/study`、`/future` 等页面路径，默认返回 `404`。
- 页面入口仍在 `5004`；这份文档只说明给前端开发人员直连测试的 `5173` API。

## 2. 授权方式

本轮采用 Bearer token 控制外部测试访问。

请求头格式：

```text
Authorization: Bearer <TEST_API_TOKEN>
```

其中 `<TEST_API_TOKEN>` 由后端负责人单独发给测试人员，不要写入前端仓库、聊天公开频道或文档明文。

当前授权规则：

| 接口 | 是否需要 token | 说明 |
| --- | --- | --- |
| `GET /api/health` | 否 | 公开探活。 |
| `GET /api/ready` | 是 | 检查 DB 就绪状态，需要授权。 |
| `GET /api/scholar/search` | 是 | 搜索接口，需要授权。 |

未带 token 或 token 错误时，返回 HTTP `401`：

```json
{
  "success": false,
  "error": "missing or invalid API token",
  "error_code": "UNAUTHORIZED",
  "error_detail": {
    "code": "UNAUTHORIZED",
    "message": "missing or invalid API token",
    "request_id": "..."
  },
  "request_id": "..."
}
```

## 3. 通用约定

### 3.1 请求格式

本轮三个接口均为 `GET`。

建议请求头：

```text
Accept: application/json
Authorization: Bearer <TEST_API_TOKEN>
X-Request-Id: <frontend-generated-request-id>
```

`X-Request-Id` 可选。如果前端不传，后端会自动生成。

### 3.2 响应公共字段

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

完整测试地址：

```text
http://43.143.246.163:5173/api/health
```

### 4.2 用途

检查 API 服务进程是否存活。

该接口不检查数据库，也不检查 VectorDB。

### 4.3 curl 示例

`health` 不需要 token：

```bash
curl -i "http://43.143.246.163:5173/api/health"
```

带 request id：

```bash
curl -i \
  -H "X-Request-Id: frontend-health-test-001" \
  "http://43.143.246.163:5173/api/health"
```

### 4.4 成功响应示例

```json
{
  "success": true,
  "status": "ok",
  "service": "langtaosha-api",
  "request_id": "frontend-health-test-001"
}
```

## 5. Ready API

### 5.1 接口

```http
GET /api/ready
```

完整测试地址：

```text
http://43.143.246.163:5173/api/ready
```

### 5.2 用途

检查 API 服务是否就绪。

当前版本检查：

- `metadata_db` 是否可连接。

暂不检查：

- Tencent VectorDB 检索是否可用。
- ontology linker 是否可用。

### 5.3 curl 示例

```bash
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://43.143.246.163:5173/api/ready"
```

### 5.4 成功响应示例

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

### 5.5 失败响应示例

如果 token 错误，返回 HTTP `401`：

```json
{
  "success": false,
  "error": "missing or invalid API token",
  "error_code": "UNAUTHORIZED",
  "error_detail": {
    "code": "UNAUTHORIZED",
    "message": "missing or invalid API token",
    "request_id": "..."
  },
  "request_id": "..."
}
```

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

完整测试地址：

```text
http://43.143.246.163:5173/api/scholar/search
```

### 6.2 参数

| 参数 | 类型 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- | --- |
| `query` | string | 是 | 无 | 用户搜索 query。 |
| `mode` | string | 否 | `smart` | 可选 `smart` 或 `vector`。 |
| `top_k` | integer | 否 | `100` | 后端会归一化为 `1..100`。当前是每组 source 拉取 top_k 后合并，不是分页 limit。 |
| `source_list` | CSV string | 否 | 后端默认 sources | 逗号分隔，例如 `langtaosha` 或 `langtaosha,biorxiv_daily,biorxiv_history`。 |

### 6.3 推荐测试请求

Langtaosha 单源智能搜索：

```bash
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  -H "X-Request-Id: frontend-search-test-001" \
  "http://43.143.246.163:5173/api/scholar/search?query=Nav1.7&mode=smart&top_k=5&source_list=langtaosha"
```

强制向量检索：

```bash
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://43.143.246.163:5173/api/scholar/search?query=ion%20channel&mode=vector&top_k=5&source_list=langtaosha"
```

作者或人名相关 query：

```bash
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://43.143.246.163:5173/api/scholar/search?query=Nieng%20Yan&mode=smart&top_k=5&source_list=langtaosha"
```

### 6.4 成功响应结构

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
    "langtaosha_top_k": 5,
    "biorxiv_top_k": 5,
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
  "request_id": "frontend-search-test-001"
}
```

### 6.5 前端展示注意事项

- `results[].work_id` 是前端推荐使用的论文主标识。
- `results[].paper_id` 是内部数据库 ID，可展示但不建议作为跨系统主键。
- `similarity` / `similarity_score` 在 hybrid retrieval 下可能是融合排序分，不等同于普通语义相似度。前端第一版建议隐藏，或标注为“排序分”。
- `count` 是本次返回数量，不代表全量命中数量。
- 当前没有分页协议。
- 当前没有 streaming。

### 6.6 常见错误

未带 token：

```bash
curl -i "http://43.143.246.163:5173/api/scholar/search?query=Nav1.7"
```

响应 HTTP `401`：

```json
{
  "success": false,
  "error": "missing or invalid API token",
  "error_code": "UNAUTHORIZED",
  "error_detail": {
    "code": "UNAUTHORIZED",
    "message": "missing or invalid API token",
    "request_id": "..."
  },
  "request_id": "..."
}
```

空 query：

```bash
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://43.143.246.163:5173/api/scholar/search?query="
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
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://43.143.246.163:5173/api/scholar/search?query=Nav1.7&mode=bad"
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

Bearer token 是访问授权机制；CORS 只是浏览器跨域兼容机制。

如果前端开发人员直接从自己的本地前端页面请求：

```text
http://43.143.246.163:5173/api/scholar/search
```

后端启动时仍需要允许对应的浏览器 origin。测试期可以使用：

```bash
FRONTEND_ALLOWED_ORIGINS=*
```

正式环境不建议长期使用 `*`，应收紧为具体前端域名。

如果只是用 `curl`、Postman、Apifox 测试，通常不受浏览器 CORS 限制，但仍需要 Bearer token。

## 8. 后端侧开放 5173 的操作清单

给外部测试人员测试 `5173` 前，后端侧需要确认以下事项。

### 8.1 生成测试 token

示例：

```bash
python3 - <<'PY'
import secrets
print("lts_test_" + secrets.token_urlsafe(32))
PY
```

将生成的 token 通过私密渠道发给前端测试人员。

### 8.2 启动后端监听 5173

开发态启动：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD
conda activate langtaosha

API_AUTH_TOKENS=<TEST_API_TOKEN> \
FRONTEND_ALLOWED_ORIGINS=* \
PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_mimic.yaml \
python app/main.py
```

生产 / 准生产建议使用 WSGI：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD
conda activate langtaosha

API_AUTH_TOKENS=<TEST_API_TOKEN> \
FRONTEND_ALLOWED_ORIGINS=* \
PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_use.yaml \
gunicorn -w 2 -b 0.0.0.0:5173 app.main:app
```

也支持多个 token：

```bash
API_AUTH_TOKENS=lts_test_frontend_001,lts_test_frontend_002
```

### 8.3 如果 5004 旧页面还需要继续搜索

当后端启用 `API_AUTH_TOKENS` 后，`5004` 旧页面通过 `/api` 搜索也需要 token。Vite proxy 可自动补 header，启动前端时设置：

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/frontend
conda activate langtaosha

VITE_BACKEND_PROXY_TARGET=http://127.0.0.1:5173 \
VITE_BACKEND_API_TOKEN=<TEST_API_TOKEN> \
npm run dev
```

### 8.4 确认本机可访问

在服务器本机运行：

```bash
curl -i "http://127.0.0.1:5173/api/health"
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://127.0.0.1:5173/api/ready"
```

### 8.5 确认进程监听公网地址

确认监听的是 `0.0.0.0:5173`，不是只监听 `127.0.0.1:5173`：

```bash
ss -lntp | grep 5173
```

预期类似：

```text
LISTEN 0 128 0.0.0.0:5173 ...
```

### 8.6 开放云服务器安全组 / 防火墙

需要在云服务器安全组或防火墙放行 TCP `5173`。

最低要求：

```text
TCP 5173 inbound
```

建议只允许测试人员 IP 段访问，避免临时 API 端口完全暴露。

如果服务器上启用了系统防火墙，也需要放行：

```bash
sudo ufw allow 5173/tcp
```

如果不用 `ufw`，按当前服务器防火墙工具执行等价规则。

### 8.7 外部机器验证

让测试人员在自己的机器上运行：

```bash
curl -i "http://43.143.246.163:5173/api/health"
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://43.143.246.163:5173/api/ready"
curl -i \
  -H "Authorization: Bearer <TEST_API_TOKEN>" \
  "http://43.143.246.163:5173/api/scholar/search?query=Nav1.7&mode=smart&top_k=5&source_list=langtaosha"
```

### 8.8 确认 5173 不是页面入口

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
