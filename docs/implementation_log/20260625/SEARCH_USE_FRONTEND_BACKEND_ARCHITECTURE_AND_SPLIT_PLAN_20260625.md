# Search Use Frontend/Backend Architecture And Split Plan

## Goal

基于当前 `search-use` 相关目录，完成两件事：

1. 画清楚当前工程里的页面层、API 层、业务层、数据层分别在哪里。
2. 给出一版最小可执行、符合标准前后端分离习惯的迁移方案，说明未来如果拆成独立前端和独立后端，应如何落目录、接口边界和演进步骤。

---

## Current Architecture Mapping

当前实现并不是“前端和后端完全混在一起”，而是已经做了明显分层，只是前端还没有独立成单独的 JS 工程。

### Current Runtime Topology

```text
Browser
  -> Frontend Flask app (:5015)
      -> app/pages/langtaosha_smart_search_page.py
      -> render_template("langtaosha_smart_search.html")
      -> /api/* forwarded by app/dev/develop_api_proxy.py
          -> Backend Flask app (:5016)
              -> app/routes/scholar.py
              -> app/routes/paper.py
                  -> src/docset_hub/indexing/paper_indexer.py
                  -> src/docset_hub/indexing/retrieval_helper.py
                  -> MetadataDB / VectorDB / config / logging
```

### Layer Mapping In Current Repo

#### 1. Page Layer

职责：

- 返回 HTML 页面
- 读取 URL query 作为初始参数
- 在浏览器里发起后续 API 请求

对应位置：

- `app/pages/langtaosha_smart_search_page.py`
- `templates/langtaosha_smart_search.html`
- `app/dev/main_search_use.py` 中的 `create_search_use_frontend_app()`

结论：

- 这是“前端入口层”，但它目前由 Flask 承载，而不是 React/Vue 独立工程。

#### 2. API Layer

职责：

- 提供 `/api/*` JSON 接口
- 解析 query/body/header
- 调用业务对象
- 统一返回 success/error payload

对应位置：

- `app/dev/main_search_use.py` 中的 `create_search_use_api_app()`
- `app/routes/scholar.py`
- `app/routes/paper.py`

结论：

- 这是标准意义上的后端控制器层。

#### 3. Service / Domain Layer

职责：

- 检索路由编排
- dense/sparse/hybrid/expanded sparse 召回
- hydrate、coverage、ranking、query understanding 等业务逻辑

对应位置：

- `src/docset_hub/indexing/paper_indexer.py`
- `src/docset_hub/indexing/retrieval_helper.py`

结论：

- 这是后端业务核心，不应该被页面模板或浏览器代码直接依赖。

#### 4. Infra / Data Layer

职责：

- 配置加载
- 向量库和元数据存储访问
- 日志记录

对应位置：

- `config/config_loader.py`
- `src/config/*.yaml`
- `MetadataDB`
- `VectorDB`
- `src/docset_hub/logging/*`

结论：

- 这是后端基础设施层。

---

## What This Architecture Is

当前实现更准确的定义是：

- 已经做到“页面服务”和“API 服务”分端口分离
- 已经做到“route”和“业务逻辑”分层
- 但前端仍然是 Flask 页面，而不是独立前端工程

因此它属于：

```text
Flask-based layered architecture
+ split frontend server and API server
+ shared Python repo
- not yet fully separated frontend project
```

它比“单 Flask 文件同时写页面和业务逻辑”规范得多，但还不是最典型的：

```text
frontend (React/Vue)
backend (Flask/FastAPI)
```

---

## Standard Frontend/Backend Separation Target

如果按最标准、也最容易被团队理解的前后端分离方式来演进，建议目标结构如下：

```text
Langtaosha_PD/
  frontend/
    src/
      pages/
      components/
      api/
      styles/
    public/
    package.json
    vite.config.ts

  backend/
    app/
      routes/
      services/
      schemas/
    src/docset_hub/
    main.py
    requirements.txt
```

### Responsibilities After Split

#### Frontend

- 只负责页面、组件、状态管理、用户交互
- 通过 `fetch` 或 `axios` 请求后端
- 不直接 import `PaperIndexer`
- 不关心数据库、配置文件、向量库

#### Backend

- 只负责 `/api/*`
- 管理配置、鉴权、日志、检索、存储访问
- 返回稳定 JSON contract

---

## Minimal Example Mapped To This Project

最小通信链路可以直接类比为：

```text
frontend page
  -> GET /api/health
  -> GET /api/search?query=...
backend route
  -> PaperIndexer.search(...) / smart_search(...)
  -> JSON response
frontend page
  -> render result list
```

在你当前项目里，这条链路已经存在，只是前端页面还在 Flask 里：

- 页面入口：`/search-use`
- API：`/api/*`
- 后端检索核心：`PaperIndexer`

---

## Recommended Migration Plan

### Stage 0: Keep Current Backend Boundaries Stable

先不动检索核心，先把 API 边界收紧稳定下来。

建议：

- 保持 `app/routes/*.py` 作为唯一 HTTP 入口
- 禁止页面模板直接碰 `PaperIndexer`
- 统一 scholar/paper 返回格式、错误码、request id 规范

目的：

- 为前端独立迁移提供稳定 API contract

### Stage 1: Extract Frontend Assets Into Independent Frontend App

把当前 Flask 页面承载的 UI 迁移到独立前端工程：

- 新建 `frontend/`
- 把 `langtaosha_smart_search.html` 对应的页面逻辑改成 React/Vue 页面
- 所有数据请求改为 `fetch('/api/...')`

此阶段完成后：

- Flask 不再负责页面模板渲染
- Flask/FastAPI 只保留 API

### Stage 2: Keep Reverse Proxy Or Gateway At Dev And Prod

开发环境：

- 前端 dev server 跑在 `:3000` 或 `:5173`
- 后端 API 跑在 `:5016`
- 用 Vite proxy 或 nginx 转发 `/api/*`

生产环境：

- nginx 统一接入
- `/` -> 前端静态资源
- `/api/*` -> Python API 服务

### Stage 3: Clarify Backend Internal Layers

后端内部建议继续收敛成：

```text
routes    HTTP contract
services  search orchestration
domain    retrieval logic
infra     db/vector/config/logging
```

结合现状，可逐步调整为：

- `app/routes/*` 继续保留 route/controller
- 新增 `app/services/search_service.py` 之类的编排层
- `src/docset_hub/indexing/*` 继续作为核心 domain logic

这样能避免 route 代码越来越厚。

---

## Proposed File Ownership After Split

### Keep In Backend

- `app/routes/paper.py`
- `app/routes/scholar.py`
- `src/docset_hub/indexing/paper_indexer.py`
- `src/docset_hub/indexing/retrieval_helper.py`
- config、db、logging 相关代码

### Move Out Of Backend-Facing UI Responsibilities

- `app/pages/langtaosha_smart_search_page.py`
- `templates/langtaosha_smart_search.html`
- 未来所有页面交互 JS

这些应最终迁移到独立 `frontend/`。

### Remove Eventually

- `app/dev/develop_api_proxy.py` 这类 Flask 内置页面到 API 的开发代理

原因：

- 当前它的作用是“让 Flask 页面层转发到 Flask API 层”
- 真正前后端分离后，这一职责更适合由 Vite proxy / nginx 承担

---

## Recommended Near-Term Todo For This Repo

如果不做大重构，只做最有价值的下一步，优先级建议如下：

1. 先冻结并文档化 `search-use` 的 API contract。
2. 把页面模板依赖的接口收敛到单一 `scholar` 或 `search` API 门面。
3. 新建独立 `frontend/` 原型页，先只接 `/api/health` 和一个检索接口。
4. 验证前端脱离 Flask 模板后，后端是否还能独立运行。

---

## Final Assessment

当前架构的优点：

- 已经具备清晰的 route / service / infra 分层意识
- 已经把页面服务与 API 服务拆成两个端口
- 检索核心没有直接耦合到页面模板

当前架构距离“标准前后端分离”还差的关键一步：

- 前端还没有独立成单独工程

因此最准确的判断是：

```text
current state = semi-separated architecture
target state  = independent frontend app + dedicated backend API
```

这意味着你不是从零开始重构，而是在已有良好分层基础上继续把页面层外提即可。
