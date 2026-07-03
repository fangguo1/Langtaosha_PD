# Langtaosha Architecture Design (V2)

## Architecture Philosophy

Langtaosha 应采用三层架构：

```text
Application Layer
    ↓
Domain Layer
    ↓
Infrastructure Layer
```

职责划分：

```text
Application Layer
    对外接口与应用入口

Domain Layer
    核心业务逻辑

Infrastructure Layer
    数据库、模型服务、GPU服务、向量库等基础设施
```

---

# Overall Structure

```text
project/

├── app/
│
├── src/docset_hub/
│
├── deployments/
│
├── scripts/
│
├── configs/
│
└── tests/
```

---

# Layer 1: Application Layer

负责：

```text
API
页面
开发工具
管理接口
```

不负责：

```text
检索算法
数据库实现
模型推理
向量检索
```

---

## app/

```text
app/
├── main.py

├── routes/
│   ├── scholar.py
│   ├── paper.py
│   ├── author.py
│   ├── feedback.py
│   └── admin.py

├── pages/

├── dev/

└── legacy/
```

---

## app/main.py

系统启动入口。

职责：

```text
创建 FastAPI

注册 Middleware

注册 Router

配置 CORS

配置生命周期事件

健康检查
```

只允许：

```text
启动逻辑
注册逻辑
配置逻辑
```

禁止：

```text
检索逻辑
数据库逻辑
Embedding逻辑
RRF逻辑
```

---

## app/routes/

正式 API 层。

职责：

```text
HTTP Endpoint

参数校验

调用 Domain Service

返回 Response
```

推荐：

```text
routes/
├── scholar.py
├── paper.py
├── author.py
├── feedback.py
└── admin.py
```

Route 不允许：

```text
写 BM25

写 Dense Retrieval

写 SQL

写 Embedding

写 Orchestrator
```

---

## app/pages/

调试与人工 Review 页面。

例如：

```text
retrieval_compare_page.py

span_matcher_page.py

feedback_review_page.py

expanded_compare_page.py
```

用途：

```text
错误分析

人工评测

调试
```

---

## app/dev/

开发辅助入口。

例如：

```text
main_develop.py

develop_api_proxy.py

run_feedback_review.py
```

特点：

```text
开发环境使用

生产环境不依赖
```

---

# Layer 2: Domain Layer

系统核心能力层。

所有业务逻辑统一放在这里。

---

## src/docset_hub/

```text
src/docset_hub/

├── services/

├── indexing/

├── metadata/

├── storage/

├── orchestrator/

├── evaluation/

├── logging/

├── crud/

└── clients/
```

原则：

```text
业务逻辑只维护一份

所有入口统一调用这里
```

---

## services/

业务服务层。

例如：

```text
SearchService

AuthorService

PaperService

FeedbackService
```

职责：

```text
组织业务流程

调用多个模块

返回统一结果
```

示例：

```text
SearchService

    ↓

Query Understanding

    ↓

Span Matcher

    ↓

Ontology Linker

    ↓

Hybrid Retrieval

    ↓

Metadata Hydration
```

---

## indexing/

负责：

```text
PaperIndexer

Hybrid Retrieval

Dense Retrieval

Sparse Retrieval

RRF

Keyword Retrieval
```

---

## metadata/

负责：

```text
Paper Metadata

Author Metadata

Source Metadata
```

---

## storage/

负责：

```text
MetadataDB

VectorDB

Storage Abstraction
```

---

## orchestrator/

负责：

```text
Daily Pipeline

Backfill

Author Enrichment

Manifest

Pipeline Status
```

---

## evaluation/

负责：

```text
Benchmark

Replay

Offline Evaluation
```

---

## logging/

负责：

```text
request_id

run_id

trace_id

structured logging
```

---

## crud/

负责：

```text
数据库 CRUD
```

---

## clients/

远程服务客户端。

这是 Domain Layer 访问 Infrastructure Layer 的唯一入口。

```text
clients/
├── span_matcher_client.py
├── ontology_linker_client.py
├── llm_client.py
```

职责：

```text
封装HTTP调用

封装认证

封装重试

封装超时

封装服务发现
```

示例：

```python
span_matcher.match(query)

ontology_linker.link(spans)

llm.generate(prompt)
```

业务代码不直接写：

```python
requests.post(...)
```

统一通过 client 调用。

---

# Layer 3: Infrastructure Layer

基础设施层。

负责运行独立服务。

---

## deployments/

```text
deployments/

├── span_matcher/

├── ontology_linker/

├── llm_gateway/
```

这些组件本质是：

```text
独立服务

独立进程

独立部署

独立扩容
```

---

## deployments/span_matcher/

负责：

```text
Span Extraction

Entity Detection

Span Matching
```

例如：

```text
FlashText

spaCy

Rules

NER
```

暴露：

```text
POST /match
```

---

## deployments/ontology_linker/

负责：

```text
Ontology Linking

UMLS Linking

MeSH Linking

Ontology Expansion
```

暴露：

```text
POST /link
```

---

## deployments/llm_gateway/

负责：

```text
Qwen

DeepSeek

GPT

Future LLMs
```

可能部署在 GPU 服务器：

```text
10.0.1.226:8001
```

暴露：

```text
POST /generate

POST /embedding
```

---

# Complete Request Flow

搜索请求：

```text
Frontend

↓
app/routes/scholar.py

↓
SearchService

↓
SpanMatcherClient

↓
OntologyLinkerClient

↓
Hybrid Retrieval

↓
MetadataDB

↓
Response
```

其中：

```text
SpanMatcherClient
        ↓
deployments/span_matcher

OntologyLinkerClient
        ↓
deployments/ontology_linker

LLMClient
        ↓
deployments/llm_gateway
```

---

# Core Design Principles

原则一：

```text
app 负责应用入口
```

原则二：

```text
docset_hub 负责业务逻辑
```

原则三：

```text
deployments 负责基础设施
```

原则四：

```text
所有业务逻辑统一放在 docset_hub
```

原则五：

```text
所有远程服务统一通过 clients 调用
```

原则六：

```text
禁止在业务代码中直接 requests.post()
```

最终目标：

```text
Application Layer
        ↓
Domain Layer
        ↓
Infrastructure Layer

职责清晰

依赖单向

可独立扩展
```

---

## 2026-06-12 Implementation Note

第一阶段实现保留当前 Flask runtime。本文中的 FastAPI 表述先作为 `app/main.py` 的职责目标，而不是本轮框架迁移目标。

本轮已落地的边界：

```text
app/pages/   调试与人工 review 页面
app/routes/  已抽离的正式 API route 模块与未来 route 占位
app/dev/     开发入口、开发 proxy、standalone review app
app/main.py  保留生产入口与注册职责，并委托 span matcher routes
app/main_prev.py  保留重构前生产入口快照
```

后续仍需单独规划：

```text
SearchService extraction
StudyService extraction
RecommendationService extraction
remote service clients
FastAPI migration
```
